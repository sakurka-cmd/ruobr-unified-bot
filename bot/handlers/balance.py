"""
Обработчики для баланса питания и информации о питании.
"""
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

from ..config import config
from ..credentials import safe_decrypt
from ..states import ThresholdStates
from ..database import (
    get_user, get_child_threshold, set_child_threshold,
    get_all_thresholds_for_chat, UserConfig
)
from ..services import (
    Child, FoodInfo, get_children_async, get_food_for_children,
    get_timetable_for_children, RuobrError, NetworkError, AuthenticationError,
    invalidate_user_cache
)
from ..utils.formatters import (
    format_balance, format_date, normalize_date_to_iso, truncate_text,
    extract_dish_names, parse_complex_menu
)
from ..utils.common import is_navigation_command
from .auth import get_main_keyboard, get_settings_keyboard

logger = logging.getLogger(__name__)

router = Router()

async def require_authentication(
    message: Message,
    user_config: Optional[UserConfig]
) -> Optional[Tuple[str, str, list]]:
    """
    Проверка аутентификации пользователя.
    
    Использует safe_decrypt() для безопасного получения пароля.
    Пароль расшифровывается только на время проверки и сразу затирается.
    
    Returns:
        Кортеж (login, password, children) или None если не аутентифицирован.
    """
    if user_config is None:
        user_config = await get_user(message.chat.id)
    
    if not user_config or not user_config.login or not user_config.password_encrypted:
        await message.answer(
            "❌ Сначала настройте учётные данные командой /set_login"
        )
        return None
    
    login, password = safe_decrypt(user_config)
    if not login:
        await message.answer(
            "❌ Сначала настройте учётные данные командой /set_login"
        )
        return None
    
    try:
        children = await get_children_async(login, password)
    except NetworkError:
        logger.error(f"Network error for user {message.chat.id}")
        await message.answer(
            "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже."
        )
        return None
    except AuthenticationError:
        logger.error(f"Authentication error for user {message.chat.id}")
        await message.answer(
            "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль."
        )
        return None
    except RuobrError as e:
        logger.error(f"Ruobr API error for user {message.chat.id}: {e}")
        await message.answer(f"❌ Ошибка доступа к Ruobr: {e}")
        return None
    
    if not children:
        await message.answer("❌ Дети не найдены в аккаунте.")
        return None
    
    return login, password, children



# ===== Баланс питания =====

@router.message(Command("balance"))
@router.message(F.text == "💰 Баланс питания")
async def cmd_balance(message: Message, user_config: Optional[UserConfig] = None):
    """Показать баланс питания всех детей."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка информации о балансе...")
    
    try:
        food_info = await get_food_for_children(login, password, children)
        thresholds = await get_all_thresholds_for_chat(message.chat.id)
        
        lines = ["💰 <b>Баланс питания</b>\n"]
        
        for idx, child in enumerate(children, 1):
            info = food_info.get(child.id)
            threshold = thresholds.get(child.id, config.default_balance_threshold)
            
            if info and info.has_food:
                balance_str = format_balance(child, info.balance, threshold)
                lines.append(f"{idx}. {balance_str}")
            else:
                lines.append(
                    f"{idx}. {child.full_name} ({child.group}): "
                    f"питание недоступно (порог {threshold:.0f} ₽)"
                )
        
        lines.append(
            "\n💡 <i>Настройте порог через /set_threshold для уведомлений</i>"
        )
        
        await status_msg.edit_text("\n".join(lines))
        
    except NetworkError:
        logger.error(f"Network error getting balance for user {message.chat.id}")
        await status_msg.edit_text(
            "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже."
        )
    except AuthenticationError:
        logger.error(f"Auth error getting balance for user {message.chat.id}")
        await status_msg.edit_text(
            "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль."
        )
    except Exception as e:
        logger.error(f"Error getting balance for user {message.chat.id}: {e}")
        await status_msg.edit_text(
            f"❌ Ошибка получения баланса: {e}"
        )


# ===== Питание сегодня =====

@router.message(Command("foodtoday"))
@router.message(F.text == "🍽 Питание сегодня")
async def cmd_foodtoday(message: Message, user_config: Optional[UserConfig] = None):
    """Показать меню на сегодня — запланированное и фактически полученное."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка меню на сегодня...")
    
    try:
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")
        
        food_info = await get_food_for_children(login, password, children)
        
        # Собираем питание всех детей за сегодня
        all_children_food = []
        
        for child in children:
            info = food_info.get(child.id)
            if not info or not info.visits:
                continue
            
            child_visits = []
            for visit in info.visits:
                vdate = visit.get("date", "")
                if normalize_date_to_iso(vdate) != today_str:
                    continue
                child_visits.append(visit)
            
            if not child_visits:
                continue
            
            all_children_food.append((child, child_visits))
        
        if not all_children_food:
            await status_msg.edit_text(
                f"ℹ️ На сегодня ({format_date(today_str)}) "
                f"нет записей о питании."
            )
            return
        
        # Формируем сообщения по каждому ребёнку
        for child_idx, (child, child_visits) in enumerate(all_children_food):
            lines = [f"🍽 <b>Меню на сегодня</b> ({format_date(today_str)})"]
            lines.append(f"👦 <b>{child.full_name}</b> ({child.group})\n")
            
            for visit in child_visits:
                state = visit.get("state", 0)
                state_str = visit.get("state_str", "")
                is_confirmed = state == 30
                
                meal_name = (
                    visit.get("complex") or
                    visit.get("line_name") or
                    visit.get("type_name") or
                    "Приём пищи"
                )
                
                price_raw = str(visit.get("price_sum", "0")).replace(",", ".")
                try:
                    price = float(price_raw)
                except ValueError:
                    price = 0.0
                
                dish_names = extract_dish_names(visit.get("dishes", []))
                if not dish_names:
                    dish_names = parse_complex_menu(visit.get("qs_unit", []))
                
                if is_confirmed:
                    lines.append(f"✅ <b>{meal_name}</b> — получено")
                else:
                    lines.append(f"📋 <b>{meal_name}</b>")
                
                if dish_names:
                    for dish in dish_names:
                        lines.append(f"  • {dish}")
                
                if price > 0:
                    lines.append(f"  💰 {price:.0f} ₽")
                
                if not is_confirmed and state != 20:
                    if state_str:
                        lines.append(f"  📌 {state_str}")
                elif state == 20:
                    lines.append(f"  ❌ Отменён")
                
                lines.append("")
            
            text = truncate_text("\n".join(lines))
            
            if child_idx == 0:
                # Первого ребёнка — edit (заменяем "Загрузка...")
                await status_msg.edit_text(text)
            else:
                # Остальных — отдельными сообщениями
                await message.answer(text)
            
    except NetworkError:
        logger.error(f"Network error getting food for user {message.chat.id}")
        await status_msg.edit_text(
            "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже."
        )
    except AuthenticationError:
        logger.error(f"Auth error getting food for user {message.chat.id}")
        await status_msg.edit_text(
            "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль."
        )
    except Exception as e:
        logger.error(f"Error getting food today for user {message.chat.id}: {e}")
        await status_msg.edit_text(
            f"❌ Ошибка получения данных о питании: {e}"
        )


# ===== Настройка порога баланса =====

@router.message(Command("set_threshold"))
@router.message(F.text == "💰 Порог баланса")
async def cmd_set_threshold(message: Message, user_config: Optional[UserConfig] = None):
    """Начало настройки порога баланса — выбор ребёнка через inline-кнопки."""
    result = await require_authentication(message, user_config)
    if result is None:
        return

    login, password, children = result
    thresholds = await get_all_thresholds_for_chat(message.chat.id)

    text_lines = ["\u2699\ufe0f <b>Настройка порога баланса</b>", ""]

    buttons = []
    for idx, child in enumerate(children, 1):
        threshold = thresholds.get(child.id, config.default_balance_threshold)
        text_lines.append(
            f"{idx}. {child.full_name} ({child.group}) \u2014 порог {threshold:.0f} \u20bd"
        )
        buttons.append([
            InlineKeyboardButton(
                text=f"{child.full_name} ({child.group}) \u2014 {threshold:.0f} \u20bd",
                callback_data=f"thr_child_{child.id}_{idx-1}",
            )
        ])

    buttons.append([InlineKeyboardButton(text="\u25c0\ufe0f Назад", callback_data="thr_back")])

    await message.answer(
        chr(10).join(text_lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("thr_child_"))
async def cb_threshold_child(callback: CallbackQuery, state: FSMContext):
    """Выбран ребёнок для настройки порога — спрашиваем значение."""
    parts = callback.data.split("_")
    # thr_child_{child_id}_{child_index}
    child_id = int(parts[2])
    child_index = int(parts[3])

    await callback.answer()

    # Сохраняем child_id в state для следующего шага
    await state.update_data(selected_child_id=child_id, selected_child_index=child_index)
    await state.set_state(ThresholdStates.waiting_for_threshold_value)

    current_threshold = await get_child_threshold(callback.message.chat.id, child_id)

    await callback.message.answer(
        f"\U0001f476 Выбран ребёнок.\n"
        f"Текущий порог: <b>{current_threshold:.0f} \u20bd</b>\n\n"
        f"\U0001f4dd Введите новый порог (число, например: 300):",
    )


@router.message(ThresholdStates.waiting_for_threshold_value)
async def process_threshold_value(message: Message, state: FSMContext):
    """Обработка ввода значения порога."""
    text = message.text.strip()

    if text in ["\u274c Отмена", "/cancel", "\u25c0\ufe0f Назад"]:
        await state.clear()
        await message.answer("\u274c Настройка отменена.", reply_markup=get_main_keyboard())
        return

    if is_navigation_command(text):
        await state.clear()
        return  # пусть обработает другой handler

    data = await state.get_data()
    child_id = data.get("selected_child_id")

    if child_id is None:
        await state.clear()
        await message.answer("\u274c Ошибка. Начните заново с /set_threshold", reply_markup=get_main_keyboard())
        return

    try:
        value = float(text.replace(",", "."))
    except ValueError:
        await message.answer("\u274c Введите число (например: 300).")
        return

    if value < 0:
        await message.answer("\u274c Порог не может быть отрицательным.")
        return
    if value > 10000:
        await message.answer("\u274c Порог слишком большой (максимум 10 000 \u20bd).")
        return

    await set_child_threshold(message.chat.id, child_id, value)

    from ..services.cache import threshold_cache
    threshold_cache.delete(f"{message.chat.id}:thresholds")

    await state.clear()

    await message.answer(
        f"\u2705 <b>Порог установлен!</b>\n\n"
        f"{value:.0f} \u20bd\n\n"
        f"Вы будете получать уведомления, когда баланс упадёт ниже этого значения.",
        reply_markup=get_main_keyboard()
    )


@router.callback_query(F.data == "thr_back")
async def cb_threshold_back(callback: CallbackQuery):
    """Возврат в настройки."""
    keyboard = get_settings_keyboard()
    try:
        await callback.message.edit_text(
            "\u2699\ufe0f <b>Настройки</b>",
            reply_markup=None,
        )
    except Exception:
        pass
    await callback.answer()
    await callback.message.answer(
        "\u2699\ufe0f <b>Настройки</b>",
        reply_markup=keyboard,
    )

