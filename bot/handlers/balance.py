"""
Обработчики для баланса питания и информации о питании.
"""
import logging
from datetime import date
from typing import Dict, List, Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from ..config import config
from ..database import (
    get_user, get_child_threshold, set_child_threshold,
    get_all_thresholds_for_chat, UserConfig
)
from ..states import ThresholdStates
from ..services import (
    Child, FoodInfo, get_children_async, get_food_for_children,
    get_timetable_for_children, RuobrError, invalidate_user_cache
)
from ..utils.formatters import (
    format_balance, format_food_visit, format_date, truncate_text
)
from .auth import get_main_keyboard, get_settings_keyboard

logger = logging.getLogger(__name__)

router = Router()


async def require_authentication(
    message: Message,
    user_config: Optional[UserConfig]
) -> Optional[tuple]:
    """
    Проверка аутентификации пользователя.
    
    Returns:
        Кортеж (login, password, children) или None если не аутентифицирован.
    """
    if user_config is None:
        user_config = await get_user(message.chat.id)
    
    if not user_config or not user_config.login or not user_config.password:
        await message.answer(
            "❌ Сначала настройте учётные данные командой /set_login"
        )
        return None
    
    try:
        children = await get_children_async(user_config.login, user_config.password)
    except RuobrError as e:
        logger.error(f"Ruobr API error for user {message.chat.id}: {e}")
        await message.answer(f"❌ Ошибка доступа к Ruobr: {e}")
        return None
    
    if not children:
        await message.answer("❌ Дети не найдены в аккаунте.")
        return None
    
    return user_config.login, user_config.password, children


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
        # Получаем информацию о питании
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
        
    except Exception as e:
        logger.error(f"Error getting balance for user {message.chat.id}: {e}")
        await status_msg.edit_text(
            f"❌ Ошибка получения баланса: {e}"
        )


# ===== Питание сегодня =====

@router.message(Command("foodtoday"))
@router.message(F.text == "🍽 Питание сегодня")
async def cmd_foodtoday(message: Message, user_config: Optional[UserConfig] = None):
    """Показать информацию о питании за сегодня."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка информации о питании...")
    
    try:
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")
        
        food_info = await get_food_for_children(login, password, children)
        
        lines = [f"🍽 <b>Питание сегодня ({format_date(today_str)})</b>"]
        found = False
        
        for child in children:
            info = food_info.get(child.id)
            if not info or not info.visits:
                continue
            
            for visit in info.visits:
                if visit.get("date") != today_str:
                    continue
                
                # Проверяем, было ли подтверждённое питание
                if not visit.get("ordered") and visit.get("state") != 30:
                    continue
                
                found = True
                visit_text = format_food_visit(visit, child.full_name)
                lines.append(visit_text)
        
        if not found:
            await status_msg.edit_text(
                f"ℹ️ На сегодня ({format_date(today_str)}) "
                f"подтверждённого питания не найдено."
            )
        else:
            text = truncate_text("\n".join(lines))
            await status_msg.edit_text(text)
            
    except Exception as e:
        logger.error(f"Error getting food today for user {message.chat.id}: {e}")
        await status_msg.edit_text(
            f"❌ Ошибка получения данных о питании: {e}"
        )


# ===== Настройка порога баланса =====

@router.message(Command("set_threshold"))
@router.message(F.text == "💰 Порог баланса")
async def cmd_set_threshold(message: Message, state: FSMContext, user_config: Optional[UserConfig] = None):
    """Начало настройки порога баланса."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    thresholds = await get_all_thresholds_for_chat(message.chat.id)
    
    lines = ["⚙️ <b>Настройка порога баланса</b>\n"]
    lines.append("Выберите ребёнка для изменения порога:\n")
    
    for idx, child in enumerate(children, 1):
        threshold = thresholds.get(child.id, config.default_balance_threshold)
        lines.append(
            f"{idx}. {child.full_name} ({child.group}) — "
            f"порог {threshold:.0f} ₽"
        )
    
    lines.append("\n📝 Ответьте номером ребёнка.")
    
    await state.update_data(children=[{"id": c.id, "name": c.full_name, "group": c.group} for c in children])
    await state.set_state(ThresholdStates.waiting_for_child_selection)
    
    await message.answer("\n".join(lines))


@router.message(ThresholdStates.waiting_for_child_selection)
async def process_threshold_child(message: Message, state: FSMContext):
    """Обработка выбора ребёнка для настройки порога."""
    text = message.text.strip()
    
    # Отмена
    if text in ["❌ Отмена", "/cancel", "◀️ Назад"]:
        await state.clear()
        await message.answer("❌ Настройка отменена.", reply_markup=get_main_keyboard())
        return
    
    data = await state.get_data()
    children = data.get("children", [])
    
    try:
        idx = int(text)
    except ValueError:
        await message.answer("❌ Введите номер ребёнка (число).")
        return
    
    if idx < 1 or idx > len(children):
        await message.answer(f"❌ Неверный номер. Введите число от 1 до {len(children)}.")
        return
    
    child = children[idx - 1]
    current_threshold = await get_child_threshold(message.chat.id, child["id"])
    
    await state.update_data(selected_child_id=child["id"], selected_child_name=child["name"])
    await state.set_state(ThresholdStates.waiting_for_threshold_value)
    
    await message.answer(
        f"👶 Выбран: <b>{child['name']}</b> ({child['group']})\n"
        f"Текущий порог: <b>{current_threshold:.0f} ₽</b>\n\n"
        f"Введите новый порог (число, например: 300):"
    )


@router.message(ThresholdStates.waiting_for_threshold_value)
async def process_threshold_value(message: Message, state: FSMContext):
    """Обработка ввода значения порога."""
    text = message.text.strip()
    
    # Отмена
    if text in ["❌ Отмена", "/cancel", "◀️ Назад"]:
        await state.clear()
        await message.answer("❌ Настройка отменена.", reply_markup=get_main_keyboard())
        return
    
    data = await state.get_data()
    child_id = data.get("selected_child_id")
    child_name = data.get("selected_child_name", "Ребёнок")
    
    if child_id is None:
        await state.clear()
        await message.answer("❌ Ошибка. Начните заново с /set_threshold")
        return
    
    try:
        value = float(text.replace(",", "."))
    except ValueError:
        await message.answer("❌ Введите число (например: 300).")
        return
    
    # Валидация диапазона
    if value < 0:
        await message.answer("❌ Порог не может быть отрицательным.")
        return
    if value > 10000:
        await message.answer("❌ Порог слишком большой (максимум 10000 ₽).")
        return
    
    # Сохраняем
    await set_child_threshold(message.chat.id, child_id, value)
    
    # Инвалидируем кэш порогов
    from ..services.cache import threshold_cache
    threshold_cache.delete(f"{message.chat.id}:thresholds")
    
    await state.clear()
    
    await message.answer(
        f"✅ <b>Порог установлен!</b>\n\n"
        f"{child_name}: {value:.0f} ₽\n\n"
        f"Вы будете получать уведомления, когда баланс упадёт ниже этого значения.",
        reply_markup=get_main_keyboard()
    )
