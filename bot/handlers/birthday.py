"""
Обработчики настройки уведомлений о днях рождения одноклассников.
"""
import logging
from typing import Optional

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from ..database import (
    get_user,
    create_or_update_user,
    get_birthday_settings,
    set_birthday_settings,
    get_all_birthday_settings,
    UserConfig,
)
from ..services import get_children_async
from ..states import BirthdaySettingsStates

logger = logging.getLogger(__name__)

router = Router()

# ===== Константы =====

WEEKDAY_NAMES = [
    "Понедельник", "Вторник", "Среда",
    "Четверг", "Пятница", "Суббота", "Воскресенье",
]

WEEKDAY_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


# ===== Вспомогательные функции =====

def _format_time_str(hour: int, minute: int) -> str:
    """Форматирование времени в строку HH:MM."""
    return f"{hour:02d}:{minute:02d}"


def _get_mode_description(settings: dict) -> str:
    """Получить описание режима уведомлений."""
    mode = settings.get("mode", "tomorrow")
    hour = settings.get("notify_hour", 7)
    minute = settings.get("notify_minute", 0)
    time_str = _format_time_str(hour, minute)

    if mode == "weekly":
        weekday = settings.get("notify_weekday", 1)
        weekday_name = WEEKDAY_NAMES[weekday] if 0 <= weekday <= 6 else "?"
        return f"Еженедельно ({weekday_name}, {time_str})"
    else:
        return f"Ежедневно ({time_str})"


# ===== Хендлер кнопки из настроек =====

@router.message(F.text == "🎂 Дни рождения")
async def cmd_birthday_settings(
    message: Message,
    user_config: Optional[UserConfig] = None,
    state: FSMContext = None,
):
    """Главный экран настроек дней рождения."""
    if state:
        await state.clear()

    if user_config is None:
        user_config = await get_user(message.chat.id)

    if user_config is None:
        user_config = await create_or_update_user(message.chat.id)

    if not user_config.login or not user_config.password:
        await message.answer(
            "❌ Сначала настройте логин/пароль через /set_login"
        )
        return

    # Получаем список детей
    try:
        children = await get_children_async(user_config.login, user_config.password)
    except Exception as e:
        logger.error(f"Error getting children: {e}")
        await message.answer("❌ Ошибка получения списка детей.")
        return

    if not children:
        await message.answer("❌ Дети не найдены.")
        return

    # Загружаем настройки для всех детей
    all_settings = await get_all_birthday_settings(message.chat.id)
    settings_map = {s["child_id"]: s for s in all_settings}

    # Глобальный статус
    global_status = "✅ ВКЛ" if user_config.birthday_enabled else "❌ ВЫКЛ"

    # Строим текст и клавиатуру
    text_lines = [
        f"🎂 <b>Уведомления о днях рождения</b>\n",
        f"Глобальное уведомление: <b>{global_status}</b>\n",
    ]

    buttons = [
        [
            InlineKeyboardButton(
                text=f"🎂 Уведомления о ДР: {global_status}",
                callback_data="bd_toggle_global",
            )
        ],
    ]

    for i, child in enumerate(children):
        child_settings = settings_map.get(child.id)
        if child_settings and child_settings.get("enabled"):
            desc = _get_mode_description(child_settings)
            text_lines.append(f"👦 <b>{child.full_name}</b> ({child.group}):")
            text_lines.append(f"   {desc}")
            text_lines.append("")
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"👦 {child.full_name} — {desc}",
                        callback_data=f"bd_child_{child.id}_{i}",
                    )
                ]
            )
        else:
            text_lines.append(f"👦 <b>{child.full_name}</b> ({child.group}): выкл")
            text_lines.append("")
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"👦 {child.full_name} — выкл",
                        callback_data=f"bd_child_{child.id}_{i}",
                    )
                ]
            )

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bd_back")])

    text = "\n".join(text_lines)
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# ===== Callback-хендлеры =====

@router.callback_query(F.data == "bd_toggle_global")
async def cb_toggle_global(
    callback: CallbackQuery,
    user_config: Optional[UserConfig] = None,
):
    """Переключение глобального уведомления о ДР."""
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!", show_alert=True)
        return

    new_status = not user_config.birthday_enabled
    await create_or_update_user(callback.message.chat.id, birthday_enabled=new_status)
    await callback.answer(f"{'Включено' if new_status else 'Выключено'}!")

    # Переоткрываем экран настроек
    updated = await get_user(callback.message.chat.id)
    await _show_birthday_menu(callback, updated)


@router.callback_query(F.data == "bd_back")
async def cb_back(callback: CallbackQuery):
    """Возврат в настройки."""
    await callback.answer()
    from .auth import get_settings_keyboard
    await callback.message.edit_text(
        "⚙️ <b>Настройки</b>",
        reply_markup=get_settings_keyboard(),
    )


@router.callback_query(F.data.startswith("bd_child_"))
async def cb_child_settings(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    """Настройки ДР для конкретного ребёнка."""
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!", show_alert=True)
        return

    await callback.answer()

    # Парсим callback data: bd_child_{child_id}_{child_index}
    parts = callback.data.split("_")
    # bd_child_{child_id}_{child_index}
    child_id = int(parts[2])
    child_index = int(parts[3])

    await _show_child_settings_screen(callback, user_config, child_id, child_index)


@router.callback_query(F.data == "bd_back_to_menu")
async def cb_back_to_menu(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    """Возврат в главное меню ДР."""
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!", show_alert=True)
        return

    await callback.answer()
    await _show_birthday_menu(callback, user_config)


@router.callback_query(F.data == "bd_noop")
async def cb_noop(callback: CallbackQuery):
    """Заглушка для информационных кнопок."""
    await callback.answer()


@router.callback_query(F.data.startswith("bd_enable_"))
async def cb_toggle_child_enable(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    """Включение/выключение уведомлений для ребёнка."""
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!", show_alert=True)
        return

    parts = callback.data.split("_")
    child_id = int(parts[2])
    child_index = int(parts[3])

    settings = await get_birthday_settings(callback.message.chat.id, child_id)
    new_enabled = not settings.get("enabled", False)

    await set_birthday_settings(
        chat_id=callback.message.chat.id,
        child_id=child_id,
        enabled=new_enabled,
        mode=settings.get("mode", "tomorrow"),
        notify_weekday=settings.get("notify_weekday", 1),
        notify_hour=settings.get("notify_hour", 7),
        notify_minute=settings.get("notify_minute", 0),
    )

    # Если включили и глобально выключено — включаем глобально
    if new_enabled and not user_config.birthday_enabled:
        await create_or_update_user(callback.message.chat.id, birthday_enabled=True)

    await callback.answer(f"{'Включено' if new_enabled else 'Выключено'}!")

    # Обновляем экран настроек ребёнка
    updated_user = await get_user(callback.message.chat.id)
    await _show_child_settings_screen(callback, updated_user, child_id, child_index)


@router.callback_query(F.data.startswith("bd_mode_tomorrow_"))
async def cb_mode_tomorrow(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    """Выбор режима «завтра» и переход к выбору времени."""
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)

    parts = callback.data.split("_")
    child_id = int(parts[3])
    child_index = int(parts[4])

    await callback.answer()

    # Сохраняем режим
    settings = await get_birthday_settings(callback.message.chat.id, child_id)
    await set_birthday_settings(
        chat_id=callback.message.chat.id,
        child_id=child_id,
        enabled=True,
        mode="tomorrow",
        notify_weekday=settings.get("notify_weekday", 1),
        notify_hour=settings.get("notify_hour", 7),
        notify_minute=settings.get("notify_minute", 0),
    )

    # Показываем выбор часа
    await _show_hour_selection(
        callback.message,
        child_id,
        child_index,
        settings.get("notify_hour", 7),
    )


@router.callback_query(F.data.startswith("bd_mode_weekly_"))
async def cb_mode_weekly(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    """Выбор режима «еженедельно» и переход к выбору дня недели."""
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)

    parts = callback.data.split("_")
    child_id = int(parts[3])
    child_index = int(parts[4])

    await callback.answer()

    # Сохраняем режим
    settings = await get_birthday_settings(callback.message.chat.id, child_id)
    await set_birthday_settings(
        chat_id=callback.message.chat.id,
        child_id=child_id,
        enabled=True,
        mode="weekly",
        notify_weekday=settings.get("notify_weekday", 1),
        notify_hour=settings.get("notify_hour", 7),
        notify_minute=settings.get("notify_minute", 0),
    )

    # Показываем выбор дня недели
    await _show_weekday_selection(
        callback.message,
        child_id,
        child_index,
        settings.get("notify_weekday", 1),
    )


@router.callback_query(F.data.startswith("bd_weekday_"))
async def cb_set_weekday(callback: CallbackQuery):
    """Установка дня недели для weekly-режима."""
    parts = callback.data.split("_")
    # bd_weekday_{child_id}_{child_index}_{weekday}
    child_id = int(parts[2])
    child_index = int(parts[3])
    weekday = int(parts[4])

    await callback.answer()

    settings = await get_birthday_settings(callback.message.chat.id, child_id)
    await set_birthday_settings(
        chat_id=callback.message.chat.id,
        child_id=child_id,
        enabled=True,
        mode="weekly",
        notify_weekday=weekday,
        notify_hour=settings.get("notify_hour", 7),
        notify_minute=settings.get("notify_minute", 0),
    )

    # Показываем выбор часа
    await _show_hour_selection(
        callback.message,
        child_id,
        child_index,
        settings.get("notify_hour", 7),
    )


@router.callback_query(F.data.startswith("bd_time_h_"))
async def cb_set_hour(callback: CallbackQuery):
    """Установка часа и переход к выбору минут."""
    parts = callback.data.split("_")
    # bd_time_h_{child_id}_{child_index}_{hour}
    child_id = int(parts[3])
    child_index = int(parts[4])
    hour = int(parts[5])

    await callback.answer()

    # Показываем выбор минут
    await _show_minute_selection(
        callback.message,
        child_id,
        child_index,
        hour,
    )


@router.callback_query(F.data.startswith("bd_time_m_"))
async def cb_set_minute(callback: CallbackQuery, state: FSMContext):
    """Установка минут и сохранение всех настроек."""
    parts = callback.data.split("_")
    # bd_time_m_{child_id}_{child_index}_{minute}
    child_id = int(parts[3])
    child_index = int(parts[4])
    minute = int(parts[5])

    await callback.answer()

    settings = await get_birthday_settings(callback.message.chat.id, child_id)
    hour = settings.get("notify_hour", 7)

    # Сохраняем финальные настройки
    await set_birthday_settings(
        chat_id=callback.message.chat.id,
        child_id=child_id,
        enabled=True,
        mode=settings.get("mode", "tomorrow"),
        notify_weekday=settings.get("notify_weekday", 1),
        notify_hour=hour,
        notify_minute=minute,
    )

    time_str = _format_time_str(hour, minute)
    mode = settings.get("mode", "tomorrow")

    if mode == "weekly":
        weekday = settings.get("notify_weekday", 1)
        weekday_name = WEEKDAY_NAMES[weekday] if 0 <= weekday <= 6 else "?"
        desc = f"Еженедельно ({weekday_name}, {time_str})"
    else:
        desc = f"Ежедневно ({time_str})"

    buttons = [
        [InlineKeyboardButton(
            text=f"✅ Настройки сохранены!",
            callback_data="bd_noop",
        )],
        [InlineKeyboardButton(text="◀️ Назад к ребёнку", callback_data=f"bd_child_{child_id}_{child_index}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="bd_back_to_menu")],
    ]

    await callback.message.edit_text(
        f"✅ <b>Настройки сохранены!</b>\n\n{desc}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


# ===== Вспомогательные экраны =====

async def _show_child_settings_screen(
    callback: CallbackQuery,
    user_config: UserConfig,
    child_id: int,
    child_index: int,
):
    """Показать экран настроек ДР для конкретного ребёнка (без callback.answer)."""
    try:
        children = await get_children_async(user_config.login, user_config.password)
    except Exception:
        await callback.message.edit_text("❌ Ошибка получения списка детей.")
        return

    if not children or child_index >= len(children):
        await callback.message.edit_text("❌ Ребёнок не найден.")
        return

    child = children[child_index]
    settings = await get_birthday_settings(callback.message.chat.id, child.id)

    is_enabled = settings.get("enabled", False)
    mode = settings.get("mode", "tomorrow")

    buttons = []

    btn_text = f"{'🔴' if is_enabled else '🟢'} Включить для {child.full_name}"
    buttons.append([InlineKeyboardButton(text=btn_text, callback_data=f"bd_enable_{child.id}_{child_index}")])

    if is_enabled:
        mode_tomorrow = "✅ " if mode == "tomorrow" else ""
        mode_weekly = "✅ " if mode == "weekly" else ""

        buttons.append([
            InlineKeyboardButton(
                text=f"{mode_tomorrow}📅 Уведомлять о ДР завтра",
                callback_data=f"bd_mode_tomorrow_{child.id}_{child_index}",
            )
        ])
        buttons.append([
            InlineKeyboardButton(
                text=f"{mode_weekly}📋 Уведомлять о ДР на предстоящей неделе",
                callback_data=f"bd_mode_weekly_{child.id}_{child_index}",
            )
        ])

        desc = _get_mode_description(settings)
        buttons.append([InlineKeyboardButton(
            text=f"⏰ Текущее время: {desc}",
            callback_data="bd_noop",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bd_back_to_menu")])

    status_text = "✅ Включено" if is_enabled else "❌ Выключено"
    text = (
        f"👦 <b>{child.full_name}</b> ({child.group})\n"
        f"Статус: <b>{status_text}</b>"
    )

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


async def _show_birthday_menu(callback: CallbackQuery, user_config: UserConfig):
    """Показать главное меню настроек ДР (для callback)."""
    if not user_config.login or not user_config.password:
        await callback.message.edit_text("❌ Сначала настройте логин/пароль через /set_login")
        return

    try:
        children = await get_children_async(user_config.login, user_config.password)
    except Exception:
        await callback.message.edit_text("❌ Ошибка получения списка детей.")
        return

    if not children:
        await callback.message.edit_text("❌ Дети не найдены.")
        return

    all_settings = await get_all_birthday_settings(user_config.chat_id)
    settings_map = {s["child_id"]: s for s in all_settings}

    global_status = "✅ ВКЛ" if user_config.birthday_enabled else "❌ ВЫКЛ"

    text_lines = [
        f"🎂 <b>Уведомления о днях рождения</b>\n",
        f"Глобальное уведомление: <b>{global_status}</b>\n",
    ]

    buttons = [
        [
            InlineKeyboardButton(
                text=f"🎂 Уведомления о ДР: {global_status}",
                callback_data="bd_toggle_global",
            )
        ],
    ]

    for i, child in enumerate(children):
        child_settings = settings_map.get(child.id)
        if child_settings and child_settings.get("enabled"):
            desc = _get_mode_description(child_settings)
            text_lines.append(f"👦 <b>{child.full_name}</b> ({child.group}):")
            text_lines.append(f"   {desc}")
            text_lines.append("")
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"👦 {child.full_name} — {desc}",
                        callback_data=f"bd_child_{child.id}_{i}",
                    )
                ]
            )
        else:
            text_lines.append(f"👦 <b>{child.full_name}</b> ({child.group}): выкл")
            text_lines.append("")
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=f"👦 {child.full_name} — выкл",
                        callback_data=f"bd_child_{child.id}_{i}",
                    )
                ]
            )

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="bd_back")])

    text = "\n".join(text_lines)
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def _show_hour_selection(message, child_id: int, child_index: int, current_hour: int):
    """Показать клавиатуру выбора часа."""
    buttons = []
    for h in range(6, 22):
        prefix = "✅ " if h == current_hour else ""
        buttons.append([InlineKeyboardButton(
            text=f"{prefix}{h}:00",
            callback_data=f"bd_time_h_{child_id}_{child_index}_{h}",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bd_child_{child_id}_{child_index}")])

    await message.edit_text(
        "⏰ <b>Выберите час уведомления:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def _show_minute_selection(message, child_id: int, child_index: int, hour: int):
    """Показать клавиатуру выбора минут."""
    buttons = []
    for m in [0, 15, 30, 45]:
        buttons.append([InlineKeyboardButton(
            text=f"{hour:02d}:{m:02d}",
            callback_data=f"bd_time_m_{child_id}_{child_index}_{m}",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bd_child_{child_id}_{child_index}")])

    await message.edit_text(
        f"⏰ <b>Выберите минуты</b> (час: {hour}:00):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


async def _show_weekday_selection(message, child_id: int, child_index: int, current_weekday: int):
    """Показать клавиатуру выбора дня недели."""
    buttons = []
    for d in range(7):
        prefix = "✅ " if d == current_weekday else ""
        buttons.append([InlineKeyboardButton(
            text=f"{prefix}{WEEKDAY_NAMES[d]}",
            callback_data=f"bd_weekday_{child_id}_{child_index}_{d}",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"bd_child_{child_id}_{child_index}")])

    await message.edit_text(
        "📅 <b>Выберите день недели:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
