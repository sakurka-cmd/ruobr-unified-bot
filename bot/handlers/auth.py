"""
Обработчики аутентификации и базовых команд.
"""
import logging
from typing import Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from ..config import config
from ..database import get_user, create_or_update_user, UserConfig
from ..states import LoginStates
from ..services import get_children_async, AuthenticationError, get_classmates_for_child, get_achievements_for_child, get_guide_for_child

logger = logging.getLogger(__name__)

router = Router()


# ===== Клавиатуры =====

def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 Расписание сегодня"), KeyboardButton(text="📅 Расписание завтра")],
            [KeyboardButton(text="📘 ДЗ на завтра"), KeyboardButton(text="⭐ Оценки сегодня")],
            [KeyboardButton(text="💰 Баланс питания"), KeyboardButton(text="🍽 Питание сегодня")],
            [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="ℹ️ Информация")],
        ],
        resize_keyboard=True,
        persistent=True
    )


def get_settings_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔑 Изменить логин/пароль"), KeyboardButton(text="💰 Порог баланса")],
            [KeyboardButton(text="🔔 Уведомления"), KeyboardButton(text="👤 Мой профиль")],
            [KeyboardButton(text="◀️ Назад")],
        ],
        resize_keyboard=True
    )


def get_info_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👥 Одноклассники"), KeyboardButton(text="👩‍🏫 Учителя")],
            [KeyboardButton(text="🏆 Достижения")],
            [KeyboardButton(text="◀️ Назад")],
        ],
        resize_keyboard=True
    )


def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )


# ===== Команды =====

@router.message(Command("start"))
async def cmd_start(message: Message, user_config: Optional[UserConfig] = None):
    if user_config is None:
        user_config = await create_or_update_user(message.chat.id)
    
    is_auth = user_config.login and user_config.password
    
    welcome_text = (
        "👋 <b>Добро пожаловать в школьный бот!</b>\n\n"
        "Я помогаю родителям следить за:\n"
        "• 💰 Балансом школьного питания\n"
        "• 📅 Расписанием уроков\n"
        "• 📘 Домашними заданиями\n"
        "• ⭐ Оценками\n\n"
    )
    
    if not is_auth:
        welcome_text += "⚠️ <b>Требуется настройка!</b>\nИспользуйте /set_login для ввода учётных данных.\n\n"
    else:
        welcome_text += "✅ Учётные данные настроены.\n\n"
    
    welcome_text += (
        "📖 <b>Команды:</b>\n"
        "/set_login — настроить логин/пароль\n"
        "/balance — баланс питания\n"
        "/ttoday — расписание сегодня\n"
        "/ttomorrow — расписание завтра"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_keyboard())


@router.message(Command("set_login"))
async def cmd_set_login(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "🔐 <b>Настройка учётных данных</b>\n\n"
        "Введите логин от cabinet.ruobr.ru:\n\n"
        "❌ Отмена — для выхода",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(LoginStates.waiting_for_login)


@router.message(LoginStates.waiting_for_login)
async def process_login(message: Message, state: FSMContext):
    text = message.text.strip()
    
    # Проверка отмены
    if text == "❌ Отмена" or text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=get_main_keyboard())
        return
    
    if not text:
        await message.answer("❌ Логин не может быть пустым. Попробуйте ещё раз:")
        return
    
    if len(text) > 100:
        await message.answer("❌ Логин слишком длинный. Попробуйте ещё раз:")
        return
    
    await state.update_data(login=text)
    await message.answer(
        "✅ Логин сохранён.\n\n"
        "Теперь введите пароль от cabinet.ruobr.ru:\n\n"
        "❌ Отмена — для выхода"
    )
    await state.set_state(LoginStates.waiting_for_password)


@router.message(LoginStates.waiting_for_password)
async def process_password(message: Message, state: FSMContext):
    password = message.text.strip()
    
    # Проверка отмены
    if password == "❌ Отмена" or password == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=get_main_keyboard())
        return
    
    if not password:
        await message.answer("❌ Пароль не может быть пустым. Попробуйте ещё раз:")
        return
    
    data = await state.get_data()
    login = data.get("login", "")
    
    # Удаляем сообщение с паролем
    try:
        await message.delete()
    except Exception:
        pass
    
    status_message = await message.answer("🔄 Проверка учётных данных...")
    
    try:
        children = await get_children_async(login, password)
        
        if not children:
            await status_message.edit_text(
                "⚠️ Учётные данные верны, но дети не найдены.\n"
                "Данные сохранены. Проверьте аккаунт на cabinet.ruobr.ru"
            )
        else:
            children_list = "\n".join([f"  • {c.full_name} ({c.group})" for c in children])
            await status_message.edit_text(
                f"✅ <b>Успешная авторизация!</b>\n\n"
                f"Найдены дети:\n{children_list}\n\n"
                f"Теперь доступны все функции бота."
            )
        
        # Сохраняем учётные данные
        await create_or_update_user(message.chat.id, login=login, password=password)
        
        # Отправляем клавиатуру отдельным сообщением
        await message.answer("🏠 Главное меню", reply_markup=get_main_keyboard())
        
    except AuthenticationError:
        await status_message.edit_text(
            "❌ <b>Ошибка авторизации!</b>\n\n"
            "Неверный логин или пароль. Попробуйте снова: /set_login"
        )
    except Exception as e:
        logger.error(f"Error during login for user {message.chat.id}: {e}")
        await status_message.edit_text(
            "❌ <b>Ошибка соединения!</b>\n\n"
            "Не удалось проверить учётные данные. Попробуйте позже."
        )
    
    await state.clear()


@router.message(Command("cancel"))
@router.message(F.text == "❌ Отмена")
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нет активной операции.", reply_markup=get_main_keyboard())
        return
    
    await state.clear()
    await message.answer("❌ Операция отменена.", reply_markup=get_main_keyboard())


@router.message(F.text == "ℹ️ Информация")
async def btn_info(message: Message):
    await message.answer(
        "ℹ️ <b>Информация</b>\n\n"
        "Выберите что хотите узнать:",
        reply_markup=get_info_keyboard()
    )


@router.message(F.text == "⚙️ Настройки")
async def btn_settings(message: Message):
    await message.answer("⚙️ <b>Настройки</b>", reply_markup=get_settings_keyboard())


@router.message(F.text == "🔑 Изменить логин/пароль")
async def btn_change_login(message: Message, state: FSMContext):
    await cmd_set_login(message, state)


# ===== Информация =====

def get_child_select_keyboard(children, action: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора ребенка"""
    buttons = []
    for i, child in enumerate(children):
        buttons.append([InlineKeyboardButton(
            text=f"👤 {child.full_name} ({child.group})",
            callback_data=f"info_{action}_{i}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def get_children_or_select(message: Message, user_config: UserConfig, action: str):
    """Получить детей или показать выбор"""
    try:
        children = await get_children_async(user_config.login, user_config.password)
        if not children:
            await message.answer("❌ Дети не найдены.")
            return None
        
        if len(children) == 1:
            return (children, 0)  # Один ребенок - возвращаем его индекс
        
        # Несколько детей - показываем выбор
        await message.answer(
            f"👦👧 <b>Выберите ребенка:</b>",
            reply_markup=get_child_select_keyboard(children, action)
        )
        return None  # Ждем callback
        
    except Exception as e:
        logger.error(f"Error getting children: {e}")
        await message.answer(f"❌ Ошибка: {e}")
        return None


async def show_classmates(message: Message, login: str, password: str, child_index: int, child_name: str):
    """Показать одноклассников"""
    status_msg = await message.answer("🔄 Загрузка списка одноклассников...")
    
    try:
        classmates = await get_classmates_for_child(login, password, child_index)
        
        if not classmates:
            await status_msg.edit_text("ℹ️ Одноклассники не найдены.")
            return
        
        classmates_sorted = sorted(classmates, key=lambda c: c.last_name)
        
        lines = [f"👥 <b>Одноклассники</b> — {child_name} ({len(classmates)} чел.):\n"]
        
        from datetime import datetime
        for i, c in enumerate(classmates_sorted, 1):
            if c.birth_date:
                try:
                    bd = datetime.strptime(c.birth_date, "%Y-%m-%d")
                    bd_str = bd.strftime("%d.%m.%Y")
                    age = datetime.now().year - bd.year
                    if (datetime.now().month, datetime.now().day) < (bd.month, bd.day):
                        age -= 1
                except:
                    bd_str = c.birth_date
                    age = "?"
            else:
                bd_str = "?"
                age = "?"
            
            lines.append(f"{i:2}. {c.full_name} {c.gender_icon} | {bd_str} ({age} лет)")
        
        text = "\n".join(lines)
        if len(text) > 4000:
            await status_msg.edit_text(text[:4000])
            remaining = text[4000:]
            while remaining:
                await message.answer(remaining[:4000])
                remaining = remaining[4000:]
        else:
            await status_msg.edit_text(text)
            
    except Exception as e:
        logger.error(f"Error getting classmates: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


async def show_teachers(message: Message, login: str, password: str, child_index: int, child_name: str):
    """Показать учителей"""
    status_msg = await message.answer("🔄 Загрузка списка учителей...")
    
    try:
        guide = await get_guide_for_child(login, password, child_index)
        
        if not guide.teachers:
            await status_msg.edit_text("ℹ️ Учителя не найдены.")
            return
        
        subject_teachers = [t for t in guide.teachers if t.subject]
        other_teachers = [t for t in guide.teachers if not t.subject]
        
        lines = [f"👩‍🏫 <b>Учителя</b> — {child_name}\n"]
        lines.append(f"<b>Школа:</b> {guide.name}")
        if guide.phone:
            lines.append(f"<b>Телефон:</b> {guide.phone}")
        if guide.url:
            lines.append(f"<b>Сайт:</b> {guide.url}")
        lines.append("")
        
        if subject_teachers:
            lines.append(f"<b>Предметники ({len(subject_teachers)}):</b>")
            for t in sorted(subject_teachers, key=lambda x: x.name):
                lines.append(f"  • {t.name} — {t.subject}")
        
        if other_teachers:
            lines.append(f"\n<b>Другие педагоги ({len(other_teachers)}):</b>")
            for t in sorted(other_teachers, key=lambda x: x.name)[:10]:
                lines.append(f"  • {t.name}")
            if len(other_teachers) > 10:
                lines.append(f"  ... и ещё {len(other_teachers) - 10}")
        
        await status_msg.edit_text("\n".join(lines))
            
    except Exception as e:
        logger.error(f"Error getting teachers: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


async def show_achievements(message: Message, login: str, password: str, child_index: int, child_name: str):
    """Показать достижения"""
    status_msg = await message.answer("🔄 Загрузка достижений...")
    
    try:
        achievements = await get_achievements_for_child(login, password, child_index)
        
        lines = [f"🏆 <b>Достижения</b> — {child_name}\n"]
        
        if achievements.directions:
            total = sum(d.count for d in achievements.directions)
            lines.append(f"<b>Всего:</b> {total}\n")
            
            for d in achievements.directions:
                bar = "█" * (d.percent // 10) + "░" * (10 - d.percent // 10)
                lines.append(f"📍 {d.direction}")
                lines.append(f"   {bar} {d.count} ({d.percent}%)")
        else:
            lines.append("Достижений пока нет.")
        
        if achievements.projects:
            lines.append(f"\n📝 <b>Проекты:</b> {len(achievements.projects)}")
        
        if achievements.gto_id:
            lines.append(f"\n🏃 <b>ГТО ID:</b> {achievements.gto_id}")
        
        await status_msg.edit_text("\n".join(lines))
            
    except Exception as e:
        logger.error(f"Error getting achievements: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


@router.message(F.text == "👥 Одноклассники")
async def btn_classmates(message: Message, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await message.answer("❌ Сначала настройте логин/пароль через /set_login")
        return
    
    result = await get_children_or_select(message, user_config, "classmates")
    if result:
        children, idx = result
        await show_classmates(message, user_config.login, user_config.password, idx, children[idx].full_name)


@router.message(F.text == "👩‍🏫 Учителя")
async def btn_teachers(message: Message, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await message.answer("❌ Сначала настройте логин/пароль через /set_login")
        return
    
    result = await get_children_or_select(message, user_config, "teachers")
    if result:
        children, idx = result
        await show_teachers(message, user_config.login, user_config.password, idx, children[idx].full_name)


@router.message(F.text == "🏆 Достижения")
async def btn_achievements(message: Message, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await message.answer("❌ Сначала настройте логин/пароль через /set_login")
        return
    
    result = await get_children_or_select(message, user_config, "achievements")
    if result:
        children, idx = result
        await show_achievements(message, user_config.login, user_config.password, idx, children[idx].full_name)


# Callback handlers для выбора ребенка
@router.callback_query(F.data.startswith("info_classmates_"))
async def cb_classmates_select(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await callback.answer("❌ Ошибка авторизации")
        return
    
    idx = int(callback.data.split("_")[-1])
    children = await get_children_async(user_config.login, user_config.password)
    
    await callback.message.delete()
    await show_classmates(callback.message, user_config.login, user_config.password, idx, children[idx].full_name)
    await callback.answer()


@router.callback_query(F.data.startswith("info_teachers_"))
async def cb_teachers_select(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await callback.answer("❌ Ошибка авторизации")
        return
    
    idx = int(callback.data.split("_")[-1])
    children = await get_children_async(user_config.login, user_config.password)
    
    await callback.message.delete()
    await show_teachers(callback.message, user_config.login, user_config.password, idx, children[idx].full_name)
    await callback.answer()


@router.callback_query(F.data.startswith("info_achievements_"))
async def cb_achievements_select(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await callback.answer("❌ Ошибка авторизации")
        return
    
    idx = int(callback.data.split("_")[-1])
    children = await get_children_async(user_config.login, user_config.password)
    
    await callback.message.delete()
    await show_achievements(callback.message, user_config.login, user_config.password, idx, children[idx].full_name)
    await callback.answer()


@router.message(F.text == "◀️ Назад")
async def btn_back(message: Message):
    await message.answer("🏠 <b>Главное меню</b>", reply_markup=get_main_keyboard())


@router.message(F.text == "👤 Мой профиль")
async def btn_profile(message: Message, user_config: Optional[UserConfig] = None):
    if user_config is None:
        user_config = await get_user(message.chat.id)
    
    if user_config is None:
        await message.answer("Профиль не найден. Используйте /start")
        return
    
    status = "✅ Настроен" if user_config.login and user_config.password else "❌ Не настроен"
    notif_status = "🔔 Включены" if user_config.enabled else "🔕 Выключены"
    marks_status = "🔔 Включены" if user_config.marks_enabled else "🔕 Выключены"
    
    await message.answer(
        f"👤 <b>Ваш профиль</b>\n\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Логин:</b> {user_config.login or 'не указан'}\n\n"
        f"<b>Уведомления о балансе:</b> {notif_status}\n"
        f"<b>Уведомления об оценках:</b> {marks_status}"
    )


@router.message(Command("enable"))
async def cmd_enable(message: Message):
    await create_or_update_user(message.chat.id, enabled=True, marks_enabled=True)
    await message.answer("🔔 <b>Уведомления включены!</b>")


@router.message(Command("disable"))
async def cmd_disable(message: Message):
    await create_or_update_user(message.chat.id, enabled=False, marks_enabled=False)
    await message.answer("🔕 <b>Уведомления отключены.</b>")


# ===== Inline клавиатуры =====

def get_notification_keyboard(user_config: UserConfig) -> InlineKeyboardMarkup:
    balance_status = "✅" if user_config.enabled else "❌"
    marks_status = "✅" if user_config.marks_enabled else "❌"
    food_status = "✅" if getattr(user_config, 'food_enabled', True) else "❌"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 Баланс: {balance_status}", callback_data="toggle_balance")],
        [InlineKeyboardButton(text=f"⭐ Оценки: {marks_status}", callback_data="toggle_marks")],
        [InlineKeyboardButton(text=f"🍽 Питание: {food_status}", callback_data="toggle_food")],
    ])


@router.message(F.text == "🔔 Уведомления")
async def btn_notifications_inline(message: Message, user_config: Optional[UserConfig] = None):
    if user_config is None:
        user_config = await get_user(message.chat.id)
    if user_config is None:
        user_config = await create_or_update_user(message.chat.id)
    
    await message.answer(
        "🔔 <b>Настройки уведомлений</b>\n\n"
        "Нажмите для включения/выключения:",
        reply_markup=get_notification_keyboard(user_config)
    )


@router.callback_query(F.data == "toggle_balance")
async def cb_toggle_balance(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!")
        return
    
    new_status = not user_config.enabled
    await create_or_update_user(callback.message.chat.id, enabled=new_status)
    await callback.answer(f"{'Включено' if new_status else 'Выключено'}!")
    
    updated = await get_user(callback.message.chat.id)
    await callback.message.edit_reply_markup(reply_markup=get_notification_keyboard(updated))


@router.callback_query(F.data == "toggle_marks")
async def cb_toggle_marks(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!")
        return
    
    new_status = not user_config.marks_enabled
    await create_or_update_user(callback.message.chat.id, marks_enabled=new_status)
    await callback.answer(f"{'Включено' if new_status else 'Выключено'}!")
    
    updated = await get_user(callback.message.chat.id)
    await callback.message.edit_reply_markup(reply_markup=get_notification_keyboard(updated))


@router.callback_query(F.data == "toggle_food")
async def cb_toggle_food(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None:
        user_config = await get_user(callback.message.chat.id)
    if user_config is None:
        await callback.answer("Ошибка!")
        return
    
    new_status = not getattr(user_config, 'food_enabled', True)
    await create_or_update_user(callback.message.chat.id, food_enabled=new_status)
    await callback.answer(f"{'Включено' if new_status else 'Выключено'}!")
    
    updated = await get_user(callback.message.chat.id)
    await callback.message.edit_reply_markup(reply_markup=get_notification_keyboard(updated))
