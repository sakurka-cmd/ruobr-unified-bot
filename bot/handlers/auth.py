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
            [KeyboardButton(text="📋 Справка")],
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
        
        # Получаем информацию о текущем ребенке для добавления в список
        children = await get_children_async(login, password)
        current_child = children[child_index] if children and child_index < len(children) else None
        
        # Если ребёнка нет в списке одноклассников, добавляем его
        if current_child:
            child_as_classmate = type('Classmate', (), {
                'last_name': current_child.last_name,
                'first_name': current_child.first_name,
                'middle_name': current_child.middle_name,
                'birth_date': current_child.birth_date,
                'gender': current_child.gender,
                'full_name': current_child.full_name,
                'gender_icon': current_child.gender_icon
            })()
            
            # Проверяем, есть ли уже ребенок в списке
            child_in_list = any(c.last_name == child_as_classmate.last_name and 
                                c.first_name == child_as_classmate.first_name 
                                for c in classmates)
            if not child_in_list:
                classmates.append(child_as_classmate)
        
        classmates_sorted = sorted(classmates, key=lambda c: c.last_name)
        
        from datetime import datetime
        
        # Формируем таблицу с увеличенной шириной для ФИО
        lines = [f"👥 <b>Классный список</b> — {child_name} ({len(classmates_sorted)} чел.):\n"]
        lines.append("<pre>№   Фамилия Имя Отчество                    | Д.р.      | Возр")
        lines.append("─" * 62)
        
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
                bd_str = "—"
                age = "—"
            
            # Форматируем имя (40 символов для полного ФИО)
            name_display = c.full_name[:40].ljust(40)
            icon = c.gender_icon
            
            lines.append(f"{i:2}. {name_display} {icon} | {bd_str:10} | {age}")
        
        lines.append("─" * 62)
        lines.append("</pre>")
        
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
        
        # Фильтруем только учителей с предметами (предметники)
        subject_teachers = [t for t in guide.teachers if t.subject]
        
        lines = [f"👩‍🏫 <b>Учителя</b> — {child_name}\n"]
        lines.append(f"<b>Школа:</b> {guide.name}")
        if guide.phone:
            lines.append(f"<b>Телефон:</b> {guide.phone}")
        if guide.url:
            lines.append(f"<b>Сайт:</b> {guide.url}")
        lines.append("")
        
        if subject_teachers:
            # Разбиваем учителей с несколькими предметами на отдельные записи
            teacher_subject_pairs = []
            for t in subject_teachers:
                # Разбиваем строку предметов по запятой
                subjects = [s.strip() for s in t.subject.split(",") if s.strip()]
                for subject in subjects:
                    teacher_subject_pairs.append((subject, t.name))
            
            # Сортируем по предмету
            teacher_subject_pairs.sort(key=lambda x: x[0])
            
            lines.append("<pre>Предмет                    | Учитель")
            lines.append("─" * 50)
            for subject, name in teacher_subject_pairs:
                subject_display = subject[:25].ljust(25)
                lines.append(f"{subject_display} | {name}")
            lines.append("─" * 50)
            lines.append("</pre>")
        else:
            lines.append("Предметники не найдены.")
        
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


@router.message(F.text == "📋 Справка")
async def btn_help(message: Message):
    """Справка о боте и его командах"""
    help_text = (
        "📋 <b>Справка по боту</b>\n\n"
        "<b>Школьный бот</b> — помогает родителям следить за учёбой детей.\n\n"
        
        "<b>📅 Расписание:</b>\n"
        "• «Расписание сегодня» — уроки на сегодня\n"
        "• «Расписание завтра» — уроки на завтра\n\n"
        
        "<b>📘 Домашние задания:</b>\n"
        "• «ДЗ на завтра» — задания на завтрашний день\n\n"
        
        "<b>⭐ Оценки:</b>\n"
        "• «Оценки сегодня» — оценки за сегодняшний день\n\n"
        
        "<b>🍽 Питание:</b>\n"
        "• «Баланс питания» — текущий баланс счёта\n"
        "• «Питание сегодня» — что ребёнок ел сегодня\n\n"
        
        "<b>ℹ️ Информация:</b>\n"
        "• «Одноклассники» — список класса с датами рождения\n"
        "• «Учителя» — предметники и контакты школы\n"
        "• «Достижения» — достижения и проекты ученика\n\n"
        
        "<b>⚙️ Настройки:</b>\n"
        "• «Изменить логин/пароль» — обновить данные\n"
        "• «Порог баланса» — настроить уведомления о балансе\n"
        "• «Уведомления» — включить/выключить оповещения\n"
        "• «Мой профиль» — информация об аккаунте\n\n"
        
        "<b>📝 Команды:</b>\n"
        "/start — главное меню\n"
        "/set_login — настроить учётные данные\n"
        "/balance — баланс питания\n"
        "/ttoday — расписание сегодня\n"
        "/ttomorrow — расписание завтра\n"
        "/enable — включить уведомления\n"
        "/disable — выключить уведомления\n\n"
        
        "<b>💡 Подсказка:</b> Бот автоматически уведомляет о:\n"
        "• Низком балансе питания\n"
        "• Новых оценках\n\n"
        
        "<b>🔗 Полезные ссылки:</b>\n"
        "• cabinet.ruobr.ru — электронный дневник"
    )
    await message.answer(help_text)


# Callback handlers для выбора ребенка
@router.callback_query(F.data.startswith("info_classmates_"))
async def cb_classmates_select(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await callback.answer("❌ Ошибка авторизации", show_alert=True)
        return
    
    # Отвечаем сразу, чтобы callback не истёк
    await callback.answer()
    
    try:
        idx = int(callback.data.split("_")[-1])
        
        # Показываем loading
        await callback.message.edit_text("🔄 Загрузка одноклассников...")
        
        children = await get_children_async(user_config.login, user_config.password)
        
        if not children or idx >= len(children):
            await callback.message.edit_text("❌ Ошибка: ребёнок не найден")
            return
        
        # Получаем одноклассников с таймаутом
        import asyncio
        try:
            classmates = await asyncio.wait_for(
                get_classmates_for_child(user_config.login, user_config.password, idx),
                timeout=25
            )
        except asyncio.TimeoutError:
            await callback.message.edit_text("⏱ Превышено время ожидания. Попробуйте позже.")
            return
        
        if not classmates:
            await callback.message.edit_text("ℹ️ Одноклассники не найдены.")
            return
        
        # Добавляем текущего ребенка в список, если его там нет
        current_child = children[idx]
        child_as_classmate = type('Classmate', (), {
            'last_name': current_child.last_name,
            'first_name': current_child.first_name,
            'middle_name': current_child.middle_name,
            'birth_date': current_child.birth_date,
            'gender': current_child.gender,
            'full_name': current_child.full_name,
            'gender_icon': current_child.gender_icon
        })()
        
        child_in_list = any(c.last_name == child_as_classmate.last_name and 
                            c.first_name == child_as_classmate.first_name 
                            for c in classmates)
        if not child_in_list:
            classmates.append(child_as_classmate)
        
        classmates_sorted = sorted(classmates, key=lambda c: c.last_name)
        
        from datetime import datetime
        
        # Формируем таблицу с увеличенной шириной для ФИО
        lines = [f"👥 <b>Классный список</b> — {children[idx].full_name} ({len(classmates_sorted)} чел.):\n"]
        lines.append("<pre>№   Фамилия Имя Отчество                    | Д.р.      | Возр")
        lines.append("─" * 62)
        
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
                bd_str = "—"
                age = "—"
            
            name_display = c.full_name[:40].ljust(40)
            icon = c.gender_icon
            
            lines.append(f"{i:2}. {name_display} {icon} | {bd_str:10} | {age}")
        
        lines.append("─" * 62)
        lines.append("</pre>")
        
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3997] + "..."
        
        await callback.message.edit_text(text)
        
    except Exception as e:
        logger.error(f"Error in cb_classmates_select: {e}")
        try:
            await callback.message.edit_text(f"❌ Ошибка: {e}")
        except:
            pass


@router.callback_query(F.data.startswith("info_teachers_"))
async def cb_teachers_select(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await callback.answer("❌ Ошибка авторизации", show_alert=True)
        return
    
    await callback.answer()
    
    try:
        idx = int(callback.data.split("_")[-1])
        await callback.message.edit_text("🔄 Загрузка учителей...")
        
        children = await get_children_async(user_config.login, user_config.password)
        
        if not children or idx >= len(children):
            await callback.message.edit_text("❌ Ошибка: ребёнок не найден")
            return
        
        import asyncio
        try:
            guide = await asyncio.wait_for(
                get_guide_for_child(user_config.login, user_config.password, idx),
                timeout=25
            )
        except asyncio.TimeoutError:
            await callback.message.edit_text("⏱ Превышено время ожидания. Попробуйте позже.")
            return
        
        if not guide.teachers:
            await callback.message.edit_text("ℹ️ Учителя не найдены.")
            return
        
        # Фильтруем только учителей с предметами (предметники)
        subject_teachers = [t for t in guide.teachers if t.subject]
        
        lines = [f"👩‍🏫 <b>Учителя</b> — {children[idx].full_name}\n"]
        lines.append(f"<b>Школа:</b> {guide.name}")
        if guide.phone:
            lines.append(f"<b>Телефон:</b> {guide.phone}")
        if guide.url:
            lines.append(f"<b>Сайт:</b> {guide.url}")
        lines.append("")
        
        if subject_teachers:
            # Разбиваем учителей с несколькими предметами на отдельные записи
            teacher_subject_pairs = []
            for t in subject_teachers:
                # Разбиваем строку предметов по запятой
                subjects = [s.strip() for s in t.subject.split(",") if s.strip()]
                for subject in subjects:
                    teacher_subject_pairs.append((subject, t.name))
            
            # Сортируем по предмету
            teacher_subject_pairs.sort(key=lambda x: x[0])
            
            lines.append("<pre>Предмет                    | Учитель")
            lines.append("─" * 50)
            for subject, name in teacher_subject_pairs:
                subject_display = subject[:25].ljust(25)
                lines.append(f"{subject_display} | {name}")
            lines.append("─" * 50)
            lines.append("</pre>")
        else:
            lines.append("Предметники не найдены.")
        
        await callback.message.edit_text("\n".join(lines))
        
    except Exception as e:
        logger.error(f"Error in cb_teachers_select: {e}")
        try:
            await callback.message.edit_text(f"❌ Ошибка: {e}")
        except:
            pass


@router.callback_query(F.data.startswith("info_achievements_"))
async def cb_achievements_select(callback: CallbackQuery, user_config: Optional[UserConfig] = None):
    if user_config is None or not user_config.login:
        await callback.answer("❌ Ошибка авторизации", show_alert=True)
        return
    
    await callback.answer()
    
    try:
        idx = int(callback.data.split("_")[-1])
        await callback.message.edit_text("🔄 Загрузка достижений...")
        
        children = await get_children_async(user_config.login, user_config.password)
        
        if not children or idx >= len(children):
            await callback.message.edit_text("❌ Ошибка: ребёнок не найден")
            return
        
        import asyncio
        try:
            achievements = await asyncio.wait_for(
                get_achievements_for_child(user_config.login, user_config.password, idx),
                timeout=25
            )
        except asyncio.TimeoutError:
            await callback.message.edit_text("⏱ Превышено время ожидания. Попробуйте позже.")
            return
        
        lines = [f"🏆 <b>Достижения</b> — {children[idx].full_name}\n"]
        
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
        
        await callback.message.edit_text("\n".join(lines))
        
    except Exception as e:
        logger.error(f"Error in cb_achievements_select: {e}")
        try:
            await callback.message.edit_text(f"❌ Ошибка: {e}")
        except:
            pass


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
