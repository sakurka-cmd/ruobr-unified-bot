"""VK Bot message handlers and FSM logic."""
import asyncio
import json
import logging
from datetime import date, timedelta, datetime

from vkbottle import Keyboard, KeyboardButtonColor, Text
from vkbottle.bot import Message

from bot.config import config
from bot.database import (
    get_user, get_all_thresholds_for_chat, create_or_update_user,
    create_link_code, consume_link_code, link_accounts, unlink_channel,
    get_birthday_settings, set_birthday_settings, get_all_birthday_settings,
    save_vk_fsm_state, get_vk_fsm_state, clear_vk_fsm_state,
    get_user_by_id, set_child_threshold as db_set_threshold, get_all_enabled_users,
)
from bot.credentials import safe_decrypt
from bot.services import (
    get_children_async, get_food_for_children, get_timetable_for_children,
    AuthenticationError, get_classmates_for_child, get_achievements_for_child,
    get_certificate_for_child, get_guide_for_child,
)
from bot.utils.formatters import (
    format_balance, format_food_visit, format_date, format_lesson, format_mark, normalize_date_to_iso,
    format_weekday, truncate_text, clean_html_text, has_meaningful_text,
    extract_homework_files,
)

from .keyboards import (
    get_vk_main_keyboard, get_vk_settings_keyboard,
    get_vk_notifications_keyboard, get_vk_info_keyboard,
    get_vk_birthday_child_keyboard, VK_BD_WEEKDAY_NAMES,
)

logger = logging.getLogger(__name__)


# ===== VK Helpers =====


async def _vk_show_classmates(message, login, password, child_idx, child):
    try:
        classmates = await get_classmates_for_child(login, password, child_idx)
        if not classmates:
            await message.answer("ℹ️ Одноклассники не найдены.")
            return
        # child может быть dict (из JSON) или Child объектом — единообразно через атрибуты
        child_ln = child.last_name if hasattr(child, 'last_name') else child.get('last_name', '')
        child_fn = child.first_name if hasattr(child, 'first_name') else child.get('first_name', '')
        child_mn = child.middle_name if hasattr(child, 'middle_name') else child.get('middle_name', '')
        child_bd = child.birth_date if hasattr(child, 'birth_date') else child.get('birth_date', None)
        child_gender = child.gender if hasattr(child, 'gender') else child.get('gender', None)
        child_full = child.full_name if hasattr(child, 'full_name') else child.get('full_name', child_ln + ' ' + child_fn)
        child_icon = child.gender_icon if hasattr(child, 'gender_icon') else child.get('gender_icon', '♂')
        child_as_classmate = type('Classmate', (), {
            'last_name': child_ln,
            'first_name': child_fn,
            'middle_name': child_mn,
            'birth_date': child_bd,
            'gender': child_gender,
            'full_name': child_full,
            'gender_icon': child_icon
        })()
        child_in_list = any(c.last_name == child_as_classmate.last_name and
                            c.first_name == child_as_classmate.first_name
                            for c in classmates)
        if not child_in_list:
            classmates.append(child_as_classmate)
        classmates_sorted = sorted(classmates, key=lambda c: c.last_name)
        lines = [f"👥 Классный список — {child_full} ({len(classmates_sorted)} чел.):\n"]
        for i, c in enumerate(classmates_sorted, 1):
            if c.birth_date:
                try:
                    bd = datetime.strptime(c.birth_date, "%Y-%m-%d")
                    bd_str = bd.strftime("%d.%m.%Y")
                    age = datetime.now().year - bd.year
                    if (datetime.now().month, datetime.now().day) < (bd.month, bd.day):
                        age -= 1
                except (ValueError, TypeError, KeyError):
                    bd_str = c.birth_date
                    age = "?"
            else:
                bd_str = "—"
                age = "—"
            icon = c.gender_icon
            lines.append(f"{i:2}. {c.full_name} {icon} | {bd_str} | {age} лет")
        await message.answer(truncate_text("\n".join(lines)))
    except Exception as e:
        logger.error(f"VK handler error: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")


async def _vk_show_teachers(message, login, password, child_idx, child):
    try:
        guide = await get_guide_for_child(login, password, child_idx)
        if not guide or not guide.teachers:
            await message.answer("ℹ️ Учителя не найдены.")
            return
        child_full = child.full_name if hasattr(child, 'full_name') else child.get('full_name', '')
        subject_teachers = [t for t in guide.teachers if t.subject]
        lines = [f"👩\u200d🏫 Учителя — {child_full}\n"]
        lines.append(f"Школа: {guide.name}")
        if guide.phone:
            lines.append(f"Телефон: {guide.phone}")
        lines.append("")
        if subject_teachers:
            teacher_subject_pairs = []
            for t in subject_teachers:
                subjects = [s.strip() for s in t.subject.split(",") if s.strip()]
                for subject in subjects:
                    teacher_subject_pairs.append((subject, t.name))
            teacher_subject_pairs.sort(key=lambda x: x[0])
            for subject, name in teacher_subject_pairs:
                lines.append(f"  {subject} — {name}")
        else:
            lines.append("Предметники не найдены.")
        await message.answer(truncate_text("\n".join(lines)))
    except Exception as e:
        logger.error(f"VK handler error: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")


async def _vk_show_achievements(message, login, password, child_idx, child):
    try:
        achievements, certificate = await asyncio.gather(
            get_achievements_for_child(login, password, child_idx),
            get_certificate_for_child(login, password, child_idx),
            return_exceptions=True
        )
        if isinstance(achievements, Exception):
            achievements = None
        if isinstance(certificate, Exception):
            certificate = None

        child_full = child.full_name if hasattr(child, 'full_name') else child.get('full_name', '')
        lines = [f"🎓 Дополнительное образование — {child_full}"]
        if not certificate:
            lines.append("\nДанных о дополнительном образовании пока нет.")
            await message.answer("\n".join(lines))
            return

        active = certificate.programs_active
        completed = certificate.programs_completed
        if not active and not completed:
            lines.append("\nДанных о дополнительном образовании пока нет.")
            await message.answer("\n".join(lines))
            return

        if active:
            for p in active:
                parts = [f"  • {p.name}"]
                if p.org:
                    parts.append(f"    🏢 {p.org}")
                if p.sum:
                    parts.append(f"    💵 {p.sum} руб.")
                lines.append("\n".join(parts))

        if completed:
            lines.append(f"\n📜 Прошлые программы ({len(completed)}):")
            for p in completed:
                parts = [f"  • {p.name}"]
                if p.org:
                    parts.append(f"    🏢 {p.org}")
                lines.append("\n".join(parts))

        if certificate.number:
            lines.append(f"\n💳 Сертификат ПФДО")
            lines.append(f"  Номер: {certificate.number}")
            if certificate.nominal:
                lines.append(f"  Номинал: {certificate.nominal} руб.")
            if certificate.balance:
                lines.append(f"  Остаток: {certificate.balance} руб.")

        await message.answer(truncate_text("\n".join(lines)))
    except Exception as e:
        logger.error(f"VK handler error: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")


# ===== VK Handler Registration =====

def register_handlers(vk_labeler):
    """Register all VK message handlers on the given labeler."""

    @vk_labeler.message(text="/start")
    async def vk_start(message: Message):
        user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
        is_auth = user and user.login
        text = ("👋 Школьный бот — ВК версия\n\n")
        if not is_auth:
            text += "⚠️ Настройте учётные данные: /set_login\n\n"
        else:
            text += "✅ Учётные данные настроены.\n\n"
        text += "📖 /set_login — логин/пароль\n/balance — баланс питания\n"
        text += "🔗 /link_tg — привязать Telegram"
        await message.answer(text, keyboard=get_vk_main_keyboard())

    @vk_labeler.message(text="/set_login")
    async def vk_set_login(message: Message):
        await save_vk_fsm_state(message.peer_id, "waiting_for_login")
        await message.answer("🔐 Введите логин от cabinet.ruobr.ru:")

    @vk_labeler.message(text="/link_tg")
    async def vk_link_tg(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.id:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        code = await create_link_code(user.id, source="vk")
        await message.answer(
            f"🔗 Привязка Telegram\n\n"
            f"Отправьте этот код боту в Telegram:\n\n"
            f"/link_vk {code}\n\n"
            f"⏰ Код действителен 10 минут."
        )

    @vk_labeler.message(text="/unlink_tg")
    async def vk_unlink_tg(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.id:
            return
        await unlink_channel(user.id, "tg")
        await message.answer("✅ Telegram отвязан.")

    @vk_labeler.message(text="👤 Мой профиль")
    async def vk_profile(message: Message):
        user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
        if not user:
            await message.answer("❌ Ошибка создания профиля. Попробуйте /start")
            return
        status = "✅ Настроен" if user.login and user.password_encrypted else "❌ Не настроен"
        tg_linked = user.chat_id is not None
        if tg_linked:
            tg_info = f"  📱 Telegram: ✅ привязан (id: {user.chat_id})"
        else:
            tg_info = "  📱 Telegram: ❌ не привязан"
        text = (
            f"👤 Ваш профиль\n\n"
            f"Статус: {status}\n"
            f"Логин: {user.login or 'не указан'}\n\n"
            f"🔗 Связанные аккаунты:\n"
            f"  💬 VK: ✅\n"
            f"{tg_info}\n\n"
        )
        if not tg_linked:
            text += (
                "💡 Для привязки Telegram:\n"
                "• /link_tg — получить код для отправки в TG бот\n"
                "• Или в TG боте нажмите 'Привязать VK' и отправьте код сюда"
            )
        else:
            text += "💡 /unlink_tg — отвязать Telegram"
        await message.answer(text)

    @vk_labeler.message(text="ℹ️ Информация")
    async def vk_info(message: Message):
        await message.answer("ℹ️ Информация\n\nВыберите что хотите узнать:", keyboard=get_vk_info_keyboard())

    @vk_labeler.message(text="⚙️ Настройки")
    async def vk_settings(message: Message):
        await message.answer("⚙️ Настройки", keyboard=get_vk_settings_keyboard())

    @vk_labeler.message(text="◀️ Назад")
    async def vk_back(message: Message):
        await clear_vk_fsm_state(message.peer_id)
        await message.answer("🏠 Главное меню", keyboard=get_vk_main_keyboard())

    @vk_labeler.message(text="/balance")
    async def vk_balance(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        if not children:
            await message.answer("❌ Дети не найдены.")
            return
        try:
            food_info = await get_food_for_children(login, password, children)
            thresholds = await get_all_thresholds_for_chat(peer_id=message.peer_id)
            lines = ["💰 Баланс питания\n"]
            for idx, child in enumerate(children, 1):
                info = food_info.get(child.id)
                threshold = thresholds.get(child.id, config.default_balance_threshold)
                if info and info.has_food:
                    lines.append(f"{idx}. {format_balance(child, info.balance, threshold)}")
                else:
                    lines.append(f"{idx}. {child.full_name} ({child.group}): питание недоступно")
            await message.answer("\n".join(lines))
        except Exception as e:
            logger.error(f"VK handler error: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    @vk_labeler.message(text="📅 Расписание сегодня")
    async def vk_ttoday(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        try:
            today = date.today()
            timetable = await get_timetable_for_children(login, password, children, today, today)
            lines = [f"📅 Расписание на сегодня ({format_weekday(today)})"]
            found = False
            for child in children:
                lessons = timetable.get(child.id, [])
                if lessons:
                    found = True
                    lines.append(f"\n👦 {child.full_name} ({child.group}):")
                    for lesson in lessons:
                        lines.append(format_lesson(lesson, show_details=True))
            await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На сегодня расписания нет.")
        except Exception as e:
            logger.error(f"VK handler error: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    @vk_labeler.message(text="📅 Расписание завтра")
    async def vk_ttomorrow(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        try:
            tomorrow = date.today() + timedelta(days=1)
            timetable = await get_timetable_for_children(login, password, children, tomorrow, tomorrow)
            lines = [f"📅 Расписание на завтра ({format_weekday(tomorrow)})"]
            found = False
            for child in children:
                lessons = timetable.get(child.id, [])
                if lessons:
                    found = True
                    lines.append(f"\n👦 {child.full_name} ({child.group}):")
                    for lesson in lessons:
                        lines.append(format_lesson(lesson, show_details=True))
            await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На завтра расписания нет.")
        except Exception as e:
            logger.error(f"VK handler error: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    @vk_labeler.message(text="🍽 Питание сегодня")
    async def vk_food(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        try:
            today_str = date.today().strftime("%Y-%m-%d")
            food_info = await get_food_for_children(login, password, children)
            lines = [f"🍽 Питание сегодня ({format_date(today_str)})"]
            found = False
            for child in children:
                info = food_info.get(child.id)
                if info and info.visits:
                    for visit in info.visits:
                        if normalize_date_to_iso(visit.get("date", "")) == today_str and (visit.get("ordered") or visit.get("state") == 30):
                            found = True
                            lines.append(format_food_visit(visit, child.full_name))
            await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На сегодня питания не найдено.")
        except Exception as e:
            logger.error(f"VK handler error: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    # ===== Оценки сегодня =====
    @vk_labeler.message(text="⭐ Оценки сегодня")
    async def vk_marks_today(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        try:
            today = date.today()
            today_str = today.strftime("%Y-%m-%d")
            timetable = await get_timetable_for_children(login, password, children, today, today)
            lines = [f"⭐ Оценки за сегодня ({format_date(today_str)})"]
            found = False
            for child in children:
                lessons = timetable.get(child.id, [])
                child_header_added = False
                for lesson in lessons:
                    if not lesson.marks:
                        continue
                    if not child_header_added:
                        lines.append(f"\n👦 {child.full_name} ({child.group}):")
                        child_header_added = True
                    for mark in lesson.marks:
                        found = True
                        question_type = mark.get("question_type", "") or mark.get("question_name", "")
                        value = mark.get("mark", "")
                        lines.append(f"  {lesson.subject}: {question_type} → {value}")
            await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ За сегодня оценок не найдено.")
        except Exception as e:
            logger.error(f"VK handler error: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    # ===== ДЗ на завтра =====
    @vk_labeler.message(text="📘 ДЗ на завтра")
    async def vk_hw_tomorrow(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        try:
            today = date.today()
            tomorrow = today + timedelta(days=1)
            tomorrow_str = tomorrow.strftime("%Y-%m-%d")
            end = today + timedelta(days=14)
            timetable = await get_timetable_for_children(login, password, children, today, end)
            lines = [f"📘 Домашнее задание на завтра ({format_date(tomorrow_str)})"]
            found = False
            for child in children:
                lessons = timetable.get(child.id, [])
                child_header_added = False
                for lesson in lessons:
                    relevant_hw = []
                    for hw in lesson.homework:
                        hw_deadline = normalize_date_to_iso(hw.get("deadline", ""))
                        if hw_deadline and hw_deadline == tomorrow_str:
                            relevant_hw.append(hw)
                        elif not hw_deadline and lesson.date == tomorrow_str:
                            relevant_hw.append(hw)
                    if not relevant_hw:
                        continue
                    found = True
                    if not child_header_added:
                        lines.append(f"\n👦 {child.full_name} ({child.group}):")
                        child_header_added = True
                    for hw in relevant_hw:
                        title = hw.get("title", "")
                        lines.append(f"  📖 {lesson.subject}: {title}")
                        hw_text = hw.get("text", "")
                        if has_meaningful_text(hw_text):
                            clean_text = clean_html_text(hw_text)
                            if len(clean_text) > 500:
                                clean_text = clean_text[:497] + "..."
                            lines.append(f"     📝 {clean_text}")
            await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На завтра домашнее задание не найдено.")
        except Exception as e:
            logger.error(f"VK handler error: {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка. Попробуйте позже.")

    # ===== Информация: Одноклассники =====
    @vk_labeler.message(text="👥 Одноклассники")
    async def vk_classmates(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        if not children:
            await message.answer("❌ Дети не найдены.")
            return
        if len(children) == 1:
            await _vk_show_classmates(message, login, password, 0, children[0])
            return
        # Multiple children — show selection
        children_data = [{"id": c.id, "idx": i, "name": c.full_name, "group": c.group} for i, c in enumerate(children)]
        k = Keyboard(one_time=False, inline=False)
        for cd in children_data:
            k.add(Text(f"👤 {cd['name']} ({cd['group']})"), color=KeyboardButtonColor.PRIMARY)
            k.row()
        k.add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
        await save_vk_fsm_state(message.peer_id, "info_select_classmates", data=json.dumps(children_data))
        await message.answer("👦👧 Выберите ребёнка:", keyboard=k.get_json())

    # ===== Информация: Учителя =====
    @vk_labeler.message(text="👩\u200d🏫 Учителя")
    async def vk_teachers(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        if not children:
            await message.answer("❌ Дети не найдены.")
            return
        if len(children) == 1:
            await _vk_show_teachers(message, login, password, 0, children[0])
            return
        # Multiple children — show selection
        children_data = [{"id": c.id, "idx": i, "name": c.full_name, "group": c.group} for i, c in enumerate(children)]
        k = Keyboard(one_time=False, inline=False)
        for cd in children_data:
            k.add(Text(f"👤 {cd['name']} ({cd['group']})"), color=KeyboardButtonColor.PRIMARY)
            k.row()
        k.add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
        await save_vk_fsm_state(message.peer_id, "info_select_teachers", data=json.dumps(children_data))
        await message.answer("👦👧 Выберите ребёнка:", keyboard=k.get_json())

    # ===== Информация: Доп. образование =====
    @vk_labeler.message(text="🎓 Доп. образование")
    async def vk_achievements(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        if not children:
            await message.answer("❌ Дети не найдены.")
            return
        if len(children) == 1:
            await _vk_show_achievements(message, login, password, 0, children[0])
            return
        # Multiple children — show selection
        children_data = [{"id": c.id, "idx": i, "name": c.full_name, "group": c.group} for i, c in enumerate(children)]
        k = Keyboard(one_time=False, inline=False)
        for cd in children_data:
            k.add(Text(f"👤 {cd['name']} ({cd['group']})"), color=KeyboardButtonColor.PRIMARY)
            k.row()
        k.add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
        await save_vk_fsm_state(message.peer_id, "info_select_achievements", data=json.dumps(children_data))
        await message.answer("👦👧 Выберите ребёнка:", keyboard=k.get_json())

    # ===== Справка =====
    @vk_labeler.message(text="📋 Справка")
    async def vk_help(message: Message):
        help_text = (
            "📋 Справка по боту\n\n"
            "Школьный бот — помогает родителям следить за учёбой детей.\n\n"
            "📅 Расписание:\n"
            "• «Расписание сегодня» — уроки на сегодня\n"
            "• «Расписание завтра» — уроки на завтра\n\n"
            "📘 Домашние задания:\n"
            "• «ДЗ на завтра» — задания на завтрашний день\n\n"
            "⭐ Оценки:\n"
            "• «Оценки сегодня» — оценки за сегодняшний день\n\n"
            "🍽 Питание:\n"
            "• «Баланс питания» — текущий баланс счёта\n"
            "• «Питание сегодня» — что ребёнок ел сегодня\n\n"
            "ℹ️ Информация:\n"
            "• «Одноклассники» — список класса\n"
            "• «Учителя» — предметники и контакты школы\n"
            "• «Доп. образование» — программы доп. образования\n\n"
            "⚙️ Настройки:\n"
            "• «Уведомления» — включить/выключить оповещения\n"
            "• «Мой профиль» — информация об аккаунте\n\n"
            "📝 Команды:\n"
            "/start — главное меню\n"
            "/set_login — настроить учётные данные\n"
            "/balance — баланс питания\n\n"
            "💡 Подсказка: Бот автоматически уведомляет о:\n"
            "• Низком балансе питания\n"
            "• Новых оценках\n\n"
            "🔗 Полезные ссылки:\n"
            "• cabinet.ruobr.ru — электронный дневник"
        )
        await message.answer(help_text)

    @vk_labeler.message(text="🔔 Уведомления")
    async def vk_notifications(message: Message):
        user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
        await message.answer(
            "🔔 Настройки уведомлений\n\nНажмите для переключения:",
            keyboard=get_vk_notifications_keyboard(user)
        )

    @vk_labeler.message(text="🔑 Изменить логин/пароль")
    async def vk_change_login(message: Message):
        await save_vk_fsm_state(message.peer_id, "waiting_for_login")
        await message.answer("🔐 Введите логин от cabinet.ruobr.ru:\n\n❌ Отмена — для выхода")

    @vk_labeler.message(text="💰 Порог баланса")
    async def vk_threshold(message: Message):
        user = await get_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        if not children:
            await message.answer("❌ Дети не найдены.")
            return
        thresholds = await get_all_thresholds_for_chat(peer_id=message.peer_id)
        lines = ["💰 Настройка порога баланса\n"]
        for idx, child in enumerate(children, 1):
            threshold = thresholds.get(child.id, 300)
            lines.append(f"{idx}. {child.full_name} ({child.group}) — порог {threshold:.0f} ₽")
        lines.append(f"\nВыберите ребёнка:")

        children_data = []
        k = Keyboard(one_time=False, inline=False)
        for i, child in enumerate(children):
            btn_text = f"👤 {child.full_name} ({child.group})"
            k.add(Text(btn_text), color=KeyboardButtonColor.PRIMARY)
            k.row()
            children_data.append({
                "id": child.id,
                "idx": i,
                "name": child.full_name,
                "group": child.group,
            })
        k.add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)

        await save_vk_fsm_state(message.peer_id, "waiting_threshold_child", data=json.dumps(children_data))
        await message.answer("\n".join(lines), keyboard=k.get_json())

    # ===== Дни рождения (FSM) =====
    @vk_labeler.message(text="🎂 Дни рождения")
    async def vk_birthday(message: Message):
        user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
        if not user or not user.login:
            await message.answer("❌ Сначала настройте логин/пароль: /set_login")
            return
        login, password = safe_decrypt(user)
        try:
            children = await get_children_async(login, password)
        except Exception:
            await message.answer("❌ Ошибка авторизации.")
            return
        if not children:
            await message.answer("❌ Дети не найдены.")
            return

        global_status = "✅ ВКЛ" if getattr(user, 'vk_birthday_enabled', False) else "❌ ВЫКЛ"
        all_settings = await get_all_birthday_settings(user.id)
        settings_map = {s["child_id"]: s for s in all_settings}

        lines = [f"🎂 Уведомления о днях рождения\n\nГлобальное: {global_status}\n"]
        for i, child in enumerate(children, 1):
            cs = settings_map.get(child.id)
            if cs and cs.get("enabled"):
                mode = cs.get("mode", "tomorrow")
                h = cs.get("notify_hour", 7)
                m = cs.get("notify_minute", 0)
                if mode == "weekly":
                    wd = cs.get("notify_weekday", 1)
                    wd_name = VK_BD_WEEKDAY_NAMES[wd].split(" — ")[1] if 0 <= wd <= 6 else "?"
                    desc = f"Еженедельно ({wd_name}, {h:02d}:{m:02d})"
                else:
                    desc = f"Ежедневно ({h:02d}:{m:02d})"
                lines.append(f"{i}. {child.full_name}: ✅ {desc}")
            else:
                lines.append(f"{i}. {child.full_name}: ❌ выкл")

        lines.append(f"\nВыберите ребёнка для настройки:")

        # Build keyboard with child buttons
        children_data = []
        k = Keyboard(one_time=False, inline=False)
        for i, child in enumerate(children):
            btn_text = f"👤 {child.full_name} ({child.group})"
            k.add(Text(btn_text), color=KeyboardButtonColor.PRIMARY)
            k.row()
            children_data.append({
                "id": child.id,
                "idx": i,
                "name": child.full_name,
                "group": child.group,
            })
        k.add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)

        await save_vk_fsm_state(message.peer_id, "bd_choose_child", data=json.dumps(children_data))
        await message.answer("\n".join(lines), keyboard=k.get_json())

    # FSM: ввод кода привязки от TG
    @vk_labeler.message(text="/enter_code")
    async def vk_enter_link_code_start(message: Message):
        await save_vk_fsm_state(message.peer_id, "waiting_for_link_code")
        await message.answer(
            "📲 Ввод кода привязки от Telegram\n\n"
            "Отправьте код, полученный в TG боте:\n\n"
            "❌ Отмена — для выхода"
        )

    # FSM: ввод логина/пароля / авто-приём кода привязки
    @vk_labeler.message()
    async def vk_handle_all(message: Message):
        text = (message.text or "").strip()
        if not text:
            return

        # Авто-приём кода привязки (8 символов, без FSM)
        if len(text) == 8 and text.isalnum():
            result = await consume_link_code(text.upper())
            if result is not None:
                tg_user_id, source = result
                if source == "tg":
                    tg_user = await get_user_by_id(tg_user_id)
                    if tg_user and tg_user.chat_id:
                        # Merge: add VK peer_id to TG user's record (has login)
                        if tg_user.id:
                            await create_or_update_user(chat_id=tg_user.chat_id, peer_id=message.peer_id)
                        await message.answer(
                            "✅ Telegram аккаунт привязан!\n\n"
                            "Теперь уведомления будут приходить и в Telegram.",
                            keyboard=get_vk_main_keyboard()
                        )
                        return
                await message.answer("❌ Не удалось привязать. Попробуйте ещё раз.")
            # Не 8-значный код — идём дальше в FSM

        # Переключатели уведомлений (кнопки с динамическим текстом)
        if text.startswith("💰 Баланс:"):
            user = await get_user(peer_id=message.peer_id)
            new_val = not user.vk_balance_enabled if user else True
            user = await create_or_update_user(peer_id=message.peer_id, vk_balance_enabled=new_val)
            await message.answer(
                f"💰 Уведомления о балансе: {'включены ✅' if new_val else 'выключены ❌'}",
                keyboard=get_vk_notifications_keyboard(user)
            )
            return

        if text.startswith("⭐ Оценки:"):
            user = await get_user(peer_id=message.peer_id)
            new_val = not user.vk_marks_enabled if user else True
            user = await create_or_update_user(peer_id=message.peer_id, vk_marks_enabled=new_val)
            await message.answer(
                f"⭐ Уведомления об оценках: {'включены ✅' if new_val else 'выключены ❌'}",
                keyboard=get_vk_notifications_keyboard(user)
            )
            return

        if text.startswith("🍽 Питание:"):
            user = await get_user(peer_id=message.peer_id)
            new_val = not user.vk_food_enabled if user else True
            user = await create_or_update_user(peer_id=message.peer_id, vk_food_enabled=new_val)
            await message.answer(
                f"🍽 Уведомления о питании: {'включены ✅' if new_val else 'выключены ❌'}",
                keyboard=get_vk_notifications_keyboard(user)
            )
            return

        if text.startswith("🎂 Дни рождения:"):
            user = await get_user(peer_id=message.peer_id)
            new_val = not getattr(user, 'vk_birthday_enabled', False) if user else True
            user = await create_or_update_user(peer_id=message.peer_id, vk_birthday_enabled=new_val)
            await message.answer(
                f"🎂 Уведомления о днях рождения: {'включены ✅' if new_val else 'выключены ❌'}",
                keyboard=get_vk_notifications_keyboard(user)
            )
            return

        state = await get_vk_fsm_state(message.peer_id)
        if not state:
            return

        state_name = state["state"]

        if text in ("/cancel", "❌ Отмена"):
            await clear_vk_fsm_state(message.peer_id)
            await message.answer("❌ Отменено.", keyboard=get_vk_main_keyboard())
            return

        if state_name == "waiting_threshold_child":
            children_data = json.loads(state.get("data", "[]"))
            # Find which child button was pressed (точное или частичное сопоставление)
            selected = None
            for cd in children_data:
                if text == f"👤 {cd['name']} ({cd['group']})":
                    selected = cd
                    break
            if not selected:
                for cd in children_data:
                    if cd['name'] in text and cd['group'] in text:
                        selected = cd
                        break
            if not selected:
                await message.answer("❌ Выберите ребёнка из списка ниже.")
                return
            child_id = selected["id"]
            child_idx = selected["idx"]
            await save_vk_fsm_state(message.peer_id, "waiting_threshold_value", data=json.dumps(selected))
            await message.answer("Введите новый порог (число, например: 300):")

        elif state_name == "waiting_threshold_value":
            try:
                value = float(text.strip().replace(",", "."))
            except ValueError:
                await message.answer("❌ Введите число (например: 300).")
                return
            if value < 0:
                await message.answer("❌ Порог не может быть отрицательным.")
                return
            if value > 10000:
                await message.answer("❌ Порог слишком большой (максимум 10 000 ₽).")
                return
            selected = json.loads(state.get("data", "{}"))
            child_id = selected.get("id")
            child_name = selected.get("name", "Ребёнок")
            child_group = selected.get("group", "")
            if not child_id:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка. Попробуйте заново.", keyboard=get_vk_main_keyboard())
                return
            await db_set_threshold(message.peer_id, child_id, value)
            await clear_vk_fsm_state(message.peer_id)
            await message.answer(
                f"✅ Порог установлен!\n\n"
                f"{child_name} ({child_group}): {value:.0f} ₽\n\n"
                f"Вы будете получать уведомления, когда баланс упадёт ниже этого значения.",
                keyboard=get_vk_settings_keyboard()
            )

        elif state_name == "info_select_classmates":
            children_data = json.loads(state.get("data", "[]"))
            # Надёжное сопоставление: точное или частичное (VK обрезает >40 символов)
            selected = None
            for cd in children_data:
                if text == f"👤 {cd['name']} ({cd['group']})":
                    selected = cd
                    break
            if not selected:
                for cd in children_data:
                    if cd['name'] in text and cd['group'] in text:
                        selected = cd
                        break
            if not selected:
                await message.answer("❌ Выберите ребёнка из списка ниже.")
                return
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка авторизации.")
                return
            login, password = safe_decrypt(user)
            # Перезапрашиваем детей для получения реального Child объекта
            try:
                real_children = await get_children_async(login, password)
                child_obj = real_children[selected["idx"]] if real_children and selected["idx"] < len(real_children) else selected
            except Exception:
                child_obj = selected
            await _vk_show_classmates(message, login, password, selected["idx"], child_obj)

        elif state_name == "info_select_teachers":
            children_data = json.loads(state.get("data", "[]"))
            selected = None
            for cd in children_data:
                if text == f"👤 {cd['name']} ({cd['group']})":
                    selected = cd
                    break
            if not selected:
                for cd in children_data:
                    if cd['name'] in text and cd['group'] in text:
                        selected = cd
                        break
            if not selected:
                await message.answer("❌ Выберите ребёнка из списка ниже.")
                return
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка авторизации.")
                return
            login, password = safe_decrypt(user)
            try:
                real_children = await get_children_async(login, password)
                child_obj = real_children[selected["idx"]] if real_children and selected["idx"] < len(real_children) else selected
            except Exception:
                child_obj = selected
            await _vk_show_teachers(message, login, password, selected["idx"], child_obj)

        elif state_name == "info_select_achievements":
            children_data = json.loads(state.get("data", "[]"))
            selected = None
            for cd in children_data:
                if text == f"👤 {cd['name']} ({cd['group']})":
                    selected = cd
                    break
            if not selected:
                for cd in children_data:
                    if cd['name'] in text and cd['group'] in text:
                        selected = cd
                        break
            if not selected:
                await message.answer("❌ Выберите ребёнка из списка ниже.")
                return
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка авторизации.")
                return
            login, password = safe_decrypt(user)
            try:
                real_children = await get_children_async(login, password)
                child_obj = real_children[selected["idx"]] if real_children and selected["idx"] < len(real_children) else selected
            except Exception:
                child_obj = selected
            await _vk_show_achievements(message, login, password, selected["idx"], child_obj)

        elif state_name == "bd_choose_child":
            children_data = json.loads(state.get("data", "[]"))
            # Find which child button was pressed (точное или частичное сопоставление)
            selected = None
            for cd in children_data:
                if text == f"👤 {cd['name']} ({cd['group']})":
                    selected = cd
                    break
            if not selected:
                for cd in children_data:
                    if cd['name'] in text and cd['group'] in text:
                        selected = cd
                        break
            if not selected:
                await message.answer("❌ Выберите ребёнка из списка ниже.")
                return
            child_id = selected["id"]
            child_idx = selected["idx"]
            child_name = selected["name"]
            child_group = selected["group"]
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка. Попробуйте /set_login", keyboard=get_vk_settings_keyboard())
                return
            settings = await get_birthday_settings(user.id, child_id)
            is_enabled = settings.get("enabled", False)
            mode = settings.get("mode", "tomorrow")
            h = settings.get("notify_hour", 7)
            m = settings.get("notify_minute", 0)
            if mode == "weekly":
                wd = settings.get("notify_weekday", 1)
                wd_name = VK_BD_WEEKDAY_NAMES[wd].split(" — ")[1] if 0 <= wd <= 6 else "?"
                mode_desc = f"Еженедельно ({wd_name}, {h:02d}:{m:02d})"
            else:
                mode_desc = f"Ежедневно ({h:02d}:{m:02d})"
            status = "✅ Включено" if is_enabled else "❌ Выключено"
            lines = [
                f"👦 {child_name} ({child_group})",
                f"Статус: {status}",
                f"Режим: {mode_desc}",
                "",
                "Нажмите кнопку ниже или введите число:",
                "1 — Включить/выключить",
                "2 — Режим: завтра",
                "3 — Режим: еженедельно",
                "4 — Изменить время",
            ]
            await save_vk_fsm_state(message.peer_id, "bd_child_menu",
                data=f"{child_id}|{child_idx}|{child_name}|{child_group}")
            await message.answer("\n".join(lines), keyboard=get_vk_birthday_child_keyboard(is_enabled, mode_desc))

        elif state_name == "bd_child_menu":
            parts_data = state.get("data", "").split("|")
            child_id = int(parts_data[0])
            child_idx = int(parts_data[1])
            child_name = parts_data[2] if len(parts_data) > 2 else "Ребёнок"
            child_group = parts_data[3] if len(parts_data) > 3 else ""
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            settings = await get_birthday_settings(user.id,
                child_id
            )
            is_enabled = settings.get("enabled", False)
            if text in ("1", "🟢 Включить/выключить", "🔴 Включить/выключить"):
                new_enabled = not is_enabled
                await set_birthday_settings(
                    user_id=user.id,
                    child_id=child_id,
                    enabled=new_enabled,
                    mode=settings.get("mode", "tomorrow"),
                    notify_weekday=settings.get("notify_weekday", 1),
                    notify_hour=settings.get("notify_hour", 7),
                    notify_minute=settings.get("notify_minute", 0),
                )
                # Если включили — включаем глобально
                if new_enabled:
                    if not getattr(user, 'vk_birthday_enabled', False):
                        user = await create_or_update_user(peer_id=message.peer_id, vk_birthday_enabled=True)
                status = "✅ Включено" if new_enabled else "❌ Выключено"
                await message.answer(f"{'✅ Уведомления включены!' if new_enabled else '❌ Уведомления выключены.'}")
                # Показываем обновлённое меню ребёнка (значения известны после set)
                is_e = new_enabled
                mode = settings.get("mode", "tomorrow")
                h = settings.get("notify_hour", 7)
                m = settings.get("notify_minute", 0)
                if mode == "weekly":
                    wd = settings.get("notify_weekday", 1)
                    wd_name = VK_BD_WEEKDAY_NAMES[wd].split(" — ")[1] if 0 <= wd <= 6 else "?"
                    mode_desc = f"Еженедельно ({wd_name}, {h:02d}:{m:02d})"
                else:
                    mode_desc = f"Ежедневно ({h:02d}:{m:02d})"
                lines = [
                    f"👦 {child_name} ({child_group})" if child_group else f"👦 {child_name}",
                    f"Статус: {'✅ Включено' if is_e else '❌ Выключено'}",
                    f"Режим: {mode_desc}",
                ]
                await message.answer("\n".join(lines), keyboard=get_vk_birthday_child_keyboard(is_e, mode_desc))
            elif text in ("2", "📅 Режим: завтра"):
                await set_birthday_settings(
                    user_id=user.id,
                    child_id=child_id,
                    enabled=True,
                    mode="tomorrow",
                    notify_weekday=settings.get("notify_weekday", 1),
                    notify_hour=settings.get("notify_hour", 7),
                    notify_minute=settings.get("notify_minute", 0),
                )
                if not getattr(user, 'vk_birthday_enabled', False):
                    await create_or_update_user(peer_id=message.peer_id, vk_birthday_enabled=True)
                await message.answer("✅ Режим установлен: уведомлять за день до ДР")
            elif text in ("3", "📋 Режим: еженедельно"):
                lines = [
                    "📅 Выберите день недели:\n",
                    "0 — Понедельник",
                    "1 — Вторник",
                    "2 — Среда",
                    "3 — Четверг",
                    "4 — Пятница",
                    "5 — Суббота",
                    "6 — Воскресенье",
                ]
                await save_vk_fsm_state(message.peer_id, "bd_set_weekday",
                    data=f"{child_id}|{child_idx}|{child_name}|{child_group}")
                await message.answer("\n".join(lines))
            elif text in ("4", "⏰ Изменить время"):
                lines = [
                    "⏰ Введите час уведомления (6-21):",
                    "Пример: 7 для 07:00",
                ]
                await save_vk_fsm_state(message.peer_id, "bd_set_hour",
                    data=f"{child_id}|{child_idx}|{child_name}|{child_group}")
                await message.answer("\n".join(lines))
            elif text == "◀️ Назад к списку":
                # Go back to birthday menu with child buttons
                if not user or not user.login:
                    await clear_vk_fsm_state(message.peer_id)
                    await message.answer("❌ Ошибка. Попробуйте /set_login", keyboard=get_vk_settings_keyboard())
                    return
                login, password = safe_decrypt(user)
                try:
                    children = await get_children_async(login, password)
                except Exception:
                    await clear_vk_fsm_state(message.peer_id)
                    await message.answer("❌ Ошибка авторизации.", keyboard=get_vk_settings_keyboard())
                    return
                if not children:
                    await clear_vk_fsm_state(message.peer_id)
                    await message.answer("❌ Дети не найдены.", keyboard=get_vk_settings_keyboard())
                    return
                global_status = "✅ ВКЛ" if getattr(user, 'vk_birthday_enabled', False) else "❌ ВЫКЛ"
                all_settings = await get_all_birthday_settings(user.id)
                settings_map = {s["child_id"]: s for s in all_settings}
                lines = [f"🎂 Уведомления о днях рождения\n\nГлобальное: {global_status}\n"]
                for i, child in enumerate(children, 1):
                    cs = settings_map.get(child.id)
                    if cs and cs.get("enabled"):
                        mode = cs.get("mode", "tomorrow")
                        h = cs.get("notify_hour", 7)
                        m = cs.get("notify_minute", 0)
                        if mode == "weekly":
                            wd = cs.get("notify_weekday", 1)
                            wd_name = VK_BD_WEEKDAY_NAMES[wd].split(" — ")[1] if 0 <= wd <= 6 else "?"
                            desc = f"Еженедельно ({wd_name}, {h:02d}:{m:02d})"
                        else:
                            desc = f"Ежедневно ({h:02d}:{m:02d})"
                        lines.append(f"{i}. {child.full_name}: ✅ {desc}")
                    else:
                        lines.append(f"{i}. {child.full_name}: ❌ выкл")
                lines.append(f"\nВыберите ребёнка для настройки:")
                bd_children_data = []
                k = Keyboard(one_time=False, inline=False)
                for i, child in enumerate(children):
                    btn_text = f"👤 {child.full_name} ({child.group})"
                    k.add(Text(btn_text), color=KeyboardButtonColor.PRIMARY)
                    k.row()
                    bd_children_data.append({
                        "id": child.id,
                        "idx": i,
                        "name": child.full_name,
                        "group": child.group,
                    })
                k.add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
                await save_vk_fsm_state(message.peer_id, "bd_choose_child", data=json.dumps(bd_children_data))
                await message.answer("\n".join(lines), keyboard=k.get_json())
            else:
                await message.answer("❌ Неизвестная команда. Введите число 1-4:")

        elif state_name == "bd_set_weekday":
            parts_data = state.get("data", "").split("|")
            child_id = int(parts_data[0])
            child_idx = int(parts_data[1])
            child_name = parts_data[2]
            child_group = parts_data[3] if len(parts_data) > 3 else ""
            try:
                weekday = int(text.strip())
            except ValueError:
                await message.answer("❌ Введите число от 0 (Пн) до 6 (Вс):")
                return
            if weekday < 0 or weekday > 6:
                await message.answer("❌ Введите число от 0 до 6:")
                return
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            settings = await get_birthday_settings(user.id, child_id)
            await set_birthday_settings(
                user_id=user.id,
                child_id=child_id,
                enabled=True,
                mode="weekly",
                notify_weekday=weekday,
                notify_hour=settings.get("notify_hour", 7),
                notify_minute=settings.get("notify_minute", 0),
            )
            if not getattr(user, 'vk_birthday_enabled', False):
                user = await create_or_update_user(peer_id=message.peer_id, vk_birthday_enabled=True)
            wd_name = VK_BD_WEEKDAY_NAMES[weekday].split(" — ")[1]
            await message.answer(f"✅ День недели: {wd_name}\n\nТеперь введите час уведомления (6-21):")
            await save_vk_fsm_state(message.peer_id, "bd_set_hour",
                data=f"{child_id}|{child_idx}|{child_name}|{child_group}")

        elif state_name == "bd_set_hour":
            parts_data = state.get("data", "").split("|")
            child_id = int(parts_data[0])
            child_idx = int(parts_data[1])
            child_name = parts_data[2]
            child_group = parts_data[3] if len(parts_data) > 3 else ""
            try:
                hour = int(text.strip())
            except ValueError:
                await message.answer("❌ Введите число от 6 до 21:")
                return
            if hour < 6 or hour > 21:
                await message.answer("❌ Час должен быть от 6 до 21:")
                return
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            settings = await get_birthday_settings(user.id, child_id)
            await set_birthday_settings(
                user_id=user.id,
                child_id=child_id,
                enabled=True,
                mode=settings.get("mode", "tomorrow"),
                notify_weekday=settings.get("notify_weekday", 1),
                notify_hour=hour,
                notify_minute=settings.get("notify_minute", 0),
            )
            await message.answer(
                f"⏰ Введите минуты:\n\n"
                f"0 — :00\n15 — :15\n30 — :30\n45 — :45"
            )
            await save_vk_fsm_state(message.peer_id, "bd_set_minute",
                data=f"{child_id}|{child_idx}|{child_name}|{child_group}|{hour}")

        elif state_name == "bd_set_minute":
            parts_data = state.get("data", "").split("|")
            child_id = int(parts_data[0])
            child_idx = int(parts_data[1])
            child_name = parts_data[2]
            child_group = parts_data[3] if len(parts_data) > 3 else ""
            hour = int(parts_data[4])
            try:
                minute = int(text.strip())
            except ValueError:
                await message.answer("❌ Введите 0, 15, 30 или 45:")
                return
            if minute not in (0, 15, 30, 45):
                await message.answer("❌ Введите 0, 15, 30 или 45:")
                return
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            settings = await get_birthday_settings(user.id, child_id)
            await set_birthday_settings(
                user_id=user.id,
                child_id=child_id,
                enabled=True,
                mode=settings.get("mode", "tomorrow"),
                notify_weekday=settings.get("notify_weekday", 1),
                notify_hour=hour,
                notify_minute=minute,
            )
            mode = settings.get("mode", "tomorrow")
            time_str = f"{hour:02d}:{minute:02d}"
            if mode == "weekly":
                wd = settings.get("notify_weekday", 1)
                wd_name = VK_BD_WEEKDAY_NAMES[wd].split(" — ")[1] if 0 <= wd <= 6 else "?"
                desc = f"Еженедельно ({wd_name}, {time_str})"
            else:
                desc = f"Ежедневно ({time_str})"
            await clear_vk_fsm_state(message.peer_id)
            await message.answer(
                f"✅ Настройки сохранены!\n\n"
                f"👦 {child_name} ({child_group})\n"
                f"Режим: {desc}",
                keyboard=get_vk_settings_keyboard()
            )

        elif state_name == "waiting_for_link_code":
            code = text.upper().strip()
            if len(code) != 8:
                await message.answer("❌ Код должен содержать 8 символов. Попробуйте ещё раз:")
                return
            result = await consume_link_code(code)
            if result is None:
                await message.answer(
                    "❌ Неверный или просроченный код.\n\n"
                    "Получите новый код в TG боте и попробуйте снова."
                )
                return
            tg_user_id, source = result
            if source != "tg":
                await message.answer("❌ Этот код предназначен для TG бота, а не для VK.")
                return
            tg_user = await get_user_by_id(tg_user_id)
            if not tg_user or not tg_user.chat_id:
                await message.answer("⚠️ TG аккаунт найден, но привязка не удалась.")
                return
            # Merge: add VK peer_id to TG user's record (has login)
            if tg_user.id:
                await create_or_update_user(chat_id=tg_user.chat_id, peer_id=message.peer_id)
            else:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка привязки.")
                return
            await clear_vk_fsm_state(message.peer_id)
            await message.answer(
                "✅ Telegram аккаунт привязан!\n\n"
                "Теперь уведомления будут приходить и в Telegram.",
                keyboard=get_vk_main_keyboard()
            )
            return

        if state_name == "waiting_for_login":
            if len(text) > 100:
                await message.answer("❌ Логин слишком длинный.")
                return
            await save_vk_fsm_state(message.peer_id, "waiting_for_password", data=text)
            await message.answer("✅ Логин сохранён.\nВведите пароль:")

        elif state_name == "waiting_for_password":
            payload = state.get("data") or ""
            try:
                children = await get_children_async(payload, text)
            except AuthenticationError:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Неверный логин или пароль. Попробуйте: /set_login")
                return
            except Exception:
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Ошибка соединения.")
                return

            if not children:
                await message.answer("⚠️ Данные верны, но дети не найдены.")
            else:
                names = "\n".join([f"  • {c.full_name} ({c.group})" for c in children])
                await message.answer(f"✅ Авторизация успешна!\n\nДети:\n{names}")

            await create_or_update_user(peer_id=message.peer_id, login=payload, password=text)
            await clear_vk_fsm_state(message.peer_id)
            await message.answer("🏠 Готово!", keyboard=get_vk_main_keyboard())


# ===== VK Bot Factory =====

async def run_vk_bot(vk_token: str):
    """Запуск VK бота (опционально)."""
    try:
        from vkbottle import Bot as VKBot, VKAPIError

        vk_bot = VKBot(token=vk_token)
        vk_labeler = vk_bot.labeler
        logger.info("VK Bot initialized")

        register_handlers(vk_labeler)

        logger.info("VK Bot handlers registered")
        return vk_bot

    except ImportError as e:
        logger.warning(f"VK bot not available: {e}")
        return None
