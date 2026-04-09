"""
Фоновые задачи для уведомлений.
Единый сервис для Telegram и VK.
Группирует пользователей по (login, password) — один API-вызов на группу.
Отправляет в TG и/или VK согласно индивидуальным настройкам канала.
"""
import asyncio
import random
import time
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

from ..config import config
from ..credentials import safe_decrypt
from ..services.cache import birthday_settings_cache
from ..database import (
    get_all_enabled_users,
    get_all_thresholds_for_chat,
    is_notification_sent,
    mark_notification_sent,
    cleanup_old_notifications,
    get_users_with_birthday_notifications,
    get_birthday_settings,
    get_user_by_id,
    UserConfig,
)
from . import (
    get_children_async,
    get_food_for_children,
    get_timetable_for_children,
    get_classmates_for_child,
    Child,
    FoodInfo
)
from ..utils.formatters import truncate_text, extract_dish_names, parse_complex_menu

logger = logging.getLogger(__name__)

# Часовой пояс Новосибирск (GMT+7) — для расписания
TZ = timezone(timedelta(hours=7))


def normalize_date(date_str: str) -> str:
    """
    Нормализация строки даты в формат YYYY-MM-DD.
    """
    if not date_str:
        return ""

    date_str = str(date_str).strip()

    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str

    if len(date_str) > 10:
        for sep in ['T', ' ']:
            if sep in date_str:
                date_str = date_str.split(sep)[0]
                break
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str

    formats = [
        "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"Could not normalize date: '{date_str}'")
    return date_str


def extract_price(visit: Dict) -> float:
    """Извлечение цены из визита с поддержкой разных ключей и форматов."""
    price_candidates = [
        visit.get("price_sum"), visit.get("price"), visit.get("sum"),
        visit.get("total"), visit.get("amount"), visit.get("cost"),
    ]
    for raw in price_candidates:
        if raw is not None and str(raw).strip():
            price_str = str(raw).strip().replace(",", ".").replace(" ", "")
            cleaned = ""
            for ch in price_str:
                if ch.isdigit() or ch == '.' or ch == '-':
                    cleaned += ch
                elif ch and not cleaned:
                    continue
            if cleaned:
                try:
                    return float(cleaned)
                except ValueError:
                    continue
    return 0.0


class DailySchedule:
    """Кешированное дневное расписание для умного поллинга."""
    __slots__ = ('date', 'school_start', 'school_end')

    def __init__(self, date_str: str, school_start: Optional[int], school_end: Optional[int]):
        self.date = date_str
        self.school_start = school_start
        self.school_end = school_end


class NotificationService:
    """
    Сервис фоновых уведомлений.
    Единый для Telegram и VK.
    """

    MARKS_CHECK_DAYS = 14
    MARKS_CHECK_INTERVAL_SCHOOL = 900    # 15 min — во время уроков
    MARKS_CHECK_INTERVAL_SLOW = 3600     # 60 min — вне уроков
    FOOD_CHECK_INTERVAL_FAST = 1800      # 30 min — 11:00–19:59
    FOOD_CHECK_INTERVAL_SLOW = 3600      # 60 min — 20:00–23:59
    _JITTER_RANGE = 300  # +-5 min per-user stagger

    def __init__(self, bot: Bot, vk_api=None):
        self._bot = bot
        self._vk_api = vk_api
        self._running = False
        self._prev_balances: Dict[int, Dict[int, float]] = {}
        self._last_balance_check: Dict[int, float] = {}
        self._last_food_check: Dict[int, float] = {}
        self._last_marks_check: Dict[int, float] = {}
        self._user_jitter: Dict[int, int] = {}
        self._last_birthday_check_hour: Dict[Tuple[str, int], int] = {}
        self._daily_schedule: Dict[int, DailySchedule] = {}
        self._first_run = True

    async def start(self) -> None:
        """Запуск фонового мониторинга."""
        self._running = True
        self._first_run = True
        logger.info("Notification service started (unified TG+VK)")

        try:
            await self._init_marks_baseline()
        except Exception as e:
            logger.error(f"Error initializing marks baseline: {e}")

        self._first_run = False

        while self._running:
            try:
                await self._check_all_users()
            except Exception as e:
                logger.error(f"Error in notification loop: {e}", exc_info=True)

            await cleanup_old_notifications(days=30)
            await asyncio.sleep(config.check_interval_seconds)

    def stop(self) -> None:
        self._running = False
        logger.info("Notification service stopped")

    # ===== Отправка уведомлений =====

    async def _send_tg(self, chat_id: int, text: str) -> None:
        """Отправить уведомление в Telegram."""
        try:
            await self._bot.send_message(chat_id, text)
            logger.info(f"TG notification sent to {chat_id}")
        except TelegramAPIError as e:
            logger.error(f"Failed to send TG notification to {chat_id}: {e}")
            if "blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
                from ..database import create_or_update_user
                await create_or_update_user(
                    chat_id=chat_id,
                    enabled=False, marks_enabled=False,
                    food_enabled=False, birthday_enabled=False
                )
                logger.info(f"Disabled TG notifications for blocked user {chat_id}")

    async def _send_vk(self, peer_id: int, text: str) -> None:
        """Отправить уведомление в VK."""
        if self._vk_api is None:
            return
        try:
            # Убираем HTML-теги для VK
            import re
            clean = re.sub(r'<[^>]+>', '', text)
            await self._vk_api.messages.send(
                peer_id=peer_id,
                message=clean,
                random_id=random.randint(1, 2**31)
            )
            logger.info(f"VK notification sent to {peer_id}")
        except Exception as e:
            logger.error(f"Failed to send VK notification to {peer_id}: {e}")
            if "blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
                from ..database import create_or_update_user
                await create_or_update_user(
                    peer_id=peer_id,
                    vk_balance_enabled=False, vk_marks_enabled=False,
                    vk_food_enabled=False, vk_birthday_enabled=False
                )
                logger.info(f"Disabled VK notifications for blocked user {peer_id}")

    async def _send_to_user(self, user: UserConfig, text: str, channel: str = "both") -> None:
        """Отправить уведомление пользователю по доступным каналам."""
        if channel in ("both", "tg") and user.chat_id:
            await self._send_tg(user.chat_id, text)
        if channel in ("both", "vk") and user.peer_id:
            await self._send_vk(user.peer_id, text)

    # ===== Базовый flow =====

    async def _check_all_users(self) -> None:
        """Проверка всех пользователей — группировка по логину."""
        users = await get_all_enabled_users()
        if not users:
            logger.debug("No users with enabled notifications")
            return

        logger.info(f"Checking notifications for {len(users)} users")

        # Группируем по (login, password_encrypted)
        groups: Dict[Tuple[str, str], List[UserConfig]] = {}
        for user in users:
            if not user.login or not user.password_encrypted:
                continue
            key = (user.login, user.password_encrypted)
            groups.setdefault(key, []).append(user)

        if not groups:
            return

        logger.info(f"Grouped into {len(groups)} credential group(s)")

        semaphore = asyncio.Semaphore(5)

        async def process_group(group_users: List[UserConfig]):
            async with semaphore:
                try:
                    await self._process_group(group_users)
                except Exception as e:
                    logger.error(f"Error processing group: {e}")

        tasks = [process_group(g) for g in groups.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ===== Утилиты =====

    def _get_jitter(self, user_id: int) -> int:
        if user_id not in self._user_jitter:
            self._user_jitter[user_id] = random.randint(0, self._JITTER_RANGE)
        return self._user_jitter[user_id]

    def _parse_time_minutes(self, time_str: str) -> Optional[int]:
        if not time_str:
            return None
        parts = time_str.split(":")
        if len(parts) >= 2:
            try:
                return int(parts[0]) * 60 + int(parts[1])
            except ValueError:
                return None
        return None

    async def _ensure_daily_schedule(self, user_id: int, login: str, password: str, children: List[Child]) -> None:
        today_str = date.today().strftime("%Y-%m-%d")
        cached = self._daily_schedule.get(user_id)
        if cached and cached.date == today_str:
            return
        try:
            timetable = await get_timetable_for_children(login, password, children, date.today(), date.today())
            earliest: Optional[int] = None
            latest: Optional[int] = None
            for child in children:
                for lesson in timetable.get(child.id, []):
                    s = self._parse_time_minutes(lesson.time_start)
                    e = self._parse_time_minutes(lesson.time_end)
                    if s is not None and (earliest is None or s < earliest):
                        earliest = s
                    if e is not None and (latest is None or e > latest):
                        latest = e
            if earliest is not None and latest is not None:
                schedule = DailySchedule(today_str, earliest, latest + 120)
                logger.info(f"Schedule for user {user_id}: school {earliest//60:02d}:{earliest%60:02d}–{(latest+120)//60:02d}:{(latest+120)%60:02d}")
            else:
                schedule = DailySchedule(today_str, None, None)
                logger.info(f"Schedule for user {user_id}: no lessons today")
            self._daily_schedule[user_id] = schedule
        except Exception as e:
            logger.warning(f"Failed to fetch schedule for user {user_id}: {e}")
            self._daily_schedule[user_id] = DailySchedule(today_str, None, None)

    def _get_marks_interval(self, user_id: int, now_minutes: int) -> int:
        schedule = self._daily_schedule.get(user_id)
        if schedule and schedule.school_start is not None:
            if schedule.school_start <= now_minutes <= schedule.school_end:
                return self.MARKS_CHECK_INTERVAL_SCHOOL
        return self.MARKS_CHECK_INTERVAL_SLOW

    @staticmethod
    def _get_food_interval(hour: int) -> Optional[int]:
        if 11 <= hour <= 19:
            return 1800
        elif 20 <= hour <= 23:
            return 3600
        return None

    # ===== Обработка группы пользователей =====

    async def _process_group(self, users: List[UserConfig]) -> None:
        """Обработка группы пользователей с одинаковыми учётными данными Ruobr."""
        rep = users[0]  # representative
        login, password = safe_decrypt(rep)
        if not login or not password:
            return

        try:
            children = await get_children_async(login, password)
        except Exception as e:
            logger.warning(f"Failed to get children for group: {e}")
            return
        if not children:
            return

        now_tz = datetime.now(TZ)
        now = time.time()
        now_minutes = now_tz.hour * 60 + now_tz.minute
        hour = now_tz.hour

        # Расписание — кешируем по первому юзеру в группе (расписание общее)
        rep_id = rep.id if rep.id else rep.chat_id
        await self._ensure_daily_schedule(rep_id, login, password, children)

        marks_interval = self._get_marks_interval(rep_id, now_minutes)
        food_interval = self._get_food_interval(hour)

        # Проверяем, нужен ли вызов food API
        need_food = False
        for u in users:
            j = self._get_jitter(u.id if u.id else u.chat_id)
            # TG настройки
            if u.chat_id:
                if u.enabled and (now - self._last_balance_check.get(u.id, 0) >= (food_interval or 99999) + j):
                    need_food = True
                if u.food_enabled and (now - self._last_food_check.get(u.id, 0) >= (food_interval or 99999) + j):
                    need_food = True
            # VK настройки
            if u.peer_id:
                if u.vk_balance_enabled and (now - self._last_balance_check.get(u.id, 0) >= (food_interval or 99999) + j):
                    need_food = True
                if u.vk_food_enabled and (now - self._last_food_check.get(u.id, 0) >= (food_interval or 99999) + j):
                    need_food = True

        food_info: Dict[int, FoodInfo] = {}
        if need_food and food_interval:
            try:
                food_info = await get_food_for_children(login, password, children)
            except Exception as e:
                logger.error(f"Error fetching food for group: {e}")

        thresholds = {}
        for u in users:
            uid = u.id
            if uid and uid not in thresholds:
                thresholds[uid] = await get_all_thresholds_for_chat(user_id=uid)

        # Баланс
        if food_info:
            for u in users:
                uid = u.id
                j = self._get_jitter(uid)
                last_check = self._last_balance_check.get(uid, 0)
                interval = food_interval or 99999
                if now - last_check >= interval + j:
                    self._last_balance_check[uid] = now
                    await self._check_balance(u, children, food_info, thresholds.get(uid, {}))

        # Оценки
        need_marks = False
        for u in users:
            uid = u.id
            j = self._get_jitter(uid)
            last_check = self._last_marks_check.get(uid, 0)
            if now - last_check >= marks_interval + j:
                need_marks = True
                break

        if need_marks:
            try:
                timetable = await get_timetable_for_children(
                    login, password, children,
                    date.today() - timedelta(days=self.MARKS_CHECK_DAYS),
                    date.today()
                )
                for u in users:
                    uid = u.id
                    j = self._get_jitter(uid)
                    last_check = self._last_marks_check.get(uid, 0)
                    if now - last_check >= marks_interval + j:
                        self._last_marks_check[uid] = now
                        await self._check_marks(u, children, timetable)
            except Exception as e:
                logger.error(f"Error fetching marks for group: {e}")

        # Питание
        if food_info:
            for u in users:
                uid = u.id
                j = self._get_jitter(uid)
                last_check = self._last_food_check.get(uid, 0)
                interval = food_interval or 99999
                if now - last_check >= interval + j:
                    self._last_food_check[uid] = now
                    await self._check_food(u, children, food_info)

        # Дни рождения
        for u in users:
            if u.birthday_enabled or u.vk_birthday_enabled:
                await self._check_birthday(u, children, login, password)

    # ===== Проверка баланса =====

    async def _check_balance(self, user: UserConfig, children: List[Child], food_info: Dict[int, FoodInfo], thresholds: Dict[int, float]) -> None:
        uid = user.id
        try:
            alerts = []
            new_balances: Dict[int, float] = {}

            for child in children:
                info = food_info.get(child.id)
                if not info:
                    new_balances[child.id] = 0.0
                    continue
                balance = info.balance
                new_balances[child.id] = balance
                threshold = thresholds.get(child.id, config.default_balance_threshold)
                prev_balance = self._prev_balances.get(uid, {}).get(child.id)

                if balance < threshold:
                    if prev_balance is None or prev_balance >= threshold:
                        notif_key = f"low_balance:{child.id}:{int(balance)}"
                        # Проверяем TG
                        tg_sent = False
                        if user.chat_id and user.enabled:
                            if not await is_notification_sent(user_id=uid, notification_type="balance", notification_key=notif_key, channel="tg"):
                                tg_sent = True
                        # Проверяем VK
                        vk_sent = False
                        if user.peer_id and user.vk_balance_enabled:
                            if not await is_notification_sent(user_id=uid, notification_type="balance", notification_key=notif_key, channel="vk"):
                                vk_sent = True
                        if not tg_sent and not vk_sent:
                            continue

                        alert = (
                            f"⚠️ {child.full_name} ({child.group}):\n"
                            f"  💰 Баланс: <b>{balance:.0f} ₽</b>\n"
                            f"  📉 Порог: {threshold:.0f} ₽\n"
                            f"  ❗ Необходимо пополнить счёт!"
                        )
                        alerts.append(alert)
                        if tg_sent:
                            await mark_notification_sent(user_id=uid, notification_type="balance", notification_key=notif_key, channel="tg")
                        if vk_sent:
                            await mark_notification_sent(user_id=uid, notification_type="balance", notification_key=notif_key, channel="vk")

            self._prev_balances[uid] = new_balances

            if alerts:
                text = "⚠️ <b>Низкий баланс питания!</b>\n\n" + "\n\n".join(alerts)
                await self._send_to_user(user, text)

        except Exception as e:
            logger.error(f"Error checking balance for user {uid}: {e}")

    # ===== Проверка оценок =====

    async def _check_marks(self, user: UserConfig, children: List[Child], timetable: Dict[int, list]) -> None:
        uid = user.id
        try:
            all_marks: List[dict] = []
            for child in children:
                lessons = timetable.get(child.id, [])
                for lesson in lessons:
                    for mark in lesson.marks:
                        all_marks.append({
                            "child_name": child.full_name,
                            "child_group": child.group,
                            "date": lesson.date,
                            "subject": lesson.subject,
                            "question_type": mark.get("question_type") or mark.get("question_name"),
                            "value": mark.get("mark"),
                            "question_id": mark.get("question_id")
                        })

            new_marks = []
            for m in all_marks:
                notif_key = f"{m['date']}|{m['subject']}|{m['question_id']}|{m['value']}"
                # TG
                if user.chat_id and user.marks_enabled:
                    if not await is_notification_sent(user_id=uid, notification_type="mark", notification_key=notif_key, channel="tg"):
                        new_marks.append(m)
                        await mark_notification_sent(user_id=uid, notification_type="mark", notification_key=notif_key, channel="tg")
                # VK
                if user.peer_id and user.vk_marks_enabled:
                    if not await is_notification_sent(user_id=uid, notification_type="mark", notification_key=notif_key, channel="vk"):
                        new_marks.append(m)
                        await mark_notification_sent(user_id=uid, notification_type="mark", notification_key=notif_key, channel="vk")

            if new_marks:
                lines = ["⭐ <b>Новые оценки!</b>\n"]
                for m in new_marks:
                    lines.append(
                        f"👤 {m['child_name']} ({m['child_group']})\n"
                        f"📚 {m['subject']}: {m['question_type']} → <b>{m['value']}</b>\n"
                        f"📅 {m['date']}"
                    )
                text = truncate_text("\n".join(lines))
                await self._send_to_user(user, text)

        except Exception as e:
            logger.error(f"Error checking marks for user {uid}: {e}")

    # ===== Проверка питания =====

    async def _check_food(self, user: UserConfig, children: List[Child], food_info: Dict[int, FoodInfo]) -> None:
        uid = user.id
        try:
            today_str = date.today().strftime("%Y-%m-%d")
            logger.info(f"Food check for user {uid}, date={today_str}, children={len(children)}")
            alerts = []

            for child in children:
                info = food_info.get(child.id)
                if not info or not info.visits:
                    continue

                for visit_idx, visit in enumerate(info.visits):
                    if not isinstance(visit, dict):
                        continue
                    raw_date = visit.get("date", "")
                    visit_date = normalize_date(raw_date)
                    if visit_date and visit_date != today_str:
                        continue

                    ordered = visit.get("ordered")
                    state = visit.get("state")
                    dishes = visit.get("dishes", [])

                    has_meal = False
                    if state == 30:
                        has_meal = True
                    elif ordered and str(ordered) in ("1", "True"):
                        if dishes:
                            dish_names = extract_dish_names(dishes)
                            if dish_names:
                                has_meal = True

                    if not has_meal:
                        continue

                    line = visit.get("line", visit.get("line_id", 0))
                    time_start = visit.get("time_start", visit.get("time", ""))
                    visit_key = f"food:{child.id}:{visit_date}:{line}:{time_start}"

                    # TG
                    tg_new = False
                    if user.chat_id and user.food_enabled:
                        if not await is_notification_sent(user_id=uid, notification_type="food", notification_key=visit_key, channel="tg"):
                            tg_new = True
                    # VK
                    vk_new = False
                    if user.peer_id and user.vk_food_enabled:
                        if not await is_notification_sent(user_id=uid, notification_type="food", notification_key=visit_key, channel="vk"):
                            vk_new = True
                    if not tg_new and not vk_new:
                        continue

                    complex_name = visit.get("complex", "")
                    meal_type = complex_name or visit.get("line_name") or visit.get("type_name") or "Питание"
                    price = extract_price(visit)
                    dish_names = extract_dish_names(dishes)
                    if not dish_names:
                        dish_names = parse_complex_menu(visit.get("qs_unit", []))

                    msg_lines = [f"🍽 <b>{child.full_name}</b> ({child.group})"]
                    msg_lines.append(f"🕐 {meal_type}")
                    if dish_names:
                        msg_lines.append("📋 <b>Меню:</b>")
                        for dish in dish_names:
                            msg_lines.append(f"  • {dish}")
                    msg_lines.append(f"💰 Списано: <b>{price:.0f} ₽</b>")
                    alerts.append("\n".join(msg_lines))

                    if tg_new:
                        await mark_notification_sent(user_id=uid, notification_type="food", notification_key=visit_key, channel="tg")
                    if vk_new:
                        await mark_notification_sent(user_id=uid, notification_type="food", notification_key=visit_key, channel="vk")

            if alerts:
                text = f"🍽 <b>Ребёнок поел!</b> ({today_str})\n\n" + "\n\n".join(alerts)
                await self._send_to_user(user, text)

        except Exception as e:
            logger.error(f"Error checking food for user {uid}: {e}", exc_info=True)

    # ===== Дни рождения =====

    async def _check_birthday(self, user: UserConfig, children: List[Child], login: str, password: str) -> None:
        uid = user.id
        try:
            tz = timezone(timedelta(hours=7))
            now = datetime.now(tz)
            current_hour = now.hour

            # Проверяем отдельно TG и VK
            for channel, enabled in [("tg", user.birthday_enabled), ("vk", user.vk_birthday_enabled)]:
                if not enabled:
                    continue
                bh_key = (channel, uid)
                last_hour = self._last_birthday_check_hour.get(bh_key, -1)
                if last_hour == current_hour:
                    continue
                self._last_birthday_check_hour[bh_key] = current_hour

                # Получаем настройки ДР для этого канала
                bday_users = await get_users_with_birthday_notifications()
                relevant = [b for b in bday_users if b["user_id"] == uid and b["channel"] == channel]
                if not relevant:
                    continue

                for child_idx, child in enumerate(children):
                    bd_cache_key = f"bd_settings:{uid}:{child.id}"
                    settings = birthday_settings_cache.get(bd_cache_key)
                    if settings is None:
                        settings = await get_birthday_settings(uid, child.id)
                        birthday_settings_cache.set(bd_cache_key, settings)
                    if not settings.get("enabled", False):
                        continue

                    mode = settings.get("mode", "tomorrow")
                    notify_hour = settings.get("notify_hour", 7)
                    notify_minute = settings.get("notify_minute", 0)

                    if now.hour != notify_hour or now.minute < notify_minute:
                        continue
                    if now.minute > notify_minute + 2:
                        continue

                    if mode == "tomorrow":
                        await self._process_tomorrow_mode(user, child, child_idx, now, tz, login, password, channel)
                    elif mode == "weekly":
                        await self._process_weekly_mode(user, child, child_idx, now, tz, login, password, channel)

        except Exception as e:
            logger.error(f"Error checking birthday for user {uid}: {e}", exc_info=True)

    async def _process_tomorrow_mode(self, user, child, child_idx, now, tz, login, password, channel):
        tomorrow = (now + timedelta(days=1)).date()
        notif_key = f"birthday:{child.id}:{tomorrow.isoformat()}:{channel}"
        uid = user.id
        if await is_notification_sent(user_id=uid, notification_type="birthday", notification_key=notif_key, channel=channel):
            return
        try:
            classmates = await get_classmates_for_child(login, password, child_idx)
        except Exception as e:
            logger.warning(f"Failed to fetch classmates for child {child.id}: {e}")
            return

        birthday_kids = []
        for c in classmates:
            if not c.birth_date:
                continue
            try:
                bd = datetime.strptime(c.birth_date, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if (bd.month, bd.day) == (tomorrow.month, tomorrow.day):
                birthday_kids.append((c, tomorrow.year - bd.year))

        if not birthday_kids:
            return

        tomorrow_str = tomorrow.strftime("%d.%m")
        lines = [f"🎂 <b>Дни рождения одноклассников</b>\n"]
        lines.append(f"👦 <b>{child.full_name}</b> ({child.group}):\n")
        lines.append(f"Завтра ({tomorrow_str}):")
        for c, age in birthday_kids:
            lines.append(f"  🎁 {c.full_name} — {age} лет")
        text = "\n".join(lines)
        await self._send_to_user(user, text, channel=channel)
        await mark_notification_sent(user_id=uid, notification_type="birthday", notification_key=notif_key, channel=channel)

    async def _process_weekly_mode(self, user, child, child_idx, now, tz, login, password, channel):
        from ..database import get_birthday_settings
        notify_weekday = (await get_birthday_settings(user.id, child.id)).get("notify_weekday", 1)
        if now.weekday() != notify_weekday:
            return
        week_start = now.date()
        year_week = now.strftime("%Y-W%W")
        notif_key = f"birthday:{child.id}:{year_week}:{channel}"
        uid = user.id
        if await is_notification_sent(user_id=uid, notification_type="birthday", notification_key=notif_key, channel=channel):
            return
        try:
            classmates = await get_classmates_for_child(login, password, child_idx)
        except Exception as e:
            logger.warning(f"Failed to fetch classmates for child {child.id}: {e}")
            return

        birthday_by_date: Dict[date, List[tuple]] = {}
        for c in classmates:
            if not c.birth_date:
                continue
            try:
                bd = datetime.strptime(c.birth_date, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            for day_offset in range(7):
                check_date = week_start + timedelta(days=day_offset)
                if (bd.month, bd.day) == (check_date.month, check_date.day):
                    birthday_by_date.setdefault(check_date, []).append((c, check_date.year - bd.year))
                    break

        if not birthday_by_date:
            return

        lines = [f"🎂 <b>Дни рождения одноклассников</b>\n"]
        lines.append(f"👦 <b>{child.full_name}</b> ({child.group}):\n")
        for check_date in sorted(birthday_by_date.keys()):
            lines.append(f"  {check_date.strftime('%d.%m')}:")
            for c, age in birthday_by_date[check_date]:
                lines.append(f"    🎁 {c.full_name} — {age} лет")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3997] + "..."
        await self._send_to_user(user, text, channel=channel)
        await mark_notification_sent(user_id=uid, notification_type="birthday", notification_key=notif_key, channel=channel)

    # ===== Baseline (первый запуск) =====

    async def _init_marks_baseline(self) -> None:
        users = await get_all_enabled_users()
        if not users:
            return
        marked_count = 0
        for user in users:
            if not user.login or not user.password_encrypted:
                continue
            try:
                login, password = safe_decrypt(user)
                if not login:
                    continue
                children = await get_children_async(login, password)
                if not children:
                    continue
                today = date.today()
                start = today - timedelta(days=self.MARKS_CHECK_DAYS)
                timetable = await get_timetable_for_children(login, password, children, start, today)

                uid = user.id
                for child in children:
                    for lesson in timetable.get(child.id, []):
                        for mark in lesson.marks:
                            notif_key = f"{lesson.date}|{lesson.subject}|{mark.get('question_id')}|{mark.get('mark')}"
                            for ch in ("tg", "vk"):
                                if not await is_notification_sent(user_id=uid, notification_type="mark", notification_key=notif_key, channel=ch):
                                    await mark_notification_sent(user_id=uid, notification_type="mark", notification_key=notif_key, channel=ch)
                                    marked_count += 1

                food_info = await get_food_for_children(login, password, children)
                today_str = today.strftime("%Y-%m-%d")
                for child in children:
                    info = food_info.get(child.id)
                    if not info or not info.visits:
                        continue
                    for visit in info.visits:
                        if not isinstance(visit, dict):
                            continue
                        visit_date = normalize_date(visit.get("date", ""))
                        if visit_date != today_str:
                            continue
                        line = visit.get("line", visit.get("line_id", 0))
                        time_start = visit.get("time_start", visit.get("time", ""))
                        visit_key = f"food:{child.id}:{visit_date}:{line}:{time_start}"
                        for ch in ("tg", "vk"):
                            if not await is_notification_sent(user_id=uid, notification_type="food", notification_key=visit_key, channel=ch):
                                await mark_notification_sent(user_id=uid, notification_type="food", notification_key=visit_key, channel=ch)
                                marked_count += 1
            except Exception as e:
                logger.warning(f"Error initializing baseline for user {user.id}: {e}")
        logger.info(f"Baseline initialized: {marked_count} items marked as seen")
