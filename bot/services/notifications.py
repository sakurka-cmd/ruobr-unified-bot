"""
Фоновые задачи для уведомлений.
"""
import asyncio
import random
import time
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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


def normalize_date(date_str: str) -> str:
    """
    Нормализация строки даты в формат YYYY-MM-DD.
    Поддерживает: YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY, YYYY/MM/DD,
    ISO datetime (YYYY-MM-DDTHH:MM:SS), Unix timestamp.

    Args:
        date_str: Исходная строка даты.

    Returns:
        Дата в формате YYYY-MM-DD или исходная строка, если парсинг не удался.
    """
    if not date_str:
        return ""

    date_str = str(date_str).strip()

    # Уже в формате YYYY-MM-DD (ровно 10 символов)
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str

    # ISO datetime с временем: YYYY-MM-DDTHH:MM:SS или YYYY-MM-DD HH:MM:SS
    if len(date_str) > 10:
        # Берём только часть до T или пробела
        for sep in ['T', ' ']:
            if sep in date_str:
                date_str = date_str.split(sep)[0]
                break
        if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return date_str

    # Пробуем различные форматы
    formats = [
        "%d.%m.%Y",   # 02.04.2026
        "%d/%m/%Y",   # 02/04/2026
        "%Y/%m/%d",   # 2026/04/02
        "%d-%m-%Y",   # 02-04-2026
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Если ничего не подошло, возвращаем как есть (для диагностики)
    logger.warning(f"Could not normalize date: '{date_str}'")
    return date_str


def extract_price(visit: Dict) -> float:
    """
    Извлечение цены из визита с поддержкой разных ключей и форматов.

    Args:
        visit: Словарь с данными визита.

    Returns:
        Цена в виде float (0.0 если не удалось извлечь).
    """
    price_candidates = [
        visit.get("price_sum"),
        visit.get("price"),
        visit.get("sum"),
        visit.get("total"),
        visit.get("amount"),
        visit.get("cost"),
    ]

    for raw in price_candidates:
        if raw is not None and str(raw).strip():
            price_str = str(raw).strip().replace(",", ".").replace(" ", "")
            # Убираем нечисловые символы кроме точки и минуса
            cleaned = ""
            for ch in price_str:
                if ch.isdigit() or ch == '.' or ch == '-':
                    cleaned += ch
                elif ch and not cleaned:
                    # Пропускаем ведущие нечисловые символы (валюту и т.д.)
                    continue
            if cleaned:
                try:
                    return float(cleaned)
                except ValueError:
                    continue

    return 0.0




class NotificationService:
    """
    Сервис фоновых уведомлений.
    Отслеживает изменения баланса и новые оценки.
    """

    MARKS_CHECK_DAYS = 14
    MARKS_CHECK_INTERVAL = 900  # 15 minutes
    BALANCE_FOOD_CHECK_INTERVAL = 1800  # 30 minutes
    _JITTER_RANGE = 300  # +-5 min per-user stagger

    def __init__(self, bot: Bot):
        self._bot = bot
        self._running = False
        self._prev_balances: Dict[int, Dict[int, float]] = {}
        self._last_balance_check: Dict[int, float] = {}
        self._last_food_check: Dict[int, float] = {}
        self._last_marks_check: Dict[int, float] = {}
        self._user_jitter: Dict[int, int] = {}
        self._last_birthday_check_hour: Dict[int, int] = {}
        self._first_run = True  # Флаг первого запуска

    async def start(self) -> None:
        """Запуск фонового мониторинга."""
        self._running = True
        self._first_run = True
        logger.info("Notification service started")

        # На первом запуске — фиксируем текущие оценки без отправки уведомлений
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
        """Остановка мониторинга."""
        self._running = False
        logger.info("Notification service stopped")

    async def _init_marks_baseline(self) -> None:
        """
        При первом запуске фиксируем все текущие оценки как «уже виденные»,
        чтобы не отправлять при старте уведомления за прошлые дни.
        """
        users = await get_all_enabled_users()
        if not users:
            return

        marked_count = 0
        for user in users:
            if not user.login or not user.password_encrypted or not user.marks_enabled:
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

                timetable = await get_timetable_for_children(
                    login, password, children, start, today
                )

                for child in children:
                    lessons = timetable.get(child.id, [])
                    for lesson in lessons:
                        for mark in lesson.marks:
                            notif_key = f"{lesson.date}|{lesson.subject}|{mark.get('question_id')}|{mark.get('mark')}"
                            if not await is_notification_sent(user.chat_id, "mark", notif_key):
                                await mark_notification_sent(user.chat_id, "mark", notif_key)
                                marked_count += 1

                # Также фиксируем текущие визиты питания как «уже виденные»
                food_info = await get_food_for_children(login, password, children)
                today_str = today.strftime("%Y-%m-%d")
                for child in children:
                    info = food_info.get(child.id)
                    if not info or not info.visits:
                        continue
                    for visit in info.visits:
                        if not isinstance(visit, dict):
                            continue
                        raw_date = visit.get("date", "")
                        visit_date = normalize_date(raw_date)
                        if visit_date != today_str:
                            continue
                        line = visit.get("line", visit.get("line_id", 0))
                        time_start = visit.get("time_start", visit.get("time", ""))
                        visit_key = f"food:{child.id}:{visit_date}:{line}:{time_start}"
                        if not await is_notification_sent(user.chat_id, "food", visit_key):
                            await mark_notification_sent(user.chat_id, "food", visit_key)
                            marked_count += 1

            except Exception as e:
                logger.warning(f"Error initializing baseline for user {user.chat_id}: {e}")

        logger.info(f"Baseline initialized: {marked_count} existing items marked as seen (no notifications sent)")

    async def _check_all_users(self) -> None:
        """Проверка всех пользователей с включёнными уведомлениями."""
        users = await get_all_enabled_users()

        if not users:
            logger.debug("No users with enabled notifications")
            return

        logger.info(f"Checking notifications for {len(users)} users")

        semaphore = asyncio.Semaphore(5)

        async def process_with_limit(user: UserConfig):
            async with semaphore:
                try:
                    await self._process_user(user)
                except Exception as e:
                    logger.error(f"Error processing user {user.chat_id}: {e}")

        tasks = [process_with_limit(user) for user in users]
        await asyncio.gather(*tasks, return_exceptions=True)

    def _get_jitter(self, chat_id: int) -> int:
        """
        Период-пользовательский случайный сдвиг для разброски запросов.
        Фиксирован на всё жизнь процесса.
        """
        if chat_id not in self._user_jitter:
            self._user_jitter[chat_id] = random.randint(0, self._JITTER_RANGE)
        return self._user_jitter[chat_id]

    async def _process_user(self, user: UserConfig) -> None:
        """Обработка уведомлений для одного пользователя."""
        if not user.login or not user.password_encrypted:
            return

        login, password = safe_decrypt(user)
        if not login or not password:
            return

        try:
            children = await get_children_async(login, password)
        except Exception as e:
            logger.warning(f"Failed to get children for user {user.chat_id}: {e}")
            return

        if not children:
            return

        logger.info(
            f"Processing user {user.chat_id}: "
            f"balance={user.enabled}, marks={user.marks_enabled}, food={user.food_enabled}, "
            f"children={len(children)}"
        )

        # Получаем данные о питании только если баланс/еда проверяются в этом цикле
        now = time.time()
        need_food = False
        jitter = self._get_jitter(user.chat_id)
        if user.enabled and (now - self._last_balance_check.get(user.chat_id, 0) >= self.BALANCE_FOOD_CHECK_INTERVAL + jitter):
            need_food = True
        if user.food_enabled and (now - self._last_food_check.get(user.chat_id, 0) >= self.BALANCE_FOOD_CHECK_INTERVAL + jitter):
            need_food = True

        food_info: Dict[int, FoodInfo] = {}
        if need_food:
            try:
                food_info = await get_food_for_children(login, password, children)
            except Exception as e:
                logger.error(f"Error fetching food for user {user.chat_id}: {e}")

        # Уведомления о балансе (проверка раз в 30 минут)
        if user.enabled:
            now = time.time()
            last_check = self._last_balance_check.get(user.chat_id, 0)
            if now - last_check >= self.BALANCE_FOOD_CHECK_INTERVAL + self._get_jitter(user.chat_id):
                self._last_balance_check[user.chat_id] = now
                await self._check_balance_notifications(user, children, food_info)

        # Уведомления об оценках (проверка раз в 15 мин + jitter)
        if user.marks_enabled:
            now = time.time()
            last_check = self._last_marks_check.get(user.chat_id, 0)
            if now - last_check >= self.MARKS_CHECK_INTERVAL + self._get_jitter(user.chat_id):
                self._last_marks_check[user.chat_id] = now
                await self._check_marks_notifications(user, children, login, password)

        # Уведомления о питании (проверка раз в 30 минут)
        if user.food_enabled:
            now = time.time()
            last_check = self._last_food_check.get(user.chat_id, 0)
            if now - last_check >= self.BALANCE_FOOD_CHECK_INTERVAL + self._get_jitter(user.chat_id):
                self._last_food_check[user.chat_id] = now
                await self._check_food_notifications(user, children, food_info)

        # Уведомления о днях рождения
        if getattr(user, 'birthday_enabled', False):
            await self._check_birthday_notifications(user, children, login, password)

    async def _check_balance_notifications(
        self,
        user: UserConfig,
        children: List[Child],
        food_info: Dict[int, FoodInfo]
    ) -> None:
        """
        Проверка и отправка уведомлений о балансе.
        Уведомление приходит ТОЛЬКО когда баланс упал ниже порога.
        """
        try:
            thresholds = await get_all_thresholds_for_chat(user.chat_id)

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
                prev_balance = self._prev_balances.get(user.chat_id, {}).get(child.id)

                # Уведомление ТОЛЬКО когда баланс стал ниже порога
                # (раньше был выше или равен, а теперь ниже)
                if balance < threshold:
                    # Проверяем, что это новое падение ниже порога
                    if prev_balance is None or prev_balance >= threshold:
                        # Дедупликация - не отправлять повторно для того же баланса
                        notif_key = f"low_balance:{child.id}:{int(balance)}"
                        if await is_notification_sent(user.chat_id, "balance", notif_key):
                            continue

                        alerts.append(
                            f"⚠️ {child.full_name} ({child.group}):\n"
                            f"  💰 Баланс: <b>{balance:.0f} ₽</b>\n"
                            f"  📉 Порог: {threshold:.0f} ₽\n"
                            f"  ❗ Необходимо пополнить счёт!"
                        )

                        await mark_notification_sent(user.chat_id, "balance", notif_key)

            self._prev_balances[user.chat_id] = new_balances

            if alerts:
                text = "⚠️ <b>Низкий баланс питания!</b>\n\n" + "\n\n".join(alerts)
                await self._send_notification(user.chat_id, text)

        except Exception as e:
            logger.error(f"Error checking balance for user {user.chat_id}: {e}")

    async def _check_marks_notifications(
        self,
        user: UserConfig,
        children: List[Child],
        login: str,
        password: str
    ) -> None:
        """Проверка и отправка уведомлений о новых оценках."""
        try:
            today = date.today()
            start = today - timedelta(days=self.MARKS_CHECK_DAYS)

            timetable = await get_timetable_for_children(
                login, password, children, start, today
            )

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

            # Проверяем новые оценки через БД дедупликацию
            new_marks = []
            for m in all_marks:
                # Уникальный ключ оценки
                notif_key = f"{m['date']}|{m['subject']}|{m['question_id']}|{m['value']}"

                # Проверяем, было ли уже отправлено уведомление
                if not await is_notification_sent(user.chat_id, "mark", notif_key):
                    new_marks.append(m)
                    await mark_notification_sent(user.chat_id, "mark", notif_key)

            if new_marks:
                lines = ["⭐ <b>Новые оценки!</b>\n"]

                for m in new_marks:
                    lines.append(
                        f"👤 {m['child_name']} ({m['child_group']})\n"
                        f"📚 {m['subject']}: {m['question_type']} → <b>{m['value']}</b>\n"
                        f"📅 {m['date']}"
                    )

                text = truncate_text("\n".join(lines))
                await self._send_notification(user.chat_id, text)

        except Exception as e:
            logger.error(f"Error checking marks for user {user.chat_id}: {e}")

    async def _check_food_notifications(
        self,
        user: UserConfig,
        children: List[Child],
        food_info: Dict[int, FoodInfo]
    ) -> None:
        """
        Проверка и отправка уведомлений о питании.
        Показывает что поел ребёнок и сколько списано.

        Логика: уведомление отправляется когда ПОЯВЛЯЕТСЯ подтверждённое питание.

        Состояния API:
        - state=10, state_str="Заказ сделан" — предзаказ, ребёнок ещё НЕ поел
        - state=20, state_str="Заказ отменён" — заказ отменён
        - state=30, state_str="Заказ подтверждён" — ребёнок поел

        Уведомление отправляем только при state=30 или ordered=1 с реальными блюдами.
        """
        try:
            today = date.today()
            today_str = today.strftime("%Y-%m-%d")

            logger.info(f"Food check for user {user.chat_id}, date={today_str}, children={len(children)}")

            alerts = []

            for child in children:
                info = food_info.get(child.id)
                if not info:
                    logger.warning(f"No food info for child {child.id} ({child.full_name})")
                    continue

                if not info.visits:
                    logger.info(
                        f"No visits for child {child.id} ({child.full_name}), "
                        f"balance={info.balance:.0f}"
                    )
                    continue

                logger.info(
                    f"Child {child.id} ({child.full_name}) has {len(info.visits)} visit(s), "
                    f"balance={info.balance:.0f}"
                )

                for visit_idx, visit in enumerate(info.visits):
                    if not isinstance(visit, dict):
                        logger.warning(f"Visit #{visit_idx} is not a dict: {type(visit)}")
                        continue

                    # Нормализуем дату визита
                    raw_date = visit.get("date", "")
                    visit_date = normalize_date(raw_date)

                    if visit_date and visit_date != today_str:
                        logger.debug(
                            f"Visit #{visit_idx} date {visit_date} != today {today_str}, skipping"
                        )
                        continue

                    # Собираем информацию о визите
                    ordered = visit.get("ordered")
                    state = visit.get("state")
                    dishes = visit.get("dishes", [])
                    state_str = str(visit.get("state_str", "")).lower()

                    # Логируем данные визита на INFO уровне для диагностики
                    logger.info(
                        f"Visit #{visit_idx}: child={child.id}, raw_date='{raw_date}', "
                        f"ordered={ordered}, state={state}, state_str='{state_str}', "
                        f"dishes_count={len(dishes) if dishes else 0}, "
                        f"price_sum={visit.get('price_sum')}, price={visit.get('price')}"
                    )

                    # Определяем, ПОЕЛ ли ребёнок (факт приёма пищи):
                    # state=30 — заказ подтверждён (ребёнок поел)
                    # ordered=1 с непустыми dishes — зафиксировано с блюдами
                    # ordered=1 со state=30 — двойное подтверждение
                    has_meal = False
                    meal_reason = ""

                    # 1. state=30 — заказ подтверждён (основной признак)
                    if state == 30:
                        has_meal = True
                        meal_reason = f"state=30 ({state_str})"

                    # 2. ordered=1 с непустыми блюдами
                    elif ordered and (ordered == 1 or ordered is True or str(ordered) == "1"):
                        if dishes and len(dishes) > 0:
                            dish_names = extract_dish_names(dishes)
                            if dish_names:
                                has_meal = True
                                meal_reason = f"ordered=1 + {len(dish_names)} dishes"

                    # Пропускаем предзаказы (state=10) и отмены (state=20)
                    if not has_meal:
                        logger.debug(f"No confirmed meal for visit #{visit_idx} (state={state})")
                        continue

                    logger.info(f"Meal detected: {meal_reason}")

                    # Уникальный ключ визита для дедупликации
                    line = visit.get("line", visit.get("line_id", 0))
                    time_start = visit.get("time_start", visit.get("time", ""))
                    visit_key = f"food:{child.id}:{visit_date}:{line}:{time_start}"

                    # Проверяем через БД, было ли уже отправлено уведомление
                    if await is_notification_sent(user.chat_id, "food", visit_key):
                        logger.info(f"Already notified for visit {visit_key}")
                        continue

                    # Новое питание!
                    complex_name = visit.get("complex", "")

                    meal_type = complex_name or (
                        visit.get("line_name") or
                        visit.get("type_name") or
                        visit.get("meal_type") or
                        "Питание"
                    )

                    # Цена
                    price = extract_price(visit)

                    # Блюда: пробуем из dishes, потом из qs_unit (комплексное меню)
                    dish_names = extract_dish_names(dishes)
                    if not dish_names:
                        qs_units = visit.get("qs_unit", [])
                        dish_names = parse_complex_menu(qs_units)

                    # Формируем сообщение
                    msg_lines = [f"🍽 <b>{child.full_name}</b> ({child.group})"]
                    msg_lines.append(f"🕐 {meal_type}")

                    if dish_names:
                        msg_lines.append("📋 <b>Меню:</b>")
                        for dish in dish_names:
                            msg_lines.append(f"  • {dish}")

                    msg_lines.append(f"💰 Списано: <b>{price:.0f} ₽</b>")

                    alerts.append("\n".join(msg_lines))

                    # Отмечаем в БД что уведомление отправлено
                    await mark_notification_sent(user.chat_id, "food", visit_key)
                    logger.info(f"Marked food notification as sent: {visit_key}")

            if alerts:
                text = f"🍽 <b>Ребёнок поел!</b> ({today_str})\n\n" + "\n\n".join(alerts)
                await self._send_notification(user.chat_id, text)
            else:
                logger.debug(f"No new food notifications for user {user.chat_id}")

        except Exception as e:
            logger.error(f"Error checking food for user {user.chat_id}: {e}", exc_info=True)

    async def _send_notification(self, chat_id: int, text: str) -> None:
        """Отправка уведомления пользователю."""
        try:
            await self._bot.send_message(chat_id, text)
            logger.info(f"Notification sent to user {chat_id}")
        except TelegramAPIError as e:
            logger.error(f"Failed to send notification to {chat_id}: {e}")

            if "blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
                from ..database import create_or_update_user
                await create_or_update_user(
                    chat_id,
                    enabled=False,
                    marks_enabled=False,
                    food_enabled=False,
                    birthday_enabled=False
                )
                logger.info(f"Disabled notifications for blocked user {chat_id}")

    async def _check_birthday_notifications(
        self,
        user: UserConfig,
        children: List[Child],
        login: str,
        password: str
    ) -> None:
        """
        Проверка и отправка уведомлений о днях рождения одноклассников.
        Поддерживает два режима: 'tomorrow' и 'weekly'.
        Проверяется не чаще 1 раза в час (окно уведомления = 2 минуты).
        """
        try:
            # Часовой пояс Новосибирск (GMT+7)
            tz = timezone(timedelta(hours=7))
            now = datetime.now(tz)
            current_hour = now.hour

            # Пропускаем если уже проверяли в этом часу
            last_checked_hour = self._last_birthday_check_hour.get(user.chat_id, -1)
            if last_checked_hour == current_hour:
                return
            self._last_birthday_check_hour[user.chat_id] = current_hour

            for child_idx, child in enumerate(children):
                # Используем кеш настроек ДР (TTL 24ч)
                bd_cache_key = f"bd_settings:{user.chat_id}:{child.id}"
                settings = birthday_settings_cache.get(bd_cache_key)
                if settings is None:
                    settings = await get_birthday_settings(user.chat_id, child.id)
                    birthday_settings_cache.set(bd_cache_key, settings)
                if not settings.get("enabled", False):
                    continue

                mode = settings.get("mode", "tomorrow")
                notify_hour = settings.get("notify_hour", 7)
                notify_minute = settings.get("notify_minute", 0)

                # Проверяем совпадение времени
                if now.hour != notify_hour or now.minute < notify_minute:
                    continue
                if now.minute > notify_minute + 2:
                    # Прошло больше 2 минут — пропускаем этот час
                    continue

                if mode == "tomorrow":
                    await self._process_tomorrow_mode(user, child, child_idx, now, tz, login, password)
                elif mode == "weekly":
                    await self._process_weekly_mode(user, child, child_idx, now, tz, login, password)

        except Exception as e:
            logger.error(f"Error checking birthday notifications for user {user.chat_id}: {e}", exc_info=True)

    async def _process_tomorrow_mode(
        self,
        user: UserConfig,
        child: Child,
        child_idx: int,
        now: datetime,
        tz: timezone,
        login: str,
        password: str
    ) -> None:
        """Обработка режима «завтра» — уведомляет о ДР завтрашнего дня."""
        tomorrow = (now + timedelta(days=1)).date()

        # Дедупликация
        notif_key = f"birthday:{child.id}:{tomorrow.isoformat()}"
        if await is_notification_sent(user.chat_id, "birthday", notif_key):
            return

        # Получаем одноклассников
        try:
            classmates = await get_classmates_for_child(
                login, password, child_idx
            )
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
                age = tomorrow.year - bd.year
                birthday_kids.append((c, age))

        if not birthday_kids:
            return

        tomorrow_str = tomorrow.strftime("%d.%m")
        lines = [f"🎂 <b>Дни рождения одноклассников</b>\n"]
        lines.append(f"👦 <b>{child.full_name}</b> ({child.group}):\n")
        lines.append(f"Завтра ({tomorrow_str}):")

        for c, age in birthday_kids:
            lines.append(f"  🎁 {c.full_name} — {age} лет")

        text = "\n".join(lines)
        await self._send_notification(user.chat_id, text)
        await mark_notification_sent(user.chat_id, "birthday", notif_key)
        logger.info(f"Sent tomorrow birthday notification for child {child.id}: {len(birthday_kids)} birthdays")

    async def _process_weekly_mode(
        self,
        user: UserConfig,
        child: Child,
        child_idx: int,
        now: datetime,
        tz: timezone,
        login: str,
        password: str
    ) -> None:
        """Обработка еженедельного режима — уведомляет о ДР на предстоящей неделе."""
        notify_weekday = await self._get_weekday_from_settings(user.chat_id, child.id)
        if now.weekday() != notify_weekday:
            return

        # Вычисляем начало и конец недели (7 дней начиная с текущего дня)
        week_start = now.date()
        week_end = week_start + timedelta(days=6)

        # Дедупликация: по году и номеру недели
        year_week = now.strftime("%Y-W%W")
        notif_key = f"birthday:{child.id}:{year_week}"
        if await is_notification_sent(user.chat_id, "birthday", notif_key):
            return

        # Получаем одноклассников
        try:
            classmates = await get_classmates_for_child(
                login, password, child_idx
            )
        except Exception as e:
            logger.warning(f"Failed to fetch classmates for child {child.id}: {e}")
            return

        # Группируем дни рождения по датам
        birthday_by_date: Dict[date, List[tuple]] = {}
        for c in classmates:
            if not c.birth_date:
                continue
            try:
                bd = datetime.strptime(c.birth_date, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue

            # Ищем совпадение дня рождения в диапазоне недели
            for day_offset in range(7):
                check_date = week_start + timedelta(days=day_offset)
                if (bd.month, bd.day) == (check_date.month, check_date.day):
                    age = check_date.year - bd.year
                    birthday_by_date.setdefault(check_date, []).append((c, age))
                    break

        if not birthday_by_date:
            return

        lines = [f"🎂 <b>Дни рождения одноклассников</b>\n"]
        lines.append(f"👦 <b>{child.full_name}</b> ({child.group}):\n")

        for check_date in sorted(birthday_by_date.keys()):
            date_str = check_date.strftime("%d.%m")
            lines.append(f"  {date_str}:")
            for c, age in birthday_by_date[check_date]:
                lines.append(f"    🎁 {c.full_name} — {age} лет")

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3997] + "..."

        await self._send_notification(user.chat_id, text)
        await mark_notification_sent(user.chat_id, "birthday", notif_key)
        total = sum(len(v) for v in birthday_by_date.values())
        logger.info(f"Sent weekly birthday notification for child {child.id}: {total} birthdays")

    async def _get_weekday_from_settings(self, chat_id: int, child_id: int) -> int:
        """Получить день недели (0=Mon, 6=Sun) из настроек."""
        settings = await get_birthday_settings(chat_id, child_id)
        return settings.get("notify_weekday", 1)

