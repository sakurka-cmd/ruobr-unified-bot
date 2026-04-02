"""
Фоновые задачи для уведомлений.
"""
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

from ..config import config
from ..database import (
    get_all_enabled_users,
    get_all_thresholds_for_chat,
    is_notification_sent,
    mark_notification_sent,
    cleanup_old_notifications,
    UserConfig
)
from ..services import (
    get_children_async,
    get_food_for_children,
    get_timetable_for_children,
    Child
)
from ..utils.formatters import truncate_text

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


def extract_dish_names(dishes: Any) -> List[str]:
    """
    Извлечение названий блюд из различных форматов данных.
    
    Args:
        dishes: Список блюд (может быть списком словарей, списком строк, или None).
        
    Returns:
        Список названий блюд.
    """
    if not dishes:
        return []
    
    if not isinstance(dishes, list):
        return []
    
    names = []
    for dish in dishes:
        if isinstance(dish, str):
            if dish.strip():
                names.append(dish.strip())
        elif isinstance(dish, dict):
            # Пробуем разные ключи для названия блюда
            name = (
                dish.get("text") or
                dish.get("name") or
                dish.get("title") or
                dish.get("dish_name") or
                dish.get("description") or
                ""
            )
            if name and str(name).strip():
                names.append(str(name).strip())
    
    return names


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
    
    MARKS_CHECK_DAYS = 14  # Проверять оценки за последние 14 дней
    
    def __init__(self, bot: Bot):
        self._bot = bot
        self._running = False
        self._prev_balances: Dict[int, Dict[int, float]] = {}
        # Примечание: дедупликация оценок и питания теперь через БД
        # _prev_marks и _prev_food_visits удалены - используем notification_history
    
    async def start(self) -> None:
        """Запуск фонового мониторинга."""
        self._running = True
        logger.info("Notification service started")
        
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
    
    async def _process_user(self, user: UserConfig) -> None:
        """Обработка уведомлений для одного пользователя."""
        if not user.login or not user.password:
            return
        
        try:
            children = await get_children_async(user.login, user.password)
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
        
        # Уведомления о балансе (только когда ниже порога)
        if user.enabled:
            await self._check_balance_notifications(user, children)
        
        # Уведомления об оценках
        if user.marks_enabled:
            await self._check_marks_notifications(user, children)
        
        # Уведомления о питании
        if user.food_enabled:
            await self._check_food_notifications(user, children)
    
    async def _check_balance_notifications(
        self,
        user: UserConfig,
        children: List[Child]
    ) -> None:
        """
        Проверка и отправка уведомлений о балансе.
        Уведомление приходит ТОЛЬКО когда баланс упал ниже порога.
        """
        try:
            food_info = await get_food_for_children(user.login, user.password, children)
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
        children: List[Child]
    ) -> None:
        """Проверка и отправка уведомлений о новых оценках."""
        try:
            today = date.today()
            start = today - timedelta(days=self.MARKS_CHECK_DAYS)
            
            timetable = await get_timetable_for_children(
                user.login, user.password, children, start, today
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
        children: List[Child]
    ) -> None:
        """
        Проверка и отправка уведомлений о питании.
        Показывает что поел ребёнок и сколько списано.
        
        Логика определения приёма пищи (срабатывает при любом из условий):
        - ordered = истинное значение (1, True, "1")
        - state = 30 (заказ подтверждён)
        - Есть непустой список блюд в dishes
        - Есть цена > 0 (признак фактического списания)
        - state_str содержит "подтвержд" (подтверждён заказ)
        """
        try:
            today = date.today()
            today_str = today.strftime("%Y-%m-%d")
            
            food_info = await get_food_for_children(user.login, user.password, children)
            
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
                    
                    # Определяем, было ли питание — проверяем все возможные признаки:
                    has_meal = False
                    meal_reason = ""
                    
                    # 1. ordered = истинное значение
                    if ordered and (ordered == 1 or ordered is True or str(ordered) == "1"):
                        has_meal = True
                        meal_reason = f"ordered={ordered}"
                    
                    # 2. state = 30 (заказ подтверждён)
                    elif state == 30:
                        has_meal = True
                        meal_reason = f"state=30"
                    
                    # 3. Есть блюда в dishes
                    elif dishes and len(dishes) > 0:
                        dish_names = extract_dish_names(dishes)
                        if dish_names:
                            has_meal = True
                            meal_reason = f"dishes ({len(dish_names)} items)"
                    
                    # 4. Есть цена > 0 (признак списания)
                    elif extract_price(visit) > 0:
                        has_meal = True
                        meal_reason = f"price={extract_price(visit):.0f}"
                    
                    # 5. state_str содержит подтверждение
                    elif "подтвержд" in state_str:
                        has_meal = True
                        meal_reason = f"state_str='{state_str}'"
                    
                    if not has_meal:
                        logger.info(f"No meal detected for visit #{visit_idx}")
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
                    meal_type = (
                        visit.get("line_name") or
                        visit.get("type_name") or
                        visit.get("meal_type") or
                        "Питание"
                    )
                    
                    # Цена
                    price = extract_price(visit)
                    
                    # Блюда
                    dish_names = extract_dish_names(dishes)
                    
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
                    food_enabled=False
                )
                logger.info(f"Disabled notifications for blocked user {chat_id}")
