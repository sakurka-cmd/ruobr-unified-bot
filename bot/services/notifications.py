"""
Фоновые задачи для уведомлений.
"""
import asyncio
import logging
from datetime import date, timedelta
from typing import Dict, List, Set

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
        self._prev_marks: Dict[int, Set[str]] = {}
        self._prev_food_visits: Dict[int, Set[str]] = {}
    
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
        """
        try:
            today = date.today()
            today_str = today.strftime("%Y-%m-%d")
            
            food_info = await get_food_for_children(user.login, user.password, children)
            
            logger.info(f"Food check for user {user.chat_id}: food_info keys={list(food_info.keys())}")
            
            alerts = []
            new_visits: Set[str] = set()
            
            for child in children:
                info = food_info.get(child.id)
                if not info:
                    logger.info(f"No food info for child {child.id}")
                    continue
                    
                if not info.visits:
                    logger.info(f"No visits for child {child.id}")
                    continue
                
                logger.info(f"Child {child.id} has {len(info.visits)} visits")
                
                for visit in info.visits:
                    visit_date = visit.get("date", "")
                    logger.info(f"Visit date={visit_date}, today={today_str}")
                    
                    if visit_date != today_str:
                        continue
                    
                    # Проверяем подтверждённое питание
                    ordered = visit.get("ordered")
                    state = visit.get("state")
                    logger.info(f"Visit ordered={ordered}, state={state}")
                    
                    if not ordered and state != 30:
                        continue
                    
                    # Уникальный ключ визита
                    visit_key = f"{child.id}:{visit_date}:{visit.get('line', 0)}:{visit.get('time_start', '')}"
                    new_visits.add(visit_key)
                    
                    prev_visits = self._prev_food_visits.get(user.chat_id, set())
                    
                    if visit_key not in prev_visits:
                        # Новое питание!
                        meal_type = visit.get("line_name", "Питание")
                        
                        # Цена
                        price_raw = str(visit.get("price_sum") or visit.get("price", "0")).replace(",", ".")
                        try:
                            price = float(price_raw)
                        except ValueError:
                            price = 0.0
                        
                        # Блюда
                        dishes = visit.get("dishes", [])
                        dish_names = [d.get("text", "") for d in dishes if d.get("text")]
                        
                        # Формируем сообщение
                        msg_lines = [f"🍽 <b>{child.full_name}</b> ({child.group})"]
                        msg_lines.append(f"🕐 {meal_type}")
                        
                        if dish_names:
                            msg_lines.append("📋 <b>Меню:</b>")
                            for dish in dish_names:
                                msg_lines.append(f"  • {dish}")
                        
                        msg_lines.append(f"💰 Списано: <b>{price:.0f} ₽</b>")
                        
                        alerts.append("\n".join(msg_lines))
            
            self._prev_food_visits[user.chat_id] = new_visits
            
            if alerts:
                text = f"🍽 <b>Ребёнок поел!</b> ({today_str})\n\n" + "\n\n".join(alerts)
                await self._send_notification(user.chat_id, text)
                
        except Exception as e:
            logger.error(f"Error checking food for user {user.chat_id}: {e}")
    
    async def _send_notification(self, chat_id: int, text: str) -> None:
        """Отправка уведомления пользователю."""
        try:
            await self._bot.send_message(chat_id, text)
            logger.info(f"Notification sent to user {chat_id}")
        except TelegramAPIError as e:
            logger.error(f"Failed to send notification to {chat_id}: {e}")
            
            if "blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
                from ..database import create_or_update_user
                await create_or_update_user(chat_id, enabled=False, marks_enabled=False)
                logger.info(f"Disabled notifications for blocked user {chat_id}")
