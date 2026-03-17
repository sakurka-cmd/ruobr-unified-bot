"""
Middleware для бота: rate limiting, аутентификация, логирование.
"""
import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey

from .config import config
from .database import get_user, UserConfig

logger = logging.getLogger(__name__)


@dataclass
class RateLimitEntry:
    """Запись для отслеживания rate limit."""
    timestamps: list
    blocked_until: float = 0


class RateLimitMiddleware(BaseMiddleware):
    """
    Middleware для ограничения частоты запросов.
    Использует алгоритм sliding window.
    """
    
    def __init__(
        self,
        limit: int = 30,
        window_seconds: int = 60,
        block_duration: int = 30
    ):
        """
        Инициализация middleware.
        
        Args:
            limit: Максимальное количество запросов в окне.
            window_seconds: Размер окна в секундах.
            block_duration: Длительность блокировки в секундах.
        """
        self._limit = limit
        self._window = window_seconds
        self._block_duration = block_duration
        self._entries: Dict[int, RateLimitEntry] = defaultdict(
            lambda: RateLimitEntry(timestamps=[])
        )
        self._cleanup_interval = 3600  # Очистка каждый час
        self._last_cleanup = time.time()
    
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Any],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        """Обработка события с проверкой rate limit."""
        user_id = event.from_user.id if event.from_user else 0
        
        if user_id == 0:
            return await handler(event, data)
        
        # Админы не ограничены
        if config.is_admin(user_id):
            return await handler(event, data)
        
        current_time = time.time()
        entry = self._entries[user_id]
        
        # Проверка на блокировку
        if entry.blocked_until > current_time:
            remaining = int(entry.blocked_until - current_time)
            try:
                await event.answer(
                    f"⚠️ Слишком много запросов. Подождите {remaining} сек."
                )
            except Exception:
                pass
            return
        
        # Очистка старых timestamp'ов
        cutoff = current_time - self._window
        entry.timestamps = [ts for ts in entry.timestamps if ts > cutoff]
        
        # Проверка лимита
        if len(entry.timestamps) >= self._limit:
            entry.blocked_until = current_time + self._block_duration
            logger.warning(
                f"Rate limit exceeded for user {user_id}: "
                f"{len(entry.timestamps)} requests in {self._window}s"
            )
            try:
                await event.answer(
                    f"⚠️ Превышен лимит запросов. "
                    f"Попробуйте через {self._block_duration} сек."
                )
            except Exception:
                pass
            return
        
        # Добавляем текущий запрос
        entry.timestamps.append(current_time)
        
        # Периодическая очистка памяти
        if current_time - self._last_cleanup > self._cleanup_interval:
            self._cleanup_old_entries(current_time)
            self._last_cleanup = current_time
        
        return await handler(event, data)
    
    def _cleanup_old_entries(self, current_time: float) -> None:
        """Очистка старых записей для экономии памяти."""
        cutoff = current_time - self._window * 2
        to_remove = []
        
        for user_id, entry in self._entries.items():
            # Удаляем старые timestamp'ы
            entry.timestamps = [ts for ts in entry.timestamps if ts > cutoff]
            # Отмечаем пустые записи для удаления
            if not entry.timestamps and entry.blocked_until < current_time:
                to_remove.append(user_id)
        
        for user_id in to_remove:
            del self._entries[user_id]
        
        if to_remove:
            logger.debug(f"Cleaned up {len(to_remove)} rate limit entries")


class AuthMiddleware(BaseMiddleware):
    """
    Middleware для проверки аутентификации пользователя.
    Добавляет user_config в data для использования в handlers.
    Работает как с Message, так и с CallbackQuery.
    """
    
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Any],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        """Добавление конфигурации пользователя в контекст."""
        # Определяем chat_id в зависимости от типа события
        if isinstance(event, CallbackQuery):
            chat_id = event.message.chat.id if event.message else event.from_user.id
        else:
            chat_id = event.chat.id
        
        # Получаем конфигурацию пользователя
        user_config = await get_user(chat_id)
        
        # Добавляем в data для использования в handlers
        data["user_config"] = user_config
        data["is_authenticated"] = (
            user_config is not None 
            and user_config.login is not None 
            and user_config.password is not None
        )
        
        return await handler(event, data)


class LoggingMiddleware(BaseMiddleware):
    """
    Middleware для логирования сообщений.
    """
    
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Any],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        """Логирование входящего сообщения."""
        user = event.from_user
        chat = event.chat
        
        logger.info(
            f"Message from user {user.id} ({user.full_name}) "
            f"in chat {chat.id}: {event.text[:100] if event.text else '<non-text>'}"
        )
        
        start_time = time.time()
        
        try:
            result = await handler(event, data)
            elapsed = time.time() - start_time
            logger.debug(f"Handler completed in {elapsed:.3f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"Handler failed after {elapsed:.3f}s: {e}",
                exc_info=True
            )
            raise


class ThrottlingMiddleware(BaseMiddleware):
    """
    Middleware для предотвращения флуда при FSM.
    Блокирует обработку если предыдущий запрос ещё обрабатывается.
    """
    
    def __init__(self):
        self._processing: Dict[int, bool] = defaultdict(bool)
        self._lock = asyncio.Lock()
    
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Any],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id if event.from_user else 0
        
        if user_id == 0:
            return await handler(event, data)
        
        # Проверяем, обрабатывается ли уже запрос от этого пользователя
        async with self._lock:
            if self._processing[user_id]:
                logger.debug(f"Skipping duplicate request from user {user_id}")
                return
            self._processing[user_id] = True
        
        try:
            return await handler(event, data)
        finally:
            async with self._lock:
                self._processing[user_id] = False
