"""
Модуль кэширования данных.
Реализует in-memory кэш с TTL и LRU-подобной очисткой.

Без блокировок: в asyncio single-threaded event loop все dict-операции
атомарны (нет concurrent threads), поэтому threading.Lock / asyncio.Lock
не требуются.
"""
import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Generic, Optional, TypeVar

from ..config import config

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class CacheEntry(Generic[T]):
    """Запись в кэше с временем жизни."""
    value: T
    expires_at: float
    created_at: float


class MemoryCache(Generic[T]):
    """
    In-memory кэш с TTL и ограничением размера.
    Использует OrderedDict для LRU-подобной очистки.

    Без блокировок: все операции — синхронные dict-манипуляции без await,
    поэтому в asyncio event loop (single-thread) они атомарны.
    """

    def __init__(
        self,
        ttl_seconds: int = 300,
        max_size: int = 1000
    ):
        """
        Инициализация кэша.

        Args:
            ttl_seconds: Время жизни записи в секундах.
            max_size: Максимальное количество записей.
        """
        self._ttl = ttl_seconds
        self._max_size = max_size
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[T]:
        """
        Получение значения из кэша.

        Args:
            key: Ключ.

        Returns:
            Значение или None если не найдено или устарело.
        """
        if key not in self._cache:
            self._misses += 1
            return None

        entry = self._cache[key]
        current_time = time.time()

        if entry.expires_at < current_time:
            # Запись устарела
            del self._cache[key]
            self._misses += 1
            return None

        # Перемещаем в конец (недавно использованный)
        self._cache.move_to_end(key)
        self._hits += 1
        return entry.value

    def set(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        """
        Установка значения в кэш.

        Args:
            key: Ключ.
            value: Значение.
            ttl: Время жизни в секундах (опционально, использует дефолтное).
        """
        current_time = time.time()
        actual_ttl = ttl if ttl is not None else self._ttl

        # Удаляем старую запись если существует
        if key in self._cache:
            del self._cache[key]

        # Проверяем размер и удаляем старые записи
        while len(self._cache) >= self._max_size:
            # Удаляем самую старую запись (первую в OrderedDict)
            self._cache.popitem(last=False)

        self._cache[key] = CacheEntry(
            value=value,
            expires_at=current_time + actual_ttl,
            created_at=current_time
        )

    def delete(self, key: str) -> bool:
        """
        Удаление записи из кэша.

        Args:
            key: Ключ.

        Returns:
            True если запись была удалена, False если не существовала.
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def clear(self) -> None:
        """Очистка всего кэша."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def cleanup_expired(self) -> int:
        """
        Удаление устаревших записей.

        Returns:
            Количество удалённых записей.
        """
        current_time = time.time()
        expired_keys = [
            key for key, entry in self._cache.items()
            if entry.expires_at < current_time
        ]

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

        return len(expired_keys)

    @property
    def size(self) -> int:
        """Текущее количество записей в кэше."""
        return len(self._cache)

    @property
    def stats(self) -> Dict[str, Any]:
        """Статистика кэша."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "ttl": self._ttl
        }


# Глобальные кэши для разных типов данных

# Кэш списка детей (долгий TTL, редко меняется)
children_cache = MemoryCache[list](ttl_seconds=86400, max_size=500)

# Кэш расписания (средний TTL)
timetable_cache = MemoryCache[list](ttl_seconds=config.cache_ttl_seconds, max_size=1000)

# Кэш информации о питании (короткий TTL)
food_cache = MemoryCache[dict](ttl_seconds=60, max_size=500)

# Кэш порогов баланса
threshold_cache = MemoryCache[dict](ttl_seconds=600, max_size=500)

# Кэш настроек уведомлений о днях рождения (долгий TTL, меняются редко)
birthday_settings_cache = MemoryCache[dict](ttl_seconds=86400, max_size=500)



def get_cache_key(chat_id: int, *args) -> str:
    """
    Генерация ключа кэша.

    Args:
        chat_id: ID чата пользователя.
        *args: Дополнительные части ключа.

    Returns:
        Строка-ключ для кэша.
    """
    parts = [str(chat_id)] + [str(arg) for arg in args]
    return ":".join(parts)


def invalidate_children_cache(login: str) -> None:
    """
    Инвалидация кэша списка детей для конкретного логина.
    Вызывать при изменении учётных данных пользователя.
    """
    if login:
        children_cache.delete(f"{login}:children")
        logger.debug(f"Invalidated children cache for login {login}")


def invalidate_birthday_cache(chat_id: int, child_id: int) -> None:
    """
    Инвалидация кэша настроек ДР для конкретного ребёнка.
    Вызывать при изменении настроек уведомлений о днях рождения.
    """
    cache_key = f"bd_settings:{chat_id}:{child_id}"
    birthday_settings_cache.delete(cache_key)
    logger.debug(f"Invalidated birthday settings cache for chat {chat_id}, child {child_id}")


async def invalidate_user_cache(chat_id: int) -> None:
    """
    Инвалидация всего кэша пользователя.

    Args:
        chat_id: ID чата пользователя.
    """
    prefix = f"{chat_id}:"

    # Инвалидируем во всех кэшах
    for cache in [children_cache, timetable_cache, food_cache, threshold_cache, birthday_settings_cache]:
        keys_to_delete = [
            key for key in cache._cache.keys()
            if key.startswith(prefix)
        ]
        for key in keys_to_delete:
            cache.delete(key)

    logger.debug(f"Invalidated cache for user {chat_id}")


async def periodic_cache_cleanup(interval: int = 300) -> None:
    """
    Периодическая очистка устаревших записей кэша.

    Args:
        interval: Интервал очистки в секундах.
    """
    while True:
        await asyncio.sleep(interval)

        total_cleaned = 0
        for cache in [children_cache, timetable_cache, food_cache, threshold_cache, birthday_settings_cache]:
            total_cleaned += cache.cleanup_expired()

        if total_cleaned > 0:
            logger.info(f"Cache cleanup: removed {total_cleaned} expired entries")
