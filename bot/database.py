"""
Модуль работы с базой данных.
Реализует пул соединений и асинхронные операции.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, AsyncGenerator

import aiosqlite
from aiosqlite import Connection, Cursor

from .config import config
from .encryption import encrypt_password, decrypt_password

logger = logging.getLogger(__name__)


@dataclass
class UserConfig:
    """Конфигурация пользователя."""
    chat_id: int
    login: Optional[str] = None
    password_encrypted: Optional[str] = None
    password: Optional[str] = None  # Расшифрованный пароль (только для чтения)
    enabled: bool = False
    marks_enabled: bool = True
    food_enabled: bool = True
    birthday_enabled: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Дешифрование пароля при необходимости."""
        if self.password_encrypted and not self.password:
            try:
                self.password = decrypt_password(self.password_encrypted)
            except Exception as e:
                logger.warning(f"Failed to decrypt password for user {self.chat_id}: {e}")


@dataclass
class ChildThreshold:
    """Настройки порога баланса для ребёнка."""
    chat_id: int
    child_id: int
    threshold: float
    updated_at: Optional[datetime] = None


class DatabasePool:
    """
    Пул соединений с базой данных SQLite.
    Обеспечивает потокобезопасный доступ к БД.
    """
    
    _instance: Optional['DatabasePool'] = None
    _lock = asyncio.Lock()
    
    def __new__(cls) -> 'DatabasePool':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._pool: List[Connection] = []
        self._pool_size = 5
        self._db_path: Optional[Path] = None
    
    async def initialize(self, db_path: Optional[Path] = None) -> None:
        """
        Инициализация пула соединений.
        
        Args:
            db_path: Путь к файлу базы данных.
        """
        self._db_path = db_path or config.db_path
        
        # Создаём директорию если не существует
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Создаём начальные соединения
        for _ in range(self._pool_size):
            conn = await aiosqlite.connect(self._db_path)
            conn.row_factory = aiosqlite.Row
            self._pool.append(conn)
        
        # Инициализируем схему
        await self._init_schema()
        logger.info(f"Database pool initialized: {self._db_path}")
    
    async def _init_schema(self) -> None:
        """Создание схемы базы данных."""
        async with self.connection() as conn:
            await conn.executescript("""
                -- Таблица пользователей
                CREATE TABLE IF NOT EXISTS users (
                    chat_id INTEGER PRIMARY KEY,
                    login TEXT,
                    password_encrypted TEXT,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    marks_enabled INTEGER NOT NULL DEFAULT 1,
                    food_enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Таблица порогов баланса
                CREATE TABLE IF NOT EXISTS thresholds (
                    chat_id INTEGER NOT NULL,
                    child_id INTEGER NOT NULL,
                    threshold REAL NOT NULL DEFAULT 300.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, child_id),
                    FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
                );
                
                -- Таблица FSM состояний
                CREATE TABLE IF NOT EXISTS fsm_states (
                    chat_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    data TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Таблица истории уведомлений (для дедупликации)
                CREATE TABLE IF NOT EXISTS notification_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    notification_type TEXT NOT NULL,
                    notification_key TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, notification_type, notification_key)
                );
                
                -- Таблица настроек уведомлений о днях рождения
                CREATE TABLE IF NOT EXISTS birthday_settings (
                    chat_id INTEGER NOT NULL,
                    child_id INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    mode TEXT NOT NULL DEFAULT 'tomorrow',
                    notify_weekday INTEGER DEFAULT 1,
                    notify_hour INTEGER DEFAULT 7,
                    notify_minute INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, child_id),
                    FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
                );
                
                -- Индексы для оптимизации
                CREATE INDEX IF NOT EXISTS idx_thresholds_chat_id ON thresholds(chat_id);
                CREATE INDEX IF NOT EXISTS idx_users_enabled ON users(enabled);
                CREATE INDEX IF NOT EXISTS idx_notification_history_chat ON notification_history(chat_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_birthday_settings_chat ON birthday_settings(chat_id);
            """)
            await conn.commit()
            
            # Миграция: добавляем колонку food_enabled если её нет
            try:
                async with conn.execute("PRAGMA table_info(users)") as cursor:
                    columns = await cursor.fetchall()
                    column_names = [col[1] for col in columns]
                
                if "food_enabled" not in column_names:
                    await conn.execute("ALTER TABLE users ADD COLUMN food_enabled INTEGER NOT NULL DEFAULT 1")
                    await conn.commit()
                    logger.info("Migration: added food_enabled column to users table")
                
                if "birthday_enabled" not in column_names:
                    await conn.execute("ALTER TABLE users ADD COLUMN birthday_enabled INTEGER NOT NULL DEFAULT 0")
                    await conn.commit()
                    logger.info("Migration: added birthday_enabled column to users table")
            except Exception as e:
                logger.warning(f"Migration check failed (may be normal): {e}")
    
    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[Connection, None]:
        """
        Контекстный менеджер для получения соединения из пула.
        
        Yields:
            Соединение с базой данных.
        """
        conn = None
        try:
            if self._pool:
                conn = self._pool.pop()
            else:
                # Создаём новое соединение если пул пуст
                conn = await aiosqlite.connect(self._db_path)
                conn.row_factory = aiosqlite.Row
            
            yield conn
        finally:
            if conn:
                # Возвращаем соединение в пул
                if len(self._pool) < self._pool_size:
                    self._pool.append(conn)
                else:
                    await conn.close()
    
    async def close(self) -> None:
        """Закрытие всех соединений в пуле."""
        for conn in self._pool:
            await conn.close()
        self._pool.clear()
        logger.info("Database pool closed")


# Глобальный экземпляр пула
db_pool = DatabasePool()


# ===== Операции с пользователями =====

async def get_user(chat_id: int) -> Optional[UserConfig]:
    """
    Получение конфигурации пользователя.
    
    Args:
        chat_id: ID чата пользователя.
        
    Returns:
        Конфигурация пользователя или None если не найден.
    """
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT * FROM users WHERE chat_id = ?", (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            
            return UserConfig(
                chat_id=row["chat_id"],
                login=row["login"],
                password_encrypted=row["password_encrypted"],
                enabled=bool(row["enabled"]),
                marks_enabled=bool(row["marks_enabled"]),
                food_enabled=bool(row["food_enabled"]) if "food_enabled" in row.keys() else True,
                birthday_enabled=bool(row["birthday_enabled"]) if "birthday_enabled" in row.keys() else False,
                created_at=row["created_at"],
                updated_at=row["updated_at"]
            )


async def create_or_update_user(
    chat_id: int,
    login: Optional[str] = None,
    password: Optional[str] = None,
    enabled: Optional[bool] = None,
    marks_enabled: Optional[bool] = None,
    food_enabled: Optional[bool] = None,
    birthday_enabled: Optional[bool] = None
) -> UserConfig:
    """
    Создание или обновление пользователя.
    
    Args:
        chat_id: ID чата пользователя.
        login: Логин от Ruobr.
        password: Пароль от Ruobr (будет зашифрован).
        enabled: Включены ли уведомления о балансе.
        marks_enabled: Включены ли уведомления об оценках.
        food_enabled: Включены ли уведомления о питании.
        birthday_enabled: Включены ли уведомления о днях рождения.
        
    Returns:
        Обновлённая конфигурация пользователя.
    """
    # Шифруем пароль если он передан
    password_encrypted = None
    if password:
        password_encrypted = encrypt_password(password)
    
    async with db_pool.connection() as conn:
        # Проверяем существование пользователя
        async with conn.execute(
            "SELECT chat_id FROM users WHERE chat_id = ?", (chat_id,)
        ) as cursor:
            exists = await cursor.fetchone() is not None
        
        if exists:
            # Обновляем существующего пользователя
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params: List[Any] = []
            
            if login is not None:
                updates.append("login = ?")
                params.append(login)
            if password_encrypted is not None:
                updates.append("password_encrypted = ?")
                params.append(password_encrypted)
            if enabled is not None:
                updates.append("enabled = ?")
                params.append(1 if enabled else 0)
            if marks_enabled is not None:
                updates.append("marks_enabled = ?")
                params.append(1 if marks_enabled else 0)
            if food_enabled is not None:
                updates.append("food_enabled = ?")
                params.append(1 if food_enabled else 0)
            if birthday_enabled is not None:
                updates.append("birthday_enabled = ?")
                params.append(1 if birthday_enabled else 0)
            
            params.append(chat_id)
            await conn.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE chat_id = ?",
                params
            )
        else:
            # Создаём нового пользователя
            await conn.execute(
                """INSERT INTO users (chat_id, login, password_encrypted, enabled, marks_enabled, food_enabled, birthday_enabled)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    chat_id,
                    login,
                    password_encrypted,
                    1 if enabled else 0 if enabled is not None else 0,
                    1 if marks_enabled else 0 if marks_enabled is not None else 1,
                    1 if food_enabled else 0 if food_enabled is not None else 1,
                    1 if birthday_enabled else 0 if birthday_enabled is not None else 0
                )
            )
        
        await conn.commit()
    
    return await get_user(chat_id)


async def get_all_enabled_users() -> List[UserConfig]:
    """Получение всех пользователей с включёнными уведомлениями."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT * FROM users WHERE enabled = 1 OR marks_enabled = 1 OR food_enabled = 1 OR birthday_enabled = 1"
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                UserConfig(
                    chat_id=row["chat_id"],
                    login=row["login"],
                    password_encrypted=row["password_encrypted"],
                    enabled=bool(row["enabled"]),
                    marks_enabled=bool(row["marks_enabled"]),
                    food_enabled=bool(row["food_enabled"]) if "food_enabled" in row.keys() else True,
                    birthday_enabled=bool(row["birthday_enabled"]) if "birthday_enabled" in row.keys() else False
                )
                for row in rows
            ]


# ===== Операции с порогами =====

async def get_child_threshold(chat_id: int, child_id: int) -> float:
    """Получение порога баланса для ребёнка."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT threshold FROM thresholds WHERE chat_id = ? AND child_id = ?",
            (chat_id, child_id)
        ) as cursor:
            row = await cursor.fetchone()
            return float(row["threshold"]) if row else config.default_balance_threshold


async def set_child_threshold(chat_id: int, child_id: int, threshold: float) -> None:
    """Установка порога баланса для ребёнка."""
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO thresholds (chat_id, child_id, threshold, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(chat_id, child_id) DO UPDATE SET 
                   threshold = excluded.threshold,
                   updated_at = CURRENT_TIMESTAMP""",
            (chat_id, child_id, threshold)
        )
        await conn.commit()


async def get_all_thresholds_for_chat(chat_id: int) -> Dict[int, float]:
    """Получение всех порогов для чата."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT child_id, threshold FROM thresholds WHERE chat_id = ?",
            (chat_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return {int(row["child_id"]): float(row["threshold"]) for row in rows}


# ===== Операции с историей уведомлений =====

async def is_notification_sent(chat_id: int, notification_type: str, notification_key: str) -> bool:
    """Проверка было ли уже отправлено уведомление."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            """SELECT 1 FROM notification_history 
               WHERE chat_id = ? AND notification_type = ? AND notification_key = ?""",
            (chat_id, notification_type, notification_key)
        ) as cursor:
            return await cursor.fetchone() is not None


async def mark_notification_sent(chat_id: int, notification_type: str, notification_key: str) -> None:
    """Отметить уведомление как отправленное."""
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT OR IGNORE INTO notification_history 
               (chat_id, notification_type, notification_key) VALUES (?, ?, ?)""",
            (chat_id, notification_type, notification_key)
        )
        await conn.commit()


async def cleanup_old_notifications(days: int = 30) -> None:
    """Очистка старых записей истории уведомлений."""
    async with db_pool.connection() as conn:
        await conn.execute(
            f"DELETE FROM notification_history WHERE created_at < datetime('now', '-{days} days')"
        )
        await conn.commit()


# ===== FSM операции =====

async def save_fsm_state(chat_id: int, state: str, data: Optional[str] = None) -> None:
    """Сохранение состояния FSM."""
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO fsm_states (chat_id, state, data, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(chat_id) DO UPDATE SET 
                   state = excluded.state,
                   data = excluded.data,
                   updated_at = CURRENT_TIMESTAMP""",
            (chat_id, state, data)
        )
        await conn.commit()


async def get_fsm_state(chat_id: int) -> Optional[Dict[str, Any]]:
    """Получение состояния FSM."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT state, data FROM fsm_states WHERE chat_id = ?", (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {"state": row["state"], "data": row["data"]}
            return None


async def clear_fsm_state(chat_id: int) -> None:
    """Очистка состояния FSM."""
    async with db_pool.connection() as conn:
        await conn.execute("DELETE FROM fsm_states WHERE chat_id = ?", (chat_id,))
        await conn.commit()


# ===== Операции с настройками дней рождения =====

BIRTHDAY_DEFAULTS = {
    "enabled": 0,
    "mode": "tomorrow",
    "notify_weekday": 1,
    "notify_hour": 7,
    "notify_minute": 0,
}


async def get_birthday_settings(chat_id: int, child_id: int) -> Dict[str, Any]:
    """
    Получение настроек уведомлений о днях рождения для ребёнка.
    
    Args:
        chat_id: ID чата пользователя.
        child_id: ID ребёнка.
        
    Returns:
        Словарь с настройками или значения по умолчанию.
    """
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT * FROM birthday_settings WHERE chat_id = ? AND child_id = ?",
            (chat_id, child_id)
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return dict(BIRTHDAY_DEFAULTS)
            
            return {
                "enabled": bool(row["enabled"]),
                "mode": row["mode"],
                "notify_weekday": row["notify_weekday"],
                "notify_hour": row["notify_hour"],
                "notify_minute": row["notify_minute"],
            }


async def set_birthday_settings(
    chat_id: int,
    child_id: int,
    enabled: bool,
    mode: str,
    notify_weekday: int,
    notify_hour: int,
    notify_minute: int
) -> None:
    """
    Установка настроек уведомлений о днях рождения для ребёнка.
    
    Args:
        chat_id: ID чата пользователя.
        child_id: ID ребёнка.
        enabled: Включены ли уведомления.
        mode: Режим ('tomorrow' или 'weekly').
        notify_weekday: День недели (0=Mon, 6=Sun) для weekly режима.
        notify_hour: Час уведомления (0-23).
        notify_minute: Минута уведомления (0-59).
    """
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO birthday_settings 
               (chat_id, child_id, enabled, mode, notify_weekday, notify_hour, notify_minute, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(chat_id, child_id) DO UPDATE SET 
                   enabled = excluded.enabled,
                   mode = excluded.mode,
                   notify_weekday = excluded.notify_weekday,
                   notify_hour = excluded.notify_hour,
                   notify_minute = excluded.notify_minute,
                   updated_at = CURRENT_TIMESTAMP""",
            (chat_id, child_id, 1 if enabled else 0, mode, notify_weekday, notify_hour, notify_minute)
        )
        await conn.commit()


async def get_all_birthday_settings(chat_id: int) -> List[Dict[str, Any]]:
    """
    Получение настроек дней рождения для всех детей пользователя.
    
    Args:
        chat_id: ID чата пользователя.
        
    Returns:
        Список словарей с настройками.
    """
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT * FROM birthday_settings WHERE chat_id = ?",
            (chat_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "chat_id": row["chat_id"],
                    "child_id": row["child_id"],
                    "enabled": bool(row["enabled"]),
                    "mode": row["mode"],
                    "notify_weekday": row["notify_weekday"],
                    "notify_hour": row["notify_hour"],
                    "notify_minute": row["notify_minute"],
                }
                for row in rows
            ]


async def get_users_with_birthday_notifications() -> List[Dict[str, Any]]:
    """
    Получение всех пользователей, у которых включены уведомления о ДР
    хотя бы для одного ребёнка.
    
    Returns:
        Список словарей с chat_id и настройками.
    """
    async with db_pool.connection() as conn:
        async with conn.execute(
            """SELECT DISTINCT u.chat_id, u.login, u.password_encrypted, u.birthday_enabled,
                      bs.child_id, bs.mode, bs.notify_weekday, bs.notify_hour, bs.notify_minute
               FROM users u
               INNER JOIN birthday_settings bs ON u.chat_id = bs.chat_id
               WHERE u.birthday_enabled = 1 AND bs.enabled = 1"""
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "chat_id": row["chat_id"],
                    "login": row["login"],
                    "password_encrypted": row["password_encrypted"],
                    "child_id": row["child_id"],
                    "mode": row["mode"],
                    "notify_weekday": row["notify_weekday"],
                    "notify_hour": row["notify_hour"],
                    "notify_minute": row["notify_minute"],
                }
                for row in rows
            ]
