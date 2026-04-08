"""
Модуль работы с базой данных.
Реализует пул соединений и асинхронные операции.

Поддерживает единый аккаунт с двумя мессенджерами:
- chat_id  — Telegram (nullable)
- peer_id  — VK (nullable)
Настройки уведомлений независимы для каждого канала.
"""
import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, AsyncGenerator

import aiosqlite
from aiosqlite import Connection, Cursor

from .config import config
from .encryption import encrypt_password
from .services.cache import invalidate_children_cache

logger = logging.getLogger(__name__)


@dataclass
class UserConfig:
    """Конфигурация пользователя.

    Пароль хранится только в зашифрованном виде (password_encrypted).
    Для получения расшифрованных учётных данных используйте
    credentials.safe_decrypt().

    Поля enabled/marks_enabled/food_enabled/birthday_enabled —
    это настройки Telegram (backward compat с существующими TG handlers).
    VK-настройки — в vk_* полях.
    """
    id: Optional[int] = None
    chat_id: Optional[int] = None
    peer_id: Optional[int] = None
    login: Optional[str] = None
    password_encrypted: Optional[str] = None
    # TG notification settings (backward compat — TG handlers use these)
    enabled: bool = False          # TG: баланс
    marks_enabled: bool = True     # TG: оценки
    food_enabled: bool = True      # TG: питание
    birthday_enabled: bool = False # TG: дни рождения
    # VK notification settings
    vk_balance_enabled: bool = False
    vk_marks_enabled: bool = True
    vk_food_enabled: bool = True
    vk_birthday_enabled: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def has_tg(self) -> bool:
        return self.chat_id is not None

    @property
    def has_vk(self) -> bool:
        return self.peer_id is not None

    @property
    def any_enabled(self) -> bool:
        """Есть ли хоть какие-то включённые уведомления."""
        return bool(
            self.enabled or self.marks_enabled or self.food_enabled or self.birthday_enabled
            or self.vk_balance_enabled or self.vk_marks_enabled
            or self.vk_food_enabled or self.vk_birthday_enabled
        )


@dataclass
class ChildThreshold:
    """Настройки порога баланса для ребёнка."""
    user_id: int
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
        self._db_path = db_path or config.db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        for _ in range(self._pool_size):
            conn = await aiosqlite.connect(self._db_path)
            conn.row_factory = aiosqlite.Row
            self._pool.append(conn)

        await self._init_schema()
        logger.info(f"Database pool initialized: {self._db_path}")

    async def _init_schema(self) -> None:
        async with self.connection() as conn:
            # Проверяем, старая ли схема (chat_id — PRIMARY KEY, нет колонки id)
            old_schema = False
            try:
                async with conn.execute("PRAGMA table_info(users)") as cur:
                    cols = await cur.fetchall()
                    col_names = [c[1] for c in cols]
                    # Старая схема: нет колонки 'id', но есть 'chat_id'
                    if "id" not in col_names and "chat_id" in col_names:
                        old_schema = True
            except Exception as e:
                # Если таблицы users нет вообще — это новая БД
                pass

            if old_schema:
                # Сначала удаляем новые таблицы если они были созданы частично
                for tbl in ("notification_settings", "vk_fsm_states", "link_codes",
                            "thresholds_new", "notification_history_new",
                            "fsm_states_new", "birthday_settings_new",
                            "users_new"):
                    try:
                        await conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                    except Exception:
                        pass
                await conn.commit()
                await self._migrate_from_old_schema(conn)
                return

            # Новая схема
            await conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER UNIQUE,
                    peer_id INTEGER UNIQUE,
                    login TEXT,
                    password_encrypted TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS notification_settings (
                    user_id INTEGER NOT NULL,
                    channel TEXT NOT NULL CHECK(channel IN ('tg', 'vk')),
                    balance_enabled INTEGER NOT NULL DEFAULT 0,
                    marks_enabled INTEGER NOT NULL DEFAULT 1,
                    food_enabled INTEGER NOT NULL DEFAULT 1,
                    birthday_enabled INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, channel),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS thresholds (
                    user_id INTEGER NOT NULL,
                    child_id INTEGER NOT NULL,
                    threshold REAL NOT NULL DEFAULT 300.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, child_id),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS fsm_states (
                    user_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    data TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS vk_fsm_states (
                    peer_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    data TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS notification_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    notification_type TEXT NOT NULL,
                    notification_key TEXT NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'tg',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, notification_type, notification_key, channel)
                );

                CREATE TABLE IF NOT EXISTS birthday_settings (
                    user_id INTEGER NOT NULL,
                    child_id INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    mode TEXT NOT NULL DEFAULT 'tomorrow',
                    notify_weekday INTEGER DEFAULT 1,
                    notify_hour INTEGER DEFAULT 7,
                    notify_minute INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, child_id),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS link_codes (
                    code TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    source TEXT NOT NULL CHECK(source IN ('tg', 'vk')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_users_chat_id ON users(chat_id);
                CREATE INDEX IF NOT EXISTS idx_users_peer_id ON users(peer_id);
                CREATE INDEX IF NOT EXISTS idx_thresholds_user_id ON thresholds(user_id);
                CREATE INDEX IF NOT EXISTS idx_notification_history_user ON notification_history(user_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_birthday_settings_user ON birthday_settings(user_id);
                CREATE INDEX IF NOT EXISTS idx_notification_settings_user ON notification_settings(user_id);
            """)
            await conn.commit()

            # Обратная совместимость: если notification_settings не существует для старых юзеров — создать
            try:
                async with conn.execute(
                    "SELECT u.id FROM users u LEFT JOIN notification_settings ns ON u.id = ns.user_id WHERE ns.user_id IS NULL"
                ) as cur:
                    missing = await cur.fetchall()
                    for row in missing:
                        uid = row["id"]
                        await conn.execute(
                            "INSERT OR IGNORE INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled) VALUES (?, 'tg', 0, 1, 1, 0)",
                            (uid,)
                        )
                    if missing:
                        await conn.commit()
                        logger.info(f"Created default TG notification_settings for {len(missing)} users")
            except Exception as e:
                logger.warning(f"Default notification_settings check: {e}")

    async def _migrate_from_old_schema(self, conn: Connection) -> None:
        """Миграция со старой схемы (chat_id PK) на новую (id PK + notification_settings)."""
        logger.info("Starting migration from old schema...")

        # 1. Создаём таблицы с новыми именами
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS users_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER UNIQUE,
                peer_id INTEGER UNIQUE,
                login TEXT,
                password_encrypted TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS notification_settings (
                user_id INTEGER NOT NULL,
                channel TEXT NOT NULL CHECK(channel IN ('tg', 'vk')),
                balance_enabled INTEGER NOT NULL DEFAULT 0,
                marks_enabled INTEGER NOT NULL DEFAULT 1,
                food_enabled INTEGER NOT NULL DEFAULT 1,
                birthday_enabled INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, channel),
                FOREIGN KEY (user_id) REFERENCES users_new(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS thresholds_new (
                user_id INTEGER NOT NULL,
                child_id INTEGER NOT NULL,
                threshold REAL NOT NULL DEFAULT 300.0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, child_id),
                FOREIGN KEY (user_id) REFERENCES users_new(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS notification_history_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                notification_type TEXT NOT NULL,
                notification_key TEXT NOT NULL,
                channel TEXT NOT NULL DEFAULT 'tg',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, notification_type, notification_key, channel)
            );

            CREATE TABLE IF NOT EXISTS fsm_states_new (
                user_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                data TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS birthday_settings_new (
                user_id INTEGER NOT NULL,
                child_id INTEGER NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 0,
                mode TEXT NOT NULL DEFAULT 'tomorrow',
                notify_weekday INTEGER DEFAULT 1,
                notify_hour INTEGER DEFAULT 7,
                notify_minute INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, child_id),
                FOREIGN KEY (user_id) REFERENCES users_new(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS vk_fsm_states (
                peer_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                data TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS link_codes (
                code TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                source TEXT NOT NULL CHECK(source IN ('tg', 'vk')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users_new(id) ON DELETE CASCADE
            );
        """)

        # 2. Копируем данные: users (old chat_id → new id + chat_id)
        await conn.execute("""
            INSERT INTO users_new (id, chat_id, login, password_encrypted, created_at, updated_at)
            SELECT chat_id, chat_id, login, password_encrypted, created_at, updated_at FROM users
        """)

        # 3. Копируем notification_settings из старых полей users.enabled/marks_enabled/...
        # Проверяем наличие колонок
        has_food = False
        has_birthday = False
        try:
            async with conn.execute("PRAGMA table_info(users)") as cur:
                cols = {row[1] for row in await cur.fetchall()}
                has_food = "food_enabled" in cols
                has_birthday = "birthday_enabled" in cols
        except Exception:
            pass

        if has_food and has_birthday:
            await conn.execute("""
                INSERT INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled)
                SELECT chat_id, 'tg', enabled, marks_enabled, food_enabled, birthday_enabled FROM users
            """)
        elif has_food:
            await conn.execute("""
                INSERT INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled)
                SELECT chat_id, 'tg', enabled, marks_enabled, food_enabled, 0 FROM users
            """)
        else:
            await conn.execute("""
                INSERT INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled)
                SELECT chat_id, 'tg', enabled, marks_enabled, 1, 0 FROM users
            """)

        # 4. thresholds: chat_id → user_id
        try:
            await conn.execute("""
                INSERT INTO thresholds_new (user_id, child_id, threshold, updated_at)
                SELECT chat_id, child_id, threshold, updated_at FROM thresholds
            """)
        except Exception as e:
            logger.warning(f"Thresholds migration: {e}")

        # 5. notification_history: chat_id → user_id
        try:
            # Проверяем наличие колонки channel
            has_channel = False
            try:
                async with conn.execute("PRAGMA table_info(notification_history)") as cur:
                    cols = {row[1] for row in await cur.fetchall()}
                    has_channel = "channel" in cols
            except Exception:
                pass

            if has_channel:
                await conn.execute("""
                    INSERT INTO notification_history_new (user_id, notification_type, notification_key, channel, created_at)
                    SELECT chat_id, notification_type, notification_key, channel, created_at FROM notification_history
                """)
            else:
                await conn.execute("""
                    INSERT INTO notification_history_new (user_id, notification_type, notification_key, channel, created_at)
                    SELECT chat_id, notification_type, notification_key, 'tg', created_at FROM notification_history
                """)
        except Exception as e:
            logger.warning(f"Notification history migration: {e}")

        # 6. fsm_states: chat_id → user_id
        try:
            await conn.execute("""
                INSERT INTO fsm_states_new (user_id, state, data, updated_at)
                SELECT chat_id, state, data, updated_at FROM fsm_states
            """)
        except Exception as e:
            logger.warning(f"FSM migration: {e}")

        # 7. birthday_settings: chat_id → user_id
        try:
            await conn.execute("""
                INSERT INTO birthday_settings_new (user_id, child_id, enabled, mode, notify_weekday, notify_hour, notify_minute, updated_at)
                SELECT chat_id, child_id, enabled, mode, notify_weekday, notify_hour, notify_minute, updated_at FROM birthday_settings
            """)
        except Exception as e:
            logger.warning(f"Birthday settings migration: {e}")

        # 8. Swap tables
        await conn.executescript("""
            DROP TABLE IF EXISTS notification_history;
            DROP TABLE IF EXISTS thresholds;
            DROP TABLE IF EXISTS fsm_states;
            DROP TABLE IF EXISTS birthday_settings;
            DROP TABLE IF EXISTS users;

            ALTER TABLE users_new RENAME TO users;
            ALTER TABLE notification_settings RENAME TO notification_settings;
            ALTER TABLE thresholds_new RENAME TO thresholds;
            ALTER TABLE notification_history_new RENAME TO notification_history;
            ALTER TABLE fsm_states_new RENAME TO fsm_states;
            ALTER TABLE birthday_settings_new RENAME TO birthday_settings;
        """)

        # 9. Create indexes
        await conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_users_chat_id ON users(chat_id);
            CREATE INDEX IF NOT EXISTS idx_users_peer_id ON users(peer_id);
            CREATE INDEX IF NOT EXISTS idx_thresholds_user_id ON thresholds(user_id);
            CREATE INDEX IF NOT EXISTS idx_notification_history_user ON notification_history(user_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_birthday_settings_user ON birthday_settings(user_id);
            CREATE INDEX IF NOT EXISTS idx_notification_settings_user ON notification_settings(user_id);
        """)

        await conn.commit()
        logger.info("Migration from old schema completed successfully")

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[Connection, None]:
        conn = None
        try:
            if self._pool:
                conn = self._pool.pop()
            else:
                conn = await aiosqlite.connect(self._db_path)
                conn.row_factory = aiosqlite.Row

            yield conn
        finally:
            if conn:
                if len(self._pool) < self._pool_size:
                    self._pool.append(conn)
                else:
                    await conn.close()

    async def close(self) -> None:
        for conn in self._pool:
            await conn.close()
        self._pool.clear()
        logger.info("Database pool closed")


# Глобальный экземпляр пула
db_pool = DatabasePool()


# ===== Вспомогательные функции =====

async def _user_id_by_chat_id(conn: Connection, chat_id: int) -> Optional[int]:
    """Получить user_id по chat_id."""
    async with conn.execute("SELECT id FROM users WHERE chat_id = ?", (chat_id,)) as cur:
        row = await cur.fetchone()
        return row["id"] if row else None


async def _user_id_by_peer_id(conn: Connection, peer_id: int) -> Optional[int]:
    """Получить user_id по peer_id."""
    async with conn.execute("SELECT id FROM users WHERE peer_id = ?", (peer_id,)) as cur:
        row = await cur.fetchone()
        return row["id"] if row else None


async def _get_notif_settings(conn: Connection, user_id: int) -> Dict[str, Dict[str, bool]]:
    """Получить настройки уведомлений для user_id по каналам."""
    result = {"tg": {}, "vk": {}}
    async with conn.execute(
        "SELECT * FROM notification_settings WHERE user_id = ?", (user_id,)
    ) as cur:
        for row in await cur.fetchall():
            ch = row["channel"]
            result[ch] = {
                "balance_enabled": bool(row["balance_enabled"]),
                "marks_enabled": bool(row["marks_enabled"]),
                "food_enabled": bool(row["food_enabled"]),
                "birthday_enabled": bool(row["birthday_enabled"]),
            }
    return result


def _apply_notif_settings(user: UserConfig, settings: Dict[str, Dict[str, bool]]) -> None:
    """Применить настройки уведомлений к UserConfig."""
    tg = settings.get("tg", {})
    vk = settings.get("vk", {})
    # TG (backward compat — TG handlers используют эти поля)
    user.enabled = tg.get("balance_enabled", False)
    user.marks_enabled = tg.get("marks_enabled", True)
    user.food_enabled = tg.get("food_enabled", True)
    user.birthday_enabled = tg.get("birthday_enabled", False)
    # VK
    user.vk_balance_enabled = vk.get("balance_enabled", False)
    user.vk_marks_enabled = vk.get("marks_enabled", True)
    user.vk_food_enabled = vk.get("food_enabled", True)
    user.vk_birthday_enabled = vk.get("birthday_enabled", False)


# ===== Операции с пользователями =====

async def get_user(chat_id: int = None, *, peer_id: int = None) -> Optional[UserConfig]:
    """Получить пользователя по chat_id или peer_id.

    backward compat: get_user(chat_id) — возвращает UserConfig с TG-настройками.
    """
    async with db_pool.connection() as conn:
        if chat_id is not None:
            async with conn.execute(
                "SELECT * FROM users WHERE chat_id = ?", (chat_id,)
            ) as cursor:
                row = await cursor.fetchone()
        elif peer_id is not None:
            async with conn.execute(
                "SELECT * FROM users WHERE peer_id = ?", (peer_id,)
            ) as cursor:
                row = await cursor.fetchone()
        else:
            return None

        if row is None:
            return None

        user = UserConfig(
            id=row["id"],
            chat_id=row["chat_id"],
            peer_id=row["peer_id"],
            login=row["login"],
            password_encrypted=row["password_encrypted"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

        settings = await _get_notif_settings(conn, user.id)
        _apply_notif_settings(user, settings)
        return user


async def get_user_by_id(user_id: int) -> Optional[UserConfig]:
    """Получить пользователя по внутреннему id."""
    async with db_pool.connection() as conn:
        async with conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None

            user = UserConfig(
                id=row["id"],
                chat_id=row["chat_id"],
                peer_id=row["peer_id"],
                login=row["login"],
                password_encrypted=row["password_encrypted"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

            settings = await _get_notif_settings(conn, user.id)
            _apply_notif_settings(user, settings)
            return user


async def create_or_update_user(
    chat_id: int = None,
    peer_id: int = None,
    login: Optional[str] = None,
    password: Optional[str] = None,
    enabled: Optional[bool] = None,
    marks_enabled: Optional[bool] = None,
    food_enabled: Optional[bool] = None,
    birthday_enabled: Optional[bool] = None,
    # VK notification settings
    vk_balance_enabled: Optional[bool] = None,
    vk_marks_enabled: Optional[bool] = None,
    vk_food_enabled: Optional[bool] = None,
    vk_birthday_enabled: Optional[bool] = None,
) -> UserConfig:
    """Создать или обновить пользователя.

    backward compat: chat_id как позиционный аргумент работает как раньше.
    enabled/marks_enabled/food_enabled/birthday_enabled — TG-настройки.
    vk_* — VK-настройки.
    """
    password_encrypted = None
    if password:
        password_encrypted = encrypt_password(password)

    async with db_pool.connection() as conn:
        # Найти существующего пользователя
        existing_id = None
        if chat_id is not None:
            existing_id = await _user_id_by_chat_id(conn, chat_id)
        if existing_id is None and peer_id is not None:
            existing_id = await _user_id_by_peer_id(conn, peer_id)

        if existing_id:
            # Обновляем пользователя
            updates = ["updated_at = CURRENT_TIMESTAMP"]
            params: List[Any] = []

            if chat_id is not None:
                updates.append("chat_id = ?")
                params.append(chat_id)
            if peer_id is not None:
                # Clear peer_id from other records to avoid UNIQUE constraint violation
                await conn.execute("UPDATE users SET peer_id = NULL WHERE peer_id = ? AND id != ?", (peer_id, existing_id))
                updates.append("peer_id = ?")
                params.append(peer_id)
            if login is not None:
                updates.append("login = ?")
                params.append(login)
            if password_encrypted is not None:
                updates.append("password_encrypted = ?")
                params.append(password_encrypted)

            params.append(existing_id)
            await conn.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE id = ?", params
            )
        else:
            # Создаём нового
            await conn.execute(
                "INSERT INTO users (chat_id, peer_id, login, password_encrypted) VALUES (?, ?, ?, ?)",
                (chat_id, peer_id, login, password_encrypted)
            )
            async with conn.execute("SELECT last_insert_rowid()") as cur:
                existing_id = (await cur.fetchone())[0]

        await conn.commit()

        # Обновляем TG notification_settings
        tg_updates = []
        tg_params: List[Any] = [existing_id]
        if enabled is not None:
            tg_updates.append("balance_enabled = ?")
            tg_params.append(1 if enabled else 0)
        if marks_enabled is not None:
            tg_updates.append("marks_enabled = ?")
            tg_params.append(1 if marks_enabled else 0)
        if food_enabled is not None:
            tg_updates.append("food_enabled = ?")
            tg_params.append(1 if food_enabled else 0)
        if birthday_enabled is not None:
            tg_updates.append("birthday_enabled = ?")
            tg_params.append(1 if birthday_enabled else 0)

        if tg_updates:
            await conn.execute(
                f"INSERT INTO notification_settings (user_id, channel) VALUES (?, 'tg') "
                f"ON CONFLICT(user_id, channel) DO NOTHING",
                (existing_id,)
            )
            await conn.execute(
                f"UPDATE notification_settings SET {', '.join(tg_updates)} "
                f"WHERE user_id = ? AND channel = 'tg'",
                tg_params
            )
            await conn.commit()

        # Обновляем VK notification_settings
        vk_updates = []
        vk_params: List[Any] = [existing_id]
        if vk_balance_enabled is not None:
            vk_updates.append("balance_enabled = ?")
            vk_params.append(1 if vk_balance_enabled else 0)
        if vk_marks_enabled is not None:
            vk_updates.append("marks_enabled = ?")
            vk_params.append(1 if vk_marks_enabled else 0)
        if vk_food_enabled is not None:
            vk_updates.append("food_enabled = ?")
            vk_params.append(1 if vk_food_enabled else 0)
        if vk_birthday_enabled is not None:
            vk_updates.append("birthday_enabled = ?")
            vk_params.append(1 if vk_birthday_enabled else 0)

        if vk_updates:
            await conn.execute(
                f"INSERT INTO notification_settings (user_id, channel) VALUES (?, 'vk') "
                f"ON CONFLICT(user_id, channel) DO NOTHING",
                (existing_id,)
            )
            await conn.execute(
                f"UPDATE notification_settings SET {', '.join(vk_updates)} "
                f"WHERE user_id = ? AND channel = 'vk'",
                vk_params
            )
            await conn.commit()
        elif peer_id is not None:
            # Создаём настройки VK по умолчанию при создании VK-пользователя
            await conn.execute(
                "INSERT OR IGNORE INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled) "
                "VALUES (?, 'vk', 0, 1, 1, 0)",
                (existing_id,)
            )
            await conn.commit()

    # Инвалидируем кэш детей при изменении пароля
    if password is not None and login:
        invalidate_children_cache(login)

    if chat_id is not None:
        return await get_user(chat_id=chat_id)
    elif peer_id is not None:
        return await get_user(peer_id=peer_id)
    else:
        return await get_user_by_id(existing_id)


async def get_all_enabled_users() -> List[UserConfig]:
    """Получить всех пользователей с хоть какими-то включёнными уведомлениями."""
    async with db_pool.connection() as conn:
        async with conn.execute("""
            SELECT u.* FROM users u
            INNER JOIN notification_settings ns ON u.id = ns.user_id
            WHERE ns.balance_enabled = 1 OR ns.marks_enabled = 1
                  OR ns.food_enabled = 1 OR ns.birthday_enabled = 1
            GROUP BY u.id
        """) as cursor:
            rows = await cursor.fetchall()
            users = []
            for row in rows:
                user = UserConfig(
                    id=row["id"],
                    chat_id=row["chat_id"],
                    peer_id=row["peer_id"],
                    login=row["login"],
                    password_encrypted=row["password_encrypted"],
                )
                settings = await _get_notif_settings(conn, user.id)
                _apply_notif_settings(user, settings)
                users.append(user)
            return users


async def link_accounts(user_id: int, *, chat_id: int = None, peer_id: int = None) -> bool:
    """Привязать chat_id или peer_id к существующему пользователю."""
    async with db_pool.connection() as conn:
        updates = []
        params: List[Any] = []
        if chat_id is not None:
            updates.append("chat_id = ?")
            params.append(chat_id)
        if peer_id is not None:
            updates.append("peer_id = ?")
            params.append(peer_id)

        if not updates:
            return False

        # Clear UNIQUE constraint conflicts before updating
        if chat_id is not None:
            await conn.execute("UPDATE users SET chat_id = NULL WHERE chat_id = ? AND id != ?", (chat_id, user_id))
        if peer_id is not None:
            await conn.execute("UPDATE users SET peer_id = NULL WHERE peer_id = ? AND id != ?", (peer_id, user_id))

        params.append(user_id)
        try:
            await conn.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE id = ?", params
            )
            await conn.commit()
            logger.info(f"Linked account: user_id={user_id}, chat_id={chat_id}, peer_id={peer_id}")

            # Создаём настройки уведомлений для нового канала если нет
            if peer_id is not None:
                await conn.execute(
                    "INSERT OR IGNORE INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled) "
                    "VALUES (?, 'vk', 0, 1, 1, 0)",
                    (user_id,)
                )
                await conn.commit()
            if chat_id is not None:
                await conn.execute(
                    "INSERT OR IGNORE INTO notification_settings (user_id, channel, balance_enabled, marks_enabled, food_enabled, birthday_enabled) "
                    "VALUES (?, 'tg', 0, 1, 1, 0)",
                    (user_id,)
                )
                await conn.commit()

            return True
        except Exception as e:
            logger.error(f"Failed to link account: {e}")
            return False


async def unlink_channel(user_id: int, channel: str) -> bool:
    """Отвязать канал от пользователя (chat_id или peer_id = NULL)."""
    async with db_pool.connection() as conn:
        if channel == "vk":
            await conn.execute("UPDATE users SET peer_id = NULL WHERE id = ?", (user_id,))
        elif channel == "tg":
            await conn.execute("UPDATE users SET chat_id = NULL WHERE id = ?", (user_id,))
        else:
            return False
        await conn.commit()
        logger.info(f"Unlinked {channel} from user_id={user_id}")

        # Удалить настройки уведомлений для отвязанного канала
        await conn.execute(
            "DELETE FROM notification_settings WHERE user_id = ? AND channel = ?",
            (user_id, channel)
        )
        await conn.commit()
        return True


# ===== Link codes (кросс-линковка TG ↔ VK) =====

async def create_link_code(user_id: int, source: str) -> str:
    """Создать код для привязки аккаунта. source='tg' или 'vk'."""
    code = secrets.token_hex(4).upper()  # 8 символов
    async with db_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO link_codes (code, user_id, source) VALUES (?, ?, ?)",
            (code, user_id, source)
        )
        await conn.commit()
    return code


async def consume_link_code(code: str):
    """Проверить и удалить код привязки. Возвращает (user_id, source) или None."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT user_id, source FROM link_codes WHERE code = ? AND created_at > datetime('now', '-10 minutes')",
            (code,)
        ) as cur:
            row = await cur.fetchone()
            if row is None:
                return None
            user_id = row["user_id"]
            source = row["source"]
            await conn.execute("DELETE FROM link_codes WHERE code = ?", (code,))
            await conn.commit()
            return user_id, source


# ===== Операции с порогами (backward compat: принимают chat_id, внутри ищут user_id) =====

async def _resolve_user_id(chat_id: int = None, peer_id: int = None) -> Optional[int]:
    """Разрешить user_id по chat_id или peer_id."""
    if chat_id is not None:
        user = await get_user(chat_id=chat_id)
    elif peer_id is not None:
        user = await get_user(peer_id=peer_id)
    else:
        return None
    return user.id if user else None


async def get_child_threshold(chat_id: int = None, *, peer_id: int = None, child_id: int = None, user_id: int = None) -> float:
    async with db_pool.connection() as conn:
        uid = user_id or await _resolve_user_id(chat_id=chat_id, peer_id=peer_id)
        if uid is None or child_id is None:
            return config.default_balance_threshold
        async with conn.execute(
            "SELECT threshold FROM thresholds WHERE user_id = ? AND child_id = ?",
            (uid, child_id)
        ) as cursor:
            row = await cursor.fetchone()
            return float(row["threshold"]) if row else config.default_balance_threshold


async def set_child_threshold(chat_id: int = None, *, peer_id: int = None, child_id: int = None, user_id: int = None, threshold: float = 300.0) -> None:
    uid = user_id or await _resolve_user_id(chat_id=chat_id, peer_id=peer_id)
    if uid is None or child_id is None:
        return
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO thresholds (user_id, child_id, threshold, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(user_id, child_id) DO UPDATE SET
                   threshold = excluded.threshold,
                   updated_at = CURRENT_TIMESTAMP""",
            (uid, child_id, threshold)
        )
        await conn.commit()


async def get_all_thresholds_for_chat(chat_id: int = None, *, peer_id: int = None, user_id: int = None) -> Dict[int, float]:
    uid = user_id or await _resolve_user_id(chat_id=chat_id, peer_id=peer_id)
    if uid is None:
        return {}
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT child_id, threshold FROM thresholds WHERE user_id = ?",
            (uid,)
        ) as cursor:
            rows = await cursor.fetchall()
            return {int(row["child_id"]): float(row["threshold"]) for row in rows}


# ===== Операции с историей уведомлений =====

async def is_notification_sent(
    user_id: int = None,
    chat_id: int = None,
    notification_type: str = "",
    notification_key: str = "",
    channel: str = "tg"
) -> bool:
    uid = user_id or await _resolve_user_id(chat_id=chat_id)
    if uid is None:
        return False
    async with db_pool.connection() as conn:
        async with conn.execute(
            """SELECT 1 FROM notification_history
               WHERE user_id = ? AND notification_type = ? AND notification_key = ? AND channel = ?""",
            (uid, notification_type, notification_key, channel)
        ) as cursor:
            return await cursor.fetchone() is not None


async def mark_notification_sent(
    user_id: int = None,
    chat_id: int = None,
    notification_type: str = "",
    notification_key: str = "",
    channel: str = "tg"
) -> None:
    uid = user_id or await _resolve_user_id(chat_id=chat_id)
    if uid is None:
        return
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT OR IGNORE INTO notification_history
               (user_id, notification_type, notification_key, channel) VALUES (?, ?, ?, ?)""",
            (uid, notification_type, notification_key, channel)
        )
        await conn.commit()


async def cleanup_old_notifications(days: int = 30) -> None:
    async with db_pool.connection() as conn:
        await conn.execute(
            f"DELETE FROM notification_history WHERE created_at < datetime('now', '-{days} days')"
        )
        await conn.commit()


# ===== FSM операции (TG — backward compat) =====

async def save_fsm_state(chat_id: int, state: str, data: Optional[str] = None) -> None:
    uid = await _resolve_user_id(chat_id=chat_id)
    if uid is None:
        return
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO fsm_states (user_id, state, data, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(user_id) DO UPDATE SET
                   state = excluded.state,
                   data = excluded.data,
                   updated_at = CURRENT_TIMESTAMP""",
            (uid, state, data)
        )
        await conn.commit()


async def get_fsm_state(chat_id: int) -> Optional[Dict[str, Any]]:
    uid = await _resolve_user_id(chat_id=chat_id)
    if uid is None:
        return None
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT state, data FROM fsm_states WHERE user_id = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {"state": row["state"], "data": row["data"]}
            return None


async def clear_fsm_state(chat_id: int) -> None:
    uid = await _resolve_user_id(chat_id=chat_id)
    if uid is None:
        return
    async with db_pool.connection() as conn:
        await conn.execute("DELETE FROM fsm_states WHERE user_id = ?", (uid,))
        await conn.commit()


# ===== VK FSM операции =====

async def save_vk_fsm_state(peer_id: int, state: str, data: Optional[str] = None) -> None:
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO vk_fsm_states (peer_id, state, data, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(peer_id) DO UPDATE SET
                   state = excluded.state,
                   data = excluded.data,
                   updated_at = CURRENT_TIMESTAMP""",
            (peer_id, state, data)
        )
        await conn.commit()


async def get_vk_fsm_state(peer_id: int) -> Optional[Dict[str, Any]]:
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT state, data FROM vk_fsm_states WHERE peer_id = ?", (peer_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {"state": row["state"], "data": row["data"]}
            return None


async def clear_vk_fsm_state(peer_id: int) -> None:
    async with db_pool.connection() as conn:
        await conn.execute("DELETE FROM vk_fsm_states WHERE peer_id = ?", (peer_id,))
        await conn.commit()


# ===== Операции с настройками дней рождения =====

BIRTHDAY_DEFAULTS = {
    "enabled": 0,
    "mode": "tomorrow",
    "notify_weekday": 1,
    "notify_hour": 7,
    "notify_minute": 0,
}


async def get_birthday_settings(user_id: int, child_id: int) -> Dict[str, Any]:
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT * FROM birthday_settings WHERE user_id = ? AND child_id = ?",
            (user_id, child_id)
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
    user_id: int,
    child_id: int,
    enabled: bool,
    mode: str,
    notify_weekday: int,
    notify_hour: int,
    notify_minute: int
) -> None:
    async with db_pool.connection() as conn:
        await conn.execute(
            """INSERT INTO birthday_settings
               (user_id, child_id, enabled, mode, notify_weekday, notify_hour, notify_minute, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(user_id, child_id) DO UPDATE SET
                   enabled = excluded.enabled,
                   mode = excluded.mode,
                   notify_weekday = excluded.notify_weekday,
                   notify_hour = excluded.notify_hour,
                   notify_minute = excluded.notify_minute,
                   updated_at = CURRENT_TIMESTAMP""",
            (user_id, child_id, 1 if enabled else 0, mode, notify_weekday, notify_hour, notify_minute)
        )
        await conn.commit()


async def get_all_birthday_settings(user_id: int) -> List[Dict[str, Any]]:
    async with db_pool.connection() as conn:
        async with conn.execute(
            "SELECT * FROM birthday_settings WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "user_id": row["user_id"],
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
    """Получить пользователей с включёнными ДР-уведомлениями (по каналам)."""
    async with db_pool.connection() as conn:
        async with conn.execute(
            """SELECT u.id as user_id, u.chat_id, u.peer_id, u.login, u.password_encrypted,
                      ns.channel, bs.child_id, bs.mode, bs.notify_weekday, bs.notify_hour, bs.notify_minute
               FROM users u
               INNER JOIN notification_settings ns ON u.id = ns.user_id
               INNER JOIN birthday_settings bs ON u.id = bs.user_id
               WHERE ns.birthday_enabled = 1 AND bs.enabled = 1"""
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "user_id": row["user_id"],
                    "chat_id": row["chat_id"],
                    "peer_id": row["peer_id"],
                    "login": row["login"],
                    "password_encrypted": row["password_encrypted"],
                    "channel": row["channel"],
                    "child_id": row["child_id"],
                    "mode": row["mode"],
                    "notify_weekday": row["notify_weekday"],
                    "notify_hour": row["notify_hour"],
                    "notify_minute": row["notify_minute"],
                }
                for row in rows
            ]
