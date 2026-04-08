"""
Конфигурация бота с загрузкой из переменных окружения.
Поддерживает Telegram и VK ботов (один или оба).
"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    """Конфигурация приложения с валидацией."""

    # Telegram
    bot_token: str = field(default_factory=lambda: os.getenv("BOT_TOKEN", ""))

    # VK
    vk_token: str = field(default_factory=lambda: os.getenv("VK_TOKEN", ""))

    # Encryption
    encryption_key: str = field(default_factory=lambda: os.getenv("ENCRYPTION_KEY", ""))

    # Admin
    admin_ids: List[int] = field(default_factory=lambda: _parse_int_list(os.getenv("ADMIN_IDS", "")))

    # Database
    database_url: str = field(
        default_factory=lambda: os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/ruobr_bot.db")
    )

    # Rate Limiting
    rate_limit_per_minute: int = field(
        default_factory=lambda: int(os.getenv("RATE_LIMIT_PER_MINUTE", "30"))
    )
    rate_limit_ruobr_per_minute: int = field(
        default_factory=lambda: int(os.getenv("RATE_LIMIT_RUOBR_PER_MINUTE", "10"))
    )

    # Background Tasks
    check_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
    )
    default_balance_threshold: float = field(
        default_factory=lambda: float(os.getenv("DEFAULT_BALANCE_THRESHOLD", "300.0"))
    )

    # Caching
    cache_ttl_seconds: int = field(
        default_factory=lambda: int(os.getenv("CACHE_TTL_SECONDS", "300"))
    )
    cache_max_size: int = field(
        default_factory=lambda: int(os.getenv("CACHE_MAX_SIZE", "1000"))
    )

    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # Paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent)
    data_dir: Path = field(init=False)

    def __post_init__(self):
        """Валидация и инициализация путей."""
        self.data_dir = self.base_dir / "data"
        self.data_dir.mkdir(exist_ok=True)

        # At least one bot token is required
        if not self.bot_token and not self.vk_token:
            raise ValueError("BOT_TOKEN или VK_TOKEN должны быть указаны")

        if not self.encryption_key:
            raise ValueError("ENCRYPTION_KEY не указан в переменных окружения")

    @property
    def db_path(self) -> Path:
        """Путь к файлу базы данных SQLite."""
        # Извлекаем путь из URL
        if ":///" in self.database_url:
            db_path = self.database_url.split(":///")[1]
            return Path(db_path)
        return self.data_dir / "ruobr_bot.db"

    def is_admin(self, user_id: int) -> bool:
        """Проверка является ли пользователь администратором."""
        return user_id in self.admin_ids


def _parse_int_list(value: str) -> List[int]:
    """Парсинг списка целых чисел из строки."""
    if not value:
        return []
    try:
        return [int(x.strip()) for x in value.split(",") if x.strip()]
    except ValueError:
        return []


# Глобальный экземпляр конфигурации
config = Config()
