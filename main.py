#!/usr/bin/env python3
"""
Ruobr Telegram Bot - Главный файл запуска.

Улучшенная версия с:
- Модульной архитектурой
- Шифрованием паролей
- Асинхронными вызовами API
- Rate limiting
- Кэшированием
- Персистентным FSM
- SOCKS5 прокси
"""
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

# Прокси для обхода блокировки Telegram
try:
    from aiohttp_socks import ProxyConnector
    from aiogram.client.session.aiohttp import AiohttpSession
    SOCKS_SUPPORT = True
except ImportError:
    SOCKS_SUPPORT = False

from bot.config import config
from bot.database import db_pool
from bot.middlewares import (
    RateLimitMiddleware,
    AuthMiddleware,
    LoggingMiddleware,
    ThrottlingMiddleware
)
from aiogram.types import CallbackQuery
from bot.handlers import auth, balance, schedule
from bot.services.notifications import NotificationService
from bot.services.cache import periodic_cache_cleanup


def create_proxied_session(proxy_url: str):
    """Создает сессию с прокси для aiogram 3.x"""
    import aiohttp
    from aiohttp_socks import ProxyConnector
    from aiogram.client.session.aiohttp import AiohttpSession
    
    class ProxiedSession(AiohttpSession):
        def __init__(self, proxy_url: str):
            super().__init__()
            self._proxy_url = proxy_url
        
        def _create_session(self) -> aiohttp.ClientSession:
            connector = ProxyConnector.from_url(self._proxy_url)
            return aiohttp.ClientSession(connector=connector)
    
    return ProxiedSession(proxy_url)


# Настройка логирования
def setup_logging() -> None:
    """Настройка системы логирования."""
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                config.data_dir / "bot.log",
                encoding="utf-8"
            )
        ]
    )
    
    # Уменьшаем уровень логирования для aiogram
    logging.getLogger("aiogram").setLevel(logging.INFO)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


async def main() -> None:
    """Главная функция запуска бота."""
    # Настройка логирования
    setup_logging()
    logger.info("Starting Ruobr Telegram Bot v2.0")
    
    # Инициализация базы данных
    await db_pool.initialize()
    logger.info("Database initialized")
    
    # Создание бота с поддержкой прокси
    proxy_url = os.getenv("BOT_PROXY", "")
    
    if proxy_url and SOCKS_SUPPORT:
        logger.info(f"Using proxy: {proxy_url[:30]}...")
        session = create_proxied_session(proxy_url)
        bot = Bot(
            token=config.bot_token,
            session=session,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
    elif proxy_url and not SOCKS_SUPPORT:
        logger.warning("Proxy configured but aiohttp-socks not installed!")
        bot = Bot(
            token=config.bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
    else:
        bot = Bot(
            token=config.bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
    
    # Используем MemoryStorage для FSM
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Регистрация middleware для сообщений
    dp.message.middleware(RateLimitMiddleware(
        limit=config.rate_limit_per_minute,
        window_seconds=60,
        block_duration=30
    ))
    dp.message.middleware(AuthMiddleware())
    dp.message.middleware(LoggingMiddleware())
    dp.message.middleware(ThrottlingMiddleware())
    
    # Регистрация middleware для callback queries
    dp.callback_query.middleware(AuthMiddleware())
    
    # Регистрация routers
    dp.include_router(auth.router)
    dp.include_router(balance.router)
    dp.include_router(schedule.router)
    
    # Сервис уведомлений
    notification_service = NotificationService(bot)
    
    # Удаляем вебхук если был установлен
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Webhook deleted, starting polling...")
    
    # Запуск фоновых задач
    notification_task = asyncio.create_task(notification_service.start())
    cache_cleanup_task = asyncio.create_task(periodic_cache_cleanup(interval=300))
    
    logger.info("Bot started. Press Ctrl+C to stop.")
    
    # Обработка сигналов для корректного завершения
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        notification_service.stop()
        notification_task.cancel()
        cache_cleanup_task.cancel()
        dp.stop_polling()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass
    
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    except asyncio.CancelledError:
        logger.info("Polling cancelled")
    finally:
        logger.info("Shutting down...")
        
        notification_task.cancel()
        cache_cleanup_task.cancel()
        
        try:
            await notification_task
        except asyncio.CancelledError:
            pass
        
        try:
            await cache_cleanup_task
        except asyncio.CancelledError:
            pass
        
        await db_pool.close()
        await bot.session.close()
        
        logger.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)
