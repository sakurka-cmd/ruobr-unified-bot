#!/usr/bin/env python3
"""
Ruobr Unified Bot — Telegram + VK.
Запускает оба мессенджера с единым сервисом уведомлений.
"""
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

# ===== QRATOR User-Agent patch =====
# ruobr_api использует httpx.AsyncClient для запросов к api3d.ruobr.ru.
# QRATOR (DDoS-защита ruobr.ru) блокирует запросы с дефолтным
# User-Agent "python-httpx/x.y.z" → 403 Forbidden.
# Патчим константу USER_AGENT до импорта ruobr_api.
import httpx._client as _httpx_client
try:
    _httpx_client.USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
except Exception:
    pass  # non-critical

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

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
    LoggingMiddleware
)
from aiogram.types import CallbackQuery
from bot.handlers import auth, balance, schedule, birthday
from bot.services.notifications import NotificationService
from bot.services.cache import periodic_cache_cleanup


def create_proxied_session(proxy_url: str):
    """Создает сессию с прокси для aiogram 3.x"""
    import aiohttp
    from aiohttp_socks import ProxyConnector
    from aiogram.client.session.aiohttp import AiohttpSession

    connector = ProxyConnector.from_url(proxy_url)

    class ProxiedSession(AiohttpSession):
        def __init__(self):
            super().__init__(connector=connector)

        def _create_session(self) -> aiohttp.ClientSession:
            return aiohttp.ClientSession(connector=self._connector, trust_env=True)

    return ProxiedSession()


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(config.data_dir / "bot.log", encoding="utf-8")
        ]
    )
    logging.getLogger("aiogram").setLevel(logging.INFO)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("vkbottle").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

from bot.vk import run_vk_bot


async def main() -> None:
    setup_logging()
    logger.info("Starting Ruobr Unified Bot (TG+VK)")

    await db_pool.initialize()
    logger.info("Database initialized")

    # ===== TG Bot (existing — без изменений) =====
    proxy_url = os.getenv("BOT_PROXY", "")

    if proxy_url and SOCKS_SUPPORT:
        logger.info(f"Using proxy: {proxy_url[:30]}...")
        session = create_proxied_session(proxy_url)
        tg_bot = Bot(token=config.bot_token, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    else:
        tg_bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    dp.message.middleware(RateLimitMiddleware(limit=config.rate_limit_per_minute, window_seconds=60, block_duration=30))
    dp.message.middleware(AuthMiddleware())
    dp.message.middleware(LoggingMiddleware())
    dp.callback_query.middleware(AuthMiddleware())

    dp.include_router(auth.router)
    dp.include_router(balance.router)
    dp.include_router(schedule.router)
    dp.include_router(birthday.router)

    # ===== Global unhandled TG error handler =====
    @dp.errors()
    async def error_handler(event):
        logger.error(f"Unhandled TG handler error: {event.exception}", exc_info=event.exception)
        try:
            if event.update and event.update.event:
                msg = event.update.event.message
                if msg:
                    await tg_bot.send_message(msg.chat.id, "⚠️ Внутренняя ошибка бота. Попробуйте позже.")
        except Exception:
            pass

    # ===== Test TG connectivity =====
    try:
        me = await asyncio.wait_for(tg_bot.get_me(), timeout=15)
        logger.info(f"TG bot connected: @{me.username} (id={me.id})")
    except Exception as e:
        logger.error(f"TG bot connection failed: {type(e).__name__}: {e}")

    try:
        await asyncio.wait_for(
            tg_bot.delete_webhook(drop_pending_updates=True),
            timeout=30
        )
        logger.info("TG webhook deleted")
    except Exception as e:
        logger.warning(f"TG webhook deletion failed: {type(e).__name__}: {e}")

    # ===== VK Bot (optional) =====
    vk_bot_instance = None
    vk_api = None
    if config.vk_token:
        vk_bot_instance = await run_vk_bot(config.vk_token)
        if vk_bot_instance:
            vk_api = vk_bot_instance.api
            logger.info("VK Bot ready")

    # ===== Unified Notification Service =====
    notification_service = NotificationService(tg_bot, vk_api)

    # ===== Запуск =====
    notification_task = asyncio.create_task(notification_service.start())
    cache_cleanup_task = asyncio.create_task(periodic_cache_cleanup(interval=300))

    # TG polling
    async def tg_polling():
        try:
            await dp.start_polling(tg_bot, allowed_updates=["message", "callback_query"])
        except asyncio.CancelledError:
            pass

    # VK polling
    async def vk_polling():
        if vk_bot_instance:
            try:
                await vk_bot_instance.run_polling()
            except asyncio.CancelledError:
                pass

    vk_polling_task = None
    tasks = [notification_task, cache_cleanup_task, tg_polling()]
    if vk_bot_instance:
        vk_polling_task = asyncio.create_task(vk_polling())
        tasks.append(vk_polling_task)

    logger.info("All services started. Press Ctrl+C to stop.")

    # Signal handling
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("Shutdown signal received")
        notification_service.stop()
        notification_task.cancel()
        cache_cleanup_task.cancel()
        dp.stop_polling()
        if vk_polling_task:
            vk_polling_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down...")
        notification_task.cancel()
        cache_cleanup_task.cancel()
        if vk_polling_task:
            vk_polling_task.cancel()
        try:
            await notification_task
        except asyncio.CancelledError:
            pass
        try:
            await cache_cleanup_task
        except asyncio.CancelledError:
            pass
        if vk_polling_task:
            try:
                await vk_polling_task
            except asyncio.CancelledError:
                pass
        await db_pool.close()
        await tg_bot.session.close()
        logger.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)
