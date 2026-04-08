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

    class ProxiedSession(AiohttpSession):
        def __init__(self, proxy_url: str):
            super().__init__()
            self._proxy_url = proxy_url

        def _create_session(self) -> aiohttp.ClientSession:
            connector = ProxyConnector.from_url(self._proxy_url)
            return aiohttp.ClientSession(connector=connector)

    return ProxiedSession(proxy_url)


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


logger = logging.getLogger(__name__)


async def run_vk_bot(vk_token: str):
    """Запуск VK бота (опционально)."""
    try:
        from vkbottle import Bot as VKBot, VKAPIError
        from vkbottle.bot import Message
        from vkbottle import Keyboard, KeyboardButtonColor, Text

        vk_bot = VKBot(token=vk_token)
        vk_labeler = vk_bot.labeler
        logger.info("VK Bot initialized")

        from bot.database import (
            get_user, get_all_thresholds_for_chat, create_or_update_user,
            create_link_code, consume_link_code, link_accounts, unlink_channel
        )
        from bot.services import get_children_async, get_food_for_children, get_timetable_for_children, AuthenticationError
        from bot.utils.formatters import format_balance, format_food_visit, format_date, format_lesson, format_mark, format_weekday, truncate_text

        # ===== VK Keyboards =====
        def get_vk_main_keyboard():
            return (
                Keyboard(one_time=False, inline=False)
                .add(Text("📅 Расписание сегодня"), color=KeyboardButtonColor.PRIMARY)
                .add(Text("📅 Расписание завтра"), color=KeyboardButtonColor.PRIMARY)
                .row()
                .add(Text("💰 Баланс питания"), color=KeyboardButtonColor.POSITIVE)
                .add(Text("🍽 Питание сегодня"), color=KeyboardButtonColor.POSITIVE)
                .row()
                .add(Text("⚙️ Настройки"), color=KeyboardButtonColor.NEGATIVE)
                .add(Text("👤 Мой профиль"), color=KeyboardButtonColor.SECONDARY)
            ).get_json()

        def get_vk_settings_keyboard():
            return (
                Keyboard(one_time=False, inline=False)
                .add(Text("🔔 Уведомления"), color=KeyboardButtonColor.PRIMARY)
                .add(Text("👤 Мой профиль"), color=KeyboardButtonColor.PRIMARY)
                .row()
                .add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
            ).get_json()

        # ===== VK Commands =====
        @vk_labeler.message(text="/start")
        async def vk_start(message: Message):
            user = await get_user(peer_id=message.peer_id)
            is_auth = user and user.login
            text = ("👋 Школьный бот — ВК версия\n\n")
            if not is_auth:
                text += "⚠️ Настройте учётные данные: /set_login\n\n"
            else:
                text += "✅ Учётные данные настроены.\n\n"
            text += "📖 /set_login — логин/пароль\n/balance — баланс питания\n"
            text += "🔗 /link_tg — привязать Telegram"
            await message.answer(text, keyboard=get_vk_main_keyboard())

        @vk_labeler.message(text="/set_login")
        async def vk_set_login(message: Message):
            from bot.database import save_vk_fsm_state, get_vk_fsm_state
            await save_vk_fsm_state(message.peer_id, "waiting_for_login")
            await message.answer("🔐 Введите логин от cabinet.ruobr.ru:")

        @vk_labeler.message(text="/link_tg")
        async def vk_link_tg(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.id:
                await message.answer("❌ Сначала настройте логин/пароль: /set_login")
                return
            code = await create_link_code(user.id, source="vk")
            await message.answer(
                f"🔗 Привязка Telegram\n\n"
                f"Отправьте этот код боту в Telegram:\n\n"
                f"/link_vk {code}\n\n"
                f"⏰ Код действителен 10 минут."
            )

        @vk_labeler.message(text="/unlink_tg")
        async def vk_unlink_tg(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.id:
                return
            await unlink_channel(user.id, "tg")
            await message.answer("✅ Telegram отвязан.")

        @vk_labeler.message(text="👤 Мой профиль")
        async def vk_profile(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user:
                await message.answer("❌ Профиль не найден. Используйте /start")
                return
            status = "✅ Настроен" if user.login and user.password_encrypted else "❌ Не настроен"
            tg_linked = user.chat_id is not None
            if tg_linked:
                tg_info = f"  📱 Telegram: ✅ привязан (id: {user.chat_id})"
            else:
                tg_info = "  📱 Telegram: ❌ не привязан"
            text = (
                f"👤 Ваш профиль\n\n"
                f"Статус: {status}\n"
                f"Логин: {user.login or 'не указан'}\n\n"
                f"🔗 Связанные аккаунты:\n"
                f"  💬 VK: ✅\n"
                f"{tg_info}\n\n"
            )
            if not tg_linked:
                text += (
                    "💡 Для привязки Telegram:\n"
                    "• /link_tg — получить код для отправки в TG бот\n"
                    "• Или в TG боте нажмите 'Привязать VK' и отправьте код сюда"
                )
            else:
                text += "💡 /unlink_tg — отвязать Telegram"
            await message.answer(text)

        @vk_labeler.message(text="⚙️ Настройки")
        async def vk_settings(message: Message):
            await message.answer("⚙️ Настройки", keyboard=get_vk_settings_keyboard())

        @vk_labeler.message(text="◀️ Назад")
        async def vk_back(message: Message):
            await message.answer("🏠 Главное меню", keyboard=get_vk_main_keyboard())

        @vk_labeler.message(text="/balance")
        async def vk_balance(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await message.answer("❌ Сначала настройте логин/пароль: /set_login")
                return
            from bot.credentials import safe_decrypt
            login, password = safe_decrypt(user)
            try:
                children = await get_children_async(login, password)
            except Exception:
                await message.answer("❌ Ошибка авторизации.")
                return
            if not children:
                await message.answer("❌ Дети не найдены.")
                return
            try:
                food_info = await get_food_for_children(login, password, children)
                thresholds = await get_all_thresholds_for_chat(peer_id=message.peer_id)
                lines = ["💰 Баланс питания\n"]
                for idx, child in enumerate(children, 1):
                    info = food_info.get(child.id)
                    threshold = thresholds.get(child.id, config.default_balance_threshold)
                    if info and info.has_food:
                        lines.append(f"{idx}. {format_balance(child, info.balance, threshold)}")
                    else:
                        lines.append(f"{idx}. {child.full_name} ({child.group}): питание недоступно")
                await message.answer("\n".join(lines))
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")

        @vk_labeler.message(text="📅 Расписание сегодня")
        async def vk_ttoday(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await message.answer("❌ Сначала настройте логин/пароль: /set_login")
                return
            from bot.credentials import safe_decrypt
            login, password = safe_decrypt(user)
            try:
                children = await get_children_async(login, password)
            except Exception:
                await message.answer("❌ Ошибка авторизации.")
                return
            try:
                today = date.today()
                timetable = await get_timetable_for_children(login, password, children, today, today)
                lines = [f"📅 Расписание на сегодня ({format_weekday(today)})"]
                found = False
                for child in children:
                    lessons = timetable.get(child.id, [])
                    if lessons:
                        found = True
                        lines.append(f"\n👦 {child.full_name} ({child.group}):")
                        for lesson in lessons:
                            lines.append(format_lesson(lesson, show_details=True))
                await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На сегодня расписания нет.")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")

        @vk_labeler.message(text="📅 Расписание завтра")
        async def vk_ttomorrow(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await message.answer("❌ Сначала настройте логин/пароль: /set_login")
                return
            from bot.credentials import safe_decrypt
            login, password = safe_decrypt(user)
            try:
                children = await get_children_async(login, password)
            except Exception:
                await message.answer("❌ Ошибка авторизации.")
                return
            try:
                tomorrow = date.today() + timedelta(days=1)
                timetable = await get_timetable_for_children(login, password, children, tomorrow, tomorrow)
                lines = [f"📅 Расписание на завтра ({format_weekday(tomorrow)})"]
                found = False
                for child in children:
                    lessons = timetable.get(child.id, [])
                    if lessons:
                        found = True
                        lines.append(f"\n👦 {child.full_name} ({child.group}):")
                        for lesson in lessons:
                            lines.append(format_lesson(lesson, show_details=True))
                await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На завтра расписания нет.")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")

        @vk_labeler.message(text="🍽 Питание сегодня")
        async def vk_food(message: Message):
            user = await get_user(peer_id=message.peer_id)
            if not user or not user.login:
                await message.answer("❌ Сначала настройте логин/пароль: /set_login")
                return
            from bot.credentials import safe_decrypt
            login, password = safe_decrypt(user)
            try:
                children = await get_children_async(login, password)
            except Exception:
                await message.answer("❌ Ошибка авторизации.")
                return
            try:
                today_str = date.today().strftime("%Y-%m-%d")
                food_info = await get_food_for_children(login, password, children)
                lines = [f"🍽 Питание сегодня ({format_date(today_str)})"]
                found = False
                for child in children:
                    info = food_info.get(child.id)
                    if info and info.visits:
                        for visit in info.visits:
                            if visit.get("date") == today_str and (visit.get("ordered") or visit.get("state") == 30):
                                found = True
                                lines.append(format_food_visit(visit, child.full_name))
                await message.answer(truncate_text("\n".join(lines)) if found else "ℹ️ На сегодня питания не найдено.")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")

        @vk_labeler.message(text="🔔 Уведомления")
        async def vk_notifications(message: Message):
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            bal = "✅" if user.vk_balance_enabled else "❌"
            marks = "✅" if user.vk_marks_enabled else "❌"
            food = "✅" if user.vk_food_enabled else "❌"
            text = (
                f"🔔 Настройки уведомлений (VK)\n\n"
                f"💰 Баланс: {bal} — /bal_notify\n"
                f"⭐ Оценки: {marks} — /marks_notify\n"
                f"🍽 Питание: {food} — /food_notify"
            )
            await message.answer(text)

        @vk_labeler.message(text="/bal_notify")
        async def vk_toggle_balance(message: Message):
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            new_val = not user.vk_balance_enabled
            await create_or_update_user(peer_id=message.peer_id, vk_balance_enabled=new_val)
            status = "включены" if new_val else "выключены"
            await message.answer(f"💰 Уведомления о балансе (VK): {status}")

        @vk_labeler.message(text="/marks_notify")
        async def vk_toggle_marks(message: Message):
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            new_val = not user.vk_marks_enabled
            await create_or_update_user(peer_id=message.peer_id, vk_marks_enabled=new_val)
            status = "включены" if new_val else "выключены"
            await message.answer(f"⭐ Уведомления об оценках (VK): {status}")

        @vk_labeler.message(text="/food_notify")
        async def vk_toggle_food(message: Message):
            user = await get_user(peer_id=message.peer_id) or await create_or_update_user(peer_id=message.peer_id)
            new_val = not user.vk_food_enabled
            await create_or_update_user(peer_id=message.peer_id, vk_food_enabled=new_val)
            status = "включены" if new_val else "выключены"
            await message.answer(f"🍽 Уведомления о питании (VK): {status}")

        # FSM: ввод кода привязки от TG
        @vk_labeler.message(text="/enter_code")
        async def vk_enter_link_code_start(message: Message):
            from bot.database import save_vk_fsm_state
            await save_vk_fsm_state(message.peer_id, "waiting_for_link_code")
            await message.answer(
                "📲 Ввод кода привязки от Telegram\n\n"
                "Отправьте код, полученный в TG боте:\n\n"
                "❌ Отмена — для выхода"
            )

        # FSM: ввод логина/пароля / авто-приём кода привязки
        @vk_labeler.message()
        async def vk_handle_all(message: Message):
            text = (message.text or "").strip()
            if not text:
                return

            from bot.database import get_vk_fsm_state, save_vk_fsm_state, clear_vk_fsm_state

            # Авто-приём кода привязки (8 символов, без FSM)
            if len(text) == 8 and text.isalnum():
                result = await consume_link_code(text.upper())
                if result is not None:
                    tg_user_id, source = result
                    if source == "tg":
                        from bot.database import get_user_by_id
                        tg_user = await get_user_by_id(tg_user_id)
                        if tg_user and tg_user.chat_id:
                            current = await get_user(peer_id=message.peer_id)
                            if current and current.id:
                                await link_accounts(current.id, chat_id=tg_user.chat_id)
                                await message.answer(
                                    f"✅ Telegram аккаунт привязан! (id: {tg_user.chat_id})\n\n"
                                    "Теперь уведомления будут приходить и в Telegram.",
                                    keyboard=get_vk_main_keyboard()
                                )
                                return
                    await message.answer("❌ Этот код предназначен для другого мессенджера или уже использован.")
                # Не 8-значный код — идём дальше в FSM

            state = await get_vk_fsm_state(message.peer_id)
            if not state:
                return

            state_name = state["state"]

            if text in ("/cancel", "❌ Отмена"):
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("❌ Отменено.", keyboard=get_vk_main_keyboard())
                return

            if state_name == "waiting_for_link_code":
                code = text.upper().strip()
                if len(code) != 8:
                    await message.answer("❌ Код должен содержать 8 символов. Попробуйте ещё раз:")
                    return
                result = await consume_link_code(code)
                if result is None:
                    await message.answer(
                        "❌ Неверный или просроченный код.\n\n"
                        "Получите новый код в TG боте и попробуйте снова."
                    )
                    return
                tg_user_id, source = result
                if source != "tg":
                    await message.answer("❌ Этот код предназначен для TG бота, а не для VK.")
                    return
                from bot.database import get_user_by_id
                tg_user = await get_user_by_id(tg_user_id)
                if not tg_user or not tg_user.chat_id:
                    await message.answer("⚠️ TG аккаунт найден, но привязка не удалась.")
                    return
                current = await get_user(peer_id=message.peer_id)
                if not current or not current.id:
                    await clear_vk_fsm_state(message.peer_id)
                    await message.answer("❌ Профиль не найден.")
                    return
                await link_accounts(current.id, chat_id=tg_user.chat_id)
                await clear_vk_fsm_state(message.peer_id)
                await message.answer(
                    f"✅ Telegram аккаунт привязан! (id: {tg_user.chat_id})\n\n"
                    "Теперь уведомления будут приходить и в Telegram.",
                    keyboard=get_vk_main_keyboard()
                )
                return

            if state_name == "waiting_for_login":
                if len(text) > 100:
                    await message.answer("❌ Логин слишком длинный.")
                    return
                await save_vk_fsm_state(message.peer_id, "waiting_for_password", data=text)
                await message.answer("✅ Логин сохранён.\nВведите пароль:")

            elif state_name == "waiting_for_password":
                payload = state.get("data") or ""
                from bot.credentials import safe_decrypt
                try:
                    children = await get_children_async(payload, text)
                except AuthenticationError:
                    await clear_vk_fsm_state(message.peer_id)
                    await message.answer("❌ Неверный логин или пароль. Попробуйте: /set_login")
                    return
                except Exception:
                    await clear_vk_fsm_state(message.peer_id)
                    await message.answer("❌ Ошибка соединения.")
                    return

                if not children:
                    await message.answer("⚠️ Данные верны, но дети не найдены.")
                else:
                    names = "\n".join([f"  • {c.full_name} ({c.group})" for c in children])
                    await message.answer(f"✅ Авторизация успешна!\n\nДети:\n{names}")

                # Проверяем, нет ли уже юзера с таким login → предложить привязку
                from bot.database import get_all_enabled_users
                existing = await get_user(peer_id=message.peer_id)
                if existing and existing.login:
                    await create_or_update_user(peer_id=message.peer_id, login=payload, password=text)
                else:
                    await create_or_update_user(peer_id=message.peer_id, login=payload, password=text)
                await clear_vk_fsm_state(message.peer_id)
                await message.answer("🏠 Готово!", keyboard=get_vk_main_keyboard())

        logger.info("VK Bot handlers registered")
        return vk_bot

    except ImportError as e:
        logger.warning(f"VK bot not available: {e}")
        return None


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

    await tg_bot.delete_webhook(drop_pending_updates=True)
    logger.info("TG webhook deleted")

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

    tasks = [notification_task, cache_cleanup_task, tg_polling()]
    if vk_bot_instance:
        tasks.append(vk_polling())

    logger.info("All services started. Press Ctrl+C to stop.")

    # Signal handling
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("Shutdown signal received")
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
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass
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
