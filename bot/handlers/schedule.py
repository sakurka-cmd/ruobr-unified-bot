"""
Обработчики для расписания, ДЗ и оценок.
"""
import asyncio
import io
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError

from ..config import config
from ..database import UserConfig
from ..services import (
    Child, Lesson, get_children_async, get_timetable_for_children,
    RuobrError, NetworkError, AuthenticationError, download_homework_file
)
from ..utils.formatters import (
    format_lesson, format_homework, format_mark, format_date,
    format_weekday, truncate_text, extract_homework_files, 
    clean_html_text, has_meaningful_text, normalize_date_to_iso
)
from .balance import require_authentication

logger = logging.getLogger(__name__)

router = Router()

# Таймаут для сетевых операций (секунды)
NETWORK_TIMEOUT = 30




async def safe_edit_message(status_msg: Message, text: str) -> bool:
    """
    Безопасное редактирование сообщения с обработкой ошибок.
    
    Returns:
        True если успешно, False если ошибка.
    """
    try:
        await asyncio.wait_for(
            status_msg.edit_text(text),
            timeout=NETWORK_TIMEOUT
        )
        return True
    except asyncio.TimeoutError:
        logger.warning(f"Timeout editing message")
        return False
    except TelegramNetworkError as e:
        logger.error(f"Network error editing message: {e}")
        return False
    except TelegramAPIError as e:
        logger.error(f"API error editing message: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error editing message: {e}")
        return False


# ===== Расписание на сегодня =====

@router.message(Command("ttoday"))
@router.message(F.text == "📅 Расписание сегодня")
async def cmd_ttoday(message: Message, user_config: Optional[UserConfig] = None):
    """Показать расписание на сегодня."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка расписания...")
    
    try:
        today = date.today()
        timetable = await asyncio.wait_for(
            get_timetable_for_children(login, password, children, today, today),
            timeout=NETWORK_TIMEOUT
        )
        
        lines = [f"📅 <b>Расписание на сегодня</b> ({format_date(str(today))}, {format_weekday(today)})"]
        found = False
        
        for child in children:
            lessons = timetable.get(child.id, [])
            if not lessons:
                continue
            
            found = True
            lines.append(f"\n👦 {child.full_name} ({child.group}):")
            
            for lesson in lessons:
                lines.append(format_lesson(lesson, show_details=True))
        
        if not found:
            await safe_edit_message(status_msg, "ℹ️ На сегодня расписание не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await safe_edit_message(status_msg, text)
            
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting timetable for user {message.chat.id}")
        await safe_edit_message(status_msg, "⏱ Превышено время ожидания. Попробуйте позже.")
    except NetworkError:
        logger.error(f"Network error getting timetable for user {message.chat.id}")
        await safe_edit_message(status_msg, "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже.")
    except AuthenticationError:
        logger.error(f"Auth error getting timetable for user {message.chat.id}")
        await safe_edit_message(status_msg, "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль.")
    except TelegramNetworkError as e:
        logger.error(f"Network error for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, "📡 Ошибка сети. Проверьте подключение.")
    except Exception as e:
        logger.error(f"Error getting timetable for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, f"❌ Ошибка получения расписания: {e}")


# ===== Расписание на завтра =====

@router.message(Command("ttomorrow"))
@router.message(F.text == "📅 Расписание завтра")
async def cmd_ttomorrow(message: Message, user_config: Optional[UserConfig] = None):
    """Показать расписание на завтра."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка расписания...")
    
    try:
        tomorrow = date.today() + timedelta(days=1)
        timetable = await asyncio.wait_for(
            get_timetable_for_children(login, password, children, tomorrow, tomorrow),
            timeout=NETWORK_TIMEOUT
        )
        
        lines = [f"📅 <b>Расписание на завтра</b> ({format_date(str(tomorrow))}, {format_weekday(tomorrow)})"]
        found = False
        
        for child in children:
            lessons = timetable.get(child.id, [])
            if not lessons:
                continue
            
            found = True
            lines.append(f"\n👦 {child.full_name} ({child.group}):")
            
            for lesson in lessons:
                lines.append(format_lesson(lesson, show_details=True))
        
        if not found:
            await safe_edit_message(status_msg, "ℹ️ На завтра расписание не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await safe_edit_message(status_msg, text)
            
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting timetable for user {message.chat.id}")
        await safe_edit_message(status_msg, "⏱ Превышено время ожидания. Попробуйте позже.")
    except NetworkError:
        logger.error(f"Network error getting timetable for user {message.chat.id}")
        await safe_edit_message(status_msg, "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже.")
    except AuthenticationError:
        logger.error(f"Auth error getting timetable for user {message.chat.id}")
        await safe_edit_message(status_msg, "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль.")
    except TelegramNetworkError as e:
        logger.error(f"Network error for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, "📡 Ошибка сети. Проверьте подключение.")
    except Exception as e:
        logger.error(f"Error getting timetable for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, f"❌ Ошибка получения расписания: {e}")


# ===== Домашнее задание на завтра =====

@router.message(Command("hwtomorrow"))
@router.message(F.text == "📘 ДЗ на завтра")
async def cmd_hwtomorrow(message: Message, user_config: Optional[UserConfig] = None):
    """Показать ДЗ на завтра."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка домашнего задания...")
    
    try:
        today = date.today()
        tomorrow = today + timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        
        # Запрашиваем расписание на 14 дней, чтобы точно покрыть завтра
        # (если сегодня воскресенье, завтра = следующий понедельник)
        end = today + timedelta(days=14)
        
        timetable = await asyncio.wait_for(
            get_timetable_for_children(login, password, children, today, end),
            timeout=NETWORK_TIMEOUT
        )
        
        # DEBUG: логируем все уроки с ДЗ (INFO for troubleshooting)
        logger.info(f"HW check: tomorrow={tomorrow_str}, children={len(children)}")
        for child in children:
            lessons = timetable.get(child.id, [])
            for lesson in lessons:
                if lesson.homework:
                    logger.info(
                        f"HW lesson: child={child.id} date={lesson.date} "
                        f"subject={lesson.subject} hw_count={len(lesson.homework)}"
                    )
                    for idx, hw in enumerate(lesson.homework):
                        import json
                        logger.info(f"  HW item [{idx}] FULL DUMP: {json.dumps(hw, ensure_ascii=False, default=str)[:2000]}")
        
        lines = [f"📘 <b>Домашнее задание на завтра</b> ({format_date(tomorrow_str)})"]
        found = False
        
        # Собираем все файлы для отправки отдельно
        all_files: List[Tuple[str, str, str]] = []  # (file_type, url, subject)
        
        for child in children:
            lessons = timetable.get(child.id, [])
            child_header_added = False
            
            for lesson in lessons:
                # Фильтруем по дедлайну или по дате урока
                relevant_hw = []
                for hw in lesson.homework:
                    hw_deadline = normalize_date_to_iso(hw.get("deadline", ""))
                    if hw_deadline and hw_deadline == tomorrow_str:
                        # Есть явный дедлайн на завтра
                        relevant_hw.append(hw)
                    elif not hw_deadline and lesson.date == tomorrow_str:
                        # Нет дедлайна — считаем ДЗ привязанным к дате урока
                        relevant_hw.append(hw)
                
                if not relevant_hw:
                    # Логируем для отладки: есть ли ДЗ с другим дедлайном
                    if lesson.homework and lesson.date == tomorrow_str:
                        for hw in lesson.homework:
                            raw_dl = hw.get("deadline", "")
                            norm_dl = normalize_date_to_iso(raw_dl)
                            logger.debug(
                                f"HW filtered out: subject={lesson.subject}, "
                                f"lesson_date={lesson.date}, tomorrow={tomorrow_str}, "
                                f"raw_deadline={raw_dl!r}, normalized_deadline={norm_dl!r}"
                            )
                    continue
                
                found = True
                if not child_header_added:
                    lines.append(f"\n👦 {child.full_name} ({child.group}):")
                    child_header_added = True
                
                for hw in relevant_hw:
                    title = hw.get("title", "")
                    lines.append(f"  📖 {lesson.subject}: {title}")
                    
                    # Показываем текст ДЗ если есть полезная информация
                    hw_text = hw.get("text", "")
                    if has_meaningful_text(hw_text):
                        clean_text = clean_html_text(hw_text)
                        # Ограничиваем длину текста (Telegram limit ~4096 per message)
                        if len(clean_text) > 500:
                            clean_text = clean_text[:497] + "..."
                        lines.append(f"     📝 {clean_text}")
                    
                    # Собираем файлы для отправки
                    files = extract_homework_files(hw_text)
                    for file_type, file_url in files:
                        all_files.append((file_type, file_url, lesson.subject))

                    # Обработка нового поля doc (вложения документов)
                    if hw.get("doc"):
                        doc_str = hw.get("doc_str", "")
                        logger.info(f"DOC attachment found: doc_str={doc_str!r} hw_id={hw.get('id')}")
                        if doc_str:
                            # doc_str может быть URL, путём или именем файла
                            if doc_str.startswith(("http", "//")):
                                file_url = "https:" + doc_str if doc_str.startswith("//") else doc_str
                                all_files.append(("doc", file_url, lesson.subject))
                            elif doc_str.startswith("/"):
                                all_files.append(("doc", f"https://ruobr.ru{doc_str}", lesson.subject))
                            else:
                                # Попробуем как относительный путь к медиа
                                all_files.append(("doc", f"https://ruobr.ru/media/{doc_str}", lesson.subject))

                # Обработка docs_for_lesson (новое поле — вложения на уровне урока)
                if lesson.docs_for_lesson:
                    logger.info(f"docs_for_lesson for {lesson.subject}: count={len(lesson.docs_for_lesson)}")
                    for doc_item in lesson.docs_for_lesson:
                        if isinstance(doc_item, dict):
                            doc_file = doc_item.get("file", "")
                            doc_name = doc_item.get("name", "") or doc_item.get("title", "")
                            doc_url = doc_item.get("url", "") or doc_item.get("link", "")
                            logger.info(f"  doc_item: {doc_item}")
                            # Ищем URL в любом из полей
                            for field_val in [doc_file, doc_url, doc_name]:
                                if field_val and isinstance(field_val, str):
                                    if field_val.startswith(("http", "//")):
                                        url = "https:" + field_val if field_val.startswith("//") else field_val
                                        all_files.append(("doc", url, lesson.subject))
                                    elif field_val.startswith("/"):
                                        all_files.append(("doc", f"https://ruobr.ru{field_val}", lesson.subject))
        
        if not found:
            await safe_edit_message(status_msg, "ℹ️ На завтра домашнее задание не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await safe_edit_message(status_msg, text)
            
            # Отправляем файлы отдельными сообщениями
            if all_files:
                for file_type, file_url, subject in all_files:
                    sent = False
                    # 1) Скачиваем файл через авторизованную сессию Ruobr
                    downloaded = await download_homework_file(file_url, login, password)
                    if downloaded:
                        file_bytes, filename = downloaded
                        try:
                            if file_type == 'img':
                                await asyncio.wait_for(
                                    message.answer_photo(
                                        photo=io.BytesIO(file_bytes),
                                        caption=f"📎 {subject}"
                                    ),
                                    timeout=NETWORK_TIMEOUT
                                )
                            else:
                                await asyncio.wait_for(
                                    message.answer_document(
                                        document=io.BytesIO(file_bytes),
                                        filename=filename,
                                        caption=f"📎 {subject}"
                                    ),
                                    timeout=NETWORK_TIMEOUT
                                )
                            sent = True
                            logger.info(f"Sent file attachment: {filename} ({len(file_bytes)} bytes)")
                        except (TelegramAPIError, asyncio.TimeoutError) as e:
                            logger.warning(f"Failed to send attachment {filename}: {e}")

                    # 2) Fallback: передаём URL напрямую (Telegram скачает сам)
                    if not sent:
                        try:
                            if file_type == 'img':
                                await asyncio.wait_for(
                                    message.answer_photo(photo=file_url, caption=f"📎 {subject}"),
                                    timeout=NETWORK_TIMEOUT
                                )
                            else:
                                await asyncio.wait_for(
                                    message.answer_document(document=file_url, caption=f"📎 {subject}"),
                                    timeout=NETWORK_TIMEOUT
                                )
                            sent = True
                        except (TelegramAPIError, asyncio.TimeoutError) as e:
                            logger.warning(f"Failed to send file by URL {file_url}: {e}")

                    # 3) Последний fallback: отправляем ссылку текстом
                    if not sent:
                        try:
                            await message.answer(f"📎 <a href=\"{file_url}\">Файл: {subject}</a>")
                        except Exception:
                            pass
            
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting homework for user {message.chat.id}")
        await safe_edit_message(status_msg, "⏱ Превышено время ожидания. Попробуйте позже.")
    except NetworkError:
        logger.error(f"Network error getting homework for user {message.chat.id}")
        await safe_edit_message(status_msg, "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже.")
    except AuthenticationError:
        logger.error(f"Auth error getting homework for user {message.chat.id}")
        await safe_edit_message(status_msg, "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль.")
    except TelegramNetworkError as e:
        logger.error(f"Network error for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, "📡 Ошибка сети. Проверьте подключение.")
    except Exception as e:
        logger.error(f"Error getting homework for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, f"❌ Ошибка получения ДЗ: {e}")


# ===== Оценки за сегодня =====

@router.message(Command("markstoday"))
@router.message(F.text == "⭐ Оценки сегодня")
async def cmd_markstoday(message: Message, user_config: Optional[UserConfig] = None):
    """Показать оценки за сегодня."""
    result = await require_authentication(message, user_config)
    if result is None:
        return
    
    login, password, children = result
    
    status_msg = await message.answer("🔄 Загрузка оценок...")
    
    try:
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")
        
        timetable = await asyncio.wait_for(
            get_timetable_for_children(login, password, children, today, today),
            timeout=NETWORK_TIMEOUT
        )
        
        lines = [f"⭐ <b>Оценки за сегодня</b> ({format_date(today_str)})"]
        found = False
        
        for child in children:
            lessons = timetable.get(child.id, [])
            child_header_added = False
            
            for lesson in lessons:
                if not lesson.marks:
                    continue
                
                if not child_header_added:
                    lines.append(f"\n👦 {child.full_name} ({child.group}):")
                    child_header_added = True
                
                for mark in lesson.marks:
                    found = True
                    mark_str = format_mark(mark, lesson.subject)
                    lines.append(f"  {mark_str}")
        
        if not found:
            await safe_edit_message(status_msg, "ℹ️ За сегодня оценок не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await safe_edit_message(status_msg, text)
            
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting marks for user {message.chat.id}")
        await safe_edit_message(status_msg, "⏱ Превышено время ожидания. Попробуйте позже.")
    except NetworkError:
        logger.error(f"Network error getting marks for user {message.chat.id}")
        await safe_edit_message(status_msg, "⚠️ Сервис ruobr.ru недоступен. Попробуйте позже.")
    except AuthenticationError:
        logger.error(f"Auth error getting marks for user {message.chat.id}")
        await safe_edit_message(status_msg, "❌ Ошибка авторизации в Ruobr. Проверьте логин и пароль.")
    except TelegramNetworkError as e:
        logger.error(f"Network error for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, "📡 Ошибка сети. Проверьте подключение.")
    except Exception as e:
        logger.error(f"Error getting marks for user {message.chat.id}: {e}")
        await safe_edit_message(status_msg, f"❌ Ошибка получения оценок: {e}")
