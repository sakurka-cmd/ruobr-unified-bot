"""
Обработчики для расписания, ДЗ и оценок.
"""
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message

from ..config import config
from ..database import UserConfig
from ..services import (
    Child, Lesson, get_children_async, get_timetable_for_children,
    RuobrError
)
from ..utils.formatters import (
    format_lesson, format_homework, format_mark, format_date,
    format_weekday, truncate_text, extract_homework_files, 
    clean_html_text, has_meaningful_text
)
from .balance import require_authentication

logger = logging.getLogger(__name__)

router = Router()


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
        timetable = await get_timetable_for_children(login, password, children, today, today)
        
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
            await status_msg.edit_text("ℹ️ На сегодня расписание не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await status_msg.edit_text(text)
            
    except Exception as e:
        logger.error(f"Error getting timetable for user {message.chat.id}: {e}")
        await status_msg.edit_text(f"❌ Ошибка получения расписания: {e}")


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
        timetable = await get_timetable_for_children(login, password, children, tomorrow, tomorrow)
        
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
            await status_msg.edit_text("ℹ️ На завтра расписание не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await status_msg.edit_text(text)
            
    except Exception as e:
        logger.error(f"Error getting timetable for user {message.chat.id}: {e}")
        await status_msg.edit_text(f"❌ Ошибка получения расписания: {e}")


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
        
        # Запрашиваем расписание на неделю, так как ДЗ может быть задано раньше
        monday = today - timedelta(days=today.weekday())
        sunday = monday + timedelta(days=6)
        
        timetable = await get_timetable_for_children(login, password, children, monday, sunday)
        
        lines = [f"📘 <b>Домашнее задание на завтра</b> ({format_date(tomorrow_str)})"]
        found = False
        
        for child in children:
            lessons = timetable.get(child.id, [])
            child_header_added = False
            
            for lesson in lessons:
                # Фильтруем по дедлайну
                relevant_hw = []
                for hw in lesson.homework:
                    if hw.get("deadline") == tomorrow_str:
                        relevant_hw.append(hw)
                
                if not relevant_hw:
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
                        # Ограничиваем длину текста
                        if len(clean_text) > 200:
                            clean_text = clean_text[:197] + "..."
                        lines.append(f"     📝 {clean_text}")
                    
                    # Извлекаем и показываем прикреплённые файлы
                    files = extract_homework_files(hw_text)
                    for file_type, file_url in files:
                        if file_type == 'img':
                            lines.append(f"     🖼 <a href=\"{file_url}\">Изображение</a>")
                        else:
                            lines.append(f"     📎 <a href=\"{file_url}\">Файл</a>")
        
        if not found:
            await status_msg.edit_text("ℹ️ На завтра домашнее задание не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await status_msg.edit_text(text)
            
    except Exception as e:
        logger.error(f"Error getting homework for user {message.chat.id}: {e}")
        await status_msg.edit_text(f"❌ Ошибка получения ДЗ: {e}")


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
        
        timetable = await get_timetable_for_children(login, password, children, today, today)
        
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
            await status_msg.edit_text("ℹ️ За сегодня оценок не найдено.")
        else:
            text = truncate_text("\n".join(lines))
            await status_msg.edit_text(text)
            
    except Exception as e:
        logger.error(f"Error getting marks for user {message.chat.id}: {e}")
        await status_msg.edit_text(f"❌ Ошибка получения оценок: {e}")
