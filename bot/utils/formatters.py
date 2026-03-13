"""
Утилиты для форматирования вывода.
"""
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from ..services.ruobr_client import Child, FoodInfo, Lesson


def format_child_info(child: Child, index: Optional[int] = None) -> str:
    """
    Форматирование информации о ребёнке.
    
    Args:
        child: Объект ребёнка.
        index: Опциональный порядковый номер.
        
    Returns:
        Отформатированная строка.
    """
    prefix = f"{index}. " if index is not None else ""
    return f"{prefix}{child.full_name} ({child.group})"


def format_balance(
    child: Child,
    balance: float,
    threshold: float,
    show_status: bool = True
) -> str:
    """
    Форматирование информации о балансе.
    
    Args:
        child: Объект ребёнка.
        balance: Текущий баланс.
        threshold: Порог баланса.
        show_status: Показывать ли статус (ниже/выше порога).
        
    Returns:
        Отформатированная строка.
    """
    status = ""
    if show_status:
        if balance < threshold:
            status = " ⚠️"
        else:
            status = " ✅"
    
    return (
        f"{child.full_name} ({child.group}): "
        f"<b>{balance:.0f} ₽</b> "
        f"(порог {threshold:.0f} ₽){status}"
    )


def format_lesson(lesson: Lesson, show_details: bool = False) -> str:
    """
    Форматирование информации об уроке.
    
    Args:
        lesson: Объект урока.
        show_details: Показывать ли детали (тема, ДЗ).
        
    Returns:
        Отформатированная строка.
    """
    time_str = f"{lesson.time_start}-{lesson.time_end}"
    base = f"{time_str} {lesson.subject}"
    
    if show_details and lesson.topic:
        base += f"\n  📝 Тема: {lesson.topic}"
    
    return base


def format_homework(lesson: Lesson) -> List[str]:
    """
    Форматирование домашних заданий урока.
    
    Args:
        lesson: Объект урока.
        
    Returns:
        Список отформатированных строк с ДЗ.
    """
    result = []
    for hw in lesson.homework:
        title = hw.get("title", "")
        deadline = hw.get("deadline", "")
        if title:
            result.append(f"  📖 {lesson.subject}: {title}")
            if deadline:
                result.append(f"     ⏰ Дедлайн: {format_date(deadline)}")
    return result


def format_mark(mark: Dict[str, Any], subject: str) -> str:
    """
    Форматирование оценки.
    
    Args:
        mark: Словарь с данными оценки.
        subject: Предмет.
        
    Returns:
        Отформатированная строка.
    """
    question_type = mark.get("question_type", "") or mark.get("question_name", "")
    value = mark.get("mark", "")
    return f"{subject}: {question_type} → <b>{value}</b>"


def format_food_visit(visit: Dict[str, Any], child_name: str) -> str:
    """
    Форматирование записи о посещении столовой.
    
    Args:
        visit: Словарь с данными о визите.
        child_name: Имя ребёнка.
        
    Returns:
        Отформатированная строка.
    """
    dishes = visit.get("dishes", [])
    dish_names = [d.get("text", "") for d in dishes if d.get("text")]
    
    price_raw = str(visit.get("price_sum", "0")).replace(",", ".")
    try:
        price = float(price_raw)
    except ValueError:
        price = 0.0
    
    lines = [f"\n{child_name}:"]
    for dish in dish_names:
        lines.append(f"  - {dish}")
    lines.append(f"  <b>Списано: {price:.0f} ₽</b>")
    
    return "\n".join(lines)


def format_date(date_str: str) -> str:
    """
    Форматирование даты в читаемый вид.
    
    Args:
        date_str: Строка с датой (YYYY-MM-DD).
        
    Returns:
        Отформатированная строка (ДД.ММ.ГГГГ).
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return date_str


def format_datetime(dt: datetime) -> str:
    """
    Форматирование datetime в читаемый вид.
    
    Args:
        dt: Объект datetime.
        
    Returns:
        Отформатированная строка.
    """
    return dt.strftime("%d.%m.%Y %H:%M")


def escape_html(text: str) -> str:
    """
    Экранирование HTML-символов.
    
    Args:
        text: Исходный текст.
        
    Returns:
        Текст с экранированными символами.
    """
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def truncate_text(text: str, max_length: int = 4000) -> str:
    """
    Обрезка текста до максимальной длины.
    
    Args:
        text: Исходный текст.
        max_length: Максимальная длина.
        
    Returns:
        Обрезанный текст с многоточием.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def format_weekday(dt: date) -> str:
    """
    Форматирование дня недели.
    
    Args:
        dt: Дата.
        
    Returns:
        Название дня недели на русском.
    """
    weekdays = [
        "понедельник", "вторник", "среда", "четверг",
        "пятница", "суббота", "воскресенье"
    ]
    return weekdays[dt.weekday()]


def extract_homework_files(text: str) -> List[Tuple[str, str]]:
    """
    Извлечение ссылок на файлы из HTML-текста ДЗ.
    
    Args:
        text: HTML-текст с ДЗ.
        
    Returns:
        Список кортежей (тип_файла, url).
    """
    if not text:
        return []
    
    files = []
    
    # Извлекаем ссылки на документы (<a href="...">)
    doc_pattern = r'<a[^>]+href=["\']([^"\']+\.(doc|docx|pdf|xls|xlsx|ppt|pptx|txt))["\']'
    for match in re.finditer(doc_pattern, text, re.IGNORECASE):
        url = match.group(1)
        if url.startswith('//'):
            url = 'https:' + url
        files.append(('doc', url))
    
    # Извлекаем ссылки на изображения (<img src="...">)
    img_pattern = r'<img[^>]+src=["\']([^"\']+\.(jpg|jpeg|png|gif|webp))["\']'
    for match in re.finditer(img_pattern, text, re.IGNORECASE):
        url = match.group(1)
        if url.startswith('//'):
            url = 'https:' + url
        files.append(('img', url))
    
    return files
