"""
Утилиты для форматирования вывода.
"""
import re
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


def normalize_date_to_iso(date_str):
    """
    Нормализация строки даты в формат YYYY-MM-DD.

    Поддерживаемые входные форматы:
    - YYYY-MM-DD (уже ISO)
    - YYYY-MM-DDTHH:MM:SS или YYYY-MM-DD HH:MM:SS
    - DD.MM.YYYY

    Args:
        date_str: Строка с датой в произвольном формате.

    Returns:
        Дата в формате YYYY-MM-DD или исходная строка, если парсинг не удался.
    """
    if not date_str:
        return ""

    date_str = str(date_str).strip()

    # Уже в ISO формате (YYYY-MM-DD)
    if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
        return date_str

    # С временем: YYYY-MM-DDTHH:MM:SS или YYYY-MM-DD HH:MM:SS
    for sep in ["T", " "]:
        if sep in date_str:
            date_str = date_str.split(sep)[0]
            return date_str

    # DD.MM.YYYY
    for fmt in ["%d.%m.%Y", "%d/%m.%Y", "%d/%m/%Y"]:
        try:
            from datetime import datetime
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

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


def clean_html_text(text: str) -> str:
    """
    Очистка HTML-текста ДЗ от тегов и получение чистого текста.
    
    Args:
        text: HTML-текст.
        
    Returns:
        Очищенный текст.
    """
    if not text:
        return ""
    
    # Заменяем <br> и </div> на переносы строк
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    
    # Заменяем &nbsp; на пробел
    text = text.replace('&nbsp;', ' ')
    
    # Удаляем все остальные HTML-теги
    text = re.sub(r'<[^>]+>', '', text)
    
    # Убираем множественные пробелы и переносы
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n', text)
    
    return text.strip()


def has_meaningful_text(text: str) -> bool:
    """
    Проверка, содержит ли текст полезную информацию (не только пробелы и пустые параграфы).
    
    Args:
        text: Текст для проверки.
        
    Returns:
        True если текст содержит полезную информацию.
    """
    if not text:
        return False
    
    # Очищаем HTML
    clean = clean_html_text(text)
    
    # Проверяем что есть хоть какой-то текст (минимум 3 символа)
    return len(clean) >= 3


def extract_dish_names(dishes) -> list:
    """
    Извлечение названий блюд из различных форматов данных.
    """
    if not dishes or not isinstance(dishes, list):
        return []
    names = []
    for dish in dishes:
        if isinstance(dish, str):
            if dish.strip():
                names.append(dish.strip())
        elif isinstance(dish, dict):
            name = (
                dish.get("text") or
                dish.get("name") or
                dish.get("title") or
                dish.get("dish_name") or
                dish.get("description") or
                ""
            )
            if name and str(name).strip():
                names.append(str(name).strip())
    return names


def parse_complex_menu(qs_units) -> list:
    """
    Парсинг комплексного меню из qs_unit.
    """
    if not qs_units or not isinstance(qs_units, list) or len(qs_units) == 0:
        return []
    unit = qs_units[0]
    if not isinstance(unit, dict):
        return []
    about = unit.get("about", "")
    if not about or not about.strip():
        return []

    if len(qs_units) > 1:
        names = []
        for u in qs_units:
            name = u.get("name", "") or u.get("title", "") or u.get("text", "")
            if name.strip():
                names.append(name.strip())
        if names:
            return names


    parts = re.split(r'(?<!\()\s*(\d{2,3}(?:/\d{1,2})?)\s*', about.strip())
    dishes = []
    for i in range(0, len(parts), 2):
        name = parts[i].strip(" ,.")
        if name:
            dishes.append(name)
    return dishes
