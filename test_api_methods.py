#!/usr/bin/env python3
"""
Тестирование всех методов Ruobr API.
Показывает структуру данных, которые возвращает каждый метод.
"""
import asyncio
import json
from datetime import datetime, timedelta
from ruobr_api import Ruobr

# === ВСТАВЬ СЮДА ЛОГИН/ПАРОЛЬ ===
USERNAME = "YOUR_LOGIN"
PASSWORD = "YOUR_PASSWORD"
# ================================

def print_result(name: str, data):
    """Красивый вывод результата"""
    print(f"\n{'='*60}")
    print(f"📌 {name}")
    print('='*60)
    if data is None:
        print("❌ None (нет данных)")
    elif isinstance(data, dict):
        if not data:
            print("❌ Пустой словарь")
        else:
            print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])
    elif isinstance(data, list):
        print(f"📋 Количество элементов: {len(data)}")
        if data:
            print("Первый элемент:")
            print(json.dumps(data[0], indent=2, ensure_ascii=False)[:1500])
    else:
        print(str(data)[:1000])


def test_all_methods():
    r = Ruobr(USERNAME, PASSWORD)

    # 1. Авторизация
    print("\n🔐 Авторизация...")
    try:
        user = r.get_user()
        print(f"✅ Успешно: {user.get('first_name', '')} {user.get('last_name', '')}")
        print(f"   Школа: {user.get('school', '')}")
        print(f"   Класс: {user.get('group', '')}")
    except Exception as e:
        print(f"❌ Ошибка авторизации: {e}")
        return

    # 2. Дети
    try:
        children = r.get_children()
        print_result("get_children()", children)
    except Exception as e:
        print(f"❌ get_children: {e}")

    # 3. Почта
    try:
        mail = r.get_mail()
        print_result("get_mail()", mail)
    except Exception as e:
        print(f"❌ get_mail: {e}")

    # 4. Получатели сообщений
    try:
        recipients = r.get_recipients()
        print_result("get_recipients()", recipients)
    except Exception as e:
        print(f"❌ get_recipients: {e}")

    # 5. Достижения
    try:
        achievements = r.get_achievements()
        print_result("get_achievements()", achievements)
    except Exception as e:
        print(f"❌ get_achievements: {e}")

    # 6. Итоговые оценки
    try:
        control_marks = r.get_control_marks()
        print_result("get_control_marks()", control_marks)
    except Exception as e:
        print(f"❌ get_control_marks: {e}")

    # 7. События
    try:
        events = r.get_events()
        print_result("get_events()", events)
    except Exception as e:
        print(f"❌ get_events: {e}")

    # 8. Дни рождения
    try:
        birthdays = r.get_birthdays()
        print_result("get_birthdays()", birthdays)
    except Exception as e:
        print(f"❌ get_birthdays: {e}")

    # 9. Питание
    try:
        food = r.get_food_info()
        print_result("get_food_info()", food)
    except Exception as e:
        print(f"❌ get_food_info: {e}")

    # 10. Одноклассники
    try:
        classmates = r.get_classmates()
        print_result("get_classmates()", classmates)
    except Exception as e:
        print(f"❌ get_classmates: {e}")

    # 11. Книги
    try:
        books = r.get_books()
        print_result("get_books()", books)
    except Exception as e:
        print(f"❌ get_books: {e}")

    # 12. Полезные ссылки
    try:
        links = r.get_useful_links()
        print_result("get_useful_links()", links)
    except Exception as e:
        print(f"❌ get_useful_links: {e}")

    # 13. Инфо о школе
    try:
        guide = r.get_guide()
        print_result("get_guide()", guide)
    except Exception as e:
        print(f"❌ get_guide: {e}")

    # 14. Сертификат
    try:
        cert = r.get_certificate()
        print_result("get_certificate()", cert)
    except Exception as e:
        print(f"❌ get_certificate: {e}")

    # 15. Расписание (уже используем, но покажем структуру)
    try:
        today = datetime.now()
        start = today - timedelta(days=today.weekday())  # начало недели
        end = start + timedelta(days=6)  # конец недели
        timetable = r.get_timetable(start, end)
        print_result("get_timetable() - текущая неделя", timetable)
    except Exception as e:
        print(f"❌ get_timetable: {e}")

    # 16. Детализация оценок по предмету (если control_marks работает)
    try:
        control_marks = r.get_control_marks()
        if control_marks and len(control_marks) > 0:
            period = control_marks[0]
            if period.get("marks") and len(period["marks"]) > 0:
                subject = period["marks"][0]
                all_marks = r.get_all_marks(period["period"], subject["subject_id"])
                print_result(f"get_all_marks() - {subject.get('subject', 'предмет')}", all_marks)
    except Exception as e:
        print(f"❌ get_all_marks: {e}")

    print("\n" + "="*60)
    print("✅ Тестирование завершено")
    print("="*60)


if __name__ == "__main__":
    test_all_methods()
