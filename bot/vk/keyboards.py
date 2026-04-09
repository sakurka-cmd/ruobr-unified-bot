from vkbottle import Keyboard, KeyboardButtonColor, Text


def get_vk_main_keyboard():
    return (
        Keyboard(one_time=False, inline=False)
        .add(Text("📅 Расписание сегодня"), color=KeyboardButtonColor.PRIMARY)
        .add(Text("📅 Расписание завтра"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("📘 ДЗ на завтра"), color=KeyboardButtonColor.PRIMARY)
        .add(Text("⭐ Оценки сегодня"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("💰 Баланс питания"), color=KeyboardButtonColor.POSITIVE)
        .add(Text("🍽 Питание сегодня"), color=KeyboardButtonColor.POSITIVE)
        .row()
        .add(Text("⚙️ Настройки"), color=KeyboardButtonColor.NEGATIVE)
        .add(Text("ℹ️ Информация"), color=KeyboardButtonColor.SECONDARY)
    ).get_json()


def get_vk_settings_keyboard():
    return (
        Keyboard(one_time=False, inline=False)
        .add(Text("🔑 Изменить логин/пароль"), color=KeyboardButtonColor.PRIMARY)
        .add(Text("💰 Порог баланса"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("🔔 Уведомления"), color=KeyboardButtonColor.PRIMARY)
        .add(Text("🎂 Дни рождения"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("👤 Мой профиль"), color=KeyboardButtonColor.SECONDARY)
        .add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
    ).get_json()


def get_vk_notifications_keyboard(user):
    """Динамическая клавиатура переключения уведомлений (как TG inline)."""
    bal = "✅" if user.vk_balance_enabled else "❌"
    marks = "✅" if user.vk_marks_enabled else "❌"
    food = "✅" if user.vk_food_enabled else "❌"
    birthday = "✅" if getattr(user, 'vk_birthday_enabled', False) else "❌"
    return (
        Keyboard(one_time=False, inline=False)
        .add(Text(f"💰 Баланс: {bal}"), color=KeyboardButtonColor.POSITIVE)
        .add(Text(f"⭐ Оценки: {marks}"), color=KeyboardButtonColor.POSITIVE)
        .row()
        .add(Text(f"🍽 Питание: {food}"), color=KeyboardButtonColor.POSITIVE)
        .add(Text(f"🎂 Дни рождения: {birthday}"), color=KeyboardButtonColor.POSITIVE)
        .row()
        .add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
    ).get_json()


def get_vk_info_keyboard():
    return (
        Keyboard(one_time=False, inline=False)
        .add(Text("👥 Одноклассники"), color=KeyboardButtonColor.PRIMARY)
        .add(Text("👩\u200d🏫 Учителя"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("🎓 Доп. образование"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("📋 Справка"), color=KeyboardButtonColor.SECONDARY)
        .row()
        .add(Text("◀️ Назад"), color=KeyboardButtonColor.NEGATIVE)
    ).get_json()


VK_BD_WEEKDAY_NAMES = [
    "0 — Понедельник", "1 — Вторник", "2 — Среда",
    "3 — Четверг", "4 — Пятница", "5 — Суббота", "6 — Воскресенье",
]


def get_vk_birthday_child_keyboard(is_enabled, mode_desc):
    k = (
        Keyboard(one_time=False, inline=False)
        .add(Text(f"{'🔴' if is_enabled else '🟢'} Включить/выключить"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("📅 Режим: завтра"), color=KeyboardButtonColor.PRIMARY)
        .add(Text("📋 Режим: еженедельно"), color=KeyboardButtonColor.PRIMARY)
        .row()
        .add(Text("⏰ Изменить время"), color=KeyboardButtonColor.SECONDARY)
        .row()
        .add(Text("◀️ Назад к списку"), color=KeyboardButtonColor.NEGATIVE)
    )
    return k.get_json()
