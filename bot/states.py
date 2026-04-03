"""
Определение состояний FSM для многошаговых операций.
"""
from aiogram.fsm.state import State, StatesGroup


class LoginStates(StatesGroup):
    """Состояния процесса входа."""
    waiting_for_login = State()
    waiting_for_password = State()


class ThresholdStates(StatesGroup):
    """Состояния настройки порога баланса."""
    waiting_for_child_selection = State()
    waiting_for_threshold_value = State()


class NotificationStates(StatesGroup):
    """Состояния настройки уведомлений."""
    choosing_notification_type = State()
    setting_parameters = State()


class BirthdaySettingsStates(StatesGroup):
    """Состояния настройки уведомлений о днях рождения."""
    choosing_child = State()
    choosing_mode = State()
    setting_weekday = State()
    setting_time = State()
