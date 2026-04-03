"""
Handlers модуль - экспорт всех обработчиков.
"""
from . import auth
from . import balance
from . import schedule
from . import birthday

__all__ = ["auth", "balance", "schedule", "birthday"]
