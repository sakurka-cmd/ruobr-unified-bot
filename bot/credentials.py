"""
Единый модуль для безопасного получения расшифрованных учётных данных.

Все хендлеры и сервисы должны использовать safe_decrypt() для получения
login/password. Это единственная точка, где происходит расшифровка пароля.
"""
import logging
from typing import Optional, Tuple

from .database import UserConfig
from .encryption import decrypt_password

logger = logging.getLogger(__name__)


def safe_decrypt(user_config: UserConfig) -> Tuple[Optional[str], Optional[str]]:
    """
    Безопасная расшифровка учётных данных пользователя.
    
    Возвращает (login, password) или (None, None) при ошибке.
    Пароль существует только в локальной переменной и затирается при выходе.
    
    Usage:
        login, password = safe_decrypt(user_config)
        if not login:
            await message.answer("Сначала настройте учётные данные")
            return
        children = await get_children_async(login, password)
        # password больше не нужен — вышел из области видимости
    """
    if not user_config or not user_config.password_encrypted:
        return None, None
    
    if not user_config.login:
        return None, None
    
    try:
        password = decrypt_password(user_config.password_encrypted)
    except Exception as e:
        logger.error(f"Failed to decrypt password for user {user_config.chat_id}: {e}")
        return None, None
    
    try:
        return user_config.login, password
    finally:
        # Затираем пароль из памяти
        password = None
