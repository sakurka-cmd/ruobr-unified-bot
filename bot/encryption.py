"""
Модуль шифрования для безопасного хранения паролей.
Использует Fernet (AES-128-CBC) из библиотеки cryptography.
"""
import base64
import logging
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

from .config import config

logger = logging.getLogger(__name__)


class EncryptionService:
    """Сервис шифрования/дешифрования данных с использованием Fernet."""
    
    def __init__(self, key: Optional[str] = None):
        """
        Инициализация сервиса шифрования.
        
        Args:
            key: Ключ шифрования в формате base64. Если не указан, берётся из конфигурации.
        """
        self._key = key or config.encryption_key
        self._fernet: Optional[Fernet] = None
        self._initialize_fernet()
    
    def _initialize_fernet(self) -> None:
        """Инициализация Fernet с валидацией ключа."""
        try:
            # Проверяем, что ключ в правильном формате
            key_bytes = self._key.encode() if isinstance(self._key, str) else self._key
            self._fernet = Fernet(key_bytes)
            logger.info("Encryption service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize encryption service: {e}")
            raise ValueError(f"Некорректный ключ шифрования: {e}")
    
    def encrypt(self, data: str) -> str:
        """
        Шифрование строки.
        
        Args:
            data: Исходная строка для шифрования.
            
        Returns:
            Зашифрованная строка в формате base64.
            
        Raises:
            ValueError: Если данные пустые или шифрование не удалось.
        """
        if not data:
            raise ValueError("Данные для шифрования не могут быть пустыми")
        
        if not self._fernet:
            raise ValueError("Сервис шифрования не инициализирован")
        
        try:
            encrypted = self._fernet.encrypt(data.encode('utf-8'))
            return encrypted.decode('utf-8')
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise ValueError(f"Ошибка шифрования: {e}")
    
    def decrypt(self, encrypted_data: str) -> str:
        """
        Дешифрование строки.
        
        Args:
            encrypted_data: Зашифрованная строка.
            
        Returns:
            Расшифрованная исходная строка.
            
        Raises:
            ValueError: Если данные повреждены или ключ неверный.
        """
        if not encrypted_data:
            raise ValueError("Зашифрованные данные не могут быть пустыми")
        
        if not self._fernet:
            raise ValueError("Сервис шифрования не инициализирован")
        
        try:
            decrypted = self._fernet.decrypt(encrypted_data.encode('utf-8'))
            return decrypted.decode('utf-8')
        except InvalidToken:
            logger.error("Decryption failed: invalid token (wrong key or corrupted data)")
            raise ValueError("Ошибка дешифрования: неверный ключ или повреждённые данные")
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise ValueError(f"Ошибка дешифрования: {e}")
    
    @staticmethod
    def generate_key() -> str:
        """
        Генерация нового ключа шифрования.
        
        Returns:
            Новый ключ в формате base64 строки.
        """
        return Fernet.generate_key().decode('utf-8')


# Глобальный экземпляр сервиса шифрования
_encryption_service: Optional[EncryptionService] = None


def get_encryption_service() -> EncryptionService:
    """Получение глобального экземпляра сервиса шифрования."""
    global _encryption_service
    if _encryption_service is None:
        _encryption_service = EncryptionService()
    return _encryption_service


def encrypt_password(password: str) -> str:
    """Удобная функция для шифрования пароля."""
    return get_encryption_service().encrypt(password)


def decrypt_password(encrypted_password: str) -> str:
    """Удобная функция для дешифрования пароля."""
    return get_encryption_service().decrypt(encrypted_password)
