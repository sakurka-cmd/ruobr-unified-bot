# 🎓 Ruobr Telegram Bot

Telegram-бот для родителей, позволяющий следить за:
- 💰 Балансом школьного питания
- 📅 Расписанием уроков
- 📘 Домашними заданиями
- ⭐ Оценками

## Быстрый старт (Docker)

```bash
# Клонировать
git clone https://github.com/sakurka-cmd/ruobr-telegram-bot.git
cd ruobr-telegram-bot

# Создать .env
cp .env.example .env
nano .env  # Заполнить данные

# Запустить
docker-compose up -d

# Логи
docker-compose logs -f
```

## Установка без Docker

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

pip install -r requirements.txt
cp .env.example .env
# Отредактировать .env
python main.py
```

## Настройка .env

```
BOT_TOKEN=your_telegram_bot_token
ENCRYPTION_KEY=your_fernet_key
ADMIN_IDS=123456789
```

Генерация ключа:
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

## Команды бота

| Команда | Описание |
|---------|----------|
| `/start` | Начало работы |
| `/set_login` | Настройка Ruobr |
| `/balance` | Баланс питания |
| `/ttoday` | Расписание сегодня |
| `/ttomorrow` | Расписание завтра |
| `/hwtomorrow` | ДЗ на завтра |
| `/markstoday` | Оценки |

## Docker команды

```bash
# Запуск
docker-compose up -d

# Остановить
docker-compose down

# Пересобрать
docker-compose up -d --build

# Логи
docker-compose logs -f

# Статус
docker-compose ps
```

## Технологии

- Python 3.12
- aiogram 3.x
- aiosqlite
- cryptography

## Лицензия

MIT
