# 🎓 Ruobr Unified Bot

Унифицированный бот для мониторинга образовательного процесса в системе «Руобр» (Новосибирская область). Работает в Telegram и ВКонтакте.

## Возможности

- 💰 **Баланс питания** — мониторинг с настраиваемым порогом на каждого ребёнка
- 📅 **Расписание** — уроки на сегодня/завтра с темами
- 📘 **Домашние задания** — с скачиванием и отправкой вложенных файлов (картинки, PDF)
- ⭐ **Оценки** — мониторинг новых оценок с уведомлениями
- 🍽️ **Питание** — что ел ребёнок, стоимость
- 🎂 **Дни рождения** — одноклассники (режим «завтора» / «еженедельный дайджест»)
- 🔗 **Кросс-линковка** — привязка TG и VK аккаунтов к единому профилю
- 🔔 **Двухканальные уведомления** — независимая настройка TG и VK

## Быстрый старт (Docker)

```bash
git clone https://github.com/sakurka-cmd/ruobr-unified-bot.git
cd ruobr-unified-bot

cp .env.example .env
nano .env  # Заполнить BOT_TOKEN, VK_TOKEN, ENCRYPTION_KEY

docker compose up -d
docker compose logs -f
```

## Установка без Docker

```bash
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# Отредактировать .env
python main.py
```

## Настройка .env

```env
BOT_TOKEN=your_telegram_bot_token
VK_TOKEN=your_vk_bot_token
VK_GROUP_ID=your_vk_group_id
ENCRYPTION_KEY=your_fernet_key
ADMIN_IDS=123456789
# BOT_PROXY=socks5://host:port  # опционально
```

Генерация ключа шифрования:
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

## Команды бота

| Команда | Описание |
|---------|----------|
| `/start` | Начало работы |
| `/set_login` | Настройка Ruobr (логин/пароль) |
| `/balance` | Баланс питания |
| `/ttoday` | Расписание сегодня |
| `/ttomorrow` | Расписание завтра |
| `/hwtomorrow` | ДЗ на завтра |
| `/markstoday` | Оценки за сегодня |
| `/settings` | Настройки уведомлений |
| `/link_vk` | Привязка VK-аккаунта |

## Архитектура

- **Платформа**: Python 3.12, модульный монолит
- **Telegram**: aiogram 3.x (long polling, FSM)
- **ВКонтакте**: vkbottle 4.x (Bot Longpoll)
- **БД**: SQLite (aiosqlite, WAL)
- **API**: ruobr_api (AsyncRuobr), httpx
- **Безопасность**: Fernet (AES-128-CBC) для паролей
- **Развёртывание**: Docker, Synology NAS + VPS
- **Прокси**: SOCKS5 + прозрачный прокси (fake-IP, dnsmasq, xray VLESS+Reality)

## Лицензия

MIT
