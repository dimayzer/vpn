# fioreVPN

Telegram-бот для VPN-сервиса fioreVPN с подписками, платежами и админ-панелью.

## Локальная разработка

1. Установить зависимости: `python -m venv .venv && .venv/Scripts/activate && pip install -r requirements.txt`
2. Создать `.env` из `env.sample` и заполнить переменные
3. Запустить через Docker: `docker-compose up -d`
4. Или локально:
   - Core API: `uvicorn core.main:app --reload`
   - Bot: `python -m bot.main`

## Деплой

См. `DEPLOY.md` для инструкций по деплою на хостинг.




