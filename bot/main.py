from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties
import os
from aiogram.fsm.storage.memory import MemoryStorage

from bot.config import get_settings
from bot.handlers import admin, user
from bot.middleware.rate_limit import RateLimitMiddleware


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
    settings = get_settings()

    logging.info("Raw ADMIN_IDS env: %s", os.getenv("ADMIN_IDS"))

    bot = Bot(
        token=settings.bot_token.get_secret_value(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    
    # Добавляем middleware для защиты от спама
    dp.message.middleware(RateLimitMiddleware(max_messages=20, time_window=60))
    dp.callback_query.middleware(RateLimitMiddleware(max_messages=30, time_window=60))

    user.register(dp)
    admin.register(dp, admin_ids=set(settings.admin_ids))

    logging.info("Admin IDs loaded: %s", settings.admin_ids)

    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Bot started with long-polling")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

