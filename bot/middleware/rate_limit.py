from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Any

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject


class RateLimitMiddleware(BaseMiddleware):
    """Middleware для ограничения частоты сообщений от пользователей"""
    
    def __init__(self, max_messages: int = 10, time_window: int = 60):
        """
        Args:
            max_messages: Максимальное количество сообщений
            time_window: Временное окно в секундах
        """
        self.max_messages = max_messages
        self.time_window = time_window
        self.user_messages: dict[int, list[datetime]] = defaultdict(list)
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Any],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message):
            return await handler(event, data)
        
        if not event.from_user:
            return await handler(event, data)
        
        user_id = event.from_user.id
        now = datetime.utcnow()
        
        # Очищаем старые сообщения
        cutoff = now - timedelta(seconds=self.time_window)
        self.user_messages[user_id] = [
            msg_time for msg_time in self.user_messages[user_id]
            if msg_time > cutoff
        ]
        
        # Проверяем лимит
        if len(self.user_messages[user_id]) >= self.max_messages:
            # Пользователь превысил лимит, игнорируем сообщение
            return
        
        # Добавляем текущее сообщение
        self.user_messages[user_id].append(now)
        
        return await handler(event, data)







