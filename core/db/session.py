from __future__ import annotations

from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from core.config import get_settings

settings = get_settings()

engine: AsyncEngine = create_async_engine(settings.db_url, echo=False, future=True)
SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def recreate_engine() -> None:
    """Пересоздает engine и SessionLocal. Используется после восстановления базы данных."""
    global engine, SessionLocal
    
    # Закрываем старый engine
    try:
        if engine:
            await engine.dispose()
    except Exception:
        pass
    
    # Создаем новый engine
    engine = create_async_engine(settings.db_url, echo=False, future=True)
    SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

