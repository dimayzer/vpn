from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    db_url: str = Field(default="sqlite+aiosqlite:///./dev.db", env="DB_URL")
    redis_url: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    admin_token: str = Field(default="", env="ADMIN_TOKEN")
    ticket_bot_link: str = Field(default="", env="TICKET_BOT_LINK")
    support_bot_token: str = Field(default="", env="SUPPORT_BOT_TOKEN")
    cryptobot_token: str = Field(default="", env="CRYPTOBOT_TOKEN")


def get_settings() -> Settings:
    return Settings()


