from __future__ import annotations

from typing import List
import os

from pydantic import Field, ValidationError, SecretStr, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bot_token: SecretStr = Field(env="BOT_TOKEN")
    admin_ids: List[int] = Field(default_factory=list, env="ADMIN_IDS")
    core_api_base: HttpUrl = Field(default="http://localhost:8000", env="CORE_API_BASE")
    admin_token: str = Field(default="", env="ADMIN_TOKEN")
    ticket_bot_link: str = Field(default="", env="TICKET_BOT_LINK")

    @field_validator("admin_ids", mode="before")
    @classmethod
    def split_admins(cls, v: str | List[int]) -> List[int]:
        if isinstance(v, list):
            return [int(item) for item in v]
        if isinstance(v, str):
            return [int(item.strip()) for item in v.split(",") if item.strip()]
        return []


def get_settings() -> Settings:
    try:
        settings = Settings()
        # Дополнительный разбор ADMIN_IDS, если Pydantic не распарсил список
        raw_admins = os.getenv("ADMIN_IDS")
        if raw_admins and not settings.admin_ids:
            settings.admin_ids = [int(item.strip()) for item in raw_admins.split(",") if item.strip()]
        return settings
    except ValidationError as exc:
        # Small helper to make bootstrap errors readable in logs
        raise RuntimeError(f"Config validation error: {exc}") from exc

