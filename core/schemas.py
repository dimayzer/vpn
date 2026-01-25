from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class UserOut(BaseModel):
    id: int
    tg_id: int
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    is_active: bool
    balance: int
    referral_code: str | None = None
    referred_by_tg_id: int | None = None
    trial_used: bool = False
    has_active_subscription: bool = False
    subscription_ends_at: datetime | None = None
    selected_server_id: int | None = None
    auto_renew_subscription: bool = True
    created_at: datetime


class UserUpsertIn(BaseModel):
    tg_id: int
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    referral_code: str | None = None


class SubscriptionStatusOut(BaseModel):
    has_active: bool
    plan_name: str | None = None
    ends_at: datetime | None = None


class AdminCreditIn(BaseModel):
    tg_id: int
    amount: int
    reason: str | None = None
    admin_tg_id: int | None = None


class AdminSetActiveIn(BaseModel):
    tg_id: int
    is_active: bool


class ReferralInfoOut(BaseModel):
    tg_id: int
    referral_code: str
    referred_by_tg_id: int | None = None
    referrals_count: int
    total_rewards_cents: int = 0


class AuditLogOut(BaseModel):
    id: int
    action: str
    user_tg_id: int | None = None
    admin_tg_id: int | None = None
    details: str | None = None
    created_at: datetime


class PromoCodeValidateIn(BaseModel):
    code: str
    tg_id: int
    amount_cents: int


class PromoCodeApplyIn(BaseModel):
    code: str
    tg_id: int
    amount_cents: int


class PaymentCreateIn(BaseModel):
    """Создание платежа для пополнения баланса"""
    tg_id: int
    amount_cents: int  # Сумма в центах
    provider: str  # "telegram_stars" или "cryptobot"
    currency: str = "USD"  # Для CryptoBot может быть USDT, BTC и т.д.


class PaymentWebhookIn(BaseModel):
    """Webhook от платежной системы"""
    payment_id: int | None = None
    external_id: str
    provider: str
    status: str  # "succeeded", "failed", "pending"
    amount_cents: int
    currency: str = "USD"
    raw_data: dict | None = None


class SubscriptionPurchaseIn(BaseModel):
    """Покупка подписки"""
    tg_id: int
    plan_months: int  # 1, 3, 6, 12 месяцев
    promo_code: str | None = None  # Промокод на скидку (процент)


class SubscriptionTrialIn(BaseModel):
    """Активация пробного периода"""
    tg_id: int


class ServerCreateIn(BaseModel):
    """Создание сервера"""
    name: str
    host: str
    location: str | None = None
    is_enabled: bool = True
    capacity: int | None = None
    xray_port: int | None = 443
    xray_uuid: str | None = None
    xray_flow: str | None = None
    xray_network: str = "tcp"
    xray_security: str = "tls"
    xray_sni: str | None = None
    xray_reality_public_key: str | None = None
    xray_reality_short_id: str | None = None
    xray_path: str | None = None
    xray_host: str | None = None
    x3ui_api_url: str | None = None
    x3ui_username: str | None = None
    x3ui_password: str | None = None
    x3ui_inbound_id: int | None = None


class ServerUpdateIn(BaseModel):
    """Обновление сервера"""
    name: str | None = None
    host: str | None = None
    location: str | None = None
    is_enabled: bool | None = None
    capacity: int | None = None
    xray_port: int | None = None
    xray_uuid: str | None = None
    xray_flow: str | None = None
    xray_network: str | None = None
    xray_security: str | None = None
    xray_sni: str | None = None
    xray_reality_public_key: str | None = None
    xray_reality_short_id: str | None = None
    xray_path: str | None = None
    xray_host: str | None = None
    x3ui_api_url: str | None = None
    x3ui_username: str | None = None
    x3ui_password: str | None = None
    x3ui_inbound_id: int | None = None


class ServerOut(BaseModel):
    """Информация о сервере"""
    id: int
    name: str
    host: str
    location: str | None = None
    is_enabled: bool
    capacity: int | None = None
    created_at: datetime
    xray_port: int | None = None
    xray_uuid: str | None = None
    xray_flow: str | None = None
    xray_network: str | None = None
    xray_security: str | None = None
    xray_sni: str | None = None
    xray_reality_public_key: str | None = None
    xray_reality_short_id: str | None = None
    xray_path: str | None = None
    xray_host: str | None = None
    x3ui_api_url: str | None = None
    x3ui_username: str | None = None
    x3ui_password: str | None = None  # Обычно не возвращаем пароль в API, но для админки можно
    x3ui_inbound_id: int | None = None
    status: dict | None = None  # Последний статус сервера

