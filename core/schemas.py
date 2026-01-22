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


class SubscriptionTrialIn(BaseModel):
    """Активация пробного периода"""
    tg_id: int

