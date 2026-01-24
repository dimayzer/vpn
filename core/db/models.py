from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    Integer,
    BigInteger,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Enum,
    Text,
    Numeric,
)
from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column

Base = declarative_base()


class SubscriptionStatus(str, enum.Enum):
    pending = "pending"
    active = "active"
    expired = "expired"
    canceled = "canceled"


class PaymentStatus(str, enum.Enum):
    pending = "pending"
    succeeded = "succeeded"
    failed = "failed"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, index=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    balance: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    referral_code: Mapped[str] = mapped_column(String(16), unique=True, nullable=True, index=True)
    referred_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True)
    trial_used: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)  # Использован ли пробный период
    has_active_subscription: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)  # Есть ли активная подписка
    subscription_ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)  # Дата окончания активной подписки
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    subscriptions: Mapped[list["Subscription"]] = relationship("Subscription", back_populates="user")
    payments: Mapped[list["Payment"]] = relationship("Payment", back_populates="user")
    credentials: Mapped[list["VpnCredential"]] = relationship("VpnCredential", back_populates="user")
    balance_transactions: Mapped[list["BalanceTransaction"]] = relationship("BalanceTransaction", back_populates="user")
    referral_rewards_received: Mapped[list["ReferralReward"]] = relationship("ReferralReward", foreign_keys="[ReferralReward.referrer_user_id]", back_populates="referrer")
    referral_rewards_received: Mapped[list["ReferralReward"]] = relationship("ReferralReward", foreign_keys="[ReferralReward.referrer_user_id]", back_populates="referrer")

    referred_by: Mapped["User | None"] = relationship(
        "User",
        remote_side=lambda: [User.__table__.c.id],
        foreign_keys=[referred_by_user_id],
        back_populates="referrals",
    )
    referrals: Mapped[list["User"]] = relationship(
        "User",
        foreign_keys=[referred_by_user_id],
        back_populates="referred_by",
    )


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    plan_name: Mapped[str] = mapped_column(String(50), nullable=False)
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), default="USD", nullable=False)
    status: Mapped[SubscriptionStatus] = mapped_column(Enum(SubscriptionStatus), default=SubscriptionStatus.pending, nullable=False)
    starts_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    user: Mapped["User"] = relationship("User", back_populates="subscriptions")


class SubscriptionPlan(Base):
    __tablename__ = "subscription_plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    months: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)  # 1, 3, 6, 12
    name: Mapped[str] = mapped_column(String(100), nullable=False)  # "1 месяц", "3 месяца" и т.д.
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)  # Описание тарифа
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)  # Цена в копейках (RUB)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)  # Активен ли тариф
    display_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)  # Порядок отображения
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), default="USD", nullable=False)
    status: Mapped[PaymentStatus] = mapped_column(Enum(PaymentStatus), default=PaymentStatus.pending, nullable=False)
    external_id: Mapped[str | None] = mapped_column(String(128))
    raw_response: Mapped[str | None] = mapped_column(Text())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    user: Mapped["User"] = relationship("User", back_populates="payments")


class Server(Base):
    __tablename__ = "servers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    host: Mapped[str] = mapped_column(String(128), nullable=False)
    location: Mapped[str | None] = mapped_column(String(64))
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    capacity: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    
    # Xray-core (VLESS) настройки сервера
    xray_port: Mapped[int | None] = mapped_column(Integer, nullable=True, default=443)  # Порт Xray (обычно 443 для TLS)
    xray_uuid: Mapped[str | None] = mapped_column(String(36), nullable=True)  # UUID сервера (для VLESS) - устарело, используется API 3x-UI
    xray_flow: Mapped[str | None] = mapped_column(String(16), nullable=True)  # Flow control (xtls-rprx-vision, xtls-rprx-direct)
    xray_network: Mapped[str | None] = mapped_column(String(16), nullable=True, default="tcp")  # tcp, ws, grpc
    xray_security: Mapped[str | None] = mapped_column(String(16), nullable=True, default="tls")  # none, tls, reality
    xray_sni: Mapped[str | None] = mapped_column(String(255), nullable=True)  # SNI для TLS (домен)
    xray_reality_public_key: Mapped[str | None] = mapped_column(String(255), nullable=True)  # Public key для Reality
    xray_reality_short_id: Mapped[str | None] = mapped_column(String(16), nullable=True)  # Short ID для Reality
    xray_path: Mapped[str | None] = mapped_column(String(255), nullable=True)  # Path для WebSocket/gRPC
    xray_host: Mapped[str | None] = mapped_column(String(255), nullable=True)  # Host header для WebSocket
    
    # 3x-UI API настройки для автоматического управления клиентами
    x3ui_api_url: Mapped[str | None] = mapped_column(String(255), nullable=True)  # URL API 3x-UI (например: http://ip:2053/api/v1)
    x3ui_username: Mapped[str | None] = mapped_column(String(64), nullable=True)  # Username для API 3x-UI
    x3ui_password: Mapped[str | None] = mapped_column(String(255), nullable=True)  # Password для API 3x-UI
    x3ui_inbound_id: Mapped[int | None] = mapped_column(Integer, nullable=True)  # ID Inbound в 3x-UI

    credentials: Mapped[list["VpnCredential"]] = relationship("VpnCredential", back_populates="server")


class VpnCredential(Base):
    __tablename__ = "vpn_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    server_id: Mapped[int] = mapped_column(ForeignKey("servers.id", ondelete="SET NULL"))
    public_key: Mapped[str | None] = mapped_column(String(255))
    private_key: Mapped[str | None] = mapped_column(String(255))
    config_text: Mapped[str | None] = mapped_column(Text())
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    user: Mapped["User"] = relationship("User", back_populates="credentials")
    server: Mapped["Server"] = relationship("Server", back_populates="credentials")


class BalanceTransaction(Base):
    __tablename__ = "balance_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False)
    admin_tg_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    user: Mapped["User"] = relationship("User", back_populates="balance_transactions")


class AdminOverride(Base):
    __tablename__ = "admin_overrides"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(16), default="user", nullable=False)  # user|moderator|admin


class AuditLogAction(str, enum.Enum):
    user_registered = "user_registered"
    balance_credited = "balance_credited"
    user_blocked = "user_blocked"
    user_unblocked = "user_unblocked"
    subscription_created = "subscription_created"
    subscription_activated = "subscription_activated"
    payment_processed = "payment_processed"
    payment_created = "payment_created"
    payment_status_changed = "payment_status_changed"
    payment_webhook_received = "payment_webhook_received"
    admin_action = "admin_action"
    backup_action = "backup_action"  # Действия с резервными копиями


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    action: Mapped[AuditLogAction] = mapped_column(Enum(AuditLogAction), nullable=False, index=True)
    user_tg_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    admin_tg_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    details: Mapped[str | None] = mapped_column(Text())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)


class TicketStatus(str, enum.Enum):
    open = "open"  # legacy value для совместимости со старой БД
    new = "new"
    in_progress = "in_progress"
    closed = "closed"

class MessageDirection(str, enum.Enum):
    incoming = "in"
    outgoing = "out"
    system = "system"


class Ticket(Base):
    __tablename__ = "tickets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_tg_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[TicketStatus] = mapped_column(Enum(TicketStatus), default=TicketStatus.new, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TicketMessage(Base):
    __tablename__ = "ticket_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticket_id: Mapped[int] = mapped_column(ForeignKey("tickets.id", ondelete="CASCADE"), index=True, nullable=False)
    user_tg_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    direction: Mapped[MessageDirection] = mapped_column(Enum(MessageDirection), nullable=False, index=True)
    admin_tg_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    text: Mapped[str] = mapped_column(Text(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)


class ReferralReward(Base):
    __tablename__ = "referral_rewards"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    referrer_user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    referred_user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    is_for_referrer: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)  # True - для пригласившего, False - для приглашенного
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)

    referrer: Mapped["User"] = relationship("User", foreign_keys=[referrer_user_id], back_populates="referral_rewards_received")
    referred: Mapped["User"] = relationship("User", foreign_keys=[referred_user_id])


class PromoCode(Base):
    __tablename__ = "promo_codes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    discount_percent: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Процент скидки (0-100)
    discount_amount_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Фиксированная скидка в центах
    max_uses: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Максимальное количество использований (None = без ограничений)
    used_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)  # Количество использований
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    created_by_tg_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


class PromoCodeUsage(Base):
    __tablename__ = "promo_code_usages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    promo_code_id: Mapped[int] = mapped_column(ForeignKey("promo_codes.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)
    discount_amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)  # Фактическая сумма скидки

    promo_code: Mapped["PromoCode"] = relationship("PromoCode")
    user: Mapped["User"] = relationship("User")


class ServerStatus(Base):
    """Статус и метрики серверов"""
    __tablename__ = "server_status"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    server_id: Mapped[int] = mapped_column(ForeignKey("servers.id", ondelete="CASCADE"), nullable=False, index=True)
    is_online: Mapped[bool] = mapped_column(Boolean, nullable=False)
    response_time_ms: Mapped[int | None] = mapped_column(Integer)  # Время отклика в миллисекундах
    connection_speed_mbps: Mapped[float | None] = mapped_column(Numeric(10, 2))  # Скорость соединения в Мбит/с
    active_connections: Mapped[int | None] = mapped_column(Integer)  # Количество активных подключений
    cpu_usage_percent: Mapped[float | None] = mapped_column(String(10))  # Использование CPU в процентах
    memory_usage_percent: Mapped[float | None] = mapped_column(String(10))  # Использование памяти в процентах
    disk_usage_percent: Mapped[float | None] = mapped_column(String(10))  # Использование диска в процентах
    error_message: Mapped[str | None] = mapped_column(Text)  # Сообщение об ошибке, если есть
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)
    
    server: Mapped["Server"] = relationship("Server")


class Backup(Base):
    """Записи о резервных копиях"""
    __tablename__ = "backups"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    backup_type: Mapped[str] = mapped_column(String(32), nullable=False)  # full, database
    file_path: Mapped[str] = mapped_column(String(512), nullable=False)  # Путь к файлу бэкапа
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)  # Размер файла в байтах
    status: Mapped[str] = mapped_column(String(16), default="completed", nullable=False)  # completed, failed, in_progress
    error_message: Mapped[str | None] = mapped_column(Text)  # Сообщение об ошибке, если есть
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)
    created_by_tg_id: Mapped[int | None] = mapped_column(BigInteger)  # Кто создал бэкап (если вручную)


class SystemSetting(Base):
    """Настройки системы (тарифы, конфигурация бота и т.д.)"""
    __tablename__ = "system_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_by_tg_id: Mapped[int | None] = mapped_column(BigInteger)

