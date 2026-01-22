from __future__ import annotations

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# User menu buttons
BTN_BUY = "🛒 Купить"
BTN_PLANS = "📦 Тарифы"
BTN_TOPUP = "💰 Пополнить баланс"
BTN_STATUS = "📊 Статус"
BTN_PROFILE = "👤 Профиль"
BTN_HELP = "🆘 Помощь"
BTN_REF = "🎁 Рефералка"
BTN_TICKET = "🧾 Тикет"
BTN_PROMO = "🎟️ Промокод"
BTN_ADMIN = "🛠 Админка"
BTN_CABINET = "💼 Личный кабинет"
BTN_PAYMENTS_HISTORY = "💳 История платежей"
BTN_BACK_TO_PROFILE = "⬅️ Назад в профиль"

# Admin menu buttons
BTN_ADMIN_USERS = "👥 Пользователи"
BTN_ADMIN_PAYMENTS = "💳 Платежи"
BTN_ADMIN_SERVERS = "🖥 Сервера"
BTN_ADMIN_LOGS = "📋 Логи"
BTN_BACK = "⬅️ Назад"
BTN_EXIT_ADMIN = "🚪 Выйти из админки"
BTN_EXPORT_USERS = "📥 Экспорт пользователей (CSV)"
BTN_CREDIT_BALANCE = "➕ Выдать баланс"
BTN_BLOCK_USER = "🚫 Заблокировать"
BTN_UNBLOCK_USER = "✅ Разблокировать"
BTN_MANAGE_USER = "⚙️ Управление пользователем"

# Admin users submenu
BTN_PREV = "⬅️"
BTN_NEXT = "➡️"
BTN_SEARCH = "🔎 Поиск по tg_id"


def user_menu(is_admin: bool = False) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text=BTN_PLANS), KeyboardButton(text=BTN_TOPUP)],
        [KeyboardButton(text=BTN_STATUS), KeyboardButton(text=BTN_PROFILE)],
        [KeyboardButton(text=BTN_REF), KeyboardButton(text=BTN_TICKET)],
        [KeyboardButton(text=BTN_PROMO), KeyboardButton(text=BTN_HELP)],
    ]
    if is_admin:
        keyboard.append([KeyboardButton(text=BTN_ADMIN)])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите действие…",
    )


def admin_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADMIN_USERS)],
            [KeyboardButton(text=BTN_ADMIN_PAYMENTS), KeyboardButton(text=BTN_ADMIN_SERVERS)],
            [KeyboardButton(text=BTN_ADMIN_LOGS)],
            [KeyboardButton(text=BTN_EXIT_ADMIN)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Админ-меню…",
    )


def admin_logs_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PREV), KeyboardButton(text=BTN_NEXT)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Логи…",
    )


def admin_users_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PREV), KeyboardButton(text=BTN_NEXT)],
            [KeyboardButton(text=BTN_SEARCH)],
            [KeyboardButton(text=BTN_MANAGE_USER)],
            [KeyboardButton(text=BTN_CREDIT_BALANCE)],
            [KeyboardButton(text=BTN_EXPORT_USERS)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Пользователи…",
    )


def admin_manage_user_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BLOCK_USER), KeyboardButton(text=BTN_UNBLOCK_USER)],
            [KeyboardButton(text=BTN_CREDIT_BALANCE)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Управление пользователем…",
    )


def admin_payments_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PREV), KeyboardButton(text=BTN_NEXT)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Платежи…",
    )


