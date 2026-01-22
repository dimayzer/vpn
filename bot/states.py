from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class AdminUsers(StatesGroup):
    browsing = State()
    waiting_tg_id = State()
    credit_waiting_tg_id = State()
    credit_waiting_amount = State()
    manage_waiting_tg_id = State()
    managing = State()


class AdminLogs(StatesGroup):
    browsing = State()


class AdminPayments(StatesGroup):
    browsing = State()


class UserTicket(StatesGroup):
    waiting_topic = State()


class UserPromoCode(StatesGroup):
    waiting_code = State()


class UserPayment(StatesGroup):
    waiting_amount_stars = State()
    waiting_amount_crypto = State()
    waiting_crypto_currency = State()


class UserSubscription(StatesGroup):
    waiting_plan_selection = State()

