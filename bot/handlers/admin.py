from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile
from aiogram.fsm.context import FSMContext

from bot.config import get_settings
from bot.core_api import CoreApi
from bot.keyboards import (
    admin_menu,
    admin_users_menu,
    admin_logs_menu,
    admin_payments_menu,
    admin_manage_user_menu,
    user_menu,
    BTN_ADMIN_USERS,
    BTN_ADMIN_PAYMENTS,
    BTN_ADMIN_SERVERS,
    BTN_ADMIN_LOGS,
    BTN_EXIT_ADMIN,
    BTN_BACK,
    BTN_NEXT,
    BTN_PREV,
    BTN_SEARCH,
    BTN_EXPORT_USERS,
    BTN_CREDIT_BALANCE,
    BTN_BLOCK_USER,
    BTN_UNBLOCK_USER,
    BTN_MANAGE_USER,
)
from bot.states import AdminUsers, AdminLogs, AdminPayments

router = Router(name="admin")


def is_admin(admin_ids: set[int], user_id: int | None) -> bool:
    return bool(user_id) and user_id in admin_ids


def admin_guard(admin_ids: set[int]):
    async def wrapper(message: Message) -> bool:
        if not is_admin(admin_ids, message.from_user.id if message.from_user else None):
            await message.answer("Нет доступа. Если это ошибка — напишите в поддержку.")
            return False
        return True

    return wrapper


def format_datetime_moscow(dt_str: str) -> str:
    """Форматирует datetime строку в московское время"""
    if not dt_str:
        return "—"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        moscow_tz = ZoneInfo("Europe/Moscow")
        dt_moscow = dt.astimezone(moscow_tz)
        return dt_moscow.strftime("%d.%m.%Y %H:%M")
    except:
        return dt_str[:10] if len(dt_str) >= 10 else dt_str


def format_user_name(user: dict) -> str:
    """Форматирует имя пользователя из данных"""
    username = user.get("username")
    first_name = user.get("first_name")
    last_name = user.get("last_name")
    
    parts = []
    if first_name:
        parts.append(first_name)
    if last_name:
        parts.append(last_name)
    name = " ".join(parts) if parts else "—"
    
    tag = f"@{username}" if username else "—"
    
    return f"{name} ({tag})" if name != "—" else tag


def format_user_card(user: dict, admin_ids: set[int] | None = None) -> str:
    """Форматирует красивую карточку пользователя"""
    tg_id = user.get("tg_id", "—")
    balance_cents = user.get("balance", 0)  # API возвращает balance в копейках (рублях * 100)
    balance_rub = balance_cents / 100  # Конвертируем копейки в рубли
    is_active = user.get("is_active", True)
    status_icon = "✅" if is_active else "❌"
    created_at = user.get("created_at", "")
    created_str = format_datetime_moscow(created_at)
    
    username = user.get("username")
    first_name = user.get("first_name")
    last_name = user.get("last_name")
    
    # Форматируем имя
    name_parts = []
    if first_name:
        name_parts.append(first_name)
    if last_name:
        name_parts.append(last_name)
    full_name = " ".join(name_parts) if name_parts else "—"
    tag = f"@{username}" if username else "—"
    
    # Определяем роль
    role = "Админ" if admin_ids and tg_id in admin_ids else "Пользователь"
    
    referral_code = user.get("referral_code", "—")
    referred_by_tg_id = user.get("referred_by_tg_id")
    ref_info = f"Реферал: {referred_by_tg_id}" if referred_by_tg_id else "Реферал: нет"
    
    # Информация о подписке
    has_active_subscription = user.get("has_active_subscription", False)
    subscription_ends_at = user.get("subscription_ends_at")
    subscription_info = "✅ Есть" if has_active_subscription else "❌ Нет"
    if has_active_subscription and subscription_ends_at:
        try:
            sub_end_str = format_datetime_moscow(subscription_ends_at)
            subscription_info += f" (до {sub_end_str})"
        except:
            pass
    
    return (
        f"👤 <b>Пользователь</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 Имя: {full_name}\n"
        f"🏷 Тег: {tag}\n"
        f"🆔 tg_id: <code>{tg_id}</code>\n"
        f"👑 Роль: {role}\n"
        f"💰 Баланс: <b>{balance_rub:.2f} RUB</b>\n"
        f"{status_icon} Статус: {'Активен' if is_active else 'Заблокирован'}\n"
        f"📦 Подписка: {subscription_info}\n"
        f"🎁 Код: <code>{referral_code}</code>\n"
        f"📊 {ref_info}\n"
        f"📅 Регистрация: {created_str} МСК\n"
        f"━━━━━━━━━━━━━━━━"
    )


def register(dp: Dispatcher, admin_ids: set[int]) -> None:
    guard = admin_guard(admin_ids)
    settings = get_settings()

    @router.message(Command("admin"))
    async def admin_root(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.clear()
        await message.answer(
            "🛠 <b>Админ-панель</b>\n\n"
            "— 👥 Пользователи\n"
            "— 💳 Платежи\n"
            "— 🖥 Сервера",
            reply_markup=admin_menu(),
        )

    async def render_users(message: Message, state: FSMContext) -> None:
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        data = await state.get_data()
        offset = int(data.get("offset", 0))
        limit = int(data.get("limit", 10))

        total = await api.users_count()
        users = await api.list_users(limit=limit, offset=offset)

        start_n = offset + 1 if total > 0 else 0
        end_n = min(offset + len(users), total)
        header = f"👥 <b>Пользователи</b>: {start_n}–{end_n} из {total}\n━━━━━━━━━━━━━━━━\n\n"
        
        if not users:
            await message.answer(header + "Пока пусто.", reply_markup=admin_users_menu())
            return
        
        # Показываем по одному пользователю на сообщение для читабельности
        if len(users) == 1:
            await message.answer(header + format_user_card(users[0], admin_ids=admin_ids), reply_markup=admin_users_menu(), parse_mode="HTML")
        else:
            # Первый пользователь с заголовком
            await message.answer(header + format_user_card(users[0], admin_ids=admin_ids), parse_mode="HTML")
            # Остальные по одному
            for u in users[1:]:
                await message.answer(format_user_card(u, admin_ids=admin_ids), parse_mode="HTML")
            await message.answer("Используйте ⬅️➡️ для навигации", reply_markup=admin_users_menu())

    @router.message(Command("users"))
    async def list_users_cmd(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            await state.set_state(AdminUsers.browsing)
            await state.update_data(offset=0, limit=10)
            await render_users(message, state)
        except Exception as e:
            await message.answer(f"Не удалось загрузить пользователей: {e}")

    @router.message(Command("payments"))
    async def list_payments(message: Message) -> None:
        if not await guard(message):
            return
        await message.answer("Платежи: скоро добавим.", reply_markup=admin_menu())

    @router.message(Command("servers"))
    async def list_servers(message: Message) -> None:
        if not await guard(message):
            return
        await message.answer("Сервера: скоро добавим.", reply_markup=admin_menu())

    # --- Admin menu via bottom buttons ---
    @router.message(F.text == BTN_ADMIN_USERS)
    async def users_btn(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await list_users_cmd(message, state)

    def format_payment_entry(payment: dict) -> str:
        """Форматирует запись о платеже"""
        payment_id = payment.get("id", "—")
        user_tg_id = payment.get("user_tg_id", "—")
        provider = payment.get("provider", "—")
        amount = payment.get("amount", 0)
        currency = payment.get("currency", "RUB")
        status = payment.get("status", "unknown")
        created_at = payment.get("created_at", "—")
        
        status_icons = {
            "succeeded": "✅",
            "pending": "⏳",
            "failed": "❌",
            "canceled": "🚫",
        }
        status_icon = status_icons.get(status, "❓")
        
        provider_names = {
            "telegram_stars": "⭐ Stars",
            "cryptobot": "💎 CryptoBot",
        }
        provider_name = provider_names.get(provider, provider)
        
        return (
            f"{status_icon} <b>Платеж #{payment_id}</b>\n"
            f"👤 Пользователь: <code>{user_tg_id}</code>\n"
            f"💳 Провайдер: {provider_name}\n"
            f"💰 Сумма: <b>{amount:.2f} {currency}</b>\n"
            f"📊 Статус: {status}\n"
            f"📅 Дата: {created_at}"
        )

    async def render_payments(message: Message, state: FSMContext) -> None:
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        data = await state.get_data()
        offset = int(data.get("payments_offset", 0))
        limit = 5  # Показываем по 5 платежей за раз

        result = await api.admin_get_payments(limit=limit, offset=offset)
        payments = result.get("payments", [])
        total = result.get("total", 0)

        start_n = offset + 1 if total > 0 else 0
        end_n = min(offset + len(payments), total)
        header = f"💳 <b>Платежи</b>: {start_n}–{end_n} из {total}\n━━━━━━━━━━━━━━━━\n\n"

        if not payments:
            await message.answer(header + "Платежей нет.", reply_markup=admin_payments_menu())
            return

        # Показываем платежи по одному для читабельности
        await message.answer(header, parse_mode="HTML")
        for payment in payments:
            await message.answer(format_payment_entry(payment), parse_mode="HTML")
        await message.answer("Используйте ⬅️➡️ для навигации", reply_markup=admin_payments_menu())

    @router.message(F.text == BTN_ADMIN_PAYMENTS)
    async def payments_btn(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.set_state(AdminPayments.browsing)
        await state.update_data(payments_offset=0)
        await render_payments(message, state)

    @router.message(AdminPayments.browsing, F.text == BTN_NEXT)
    async def payments_next(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            data = await state.get_data()
            offset = int(data.get("payments_offset", 0))
            limit = 5
            result = await api.admin_get_payments(limit=limit, offset=offset)
            total = result.get("total", 0)
            new_offset = offset + limit
            if new_offset >= total:
                await message.answer("Это последняя страница.", reply_markup=admin_payments_menu())
                return
            await state.update_data(payments_offset=new_offset)
            await render_payments(message, state)
        except Exception as e:
            await message.answer(f"Ошибка загрузки: {e}", reply_markup=admin_payments_menu())

    @router.message(AdminPayments.browsing, F.text == BTN_PREV)
    async def payments_prev(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            data = await state.get_data()
            offset = int(data.get("payments_offset", 0))
            limit = 5
            new_offset = max(0, offset - limit)
            if new_offset == offset:
                await message.answer("Это первая страница.", reply_markup=admin_payments_menu())
                return
            await state.update_data(payments_offset=new_offset)
            await render_payments(message, state)
        except Exception as e:
            await message.answer(f"Ошибка загрузки: {e}", reply_markup=admin_payments_menu())

    @router.message(AdminPayments.browsing, F.text == BTN_BACK)
    async def payments_back(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.clear()
        await message.answer("🛠 Админ-панель:", reply_markup=admin_menu())

    @router.message(F.text == BTN_ADMIN_SERVERS)
    async def servers_btn(message: Message) -> None:
        if not await guard(message):
            return
        await message.answer("Сервера: скоро добавим.", reply_markup=admin_menu())

    @router.message(F.text == BTN_EXIT_ADMIN)
    async def exit_admin(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.clear()
        is_admin_user = is_admin(admin_ids, message.from_user.id if message.from_user else None)
        await message.answer("Вышли из админки.", reply_markup=user_menu(is_admin=is_admin_user))

    # --- Users submenu navigation ---
    @router.message(AdminUsers.browsing, F.text == BTN_NEXT)
    async def users_next(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            data = await state.get_data()
            offset = int(data.get("offset", 0))
            limit = int(data.get("limit", 10))
            total = await api.users_count()
            new_offset = offset + limit
            if new_offset >= total:
                await message.answer("Это последняя страница.", reply_markup=admin_users_menu())
                return
            await state.update_data(offset=new_offset)
            await render_users(message, state)
        except Exception as e:
            await message.answer(f"Ошибка загрузки: {e}", reply_markup=admin_users_menu())

    @router.message(AdminUsers.browsing, F.text == BTN_PREV)
    async def users_prev(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            data = await state.get_data()
            offset = int(data.get("offset", 0))
            limit = int(data.get("limit", 10))
            new_offset = max(0, offset - limit)
            if new_offset == offset:
                await message.answer("Это первая страница.", reply_markup=admin_users_menu())
                return
            await state.update_data(offset=new_offset)
            await render_users(message, state)
        except Exception as e:
            await message.answer(f"Ошибка загрузки: {e}", reply_markup=admin_users_menu())

    @router.message(AdminUsers.browsing, F.text == BTN_SEARCH)
    async def users_search(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.set_state(AdminUsers.waiting_tg_id)
        await message.answer("Пришли tg_id пользователя цифрами (например 1145813854).", reply_markup=admin_users_menu())

    @router.message(AdminUsers.waiting_tg_id)
    async def users_search_input(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        text = (message.text or "").strip()
        if not text.isdigit():
            await message.answer("Нужно число. Пришли tg_id цифрами.", reply_markup=admin_users_menu())
            return
        tg_id = int(text)
        try:
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            user = await api.get_user_by_tg(tg_id)
            if not user:
                await message.answer("Пользователь не найден.", reply_markup=admin_users_menu())
            else:
                await message.answer(format_user_card(user, admin_ids=admin_ids), reply_markup=admin_users_menu(), parse_mode="HTML")
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=admin_users_menu())
        finally:
            await state.set_state(AdminUsers.browsing)

    @router.message(AdminUsers.browsing, F.text == BTN_EXPORT_USERS)
    async def export_users(message: Message) -> None:
        if not await guard(message):
            return
        try:
            await message.answer("⏳ Генерирую CSV файл...")
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            csv_data = await api.admin_export_users_csv()
            # Сохраняем временный файл
            import tempfile
            import os
            with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
                f.write(csv_data)
                temp_path = f.name
            file = FSInputFile(temp_path, filename="users_export.csv")
            await message.answer_document(file, caption="📥 Экспорт пользователей")
            os.unlink(temp_path)
        except Exception as e:
            await message.answer(f"Ошибка экспорта: {e}")

    @router.message(AdminUsers.browsing, F.text == BTN_CREDIT_BALANCE)
    async def credit_balance_start(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.set_state(AdminUsers.credit_waiting_tg_id)
        await message.answer("Пришли tg_id пользователя, которому выдать баланс:", reply_markup=admin_users_menu())

    @router.message(AdminUsers.credit_waiting_tg_id)
    async def credit_balance_tg_id(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        text = (message.text or "").strip()
        if not text.isdigit():
            await message.answer("Нужно число. Пришли tg_id цифрами.", reply_markup=admin_users_menu())
            return
        tg_id = int(text)
        await state.update_data(credit_tg_id=tg_id)
        await state.set_state(AdminUsers.credit_waiting_amount)
        await message.answer("Пришли сумму в RUB (например: 100.50 или 100):", reply_markup=admin_users_menu())

    @router.message(AdminUsers.credit_waiting_amount)
    async def credit_balance_amount(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        text = (message.text or "").strip().replace(",", ".")
        try:
            amount = float(text)
            if amount <= 0:
                await message.answer("Сумма должна быть больше 0.", reply_markup=admin_users_menu())
                return
            data = await state.get_data()
            tg_id = int(data.get("credit_tg_id", 0))
            admin_tg_id = message.from_user.id if message.from_user else None
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            # amount уже в рублях, передаем как есть (API сам конвертирует в копейки)
            result = await api.admin_credit(tg_id, int(amount), f"Выдано админом {admin_tg_id}", admin_tg_id)
            new_balance = result.get("balance", 0) / 100  # API возвращает balance в копейках
            # Обновляем данные пользователя и показываем обновленную карточку
            updated_user = await api.get_user_by_tg(tg_id)
            await message.answer(f"✅ Баланс выдан!\nНовый баланс: {new_balance:.2f} RUB\n\n{format_user_card(updated_user, admin_ids=admin_ids)}", reply_markup=admin_users_menu(), parse_mode="HTML")
            await state.set_state(AdminUsers.browsing)
            await state.update_data(credit_tg_id=None)
        except ValueError:
            await message.answer("Некорректная сумма. Пришли число (например: 10.50).", reply_markup=admin_users_menu())
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=admin_users_menu())

    @router.message(AdminUsers.browsing, F.text == BTN_MANAGE_USER)
    async def manage_user_start(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.set_state(AdminUsers.manage_waiting_tg_id)
        await message.answer("Пришли tg_id пользователя для управления:", reply_markup=admin_users_menu())

    @router.message(AdminUsers.manage_waiting_tg_id)
    async def manage_user_tg_id(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        text = (message.text or "").strip()
        if not text.isdigit():
            await message.answer("Нужно число. Пришли tg_id цифрами.", reply_markup=admin_users_menu())
            return
        tg_id = int(text)
        try:
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            user = await api.get_user_by_tg(tg_id)
            if not user:
                await message.answer("Пользователь не найден.", reply_markup=admin_users_menu())
                return
            await state.update_data(manage_tg_id=tg_id)
            await state.set_state(AdminUsers.managing)
            await message.answer(
                f"⚙️ <b>Управление пользователем</b>\n\n{format_user_card(user, admin_ids=admin_ids)}",
                reply_markup=admin_manage_user_menu(),
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=admin_users_menu())

    @router.message(AdminUsers.managing, F.text == BTN_BLOCK_USER)
    async def block_user_manage(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            data = await state.get_data()
            tg_id = int(data.get("manage_tg_id", 0))
            if not tg_id:
                await message.answer("Ошибка: tg_id не найден.", reply_markup=admin_users_menu())
                await state.set_state(AdminUsers.browsing)
                return
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            result = await api.admin_block_user(tg_id)
            updated_user = await api.get_user_by_tg(tg_id)
            await message.answer(
                f"🚫 <b>Пользователь заблокирован</b>\n\n{format_user_card(updated_user, admin_ids=admin_ids)}",
                reply_markup=admin_manage_user_menu(),
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=admin_manage_user_menu())

    @router.message(AdminUsers.managing, F.text == BTN_UNBLOCK_USER)
    async def unblock_user_manage(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            data = await state.get_data()
            tg_id = int(data.get("manage_tg_id", 0))
            if not tg_id:
                await message.answer("Ошибка: tg_id не найден.", reply_markup=admin_users_menu())
                await state.set_state(AdminUsers.browsing)
                return
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            result = await api.admin_unblock_user(tg_id)
            updated_user = await api.get_user_by_tg(tg_id)
            await message.answer(
                f"✅ <b>Пользователь разблокирован</b>\n\n{format_user_card(updated_user, admin_ids=admin_ids)}",
                reply_markup=admin_manage_user_menu(),
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=admin_manage_user_menu())

    @router.message(AdminUsers.managing, F.text == BTN_CREDIT_BALANCE)
    async def credit_balance_from_manage(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        data = await state.get_data()
        tg_id = int(data.get("manage_tg_id", 0))
        if not tg_id:
            await message.answer("Ошибка: tg_id не найден.", reply_markup=admin_users_menu())
            await state.set_state(AdminUsers.browsing)
            return
        await state.update_data(credit_tg_id=tg_id)
        await state.set_state(AdminUsers.credit_waiting_amount)
        await message.answer("Пришли сумму в RUB (например: 100.50 или 100):", reply_markup=admin_manage_user_menu())

    @router.message(AdminUsers.managing, F.text == BTN_BACK)
    async def manage_user_back(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.set_state(AdminUsers.browsing)
        await state.update_data(manage_tg_id=None)
        await message.answer("Вернулись к списку пользователей.", reply_markup=admin_users_menu())

    @router.message(AdminUsers.browsing, F.text == BTN_BACK)
    async def users_back(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.clear()
        await message.answer("🛠 Админ-панель:", reply_markup=admin_menu())

    # --- Logs handlers ---
    def format_log_entry(log: dict) -> str:
        action_icons = {
            "user_registered": "👤",
            "balance_credited": "💰",
            "user_blocked": "🚫",
            "user_unblocked": "✅",
            "subscription_created": "📦",
            "subscription_activated": "✨",
            "payment_processed": "💳",
            "admin_action": "🛠",
        }
        icon = action_icons.get(log.get("action", ""), "📝")
        action = log.get("action", "unknown").replace("_", " ").title()
        user_tg_id = log.get("user_tg_id")
        admin_tg_id = log.get("admin_tg_id")
        details = log.get("details", "")
        created_at = log.get("created_at", "")
        try:
            if created_at:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                time_str = dt.strftime("%d.%m.%Y %H:%M:%S")
            else:
                time_str = "—"
        except:
            time_str = created_at[:19] if len(created_at) >= 19 else created_at

        text = f"{icon} <b>{action}</b>\n"
        if user_tg_id:
            text += f"👤 Пользователь: <code>{user_tg_id}</code>\n"
        if admin_tg_id:
            text += f"🛠 Админ: <code>{admin_tg_id}</code>\n"
        if details:
            text += f"📄 {details}\n"
        text += f"🕐 {time_str}"
        return text

    async def render_logs(message: Message, state: FSMContext) -> None:
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        data = await state.get_data()
        offset = int(data.get("logs_offset", 0))
        limit = 5  # Показываем по 5 логов за раз

        total = await api.admin_logs_count()
        logs = await api.admin_get_logs(limit=limit, offset=offset)

        start_n = offset + 1 if total > 0 else 0
        end_n = min(offset + len(logs), total)
        header = f"📋 <b>Логи действий</b>: {start_n}–{end_n} из {total}\n━━━━━━━━━━━━━━━━\n\n"

        if not logs:
            await message.answer(header + "Логи пусты.", reply_markup=admin_logs_menu())
            return

        # Показываем логи по одному для читабельности
        await message.answer(header, parse_mode="HTML")
        for log in logs:
            await message.answer(format_log_entry(log), parse_mode="HTML")
        await message.answer("Используйте ⬅️➡️ для навигации", reply_markup=admin_logs_menu())

    @router.message(F.text == BTN_ADMIN_LOGS)
    async def logs_btn(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.set_state(AdminLogs.browsing)
        await state.update_data(logs_offset=0)
        await render_logs(message, state)

    @router.message(AdminLogs.browsing, F.text == BTN_NEXT)
    async def logs_next(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
            data = await state.get_data()
            offset = int(data.get("logs_offset", 0))
            limit = 5
            total = await api.admin_logs_count()
            new_offset = offset + limit
            if new_offset >= total:
                await message.answer("Это последняя страница.", reply_markup=admin_logs_menu())
                return
            await state.update_data(logs_offset=new_offset)
            await render_logs(message, state)
        except Exception as e:
            await message.answer(f"Ошибка загрузки: {e}", reply_markup=admin_logs_menu())

    @router.message(AdminLogs.browsing, F.text == BTN_PREV)
    async def logs_prev(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        try:
            data = await state.get_data()
            offset = int(data.get("logs_offset", 0))
            limit = 5
            new_offset = max(0, offset - limit)
            if new_offset == offset:
                await message.answer("Это первая страница.", reply_markup=admin_logs_menu())
                return
            await state.update_data(logs_offset=new_offset)
            await render_logs(message, state)
        except Exception as e:
            await message.answer(f"Ошибка загрузки: {e}", reply_markup=admin_logs_menu())

    @router.message(AdminLogs.browsing, F.text == BTN_BACK)
    async def logs_back(message: Message, state: FSMContext) -> None:
        if not await guard(message):
            return
        await state.clear()
        await message.answer("🛠 Админ-панель:", reply_markup=admin_menu())

    dp.include_router(router)
