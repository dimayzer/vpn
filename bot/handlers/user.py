from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from aiogram import Dispatcher, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, PreCheckoutQuery, SuccessfulPayment
from aiogram.fsm.context import FSMContext

from bot.config import get_settings
from bot.core_api import CoreApi
from bot.keyboards import (
    user_menu,
    admin_menu,
    BTN_BUY,
    BTN_PLANS,
    BTN_TOPUP,
    BTN_STATUS,
    BTN_PROFILE,
    BTN_HELP,
    BTN_REF,
    BTN_TICKET,
    BTN_PROMO,
    BTN_ADMIN,
    BTN_SERVERS,
    BTN_KEY,
)
from bot.states import AdminUsers, AdminLogs, UserTicket, UserPromoCode, UserPayment, UserSubscription

router = Router(name="user")

@router.message(CommandStart())
async def start(message: Message) -> None:
    # регистрация/обновление пользователя в БД
    referral_code: str | None = None
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) == 2:
            referral_code = parts[1].strip() or None

    welcome_message = "Привет! Это fioreVPN бот.\n\n— Посмотреть тарифы и купить подписку\n— Узнать статус и срок действия\n— Получить конфиг/QR после оплаты\n\nВыберите действие:"
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        if message.from_user:
            await api.upsert_user(
                message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
                referral_code=referral_code
            )
        # Получаем настройки бота
        try:
            bot_settings = await api.get_bot_settings()
            if bot_settings.get("welcome_message"):
                welcome_message = bot_settings["welcome_message"]
        except Exception:
            pass  # Используем дефолтное сообщение
    except Exception:
        # core может быть недоступен, не блокируем старт
        pass

    is_admin = bool(message.from_user) and message.from_user.id in set(get_settings().admin_ids)
    
    # Проверяем наличие активной подписки
    has_subscription = False
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        if message.from_user:
            status = await api.subscription_status(message.from_user.id)
            has_subscription = status.get("has_active", False)
    except Exception:
        pass  # Игнорируем ошибки при проверке подписки
    
    await message.answer(
        welcome_message,
        reply_markup=user_menu(is_admin=is_admin, has_subscription=has_subscription),
    )


@router.message(Command("plans"))
async def plans(message: Message) -> None:
    """Показать доступные тарифы подписки"""
    # Перенаправляем на plans_btn, который показывает тарифы с кнопками для покупки
    await plans_btn(message)


@router.message(Command("status"))
async def status(message: Message) -> None:
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        if not message.from_user:
            await message.answer("Не могу определить пользователя.")
            return
        data = await api.subscription_status(message.from_user.id)
        user_data = await api.get_user_by_tg(message.from_user.id)
        selected_server_id = user_data.get("selected_server_id") if user_data else None
        selected_server_name = None
        
        # Получаем имя выбранного сервера
        if selected_server_id:
            servers_response = await api.get_available_servers()
            servers = servers_response.get("servers", [])
            for server in servers:
                if server.get("id") == selected_server_id:
                    selected_server_name = server.get("name", f"Сервер {selected_server_id}")
                    break
        
        if data.get("has_active"):
            plan = data.get("plan_name") or "—"
            ends_at = data.get("ends_at") or "—"
            try:
                if ends_at and ends_at != "—":
                    dt = datetime.fromisoformat(ends_at.replace("Z", "+00:00"))
                    moscow_tz = ZoneInfo("Europe/Moscow")
                    dt_moscow = dt.astimezone(moscow_tz)
                    ends_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
                else:
                    ends_str = "—"
            except:
                ends_str = ends_at[:10] if len(ends_at) >= 10 else ends_at
            
            status_text = f"Статус подписки: активна ✅\nТариф: {plan}\nДо: {ends_str} МСК"
            if selected_server_name:
                status_text += f"\nСервер: {selected_server_name}"
            await message.answer(status_text)
        else:
            await message.answer("Статус подписки: нет активной. Используйте кнопку '📦 Тарифы' для покупки подписки.")
    except Exception:
        await message.answer("Не удалось получить статус (core API недоступен). Попробуйте позже.")


@router.message(Command("help"))
async def help_cmd(message: Message) -> None:
    help_message = "Поддержка: @your_support\nFAQ: скоро добавим.\nКоманды: /start /plans /status"
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        # Получаем настройки бота
        try:
            bot_settings = await api.get_bot_settings()
            if bot_settings.get("help_message"):
                help_message = bot_settings["help_message"]
        except Exception:
            pass  # Используем дефолтное сообщение
    except Exception:
        pass  # Используем дефолтное сообщение
    
    await message.answer(help_message)


@router.message(F.text == BTN_STATUS)
async def status_btn(message: Message) -> None:
    await status(message)


@router.message(F.text == BTN_HELP)
async def help_btn(message: Message) -> None:
    await help_cmd(message)


@router.message(F.text == BTN_TICKET)
async def ticket_btn(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    await state.set_state(UserTicket.waiting_topic)
    await message.answer("Укажи тему тикета (кратко).")


@router.message(UserTicket.waiting_topic)
async def ticket_topic(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    topic = (message.text or "").strip()
    if not topic:
        await message.answer("Тема не может быть пустой. Напиши кратко суть проблемы.")
        return
    settings = get_settings()
    try:
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        ticket = await api.create_ticket(message.from_user.id, topic)
        ticket_id = ticket.get("ticket_id")
        if not ticket_id:
            raise RuntimeError("ticket_id missing")

        link = settings.ticket_bot_link or ""
        if not link:
            await message.answer("Тикет создан, но support-бот не настроен. Обратитесь в поддержку.")
            await state.clear()
            return
        # username из ссылки
        support_username = link.replace("https://", "").replace("http://", "")
        support_username = support_username.split("t.me/")[-1].strip("/")
        deep_link = f"https://t.me/{support_username}?start={ticket_id}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Перейти в тикет", url=deep_link)]])
        await message.answer(
            f"✅ <b>Тикет #{ticket_id} создан</b>\n\n"
            f"📋 Тема: {topic}\n\n"
            f"💬 <b>Важно:</b> Перейди в чат с поддержкой и отправь <b>подробное сообщение</b> с описанием проблемы.\n"
            f"Администратор ответит на твое сообщение в течение рабочего времени.\n\n"
            f"Чем подробнее ты опишешь проблему, тем быстрее мы сможем помочь!",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception:
        await message.answer("Не удалось создать тикет. Попробуйте позже.")
    await state.clear()


@router.message(F.text == BTN_PLANS)
async def plans_btn(message: Message) -> None:
    """Меню тарифов подписки"""
    if not message.from_user:
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Получаем информацию о пользователе
        user_data = await api.get_user_by_tg(message.from_user.id)
        if not user_data:
            await message.answer("Пользователь не найден. Попробуйте /start")
            return
        
        # Проверяем, использован ли пробный период
        trial_used = user_data.get("trial_used", False)
        
        # Получаем тарифы
        plans_data = await api.get_subscription_plans()
        plans = plans_data.get("plans", [])
        
        # Проверяем активную подписку
        sub_data = await api.subscription_status(message.from_user.id)
        has_active = sub_data.get("has_active", False)
        
        keyboard_buttons = []
        
        # Добавляем кнопку пробного периода, если не использован
        if not trial_used and not has_active:
            keyboard_buttons.append([
                InlineKeyboardButton(text="🆓 Бесплатный пробный период (7 дней)", callback_data=f"trial_{message.from_user.id}"),
            ])
        
        # Добавляем кнопки тарифов
        for plan in plans:
            if not plan.get("is_active", True):
                continue
            days = plan.get("days", 0)
            name = plan.get("name", "")
            price_rub = plan.get("price_rub", 0)
            keyboard_buttons.append([
                InlineKeyboardButton(text=f"{name} — {price_rub:.0f} RUB", callback_data=f"buy_plan_{days}_{message.from_user.id}"),
            ])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        text = "📦 <b>Тарифы подписки</b>\n\n"
        if has_active:
            plan_name = sub_data.get("plan_name", "—")
            ends_at = sub_data.get("ends_at", "")
            text += f"✅ У вас активна подписка: <b>{plan_name}</b>\n"
            if ends_at:
                try:
                    from datetime import datetime
                    from zoneinfo import ZoneInfo
                    dt = datetime.fromisoformat(ends_at.replace("Z", "+00:00"))
                    moscow_tz = ZoneInfo("Europe/Moscow")
                    dt_moscow = dt.astimezone(moscow_tz)
                    ends_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
                    text += f"Действует до: {ends_str} МСК\n\n"
                except:
                    text += f"Действует до: {ends_at}\n\n"
            text += "Вы можете продлить подписку, выбрав тариф ниже:\n\n"
        else:
            text += "Выберите тариф подписки:\n\n"
        
        for plan in plans:
            if not plan.get("is_active", True):
                continue
            days = plan.get("days", 0)
            name = plan.get("name", "")
            price_rub = plan.get("price_rub", 0)
            description = plan.get("description", "")
            text += f"• <b>{name}</b> — {price_rub:.0f} RUB"
            if description:
                text += f"\n  {description}"
            text += "\n"
        
        if not trial_used and not has_active:
            text += "\n🆓 <b>Бесплатный пробный период</b> — 7 дней (единоразово)"
        
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        import logging
        logging.error(f"Error in plans_btn: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")


@router.message(F.text == BTN_TOPUP)
async def topup_btn(message: Message) -> None:
    """Меню пополнения баланса"""
    if not message.from_user:
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ Telegram Stars", callback_data=f"pay_stars_{message.from_user.id}"),
            InlineKeyboardButton(text="₿ CryptoBot", callback_data=f"pay_crypto_{message.from_user.id}"),
        ],
        [
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_{message.from_user.id}"),
        ],
    ])
    
    await message.answer(
        "💰 <b>Пополнение баланса</b>\n\n"
        "Выберите способ оплаты:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )


@router.message(F.text == BTN_PROFILE)
async def profile(message: Message) -> None:
    """Показывает полную информацию о профиле пользователя"""
    if not message.from_user:
        await message.answer("Не могу определить пользователя.")
        return

    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        tg_id = message.from_user.id

        # Получаем данные пользователя
        user_data = await api.get_user_by_tg(tg_id)
        if not user_data:
            await message.answer("Пользователь не найден. Попробуйте /start")
            return

        # Получаем статус подписки
        sub_data = await api.subscription_status(tg_id)

        # Получаем реферальную информацию
        ref_data = await api.referral_info(tg_id)
        
        # Получаем информацию о выбранном сервере
        selected_server_id = user_data.get("selected_server_id")
        selected_server_name = None
        if selected_server_id:
            try:
                servers_response = await api.get_available_servers()
                servers = servers_response.get("servers", [])
                for server in servers:
                    if server.get("id") == selected_server_id:
                        selected_server_name = server.get("name", f"Сервер {selected_server_id}")
                        break
            except Exception:
                pass  # Игнорируем ошибки получения серверов

        # Форматируем профиль
        balance_cents = user_data.get("balance", 0)
        # Баланс уже хранится в рублях (копейках)
        balance_rub = balance_cents / 100
        is_active = user_data.get("is_active", True)
        status_icon = "✅" if is_active else "❌"
        created_at = user_data.get("created_at", "")
        
        # Форматируем время в московское
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                moscow_tz = ZoneInfo("Europe/Moscow")
                dt_moscow = dt.astimezone(moscow_tz)
                created_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
            except:
                created_str = created_at[:10] if len(created_at) >= 10 else created_at
        else:
            created_str = "—"

        referral_code = ref_data.get("referral_code", user_data.get("referral_code", "—"))
        referrals_count = ref_data.get("referrals_count", 0)
        referred_by_tg_id = ref_data.get("referred_by_tg_id")

        profile_text = (
            f"👤 <b>Мой профиль</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: <code>{tg_id}</code>\n"
            f"💰 Баланс: <b>{balance_rub:.2f} RUB</b>\n"
            f"{status_icon} Статус: {'Активен' if is_active else 'Заблокирован'}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📦 <b>Подписка</b>\n"
        )

        if sub_data.get("has_active"):
            plan = sub_data.get("plan_name") or "—"
            ends_at = sub_data.get("ends_at") or "—"
            try:
                if ends_at:
                    dt = datetime.fromisoformat(ends_at.replace("Z", "+00:00"))
                    moscow_tz = ZoneInfo("Europe/Moscow")
                    dt_moscow = dt.astimezone(moscow_tz)
                    ends_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
                else:
                    ends_str = "—"
            except:
                ends_str = ends_at[:10] if len(ends_at) >= 10 else ends_at
            profile_text += f"✅ Активна\nТариф: {plan}\nДо: {ends_str} МСК\n"
            if selected_server_name:
                profile_text += f"📡 Сервер: {selected_server_name}\n"
        else:
            profile_text += "❌ Нет активной подписки\n"

        profile_text += (
            f"━━━━━━━━━━━━━━━━\n"
            f"🎁 <b>Реферальная система</b>\n"
            f"Код: <code>{referral_code}</code>\n"
            f"Приглашено: {referrals_count} чел.\n"
        )

        if referred_by_tg_id:
            profile_text += f"Приглашен: <code>{referred_by_tg_id}</code>\n"

        profile_text += (
            f"━━━━━━━━━━━━━━━━\n"
            f"📅 Регистрация: {created_str} МСК\n"
            f"━━━━━━━━━━━━━━━━"
        )

        # Получаем настройку автопродления
        auto_renew = user_data.get("auto_renew_subscription", True)
        auto_renew_text = "🔄 Автопродление: ВКЛ" if auto_renew else "🔄 Автопродление: ВЫКЛ"
        
        # Добавляем inline-кнопки для личного кабинета
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Пополнить баланс", callback_data=f"topup_{tg_id}")],
            [InlineKeyboardButton(text="💳 История платежей", callback_data=f"payments_{tg_id}")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data=f"stats_{tg_id}")],
            [InlineKeyboardButton(text=auto_renew_text, callback_data=f"toggle_autorenew_{tg_id}")],
        ])

        await message.answer(profile_text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await message.answer(f"Не удалось загрузить профиль: {e}")


@router.message(Command("ref"))
@router.message(F.text == BTN_REF)
async def referral(message: Message) -> None:
    if not message.from_user:
        return
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        info = await api.referral_info(message.from_user.id)
        code = info.get("referral_code")
        count = info.get("referrals_count", 0)
        me = await message.bot.get_me()
        link = f"https://t.me/{me.username}?start={code}"
        await message.answer(
            "<b>Реферальная система</b>\n"
            f"Ваш код: <code>{code}</code>\n"
            f"Приглашено: <b>{count}</b>\n\n"
            f"Ссылка для приглашения: {link}"
        )
    except Exception:
        await message.answer("Не удалось загрузить реферальную информацию (core API недоступен).")


@router.message(F.text == BTN_PROMO)
async def promo_code_btn(message: Message, state: FSMContext) -> None:
    """Обработчик кнопки промокода"""
    if not message.from_user:
        return
    
    await state.set_state(UserPromoCode.waiting_code)
    await message.answer(
        "🎟️ <b>Введите промокод</b>\n\n"
        "Напишите код промокода, который хотите применить.",
        parse_mode="HTML"
    )


@router.message(UserPromoCode.waiting_code)
async def promo_code_apply(message: Message, state: FSMContext) -> None:
    """Применение промокода"""
    if not message.from_user:
        await state.clear()
        return
    
    code = (message.text or "").strip().upper()
    if not code:
        await message.answer("Промокод не может быть пустым. Попробуйте еще раз.")
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        tg_id = message.from_user.id
        
        # Получаем информацию о пользователе
        user_data = await api.get_user_by_tg(tg_id)
        if not user_data:
            await message.answer("Пользователь не найден. Попробуйте /start")
            await state.clear()
            return
        
        # Используем временную сумму для проверки промокода (не важна для фикс суммы)
        temp_amount_cents = 10000  # 100 RUB для проверки
        
        # Проверяем промокод
        validation_result = await api.validate_promo_code(code, tg_id, temp_amount_cents)
        
        if not validation_result.get("valid"):
            error_msg = validation_result.get("error", "Промокод недействителен")
            await message.answer(f"❌ {error_msg}")
            await state.clear()
            return
        
        # Определяем тип промокода из результата валидации
        promo_type = validation_result.get("promo_type")
        discount_percent = validation_result.get("discount_percent")
        discount_amount_cents = validation_result.get("discount_amount_cents")
        
        # Применяем промокод
        apply_result = await api.apply_promo_code(code, tg_id, temp_amount_cents)
        
        if apply_result.get("success"):
            if promo_type == "fixed" and discount_amount_cents:
                # Фиксированная сумма - начисляем на баланс (уже начислено в API)
                discount_rub = discount_amount_cents / 100
                
                # Получаем обновленный баланс
                user_data = await api.get_user_by_tg(tg_id)
                balance_rub = (user_data.get("balance", 0) or 0) / 100
                
                await message.answer(
                    f"✅ <b>Промокод применен!</b>\n\n"
                    f"Вы получили: <b>{discount_rub:.2f} RUB</b>\n"
                    f"Сумма добавлена на ваш баланс.\n"
                    f"💵 Текущий баланс: <b>{balance_rub:.2f} RUB</b>",
                    parse_mode="HTML"
                )
            elif promo_type == "percent" and discount_percent:
                # Процентная скидка - не начисляем на баланс, просто применяем
                await message.answer(
                    f"✅ <b>Промокод применен!</b>\n\n"
                    f"Скидка <b>{discount_percent}%</b> будет применена при покупке подписки.",
                    parse_mode="HTML"
                )
            else:
                await message.answer("✅ Промокод применен!")
        else:
            error_msg = apply_result.get("error", "Не удалось применить промокод")
            await message.answer(f"❌ {error_msg}")
        
        await state.clear()
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при применении промокода: {e}")
        await state.clear()


@router.message(F.text == BTN_ADMIN)
async def open_admin(message: Message, state: FSMContext) -> None:
    settings = get_settings()
    admin_ids = set(settings.admin_ids)
    if message.from_user and message.from_user.id in admin_ids:
        await state.clear()
        await message.answer(
            "🛠 <b>Админ-панель</b>\n\n"
            "— 👥 Пользователи\n"
            "— 💳 Платежи\n"
            "— 🖥 Сервера",
            reply_markup=admin_menu(),
        )
    else:
        await message.answer("Нет доступа.")


@router.callback_query(F.data.startswith("payments_"))
async def show_payments_history(callback: CallbackQuery) -> None:
    """Показывает историю платежей пользователя"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    try:
        tg_id_str = callback.data.split("_", 1)[1]
        tg_id = int(tg_id_str)
        
        # Проверяем, что пользователь запрашивает свою историю
        if callback.from_user.id != tg_id:
            await callback.answer("Нет доступа", show_alert=True)
            return
        
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        payments = await api.get_user_payments(tg_id, limit=10)
        
        if not payments:
            await callback.answer("История платежей пуста", show_alert=True)
            return
        
        text = "💳 <b>История платежей</b>\n━━━━━━━━━━━━━━━━\n\n"
        total = 0
        for p in payments:
            amount = p.get("amount", 0)
            status = p.get("status", "unknown")
            date = p.get("created_at", "—")
            status_icon = "✅" if status == "succeeded" else "⏳" if status == "pending" else "❌"
            text += f"{status_icon} <b>{amount:.2f} RUB</b>\n"
            text += f"   Статус: {status}\n"
            text += f"   Дата: {date}\n\n"
            if status == "succeeded":
                total += amount
        
        text += f"━━━━━━━━━━━━━━━━\n"
        # total уже в рублях
        text += f"💰 Всего оплачено: <b>{total:.2f} RUB</b>"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"profile_{tg_id}")],
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        await callback.answer()
        
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("stats_"))
async def show_user_stats(callback: CallbackQuery) -> None:
    """Показывает статистику пользователя"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    try:
        tg_id_str = callback.data.split("_", 1)[1]
        tg_id = int(tg_id_str)
        
        if callback.from_user.id != tg_id:
            await callback.answer("Нет доступа", show_alert=True)
            return
        
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        user_data = await api.get_user_by_tg(tg_id)
        payments = await api.get_user_payments(tg_id, limit=100)
        sub_data = await api.subscription_status(tg_id)
        ref_data = await api.referral_info(tg_id)
        
        # Статистика платежей
        total_payments = len(payments)
        succeeded_payments = len([p for p in payments if p.get("status") == "succeeded"])
        total_spent = sum([p.get("amount", 0) for p in payments if p.get("status") == "succeeded"])
        
        # Статистика подписок
        has_active = sub_data.get("has_active", False)
        
        # Статистика рефералов
        referrals_count = ref_data.get("referrals_count", 0)
        total_rewards_cents = ref_data.get("total_rewards_cents", 0)
        total_rewards_rub = total_rewards_cents / 100  # Уже в рублях (копейках)
        
        text = (
            "📊 <b>Статистика</b>\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "💳 <b>Платежи</b>\n"
            f"Всего: {total_payments}\n"
            f"Успешных: {succeeded_payments}\n"
            f"Потрачено: {total_spent:.2f} RUB\n\n"
            "📦 <b>Подписки</b>\n"
            f"Статус: {'✅ Активна' if has_active else '❌ Нет'}\n\n"
            "🎁 <b>Рефералы</b>\n"
            f"Приглашено: {referrals_count} чел.\n"
            f"Заработано: {total_rewards_rub:.2f} RUB\n"
            "━━━━━━━━━━━━━━━━"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"profile_{tg_id}")],
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        await callback.answer()
        
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("profile_"))
async def show_profile_callback(callback: CallbackQuery) -> None:
    """Показывает профиль через callback (для возврата из других разделов)"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    try:
        tg_id_str = callback.data.split("_", 1)[1]
        tg_id = int(tg_id_str)
        
        if callback.from_user.id != tg_id:
            await callback.answer("Нет доступа", show_alert=True)
            return
        
        # Используем существующую логику профиля
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        user_data = await api.get_user_by_tg(tg_id)
        if not user_data:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        
        sub_data = await api.subscription_status(tg_id)
        ref_data = await api.referral_info(tg_id)
        
        balance_cents = user_data.get("balance", 0)
        # Баланс уже хранится в рублях (копейках)
        balance_rub = balance_cents / 100
        is_active = user_data.get("is_active", True)
        status_icon = "✅" if is_active else "❌"
        created_at = user_data.get("created_at", "")
        
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                moscow_tz = ZoneInfo("Europe/Moscow")
                dt_moscow = dt.astimezone(moscow_tz)
                created_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
            except:
                created_str = created_at[:10] if len(created_at) >= 10 else created_at
        else:
            created_str = "—"
        
        referral_code = ref_data.get("referral_code", user_data.get("referral_code", "—"))
        referrals_count = ref_data.get("referrals_count", 0)
        referred_by_tg_id = ref_data.get("referred_by_tg_id")
        
        profile_text = (
            f"👤 <b>Мой профиль</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: <code>{tg_id}</code>\n"
            f"💰 Баланс: <b>{balance_rub:.2f} RUB</b>\n"
            f"{status_icon} Статус: {'Активен' if is_active else 'Заблокирован'}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📦 <b>Подписка</b>\n"
        )
        
        if sub_data.get("has_active"):
            plan = sub_data.get("plan_name") or "—"
            ends_at = sub_data.get("ends_at") or "—"
            try:
                if ends_at:
                    dt = datetime.fromisoformat(ends_at.replace("Z", "+00:00"))
                    moscow_tz = ZoneInfo("Europe/Moscow")
                    dt_moscow = dt.astimezone(moscow_tz)
                    ends_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
                else:
                    ends_str = "—"
            except:
                ends_str = ends_at[:10] if len(ends_at) >= 10 else ends_at
            profile_text += f"✅ Активна\nТариф: {plan}\nДо: {ends_str} МСК\n"
            if selected_server_name:
                profile_text += f"📡 Сервер: {selected_server_name}\n"
        else:
            profile_text += "❌ Нет активной подписки\n"
        
        profile_text += (
            f"━━━━━━━━━━━━━━━━\n"
            f"🎁 <b>Реферальная система</b>\n"
            f"Код: <code>{referral_code}</code>\n"
            f"Приглашено: {referrals_count} чел.\n"
        )
        
        if referred_by_tg_id:
            profile_text += f"Приглашен: <code>{referred_by_tg_id}</code>\n"
        
        # Добавляем информацию об автопродлении
        auto_renew = user_data.get("auto_renew_subscription", True)
        auto_renew_status = "✅ Включено" if auto_renew else "❌ Выключено"
        profile_text += (
            f"━━━━━━━━━━━━━━━━\n"
            f"🔄 Автопродление: {auto_renew_status}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📅 Регистрация: {created_str} МСК\n"
            f"━━━━━━━━━━━━━━━━"
        )
        
        # Получаем настройку автопродления для кнопки
        auto_renew_text = "🔄 Автопродление: ВКЛ" if auto_renew else "🔄 Автопродление: ВЫКЛ"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 Пополнить баланс", callback_data=f"topup_{tg_id}")],
            [InlineKeyboardButton(text="💳 История платежей", callback_data=f"payments_{tg_id}")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data=f"stats_{tg_id}")],
            [InlineKeyboardButton(text=auto_renew_text, callback_data=f"toggle_autorenew_{tg_id}")],
        ])
        
        await callback.message.edit_text(profile_text, parse_mode="HTML", reply_markup=keyboard)
        await callback.answer()
        
    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("pay_stars_"))
async def pay_stars_handler(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка выбора оплаты через Telegram Stars"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    tg_id_str = callback.data.split("_", 2)[2]
    if callback.from_user.id != int(tg_id_str):
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.answer()
    await state.set_state(UserPayment.waiting_amount_stars)
    
    # Получаем минимальную сумму из настроек
    settings = get_settings()
    api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
    min_amount_rub = 1.0
    try:
        bot_settings = await api.get_bot_settings()
        if "min_topup_amount_rub" in bot_settings:
            min_amount_rub = bot_settings["min_topup_amount_rub"]
    except Exception:
        pass
    
    await callback.message.answer(
        "⭐ <b>Пополнение через Telegram Stars</b>\n\n"
        f"Введите количество Stars для пополнения (минимум эквивалент {min_amount_rub:.2f} RUB):\n"
        "Например: 1, 5, 10\n\n"
        "💡 <i>Курс конвертации: Stars → USD → RUB (по актуальному курсу ЦБ РФ)</i>"
    )


@router.message(UserPayment.waiting_amount_stars)
async def process_stars_amount(message: Message, state: FSMContext) -> None:
    """Обработка количества Stars для оплаты"""
    if not message.from_user or not message.text:
        return
    
    try:
        from core.currency import stars_to_rub
        from aiogram.types import LabeledPrice
        
        # Получаем настройки минимальной/максимальной суммы
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        min_amount_rub = 1.0
        max_amount_rub = None
        
        try:
            bot_settings = await api.get_bot_settings()
            if "min_topup_amount_rub" in bot_settings:
                min_amount_rub = bot_settings["min_topup_amount_rub"]
            if "max_topup_amount_rub" in bot_settings:
                max_amount_rub = bot_settings["max_topup_amount_rub"]
        except Exception:
            pass  # Используем дефолтные значения
        
        # Пользователь вводит количество Stars
        stars_amount = int(float(message.text.strip()))  # Принимаем целое число Stars
        
        if stars_amount < 1:
            await message.answer("❌ Минимальное количество: 1 Star")
            return
        
        # Конвертируем Stars в рубли через USD (без комиссии) для проверки минимальной/максимальной суммы
        stars_amount_rub = await stars_to_rub(stars_amount=stars_amount)
        
        # Проверяем минимальную сумму в рублях
        if stars_amount_rub < min_amount_rub:
            await message.answer(
                f"❌ Минимальная сумма пополнения: {min_amount_rub:.2f} RUB\n\n"
                f"Введенное количество Stars эквивалентно {stars_amount_rub:.2f} RUB.\n"
                f"Пожалуйста, введите больше Stars."
            )
            return
        
        # Проверяем максимальную сумму в рублях
        if max_amount_rub and stars_amount_rub > max_amount_rub:
            await message.answer(
                f"❌ Максимальная сумма пополнения: {max_amount_rub:.2f} RUB\n\n"
                f"Введенное количество Stars эквивалентно {stars_amount_rub:.2f} RUB.\n"
                f"Пожалуйста, введите меньше Stars."
            )
            return
        
        amount_cents = int(stars_amount_rub * 100)  # Сохраняем эквивалент Stars в рублях (копейках)
        payment_data = await api.create_payment(
            tg_id=message.from_user.id,
            amount_cents=amount_cents,
            provider="telegram_stars",
            currency="XTR"
        )
        
        # Показываем пользователю, сколько он получит на баланс
        balance_rub = stars_amount_rub
        
        await message.bot.send_invoice(
            chat_id=message.from_user.id,
            title="Пополнение баланса fioreVPN",
            description=f"Пополнение баланса на {stars_amount} Star{'s' if stars_amount > 1 else ''}\n"
                       f"К начислению: ~{balance_rub:.2f} RUB",
            payload=f"payment_{payment_data['payment_id']}",
            provider_token="",  # Для Stars не нужен provider_token
            currency="XTR",  # Telegram Stars валюта
            prices=[LabeledPrice(label=f"{stars_amount} Star{'s' if stars_amount > 1 else ''}", amount=stars_amount)],
            start_parameter=f"payment_{payment_data['payment_id']}",
        )
        
        await state.clear()
    except ValueError:
        await message.answer("❌ Неверный формат. Введите целое число Stars (например: 1, 5, 10)")
    except Exception as e:
        import logging
        logging.error(f"Error creating Stars payment: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")
        await state.clear()


@router.callback_query(F.data.startswith("pay_crypto_"))
async def pay_crypto_handler(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка выбора оплаты через CryptoBot"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    tg_id_str = callback.data.split("_", 2)[2]
    if callback.from_user.id != int(tg_id_str):
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.answer()
    await state.set_state(UserPayment.waiting_crypto_currency)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="USDT", callback_data=f"crypto_currency_USDT_{callback.from_user.id}"),
            InlineKeyboardButton(text="BTC", callback_data=f"crypto_currency_BTC_{callback.from_user.id}"),
        ],
        [
            InlineKeyboardButton(text="ETH", callback_data=f"crypto_currency_ETH_{callback.from_user.id}"),
            InlineKeyboardButton(text="TON", callback_data=f"crypto_currency_TON_{callback.from_user.id}"),
        ],
        [
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_{callback.from_user.id}"),
        ],
    ])
    
    await callback.message.answer(
        "₿ <b>Пополнение через CryptoBot</b>\n\n"
        "Выберите криптовалюту:",
        reply_markup=keyboard
    )


@router.callback_query(F.data.startswith("crypto_currency_"))
async def crypto_currency_handler(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка выбора криптовалюты"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    parts = callback.data.split("_")
    currency = parts[2]
    tg_id = int(parts[3])
    
    if callback.from_user.id != tg_id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.answer()
    await state.update_data(crypto_currency=currency)
    await state.set_state(UserPayment.waiting_amount_crypto)
    
    # Получаем минимальную сумму из настроек
    settings = get_settings()
    api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
    min_amount_rub = 1.0
    try:
        bot_settings = await api.get_bot_settings()
        if "min_topup_amount_rub" in bot_settings:
            min_amount_rub = bot_settings["min_topup_amount_rub"]
    except Exception:
        pass
    
    await callback.message.answer(
        f"₿ <b>Пополнение через {currency}</b>\n\n"
        f"Введите сумму пополнения в рублях (минимум: {min_amount_rub:.2f} RUB):\n"
        "Например: 100 или 50.50"
    )


@router.message(UserPayment.waiting_amount_crypto)
async def process_crypto_amount(message: Message, state: FSMContext) -> None:
    """Обработка суммы для оплаты через CryptoBot (в рублях)"""
    if not message.from_user or not message.text:
        return
    
    state_data = await state.get_data()
    currency = state_data.get("crypto_currency", "USDT")
    
    try:
        from core.currency import get_usd_to_rub_rate
        
        amount_rub = float(message.text.strip())
        
        # Получаем настройки минимальной/максимальной суммы
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        min_amount_rub = 1.0
        max_amount_rub = 1000000.0
        
        try:
            bot_settings = await api.get_bot_settings()
            if "min_topup_amount_rub" in bot_settings:
                min_amount_rub = bot_settings["min_topup_amount_rub"]
            if "max_topup_amount_rub" in bot_settings:
                max_amount_rub = bot_settings["max_topup_amount_rub"]
        except Exception:
            pass  # Используем дефолтные значения
        
        if amount_rub < min_amount_rub:
            await message.answer(f"❌ Минимальная сумма пополнения: {min_amount_rub:.2f} RUB")
            return
        # Проверяем, что сумма в USD будет >= 0.01 USD (требование CryptoBot)
        # При курсе 100 RUB = 1 USD, 1 RUB = 0.01 USD, что соответствует минимуму
        # Но для большей надежности рекомендуем минимум 2-3 RUB
        if amount_rub < 2:
            await message.answer(
                "⚠️ <b>Внимание!</b>\n\n"
                "Минимальная сумма для CryptoBot: <b>2 RUB</b> (эквивалент 0.02 USD).\n"
                "Попробуйте ввести сумму от 2 RUB."
            )
            return
        if max_amount_rub and amount_rub > max_amount_rub:
            await message.answer(f"❌ Максимальная сумма пополнения: {max_amount_rub:.2f} RUB")
            return
        
        # Сохраняем сумму в рублях (копейках) - без конвертации в USD
        amount_cents = int(amount_rub * 100)
        # Для отображения в сообщениях конвертируем в USD
        usd_rate = await get_usd_to_rub_rate()
        amount_usd = amount_rub / usd_rate
        
        # Создаем платеж в системе
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        payment_data = await api.create_payment(
            tg_id=message.from_user.id,
            amount_cents=amount_cents,
            provider="cryptobot",
            currency=currency
        )
        
        # Если есть ссылка на оплату, отправляем её
        if payment_data.get("invoice_url"):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=payment_data["invoice_url"])],
            ])
            await message.answer(
                f"₿ <b>Платеж создан</b>\n\n"
                f"Сумма: <b>{amount_rub:.2f} RUB</b> (~{amount_usd:.2f} USD)\n"
                f"Валюта: <b>{currency}</b>\n"
                f"ID платежа: <b>#{payment_data['payment_id']}</b>\n\n"
                f"Нажмите кнопку ниже для оплаты:",
                reply_markup=keyboard
            )
        else:
            await message.answer(
                f"₿ <b>Платеж создан</b>\n\n"
                f"Сумма: <b>{amount_rub:.2f} RUB</b> (~{amount_usd:.2f} USD)\n"
                f"Валюта: <b>{currency}</b>\n"
                f"ID платежа: <b>#{payment_data['payment_id']}</b>\n\n"
                f"⚠️ Ссылка на оплату не была создана. Проверьте настройки CryptoBot."
            )
        
        await state.clear()
    except ValueError:
        await message.answer("❌ Неверный формат суммы. Введите число (например: 10 или 5.50)")
    except Exception as e:
        import logging
        logging.error(f"Error creating crypto payment: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")
        await state.clear()


@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery) -> None:
    """Обработка предварительной проверки платежа (Telegram Stars)"""
    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def process_successful_payment(message: Message) -> None:
    """Обработка успешного платежа через Telegram Stars"""
    if not message.from_user or not message.successful_payment:
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Получаем информацию о платеже
        payment_info = message.successful_payment
        stars_amount = payment_info.total_amount  # Количество списанных Stars
        
        # Конвертируем Stars в рубли через USD (без комиссии)
        from core.currency import stars_to_rub
        
        stars_amount_rub = await stars_to_rub(stars_amount=stars_amount)
        amount_cents = int(stars_amount_rub * 100)  # Конвертируем в рубли (копейки)
        
        # Извлекаем payment_id из payload
        payload = payment_info.invoice_payload
        payment_id = None
        if payload and payload.startswith("payment_"):
            payment_id = int(payload.split("_")[1])
        
        # Отправляем webhook для обработки платежа
        # amount_cents уже в рублях (эквивалент списанных Stars)
        await api.payment_webhook(
            payment_id=payment_id,
            external_id=payment_info.telegram_payment_charge_id,
            provider="telegram_stars",
            status="succeeded",
            amount_cents=amount_cents,
            currency="XTR",
            raw_data={
                "telegram_payment_charge_id": payment_info.telegram_payment_charge_id,
                "provider_payment_charge_id": payment_info.provider_payment_charge_id,
                "stars_amount": stars_amount,
            }
        )
        
        # Получаем обновленный баланс
        # amount_cents и new_balance_cents уже в рублях (копейках)
        user_data = await api.get_user_by_tg(message.from_user.id)
        new_balance_cents = user_data.get("balance", 0)
        new_balance_rub = new_balance_cents / 100
        amount_rub = amount_cents / 100
        
        await message.answer(
            f"✅ <b>Платеж успешно обработан!</b>\n\n"
            f"💰 Пополнено: <b>{amount_rub:.2f} RUB</b>\n"
            f"💵 Текущий баланс: <b>{new_balance_rub:.2f} RUB</b>"
        )
    except Exception as e:
        import logging
        logging.error(f"Error processing payment: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке платежа. Обратитесь в поддержку.")


@router.callback_query(F.data.startswith("cancel_"))
async def cancel_payment(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена платежа"""
    await callback.answer()
    await state.clear()
    await callback.message.answer("❌ Операция отменена")


@router.callback_query(F.data.startswith("trial_"))
async def trial_handler(callback: CallbackQuery) -> None:
    """Обработка активации пробного периода"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    tg_id_str = callback.data.split("_", 1)[1]
    if callback.from_user.id != int(tg_id_str):
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        result = await api.activate_trial(callback.from_user.id)
        
        ends_at = result.get("ends_at", "")
        try:
            dt = datetime.fromisoformat(ends_at.replace("Z", "+00:00"))
            moscow_tz = ZoneInfo("Europe/Moscow")
            dt_moscow = dt.astimezone(moscow_tz)
            ends_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
        except:
            ends_str = ends_at
        
        await callback.answer("✅ Пробный период активирован!", show_alert=True)
        await callback.message.answer(
            f"🆓 <b>Пробный период активирован!</b>\n\n"
            f"Вы получили бесплатный доступ на 7 дней.\n"
            f"Подписка действует до: {ends_str} МСК\n\n"
            f"Используйте /status для проверки статуса подписки.",
            parse_mode="HTML"
        )
    except Exception as e:
        import logging
        logging.error(f"Error activating trial: {e}", exc_info=True)
        error_msg = str(e)
        if "trial_already_used" in error_msg:
            await callback.answer("❌ Пробный период уже использован", show_alert=True)
        elif "active_subscription_exists" in error_msg:
            await callback.answer("❌ У вас уже есть активная подписка", show_alert=True)
        else:
            await callback.answer("❌ Ошибка активации пробного периода", show_alert=True)


@router.callback_query(F.data.startswith("buy_plan_"))
async def buy_plan_handler(callback: CallbackQuery) -> None:
    """Обработка покупки подписки"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    parts = callback.data.split("_")
    plan_days = int(parts[2])
    tg_id = int(parts[3])
    
    if callback.from_user.id != tg_id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Покупаем подписку
        result = await api.purchase_subscription(callback.from_user.id, plan_days)
        
        plan_name = result.get("plan_name", "")
        price_rub = result.get("price_rub", 0)
        balance_remaining = result.get("balance_remaining", 0)
        ends_at = result.get("ends_at", "")
        
        try:
            dt = datetime.fromisoformat(ends_at.replace("Z", "+00:00"))
            moscow_tz = ZoneInfo("Europe/Moscow")
            dt_moscow = dt.astimezone(moscow_tz)
            ends_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
        except:
            ends_str = ends_at
        
        await callback.answer("✅ Подписка активирована!", show_alert=True)
        # Удаляем сообщение с тарифами
        try:
            await callback.message.delete()
        except Exception:
            pass
        # Отправляем новое сообщение с подтверждением и обновляем меню
        is_admin = callback.from_user.id in set(get_settings().admin_ids)
        await callback.message.answer(
            f"✅ <b>Подписка успешно активирована!</b>\n\n"
            f"📦 Тариф: <b>{plan_name}</b>\n"
            f"💰 Стоимость: {price_rub:.0f} RUB\n"
            f"📅 Действует до: {ends_str} МСК\n"
            f"💵 Остаток баланса: {balance_remaining:.2f} RUB\n\n"
            f"Теперь вы можете выбрать сервер и получить ключ в меню.",
            parse_mode="HTML",
            reply_markup=user_menu(is_admin=is_admin, has_subscription=True)
        )
    except Exception as e:
        import logging
        import httpx
        
        # Сначала проверяем, это ли ошибка недостатка баланса
        is_balance_error = False
        if isinstance(e, httpx.HTTPStatusError):
            try:
                error_response = e.response.json()
                error_detail = error_response.get("detail", "")
                if "insufficient_balance" in error_detail.lower():
                    is_balance_error = True
            except:
                pass
        
        # Логируем как INFO для недостатка баланса (это ожидаемая ситуация), иначе как ERROR
        if is_balance_error:
            logging.info(f"Insufficient balance when purchasing subscription: {e}")
        else:
            logging.error(f"Error purchasing subscription: {e}", exc_info=True)
        
        error_msg = str(e)
        error_detail = ""
        
        # Обрабатываем httpx.HTTPStatusError
        if isinstance(e, httpx.HTTPStatusError):
            try:
                # Пытаемся получить JSON из ответа
                error_response = e.response.json()
                error_detail = error_response.get("detail", "")
                logging.info(f"Error detail from JSON: {error_detail}")
                if not error_detail:
                    # Пробуем получить из сообщения об ошибке
                    error_detail = str(e)
            except Exception as json_err:
                # Если не удалось распарсить JSON, пробуем извлечь из текста ответа
                logging.warning(f"Failed to parse JSON error response: {json_err}")
                try:
                    error_text = e.response.text
                    logging.info(f"Error detail from text: {error_text}")
                    # Пробуем распарсить как JSON, если это строка JSON
                    if error_text.startswith("{") or error_text.startswith("{"):
                        try:
                            import json
                            error_response = json.loads(error_text)
                            error_detail = error_response.get("detail", error_text)
                        except:
                            error_detail = error_text
                    else:
                        error_detail = error_text
                    if not error_detail:
                        error_detail = str(e)
                except Exception as text_err:
                    logging.warning(f"Failed to get text from response: {text_err}")
                    error_detail = str(e)
        else:
            error_detail = error_msg
        
        logging.info(f"Final error_detail: '{error_detail}', error_msg: '{error_msg}'")
        
        # Проверяем на недостаток баланса (в разных вариантах написания)
        is_insufficient_balance = (
            "insufficient_balance" in error_detail.lower() or 
            "insufficient_balance" in error_msg.lower() or
            "недостаточно" in error_detail.lower() or
            "недостаточно" in error_msg.lower() or
            "не хватает" in error_detail.lower() or
            "не хватает" in error_msg.lower()
        )
        
        logging.info(f"is_insufficient_balance: {is_insufficient_balance}")
        
        if is_insufficient_balance:
            logging.info("Processing insufficient balance error")
            # Извлекаем информацию о требуемой и доступной сумме
            required = ""
            available = ""
            if "Required:" in error_detail:
                try:
                    parts = error_detail.split("Required:")[1].split(",")
                    required = parts[0].strip()
                    if "Available:" in error_detail:
                        available = error_detail.split("Available:")[1].strip()
                except:
                    pass
            
            # Убеждаемся, что суммы отображаются с копейками и указанием валюты
            if required:
                if "RUB" not in required:
                    try:
                        # Если это просто число, добавляем "RUB"
                        required_float = float(required)
                        required = f"{required_float:.2f} RUB"
                    except:
                        required = f"{required} RUB"
            
            if available:
                if "RUB" not in available:
                    try:
                        # Если это просто число, добавляем "RUB"
                        available_float = float(available)
                        available = f"{available_float:.2f} RUB"
                    except:
                        available = f"{available} RUB"
            
            message_text = "❌ <b>Недостаточно средств на балансе</b>\n\n"
            if required and available:
                message_text += f"💰 Требуется: <b>{required}</b>\n"
                message_text += f"💵 Доступно: <b>{available}</b>\n\n"
            message_text += "Пополните баланс для покупки подписки."
            
            # Добавляем inline-кнопку для пополнения баланса
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💰 Пополнить баланс", callback_data=f"topup_{callback.from_user.id}")],
            ])
            
            logging.info(f"Sending insufficient balance message. required='{required}', available='{available}', message_text='{message_text[:100]}...'")
            await callback.answer("❌ Недостаточно средств на балансе", show_alert=True)
            await callback.message.answer(
                message_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            logging.info("Insufficient balance message sent successfully")
        else:
            # Если не удалось определить тип ошибки, но это 400, возможно это тоже недостаток баланса
            if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 400:
                logging.warning(f"Got 400 error but couldn't parse details. Showing generic insufficient balance message.")
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💰 Пополнить баланс", callback_data=f"topup_{callback.from_user.id}")],
                ])
                await callback.answer("❌ Недостаточно средств на балансе", show_alert=True)
                await callback.message.answer(
                    "❌ <b>Недостаточно средств на балансе</b>\n\n"
                    "Пополните баланс для покупки подписки.",
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            else:
                await callback.answer("❌ Ошибка покупки подписки", show_alert=True)
                await callback.message.answer(
                    f"❌ <b>Произошла ошибка</b>\n\n"
                    f"Не удалось купить подписку. Попробуйте позже или обратитесь в поддержку.",
                    parse_mode="HTML"
                )


@router.callback_query(F.data.startswith("topup_"))
async def topup_handler(callback: CallbackQuery) -> None:
    """Переход к пополнению баланса"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    tg_id_str = callback.data.split("_", 1)[1]
    if callback.from_user.id != int(tg_id_str):
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    await callback.answer()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐ Telegram Stars", callback_data=f"pay_stars_{callback.from_user.id}"),
            InlineKeyboardButton(text="₿ CryptoBot", callback_data=f"pay_crypto_{callback.from_user.id}"),
        ],
        [
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_{callback.from_user.id}"),
        ],
    ])
    
    await callback.message.answer(
        "💰 <b>Пополнение баланса</b>\n\n"
        "Выберите способ оплаты:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )


@router.message(F.text == BTN_SERVERS)
async def servers_btn(message: Message) -> None:
    """Показать список серверов для выбора"""
    if not message.from_user:
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Проверяем активную подписку
        sub_data = await api.subscription_status(message.from_user.id)
        if not sub_data.get("has_active", False):
            await message.answer("❌ У вас нет активной подписки. Сначала купите подписку в разделе '📦 Тарифы'.")
            return
        
        # Получаем список доступных серверов
        servers_response = await api.get_available_servers()
        servers = servers_response.get("servers", [])
        
        if not servers:
            await message.answer("❌ Нет доступных серверов. Обратитесь в поддержку.")
            return
        
        # Получаем выбранный сервер пользователя
        user_data = await api.get_user_by_tg(message.from_user.id)
        if not user_data:
            await message.answer("❌ Ошибка получения данных пользователя.")
            return
        
        selected_server_id = user_data.get("selected_server_id")
        
        # Формируем сообщение со списком серверов
        text_lines = ["📡 <b>Доступные серверы</b>\n"]
        
        # Формируем inline клавиатуру с кнопками выбора серверов
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard_buttons = []
        
        for server in servers:
            server_id = server.get("id")
            server_name = server.get("name", f"Сервер {server_id}")
            location = server.get("location", "")
            status = server.get("status", {})
            is_online = status.get("is_online", False)
            response_time = status.get("response_time_ms")
            
            # Индикатор статуса
            status_emoji = "🟢" if is_online else "🔴"
            status_text = "Онлайн" if is_online else "Оффлайн"
            
            # Пинг
            ping_text = ""
            if response_time is not None:
                ping_text = f" | Пинг: {response_time} мс"
            
            # Отметка выбранного сервера
            selected_mark = " ✅" if selected_server_id == server_id else ""
            
            # Текст для кнопки
            button_text = f"{status_emoji} {server_name}"
            if location:
                button_text += f" ({location})"
            button_text += selected_mark
            
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"select_server_{server_id}_{message.from_user.id}"
                )
            ])
            
            # Текст для сообщения
            line = f"{status_emoji} <b>{server_name}</b>"
            if location:
                line += f" ({location})"
            line += f"\n   Статус: {status_text}{ping_text}{selected_mark}"
            text_lines.append(line)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        await message.answer(
            "\n".join(text_lines),
            parse_mode="HTML",
            reply_markup=keyboard
        )
    except Exception as e:
        import logging
        logging.error(f"Ошибка при получении списка серверов: {e}", exc_info=True)
        await message.answer("❌ Ошибка при загрузке серверов. Попробуйте позже.")


@router.callback_query(F.data.startswith("select_server_"))
async def select_server_handler(callback: CallbackQuery) -> None:
    """Обработка выбора сервера по inline кнопке"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    parts = callback.data.split("_")
    server_id = int(parts[2])
    tg_id = int(parts[3])
    
    if callback.from_user.id != tg_id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Получаем список серверов для получения имени
        servers_response = await api.get_available_servers()
        servers = servers_response.get("servers", [])
        
        # Находим сервер по ID
        selected_server = None
        for server in servers:
            if server.get("id") == server_id:
                selected_server = server
                break
        
        if not selected_server:
            await callback.answer("❌ Сервер не найден", show_alert=True)
            return
        
        server_name = selected_server.get("name", f"Сервер {server_id}")
        
        # Устанавливаем выбранный сервер
        await api.set_selected_server(callback.from_user.id, server_id)
        
        # Удаляем сообщение с кнопками
        try:
            await callback.message.delete()
        except Exception:
            pass
        
        await callback.answer("✅ Сервер выбран!", show_alert=True)
        await callback.message.answer(
            f"✅ Сервер <b>{server_name}</b> выбран!\n\n"
            f"Теперь вы можете сгенерировать ключ в разделе '🔑 Ключ'.",
            parse_mode="HTML"
        )
    except Exception as e:
        import logging
        logging.error(f"Ошибка при выборе сервера: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при выборе сервера", show_alert=True)


@router.message(F.text == BTN_KEY)
async def key_btn(message: Message) -> None:
    """Показать/сгенерировать VPN ключ"""
    if not message.from_user:
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Проверяем активную подписку
        sub_data = await api.subscription_status(message.from_user.id)
        if not sub_data.get("has_active", False):
            await message.answer("❌ У вас нет активной подписки. Сначала купите подписку в разделе '📦 Тарифы'.")
            return
        
        # Проверяем, выбран ли сервер
        user_data = await api.get_user_by_tg(message.from_user.id)
        if not user_data:
            await message.answer("❌ Ошибка получения данных пользователя.")
            return
        
        selected_server_id = user_data.get("selected_server_id")
        
        if not selected_server_id:
            await message.answer(
                "❌ Сначала выберите сервер в разделе '📡 Сервера'."
            )
            return
        
        # Получаем текущий ключ
        try:
            key_data = await api.get_user_vpn_key(message.from_user.id)
            vpn_key = key_data.get("key")
            server_name = key_data.get("server_name", "Сервер")
            
            if vpn_key:
                # Ключ уже есть - показываем его и кнопку "Сменить ключ"
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                # Получаем базовый URL для ссылки на инструкцию
                base_url = str(settings.core_api_base).rstrip('/')
                guide_url = f"{base_url}/vpn-guide"
                
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="📖 Инструкция по использованию",
                            url=guide_url
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="🔄 Сменить ключ",
                            callback_data=f"regenerate_key_{message.from_user.id}"
                        )
                    ]
                ])
                
                await message.answer(
                    f"🔑 <b>Ваш VPN ключ</b>\n\n"
                    f"Сервер: <b>{server_name}</b>\n\n"
                    f"<code>{vpn_key}</code>\n\n"
                    f"Используйте этот ключ для подключения к VPN.\n\n"
                    f"📖 <a href=\"{guide_url}\">Инструкция по использованию</a>",
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            else:
                # Ключа нет - показываем кнопку "Сгенерировать ключ"
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="🔑 Сгенерировать ключ",
                        callback_data=f"generate_key_{message.from_user.id}"
                    )
                ]])
                
                await message.answer(
                    f"🔑 <b>Генерация VPN ключа</b>\n\n"
                    f"Сервер: <b>{server_name}</b>\n\n"
                    f"Нажмите кнопку ниже, чтобы сгенерировать ключ:",
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
        except Exception as e:
            import logging
            logging.error(f"Ошибка при получении ключа: {e}", exc_info=True)
            # Если ключа нет, показываем кнопку генерации
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="🔑 Сгенерировать ключ",
                    callback_data=f"generate_key_{message.from_user.id}"
                )
            ]])
            
            await message.answer(
                "🔑 <b>Генерация VPN ключа</b>\n\n"
                "Нажмите кнопку ниже, чтобы сгенерировать ключ:",
                parse_mode="HTML",
                reply_markup=keyboard
            )
    except Exception as e:
        import logging
        logging.error(f"Ошибка в key_btn: {e}", exc_info=True)
        await message.answer("❌ Ошибка. Попробуйте позже.")


@router.callback_query(F.data.startswith("generate_key_"))
async def generate_key_handler(callback: CallbackQuery) -> None:
    """Генерация VPN ключа"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    parts = callback.data.split("_")
    tg_id = int(parts[2])
    
    if callback.from_user.id != tg_id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Генерируем ключ (regenerate=False - создать новый)
        try:
            result = await api.generate_vpn_key(callback.from_user.id, regenerate=False)
            vpn_key = result.get("key")
            server_name = result.get("server_name", "Сервер")
            
            if not vpn_key:
                await callback.answer("❌ Не удалось сгенерировать ключ", show_alert=True)
                return
        except httpx.HTTPStatusError as e:
            import logging
            status_code = e.response.status_code
            error_detail = ""
            try:
                error_data = e.response.json()
                error_detail = error_data.get("detail", "")
            except:
                pass
            
            logging.error(f"HTTP ошибка при генерации ключа: {status_code} - {error_detail}")
            
            # Обработка разных HTTP статусов
            if status_code == 400:
                if "user_already_has_key" in error_detail:
                    await callback.answer(
                        "❌ У вас уже есть активный ключ. Используйте кнопку 'Сменить ключ' для создания нового.",
                        show_alert=True
                    )
                elif "server_configuration_error" in error_detail or "Inbound не найден" in error_detail:
                    await callback.answer(
                        "❌ Ошибка конфигурации сервера. Обратитесь в поддержку.",
                        show_alert=True
                    )
                else:
                    await callback.answer("❌ Ошибка запроса. Проверьте данные и попробуйте снова.", show_alert=True)
            elif status_code == 404:
                await callback.answer("❌ Пользователь или сервер не найден.", show_alert=True)
            elif status_code == 503:
                await callback.answer(
                    "❌ Сервер VPN временно недоступен. Попробуйте позже.",
                    show_alert=True
                )
            else:
                await callback.answer("❌ Ошибка при генерации ключа. Попробуйте позже.", show_alert=True)
            return  # Важно: возвращаемся, чтобы не доходить до кода ниже
        except Exception as e:
            import logging
            error_msg = str(e)
            logging.error(f"Ошибка при генерации ключа: {e}", exc_info=True)
            await callback.answer("❌ Ошибка при генерации ключа. Попробуйте позже.", show_alert=True)
            return
        
        # Обновляем сообщение
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        # Получаем базовый URL для ссылки на инструкцию
        base_url = str(settings.core_api_base).rstrip('/')
        guide_url = f"{base_url}/vpn-guide"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📖 Инструкция по использованию",
                    url=guide_url
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Сменить ключ",
                    callback_data=f"regenerate_key_{tg_id}"
                )
            ]
        ])
        
        await callback.message.edit_text(
            f"🔑 <b>Ваш VPN ключ</b>\n\n"
            f"Сервер: <b>{server_name}</b>\n\n"
            f"<code>{vpn_key}</code>\n\n"
            f"Используйте этот ключ для подключения к VPN.\n\n"
            f"📖 <a href=\"{guide_url}\">Инструкция по использованию</a>",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        await callback.answer("✅ Ключ сгенерирован!")
    except Exception as e:
        import logging
        logging.error(f"Ошибка при генерации ключа: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при генерации ключа", show_alert=True)


@router.callback_query(F.data.startswith("regenerate_key_"))
async def regenerate_key_handler(callback: CallbackQuery) -> None:
    """Перегенерация VPN ключа"""
    if not callback.from_user:
        await callback.answer("Ошибка")
        return
    
    parts = callback.data.split("_")
    tg_id = int(parts[2])
    
    if callback.from_user.id != tg_id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    try:
        settings = get_settings()
        api = CoreApi(str(settings.core_api_base), admin_token=settings.admin_token or "")
        
        # Генерируем новый ключ (regenerate=True - сменить существующий)
        try:
            result = await api.generate_vpn_key(callback.from_user.id, regenerate=True)
            vpn_key = result.get("key")
            server_name = result.get("server_name", "Сервер")
            
            if not vpn_key:
                await callback.answer("❌ Не удалось сгенерировать ключ", show_alert=True)
                return
        except httpx.HTTPStatusError as e:
            import logging
            status_code = e.response.status_code
            error_detail = ""
            try:
                error_data = e.response.json()
                error_detail = error_data.get("detail", "")
            except:
                pass
            
            logging.error(f"HTTP ошибка при регенерации ключа: {status_code} - {error_detail}")
            
            # Обработка разных HTTP статусов
            if status_code == 400:
                if "server_configuration_error" in error_detail or "Inbound не найден" in error_detail:
                    await callback.answer(
                        "❌ Ошибка конфигурации сервера. Обратитесь в поддержку.",
                        show_alert=True
                    )
                else:
                    await callback.answer("❌ Ошибка запроса. Проверьте данные и попробуйте снова.", show_alert=True)
            elif status_code == 404:
                await callback.answer("❌ Пользователь или сервер не найден.", show_alert=True)
            elif status_code == 503:
                await callback.answer(
                    "❌ Сервер VPN временно недоступен. Попробуйте позже.",
                    show_alert=True
                )
            else:
                await callback.answer("❌ Ошибка при генерации ключа. Попробуйте позже.", show_alert=True)
            return
        except Exception as e:
            import logging
            logging.error(f"Ошибка при регенерации ключа: {e}", exc_info=True)
            await callback.answer("❌ Ошибка при генерации ключа. Попробуйте позже.", show_alert=True)
            return
        
        # Обновляем сообщение
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        # Получаем базовый URL для ссылки на инструкцию
        base_url = str(settings.core_api_base).rstrip('/')
        guide_url = f"{base_url}/vpn-guide"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📖 Инструкция по использованию",
                    url=guide_url
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Сменить ключ",
                    callback_data=f"regenerate_key_{tg_id}"
                )
            ]
        ])
        
        await callback.message.edit_text(
            f"🔑 <b>Ваш VPN ключ</b>\n\n"
            f"Сервер: <b>{server_name}</b>\n\n"
            f"<code>{vpn_key}</code>\n\n"
            f"Используйте этот ключ для подключения к VPN.\n\n"
            f"📖 <a href=\"{guide_url}\">Инструкция по использованию</a>",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        await callback.answer("✅ Ключ обновлен!")
    except Exception as e:
        import logging
        logging.error(f"Ошибка при перегенерации ключа: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при обновлении ключа", show_alert=True)


def register(dp: Dispatcher) -> None:
    dp.include_router(router)

