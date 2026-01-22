"""Модуль для работы с валютами и конвертацией"""
from __future__ import annotations

import httpx
from typing import Any
import logging


async def get_usd_to_rub_rate() -> float:
    """Получение курса USD к RUB через публичный API"""
    try:
        # Используем API ЦБ РФ или другой публичный API
        timeout = httpx.Timeout(15.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            # API ЦБ РФ (официальный курс)
            response = await client.get(
                "https://www.cbr-xml-daily.ru/daily_json.js",
                timeout=15.0
            )
            if response.status_code == 200:
                data = response.json()
                usd_rate = data.get("Valute", {}).get("USD", {}).get("Value", 0)
                if usd_rate and usd_rate > 0:
                    logging.info(f"Successfully fetched USD rate from CBR: {usd_rate}")
                    return float(usd_rate)
    except httpx.ReadTimeout:
        logging.warning("CBR API timeout, using fallback rate")
    except httpx.ConnectTimeout:
        logging.warning("CBR API connection timeout, using fallback rate")
    except Exception as e:
        logging.warning(f"Failed to fetch USD rate from CBR: {e}")
    
    # Fallback: используем фиксированный курс (примерно 100 RUB = 1 USD)
    # В реальности лучше использовать актуальный курс
    logging.warning("Using fallback USD rate: 100 RUB = 1 USD")
    return 100.0


def rub_to_usd_cents(rub_amount: float, usd_rate: float | None = None) -> int:
    """Конвертация рублей в центы USD
    
    Args:
        rub_amount: Сумма в рублях
        usd_rate: Курс USD к RUB (если None, будет получен автоматически)
    
    Returns:
        Сумма в центах USD
    """
    if usd_rate is None:
        # Используем фиксированный курс по умолчанию
        # В реальности лучше получать курс асинхронно
        usd_rate = 100.0  # 1 USD = 100 RUB
    
    # Конвертируем: RUB -> USD -> центы
    usd_amount = rub_amount / usd_rate
    usd_cents = int(usd_amount * 100)
    return max(1, usd_cents)  # Минимум 1 цент


def usd_cents_to_rub(usd_cents: int, usd_rate: float | None = None) -> float:
    """Конвертация центов USD в рубли
    
    Args:
        usd_cents: Сумма в центах USD
        usd_rate: Курс USD к RUB
    
    Returns:
        Сумма в рублях
    """
    if usd_rate is None:
        usd_rate = 100.0  # 1 USD = 100 RUB
    
    usd_amount = usd_cents / 100.0
    rub_amount = usd_amount * usd_rate
    return rub_amount


def format_balance_rub(balance_cents: int, usd_rate: float | None = None) -> str:
    """Форматирование баланса в рублях для отображения"""
    rub_amount = usd_cents_to_rub(balance_cents, usd_rate)
    return f"{rub_amount:.2f} RUB"


def get_stars_to_usd_rate() -> float:
    """Получение курса Telegram Stars (XTR) к USD на основе таблицы конвертации
    
    Таблица конвертации:
    - 14 звезд ~ 0.182 USD
    - 15 звезд ~ 0.195 USD
    - 16 звезд ~ 0.208 USD
    
    Returns:
        Курс: сколько USD стоит 1 Star (среднее значение из таблицы)
    """
    # Вычисляем курс для каждого значения из таблицы
    rate_14 = 0.182 / 14  # ≈ 0.013 USD за звезду
    rate_15 = 0.195 / 15  # ≈ 0.013 USD за звезду
    rate_16 = 0.208 / 16  # ≈ 0.013 USD за звезду
    
    # Используем среднее значение
    avg_rate = (rate_14 + rate_15 + rate_16) / 3.0
    return avg_rate


async def stars_to_rub(stars_amount: float) -> float:
    """Конвертация Telegram Stars в рубли через USD (без комиссии)
    
    Конвертация происходит в два этапа:
    1. Stars -> USD (на основе таблицы конвертации)
    2. USD -> RUB (через курс ЦБ РФ)
    
    Args:
        stars_amount: Количество Stars
    
    Returns:
        Сумма в рублях, которую нужно начислить на баланс (БЕЗ комиссии)
    """
    # Получаем курс Stars к USD
    stars_usd_rate = get_stars_to_usd_rate()
    
    # Конвертируем Stars в USD
    usd_amount = stars_amount * stars_usd_rate
    
    # Получаем курс USD к RUB
    usd_rub_rate = await get_usd_to_rub_rate()
    
    # Конвертируем USD в RUB
    rub_amount = usd_amount * usd_rub_rate
    
    return rub_amount

