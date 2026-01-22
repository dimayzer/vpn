"""Интеграция с CryptoBot API для приема платежей"""
from __future__ import annotations

import httpx
import logging
from typing import Any


class CryptoBotAPI:
    """Клиент для работы с CryptoBot API"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://pay.crypt.bot/api"
    
    async def create_invoice(
        self,
        amount: float,
        currency: str = "USDT",
        description: str = "",
        paid_btn_name: str = "callback",
        paid_btn_url: str = "",
        payload: str = "",
    ) -> dict[str, Any]:
        """Создание инвойса для оплаты
        
        Args:
            amount: Сумма в криптовалюте (должна быть >= минимальной для валюты)
            currency: Код криптовалюты (USDT, BTC, ETH, TON и т.д.)
        """
        import logging
        
        # CryptoBot API требует строку для amount в формате "1.00"
        # CryptoBot проверяет минимальную сумму в USD (0.01 USD), а не в криптовалюте
        # Поэтому мы должны убедиться, что сумма в USD >= 0.01
        
        # Форматируем сумму для CryptoBot API
        # Используем формат с достаточным количеством знаков после запятой
        amount_str = f"{amount:.8f}".rstrip('0').rstrip('.')
        if not amount_str or amount_str == '.':
            amount_str = "0"
        
        # Минимальные суммы для разных валют (в единицах криптовалюты)
        # Эти минимумы нужны только для защиты от ошибок, основная проверка - в USD
        min_amounts = {
            "USDT": 0.01,  # Минимум 0.01 USDT (≈ 0.01 USD)
            "BTC": 0.00001,  # Минимум для BTC
            "ETH": 0.001,  # Минимум для ETH
            "TON": 0.01,  # Минимум для TON
        }
        min_amount = min_amounts.get(currency, 0.01)
        
        # Применяем минимум только если сумма действительно микроскопическая
        # Это защита от ошибок конвертации, но не должна перезаписывать правильные суммы
        if amount > 0 and amount < min_amount and amount < 0.0001:
            # Только если сумма действительно микроскопическая - применяем минимум
            amount = min_amount
            amount_str = f"{amount:.8f}".rstrip('0').rstrip('.')
        
        request_data = {
            "asset": currency,
            "amount": amount_str,
            "description": description[:255] if description else "",  # Максимум 255 символов
            "paid_btn_name": paid_btn_name or "callback",
            "payload": payload[:64] if payload else "",  # Максимум 64 символа
        }
        
        # paid_btn_url обязателен для CryptoBot API
        # Если не указан, используем дефолтный URL бота
        if paid_btn_url and paid_btn_url.strip():
            request_data["paid_btn_url"] = paid_btn_url.strip()
        else:
            # Используем дефолтный URL (можно указать URL вашего бота или сайта)
            request_data["paid_btn_url"] = "https://t.me/CryptoBot"
        
        logging.info(f"CryptoBot createInvoice request: asset={currency}, amount={amount_str}, description={description[:50]}")
        
        # Увеличиваем таймаут до 90 секунд для медленных соединений
        # connect=30.0 - таймаут на подключение
        # read=90.0 - таймаут на чтение ответа
        timeout = httpx.Timeout(90.0, connect=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/createInvoice",
                    headers={"Crypto-Pay-API-Token": self.token},
                    json=request_data,
                )
                
                # Логируем ответ для отладки
                if response.status_code != 200:
                    logging.error(f"CryptoBot API error {response.status_code}: {response.text}")
                
                response.raise_for_status()
                result = response.json()
                logging.info(f"CryptoBot createInvoice response: {result}")
                return result
            except httpx.ReadTimeout as e:
                logging.error(f"CryptoBot API timeout error: {e}. Request took too long.")
                raise Exception(f"Превышено время ожидания ответа от CryptoBot API. Попробуйте позже.") from e
            except httpx.ConnectTimeout as e:
                logging.error(f"CryptoBot API connection timeout: {e}")
                raise Exception(f"Не удалось подключиться к CryptoBot API. Проверьте интернет-соединение.") from e
            except httpx.HTTPStatusError as e:
                logging.error(f"CryptoBot API HTTP error: {e.response.status_code} - {e.response.text}")
                raise
            except Exception as e:
                logging.error(f"CryptoBot API error: {e}")
                raise
    
    async def get_invoice_status(self, invoice_id: int) -> dict[str, Any]:
        """Получение статуса инвойса"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.base_url}/getInvoices",
                headers={"Crypto-Pay-API-Token": self.token},
                params={"invoice_ids": str(invoice_id)},
            )
            response.raise_for_status()
            return response.json()
    
    async def get_exchange_rates(self) -> dict[str, Any]:
        """Получение курсов обмена"""
        timeout = httpx.Timeout(30.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                response = await client.get(
                    f"{self.base_url}/getExchangeRates",
                    headers={"Crypto-Pay-API-Token": self.token},
                )
                response.raise_for_status()
                return response.json()
            except httpx.ReadTimeout as e:
                logging.warning(f"CryptoBot getExchangeRates timeout: {e}. Using fallback rates.")
                raise
            except httpx.ConnectTimeout as e:
                logging.warning(f"CryptoBot getExchangeRates connection timeout: {e}. Using fallback rates.")
                raise
    
    async def set_webhook(self, url: str) -> dict[str, Any]:
        """Настройка webhook для получения уведомлений о платежах
        
        Примечание: CryptoBot может не поддерживать настройку webhook через API.
        Рекомендуется настраивать webhook через интерфейс бота @CryptoBot.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Пробуем POST метод
            try:
                response = await client.post(
                    f"{self.base_url}/setWebhook",
                    headers={"Crypto-Pay-API-Token": self.token},
                    json={"url": url},
                )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 405:
                    # Метод не поддерживается - возможно, нужно настраивать через бота
                    return {
                        "ok": False,
                        "error": "Method not allowed. Please set webhook through @CryptoBot interface.",
                        "error_code": 405,
                    }
                raise
    
    async def delete_webhook(self) -> dict[str, Any]:
        """Удаление webhook"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/deleteWebhook",
                headers={"Crypto-Pay-API-Token": self.token},
            )
            response.raise_for_status()
            return response.json()
    
    async def get_me(self) -> dict[str, Any]:
        """Получение информации о приложении"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{self.base_url}/getMe",
                headers={"Crypto-Pay-API-Token": self.token},
            )
            response.raise_for_status()
            return response.json()

