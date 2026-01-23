from __future__ import annotations

from typing import Any

import httpx


class CoreApi:
    def __init__(self, base_url: str, admin_token: str = ""):
        self._base_url = base_url.rstrip("/")
        self._admin_token = admin_token or ""

    def _admin_headers(self) -> dict[str, str]:
        if self._admin_token:
            return {"X-Admin-Token": self._admin_token}
        # Если токен не установлен, возвращаем пустой словарь (но это вызовет ошибку на сервере)
        import logging
        logging.warning("Admin token not set in CoreApi, requests may fail")
        return {}

    async def upsert_user(self, tg_id: int, username: str | None = None, first_name: str | None = None, last_name: str | None = None, referral_code: str | None = None) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            payload: dict[str, Any] = {"tg_id": tg_id}
            if username is not None:
                payload["username"] = username
            if first_name is not None:
                payload["first_name"] = first_name
            if last_name is not None:
                payload["last_name"] = last_name
            if referral_code:
                payload["referral_code"] = referral_code
            r = await client.post(f"{self._base_url}/users/upsert", json=payload)
            r.raise_for_status()
            return r.json()

    async def subscription_status(self, tg_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/subscriptions/status/by_tg/{tg_id}")
            r.raise_for_status()
            return r.json()

    async def get_subscription_plans(self) -> dict[str, Any]:
        """Получить список тарифов подписки"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/subscriptions/plans")
            r.raise_for_status()
            return r.json()
    
    async def purchase_subscription(self, tg_id: int, plan_months: int, promo_code: str | None = None) -> dict[str, Any]:
        """Покупка подписки"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            payload = {"tg_id": tg_id, "plan_months": plan_months}
            if promo_code:
                payload["promo_code"] = promo_code
            r = await client.post(
                f"{self._base_url}/subscriptions/purchase",
                json=payload
            )
            r.raise_for_status()
            return r.json()
    
    async def activate_trial(self, tg_id: int) -> dict[str, Any]:
        """Активация пробного периода"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{self._base_url}/subscriptions/trial",
                json={"tg_id": tg_id}
            )
            r.raise_for_status()
            return r.json()

    async def list_users(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/users", params={"limit": limit, "offset": offset})
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []

    async def users_count(self) -> int:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/users/count")
            r.raise_for_status()
            data = r.json()
            return int(data.get("total", 0)) if isinstance(data, dict) else 0

    async def get_user_by_tg(self, tg_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/users/by_tg/{tg_id}")
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {}

    async def referral_info(self, tg_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/users/referral/by_tg/{tg_id}")
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {}

    async def admin_credit(self, tg_id: int, amount: int, reason: str | None, admin_tg_id: int | None) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{self._base_url}/admin/users/credit",
                json={"tg_id": tg_id, "amount": amount, "reason": reason, "admin_tg_id": admin_tg_id},
                headers=self._admin_headers(),
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {}

    async def admin_export_users_csv(self) -> bytes:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(f"{self._base_url}/admin/users/export.csv", headers=self._admin_headers())
            r.raise_for_status()
            return r.content

    async def admin_block_user(self, tg_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{self._base_url}/admin/users/block", json={"tg_id": tg_id}, headers=self._admin_headers())
            r.raise_for_status()
            return r.json()

    async def admin_unblock_user(self, tg_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{self._base_url}/admin/users/unblock", json={"tg_id": tg_id}, headers=self._admin_headers())
            r.raise_for_status()
            return r.json()

    async def admin_get_logs(self, limit: int = 50, offset: int = 0, action: str | None = None) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            params: dict[str, Any] = {"limit": limit, "offset": offset}
            if action:
                params["action"] = action
            r = await client.get(f"{self._base_url}/admin/logs", params=params, headers=self._admin_headers())
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []

    async def admin_logs_count(self, action: str | None = None) -> int:
        async with httpx.AsyncClient(timeout=10.0) as client:
            params: dict[str, Any] = {}
            if action:
                params["action"] = action
            r = await client.get(f"{self._base_url}/admin/logs/count", params=params, headers=self._admin_headers())
            r.raise_for_status()
            data = r.json()
            return int(data.get("total", 0)) if isinstance(data, dict) else 0

    async def admin_get_payments(self, limit: int = 20, offset: int = 0, status: str | None = None, provider: str | None = None) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            params: dict[str, Any] = {"limit": limit, "offset": offset}
            if status:
                params["status"] = status
            if provider:
                params["provider"] = provider
            r = await client.get(f"{self._base_url}/admin/payments", params=params, headers=self._admin_headers())
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {"payments": [], "total": 0, "limit": limit, "offset": offset}

    async def create_ticket(self, tg_id: int, topic: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            payload = {"tg_id": tg_id, "topic": topic}
            r = await client.post(f"{self._base_url}/tickets/create", json=payload)
            r.raise_for_status()
            return r.json()

    async def get_user_payments(self, tg_id: int, limit: int = 10) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/users/{tg_id}/payments", params={"limit": limit})
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []

    async def get_user_referral_rewards(self, tg_id: int, limit: int = 10) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/users/{tg_id}/referral/rewards", params={"limit": limit})
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []

    async def validate_promo_code(self, code: str, tg_id: int, amount_cents: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{self._base_url}/promo-codes/validate",
                json={"code": code, "tg_id": tg_id, "amount_cents": amount_cents}
            )
            r.raise_for_status()
            return r.json()

    async def apply_promo_code(self, code: str, tg_id: int, amount_cents: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{self._base_url}/promo-codes/apply",
                json={"code": code, "tg_id": tg_id, "amount_cents": amount_cents}
            )
            r.raise_for_status()
            return r.json()

    async def create_payment(self, tg_id: int, amount_cents: int, provider: str, currency: str = "USD") -> dict[str, Any]:
        """Создание платежа для пополнения баланса"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(
                f"{self._base_url}/payments/create",
                json={
                    "tg_id": tg_id,
                    "amount_cents": amount_cents,
                    "provider": provider,
                    "currency": currency,
                }
            )
            r.raise_for_status()
            return r.json()
    
    async def payment_webhook(self, payment_id: int | None, external_id: str, provider: str, status: str, amount_cents: int, currency: str = "USD", raw_data: dict | None = None) -> dict[str, Any]:
        """Отправка webhook для обработки платежа"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{self._base_url}/payments/webhook",
                json={
                    "payment_id": payment_id,
                    "external_id": external_id,
                    "provider": provider,
                    "status": status,
                    "amount_cents": amount_cents,
                    "currency": currency,
                    "raw_data": raw_data,
                }
            )
            r.raise_for_status()
            return r.json()

    async def get_promo_code_info(self, code: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/promo-codes/info/{code}")
            r.raise_for_status()
            return r.json()

    async def get_bot_settings(self) -> dict[str, Any]:
        """Получить настройки бота"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base_url}/settings/bot")
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else {}


