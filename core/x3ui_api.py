"""
Модуль для работы с API 3x-UI для автоматического управления клиентами

3x-UI API работает через сессию (cookies), НЕ stateless.
Правильная последовательность:
1. POST /login → получаем cookies
2. POST /panel/api/inbounds/addClient (с теми же cookies)
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class X3UIAPI:
    """Клиент для работы с API 3x-UI через сессию"""
    
    def __init__(self, api_url: str, username: str, password: str):
        """
        Инициализация клиента API 3x-UI
        
        Args:
            api_url: URL API 3x-UI (например: http://ip:2053/panel/api или http://ip:2053/{WEBBASEPATH}/panel/api)
            username: Имя пользователя для авторизации
            password: Пароль для авторизации
        """
        self.api_url = api_url.rstrip("/")
        self.username = username
        self.password = password
        # Базовый URL (без /panel/api) для логина
        self.base_url = self.api_url.replace("/panel/api", "")
        # Сессия будет создана при login()
        self._session: httpx.AsyncClient | None = None
        self._logged_in = False
    
    async def _ensure_session(self):
        """Убедиться, что сессия создана и авторизована"""
        if self._session is None:
            self._session = httpx.AsyncClient(timeout=15.0, follow_redirects=True)
        
        if not self._logged_in:
            await self.login()
    
    async def close(self):
        """Закрыть сессию"""
        if self._session:
            await self._session.aclose()
            self._session = None
            self._logged_in = False
    
    async def login(self) -> bool:
        """
        Авторизация в API 3x-UI через POST /login
        Cookies автоматически сохраняются в сессии.
        
        Returns:
            True если авторизация успешна, False иначе
        """
        if self._session is None:
            self._session = httpx.AsyncClient(timeout=15.0, follow_redirects=True)
        
        login_endpoint = f"{self.base_url}/login"
        logger.info(f"Авторизация в 3x-UI: {login_endpoint}")
        
        try:
            response = await self._session.post(
                login_endpoint,
                data={
                    "username": self.username,
                    "password": self.password
                },
            )
            
            logger.debug(f"Ответ от login: статус {response.status_code}, cookies: {dict(self._session.cookies)}")
            
            if response.status_code == 200:
                # Проверяем успешность по ответу
                try:
                    data = response.json()
                    if data.get("success"):
                        self._logged_in = True
                        logger.info(f"Успешная авторизация в 3x-UI (API URL: {self.api_url})")
                        return True
                    else:
                        logger.warning(f"Авторизация не удалась: {data.get('msg', 'Unknown error')}")
                        return False
                except:
                    # Если ответ не JSON, проверяем cookies
                    if self._session.cookies:
                        self._logged_in = True
                        logger.info(f"Успешная авторизация в 3x-UI через cookies (API URL: {self.api_url})")
                        return True
            
            logger.warning(f"Ошибка авторизации в 3x-UI: HTTP {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"Ошибка авторизации в 3x-UI (API URL: {self.api_url}): {e}")
            return False
    
    async def list_inbounds(self) -> list[dict[str, Any]]:
        """
        Получить список всех Inbounds
        
        Returns:
            Список Inbounds или пустой список при ошибке
        """
        await self._ensure_session()
        
        try:
            response = await self._session.get(f"{self.api_url}/inbounds/list")
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and "obj" in data:
                    return data["obj"]
            
            logger.warning(f"Не удалось получить список Inbounds: HTTP {response.status_code}")
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении списка Inbounds из 3x-UI: {e}")
            return []
    
    async def find_inbound_by_port_and_protocol(self, port: int, protocol: str = "vless") -> dict[str, Any] | None:
        """Найти Inbound по порту и протоколу"""
        inbounds = await self.list_inbounds()
        for inbound in inbounds:
            if inbound.get("port") == port and inbound.get("protocol", "").lower() == protocol.lower():
                return inbound
        return None
    
    async def find_first_vless_inbound(self) -> dict[str, Any] | None:
        """Найти первый доступный VLESS Inbound"""
        inbounds = await self.list_inbounds()
        for inbound in inbounds:
            if inbound.get("protocol", "").lower() == "vless":
                return inbound
        return None
    
    async def get_inbound(self, inbound_id: int) -> dict[str, Any] | None:
        """Получить информацию о Inbound по ID"""
        inbounds = await self.list_inbounds()
        for inbound in inbounds:
            if inbound.get("id") == inbound_id:
                return inbound
        return None
    
    async def add_client(
        self,
        inbound_id: int,
        email: str,
        uuid: str | None = None,
        flow: str = "",
        expire: int = 0,
        limit_ip: int = 0,
        total_gb: int = 0,
    ) -> dict[str, Any] | None:
        """
        Добавить клиента в Inbound
        
        Согласно документации 3x-UI:
        POST /panel/api/inbounds/addClient
        form-data: id=<inbound_id>, settings=<json>
        
        Args:
            inbound_id: ID Inbound в 3x-UI
            email: Email/ID клиента
            uuid: UUID клиента (если None, будет сгенерирован)
            flow: Flow control (xtls-rprx-vision и т.д.)
            expire: Дата истечения (timestamp в миллисекундах, 0 = без ограничений)
            limit_ip: Лимит IP адресов (0 = без ограничений)
            total_gb: Лимит трафика в GB (0 = без ограничений)
        
        Returns:
            Данные созданного клиента с UUID или None при ошибке
        """
        await self._ensure_session()
        
        # Генерируем UUID если не указан
        if not uuid:
            from core.xray import generate_uuid
            uuid = generate_uuid()
        
        endpoint = f"{self.api_url}/inbounds/addClient"
        
        # Формат settings согласно документации
        settings = {
            "clients": [{
                "id": uuid,
                "email": email,
                "limitIp": limit_ip,
                "totalGB": total_gb,
                "expiryTime": expire,
                "enable": True,
                "flow": flow if flow else ""
            }]
        }
        
        form_data = {
            "id": str(inbound_id),
            "settings": json.dumps(settings)
        }
        
        logger.info(f"Добавление клиента в 3x-UI: endpoint={endpoint}, inbound_id={inbound_id}, email={email}, uuid={uuid}")
        
        try:
            response = await self._session.post(endpoint, data=form_data)
            
            logger.debug(f"Ответ от addClient: статус {response.status_code}, тело: {response.text[:500]}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    logger.info(f"Клиент {email} успешно добавлен в Inbound {inbound_id} с UUID {uuid}")
                    return {
                        "id": uuid,
                        "uuid": uuid,
                        "email": email,
                    }
                else:
                    error_msg = result.get('msg', 'Unknown error')
                    logger.warning(f"API вернул success=false: {error_msg}")
                    raise ValueError(f"3x-UI API error: {error_msg}")
            else:
                logger.error(f"HTTP {response.status_code} от {endpoint}: {response.text[:500]}")
                response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP ошибка при добавлении клиента (API URL: {self.api_url}, Inbound ID: {inbound_id}): {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Сетевая ошибка при добавлении клиента (API URL: {self.api_url}, Inbound ID: {inbound_id}): {e}")
            raise
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Ошибка при добавлении клиента (API URL: {self.api_url}, Inbound ID: {inbound_id}): {e}", exc_info=True)
            raise
        
        return None
    
    async def delete_client(self, inbound_id: int, email: str) -> bool:
        """
        Удалить клиента из Inbound
        
        Args:
            inbound_id: ID Inbound
            email: Email клиента для удаления
        
        Returns:
            True если удаление успешно
        """
        await self._ensure_session()
        
        endpoint = f"{self.api_url}/inbounds/{inbound_id}/delClient/{email}"
        
        try:
            response = await self._session.post(endpoint)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    logger.info(f"Клиент {email} удален из Inbound {inbound_id}")
                    return True
                else:
                    logger.warning(f"Не удалось удалить клиента: {result.get('msg', 'Unknown error')}")
            
            return False
        except Exception as e:
            logger.error(f"Ошибка при удалении клиента из 3x-UI: {e}")
            return False
    
    async def update_client(
        self,
        inbound_id: int,
        client_uuid: str,
        email: str | None = None,
        enable: bool | None = None,
        expire: int | None = None,
        limit_ip: int | None = None,
        total_gb: int | None = None,
    ) -> bool:
        """
        Обновить настройки клиента
        
        Args:
            inbound_id: ID Inbound
            client_uuid: UUID клиента
            email: Новый email (опционально)
            enable: Включить/выключить клиента (опционально)
            expire: Новая дата истечения (опционально)
            limit_ip: Новый лимит IP (опционально)
            total_gb: Новый лимит трафика (опционально)
        
        Returns:
            True если обновление успешно
        """
        await self._ensure_session()
        
        # Сначала получаем текущие данные клиента
        inbound = await self.get_inbound(inbound_id)
        if not inbound:
            logger.warning(f"Inbound {inbound_id} не найден")
            return False
        
        # Парсим settings
        try:
            settings = json.loads(inbound.get("settings", "{}"))
            clients = settings.get("clients", [])
        except:
            logger.error(f"Ошибка парсинга settings для Inbound {inbound_id}")
            return False
        
        # Ищем клиента по UUID
        client_found = False
        for client in clients:
            if client.get("id") == client_uuid:
                client_found = True
                if email is not None:
                    client["email"] = email
                if enable is not None:
                    client["enable"] = enable
                if expire is not None:
                    client["expiryTime"] = expire
                if limit_ip is not None:
                    client["limitIp"] = limit_ip
                if total_gb is not None:
                    client["totalGB"] = total_gb
                break
        
        if not client_found:
            logger.warning(f"Клиент с UUID {client_uuid} не найден в Inbound {inbound_id}")
            return False
        
        # Отправляем обновление через updateClient
        endpoint = f"{self.api_url}/inbounds/updateClient/{client_uuid}"
        
        form_data = {
            "id": str(inbound_id),
            "settings": json.dumps({"clients": clients})
        }
        
        try:
            response = await self._session.post(endpoint, data=form_data)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    logger.info(f"Клиент {client_uuid} обновлен в Inbound {inbound_id}")
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Ошибка при обновлении клиента в 3x-UI: {e}")
            return False
    
    async def get_client_config(self, inbound_id: int, email: str) -> dict[str, Any] | None:
        """
        Получить конфиг клиента из Inbound
        
        Args:
            inbound_id: ID Inbound
            email: Email клиента
        
        Returns:
            Данные клиента или None
        """
        inbound = await self.get_inbound(inbound_id)
        if not inbound:
            return None
        
        try:
            settings = json.loads(inbound.get("settings", "{}"))
            clients = settings.get("clients", [])
            
            for client in clients:
                if client.get("email") == email:
                    # Добавляем данные из inbound для генерации конфига
                    stream_settings = json.loads(inbound.get("streamSettings", "{}"))
                    return {
                        "uuid": client.get("id"),
                        "email": client.get("email"),
                        "flow": client.get("flow", ""),
                        "port": inbound.get("port"),
                        "protocol": inbound.get("protocol"),
                        "network": stream_settings.get("network", "tcp"),
                        "security": stream_settings.get("security", "none"),
                        "reality_settings": stream_settings.get("realitySettings", {}),
                        "tls_settings": stream_settings.get("tlsSettings", {}),
                        "ws_settings": stream_settings.get("wsSettings", {}),
                        "grpc_settings": stream_settings.get("grpcSettings", {}),
                    }
            
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении конфига клиента: {e}")
            return None
    
    async def get_client_traffic(self, email: str) -> dict[str, Any] | None:
        """
        Получить статистику трафика клиента
        
        Args:
            email: Email клиента
        
        Returns:
            Статистика трафика или None
        """
        await self._ensure_session()
        
        endpoint = f"{self.api_url}/inbounds/getClientTraffics/{email}"
        
        try:
            response = await self._session.get(endpoint)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    return result.get("obj")
            
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении трафика клиента: {e}")
            return None
    
    async def get_client_ips(self, email: str) -> list[str]:
        """
        Получить список IP адресов клиента (онлайн подключения)
        
        Args:
            email: Email клиента
        
        Returns:
            Список IP адресов
        """
        await self._ensure_session()
        
        endpoint = f"{self.api_url}/inbounds/clientIps/{email}"
        
        try:
            response = await self._session.post(endpoint)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    obj = result.get("obj")
                    # obj может быть строкой с IP через запятую или списком
                    if isinstance(obj, str):
                        if obj and obj != "No IP Record":
                            return [ip.strip() for ip in obj.split(",") if ip.strip()]
                        return []
                    elif isinstance(obj, list):
                        return obj
            
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении IP клиента: {e}")
            return []
    
    async def clear_client_ips(self, email: str) -> bool:
        """
        Очистить историю IP адресов клиента
        
        Args:
            email: Email клиента
        
        Returns:
            True если успешно
        """
        await self._ensure_session()
        
        endpoint = f"{self.api_url}/inbounds/clearClientIps/{email}"
        
        try:
            response = await self._session.post(endpoint)
            
            if response.status_code == 200:
                result = response.json()
                return result.get("success", False)
            
            return False
        except Exception as e:
            logger.error(f"Ошибка при очистке IP клиента: {e}")
            return False
    
    async def get_online_clients(self) -> list[dict[str, Any]]:
        """
        Получить список онлайн клиентов
        
        Returns:
            Список онлайн клиентов с их данными
        """
        await self._ensure_session()
        
        endpoint = f"{self.api_url}/inbounds/onlines"
        
        try:
            response = await self._session.post(endpoint)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    return result.get("obj", []) or []
            
            return []
        except Exception as e:
            logger.error(f"Ошибка при получении онлайн клиентов: {e}")
            return []
    
    async def disable_client(self, inbound_id: int, client_uuid: str) -> bool:
        """
        Отключить клиента (заблокировать)
        
        Args:
            inbound_id: ID Inbound
            client_uuid: UUID клиента
        
        Returns:
            True если успешно
        """
        return await self.update_client(inbound_id, client_uuid, enable=False)
    
    async def enable_client(self, inbound_id: int, client_uuid: str) -> bool:
        """
        Включить клиента (разблокировать)
        
        Args:
            inbound_id: ID Inbound
            client_uuid: UUID клиента
        
        Returns:
            True если успешно
        """
        return await self.update_client(inbound_id, client_uuid, enable=True)
