"""
Модуль для работы с API 3x-UI для автоматического управления клиентами
"""
from __future__ import annotations

import base64
import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class X3UIAPI:
    """Клиент для работы с API 3x-UI"""
    
    def __init__(self, api_url: str, username: str, password: str):
        """
        Инициализация клиента API 3x-UI
        
        Args:
            api_url: URL API 3x-UI (например: http://ip:2053/panel/api)
            username: Имя пользователя для авторизации
            password: Пароль для авторизации
        """
        self.api_url = api_url.rstrip("/")
        self.username = username
        self.password = password
        self._token: str | None = None
    
    def _get_auth_headers(self) -> dict[str, str]:
        """Получить заголовки для авторизации"""
        if not self._token:
            # Базовая авторизация через base64
            credentials = f"{self.username}:{self.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}
        return {"Authorization": f"Bearer {self._token}"}
    
    async def login(self) -> bool:
        """
        Авторизация в API 3x-UI
        
        Returns:
            True если авторизация успешна, False иначе
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Попробуем разные варианты API endpoints
                # 3x-UI обычно использует /login или /auth/login
                endpoints = [
                    f"{self.api_url}/login",
                    f"{self.api_url}/auth/login",
                    f"{self.api_url}/user/login",
                ]
                
                for endpoint in endpoints:
                    try:
                        response = await client.post(
                            endpoint,
                            json={"username": self.username, "password": self.password},
                            headers={"Content-Type": "application/json"},
                        )
                        if response.status_code == 200:
                            data = response.json()
                            if "token" in data:
                                self._token = data["token"]
                                return True
                            elif "access_token" in data:
                                self._token = data["access_token"]
                                return True
                    except Exception:
                        continue
                
                # Если не получилось через JSON, пробуем Basic Auth
                # Многие версии 3x-UI используют Basic Auth напрямую
                return True  # Basic Auth будет использоваться в заголовках
        except Exception as e:
            logger.error(f"Ошибка авторизации в 3x-UI: {e}")
            return False
    
    async def list_inbounds(self) -> list[dict[str, Any]]:
        """
        Получить список всех Inbounds
        
        Returns:
            Список Inbounds или пустой список при ошибке
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.api_url}/inbounds/list",
                    headers=self._get_auth_headers(),
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success") and "obj" in data:
                        return data["obj"]
                
                return []
        except Exception as e:
            logger.error(f"Ошибка при получении списка Inbounds из 3x-UI: {e}")
            return []
    
    async def find_inbound_by_port_and_protocol(self, port: int, protocol: str = "vless") -> dict[str, Any] | None:
        """
        Найти Inbound по порту и протоколу
        
        Args:
            port: Порт Inbound
            protocol: Протокол (vless, vmess, trojan и т.д.)
        
        Returns:
            Данные Inbound или None если не найден
        """
        inbounds = await self.list_inbounds()
        for inbound in inbounds:
            if inbound.get("port") == port and inbound.get("protocol", "").lower() == protocol.lower():
                return inbound
        return None
    
    async def find_first_vless_inbound(self) -> dict[str, Any] | None:
        """
        Найти первый доступный VLESS Inbound (если поиск по порту не дал результата)
        
        Returns:
            Данные первого VLESS Inbound или None если не найден
        """
        inbounds = await self.list_inbounds()
        for inbound in inbounds:
            if inbound.get("protocol", "").lower() == "vless":
                return inbound
        return None
    
    async def get_inbound(self, inbound_id: int) -> dict[str, Any] | None:
        """
        Получить информацию о Inbound
        
        Args:
            inbound_id: ID Inbound в 3x-UI
        
        Returns:
            Данные Inbound или None при ошибке
        """
        try:
            inbounds = await self.list_inbounds()
            # Ищем нужный Inbound по ID
            for inbound in inbounds:
                if inbound.get("id") == inbound_id:
                    return inbound
            
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении Inbound из 3x-UI: {e}")
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
        Добавить клиента в Inbound (упрощенный вариант по алгоритму ChatGPT)
        
        Args:
            inbound_id: ID Inbound в 3x-UI
            email: Email/ID клиента
            uuid: UUID клиента (если None, будет сгенерирован автоматически)
            flow: Flow control (xtls-rprx-vision и т.д.)
            expire: Дата истечения (timestamp в миллисекундах, 0 = без ограничений)
            limit_ip: Лимит IP адресов (0 = без ограничений)
            total_gb: Лимит трафика в GB (0 = без ограничений)
        
        Returns:
            Данные созданного клиента с UUID или None при ошибке
        """
        try:
            # Генерируем UUID если не указан
            if not uuid:
                from core.xray import generate_uuid
                uuid = generate_uuid()
            
            # Проверяем, что API URL правильный
            if not self.api_url.endswith("/panel/api"):
                logger.warning(f"API URL может быть неправильным: {self.api_url}. Ожидается формат: http://IP:2053/panel/api")
            
            logger.info(f"Попытка создать клиента через 3x-UI API: URL={self.api_url}, inbound_id={inbound_id}, email={email}, uuid={uuid}")
            logger.debug(f"Добавление клиента: inbound_id={inbound_id}, email={email}, uuid={uuid}, api_url={self.api_url}")
            
            # Пробуем разные форматы payload и endpoints (согласно документации и примеру ChatGPT)
            async with httpx.AsyncClient(timeout=10.0) as http_client:
                # Вариант 1: Формат из документации 3x-UI API
                endpoints_v1 = [
                    f"{self.api_url}/inbounds/{inbound_id}/addClient",  # С inbound_id в пути
                    f"{self.api_url}/inbounds/addClient",  # Без inbound_id в пути
                ]
                
                payload_v1 = {
                    "id": inbound_id,
                    "client": {
                        "email": email,
                        "id": uuid,
                        "flow": flow,
                        "expiryTime": expire,
                        "limitIp": limit_ip,
                        "totalGB": 0,  # Без ограничений трафика
                        "enable": True,
                    }
                }
                
                for endpoint in endpoints_v1:
                    try:
                        response = await http_client.post(
                            endpoint,
                            json=payload_v1,
                            headers=self._get_auth_headers(),
                        )
                        logger.debug(f"Ответ от {endpoint}: статус {response.status_code}, тело: {response.text[:200]}")
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
                                logger.warning(f"API вернул success=false для {endpoint}: {result}")
                        else:
                            logger.warning(f"HTTP {response.status_code} от {endpoint} (API URL: {self.api_url}): {response.text[:200]}")
                    except Exception as e:
                        logger.debug(f"Ошибка при вызове {endpoint}: {e}")
                        continue
                
                # Вариант 2: Формат из примера ChatGPT (settings как JSON строка)
                payload_v2 = {
                    "id": inbound_id,
                    "settings": json.dumps({
                        "clients": [{
                            "id": uuid,
                            "email": email,
                            "enable": True,
                            "expiryTime": expire,
                            "limitIp": limit_ip,
                            "flow": flow
                        }]
                    })
                }
                
                for endpoint in endpoints_v1:
                    try:
                        response = await http_client.post(
                            endpoint,
                            json=payload_v2,
                            headers=self._get_auth_headers(),
                        )
                        last_response = response
                        logger.debug(f"Ответ от {endpoint} (вариант 2): статус {response.status_code}, тело: {response.text[:200]}")
                        if response.status_code == 200:
                            result = response.json()
                            if result.get("success"):
                                logger.info(f"Клиент {email} успешно добавлен в Inbound {inbound_id} с UUID {uuid} (вариант 2)")
                                return {
                                    "id": uuid,
                                    "uuid": uuid,
                                    "email": email,
                                }
                            else:
                                logger.warning(f"API вернул success=false для {endpoint} (вариант 2): {result}")
                        else:
                            logger.warning(f"HTTP {response.status_code} от {endpoint} (вариант 2, API URL: {self.api_url}): {response.text[:200]}")
                    except httpx.HTTPStatusError as e:
                        last_exception = e
                        last_response = e.response
                        logger.debug(f"HTTP ошибка при вызове {endpoint} (вариант 2): {e}")
                        continue
                    except httpx.RequestError as e:
                        last_exception = e
                        logger.debug(f"Сетевая ошибка при вызове {endpoint} (вариант 2): {e}")
                        continue
                    except Exception as e:
                        last_exception = e
                        logger.debug(f"Ошибка при вызове {endpoint} (вариант 2): {e}")
                        continue
                
                # Если ничего не сработало, пробрасываем последнюю ошибку для правильной обработки
                if last_exception:
                    raise last_exception
                elif last_response and last_response.status_code != 200:
                    last_response.raise_for_status()
                
                # Если дошли сюда, значит все попытки не удались без ошибок
                logger.error(
                    f"Не удалось добавить клиента в Inbound {inbound_id}. "
                    f"API URL: {self.api_url}, "
                    f"Попробованы endpoints: {endpoints_v1}, "
                    f"Попробованы оба формата payload. "
                    f"Email: {email}, UUID: {uuid}"
                )
                raise ValueError(f"Не удалось добавить клиента в Inbound {inbound_id}")
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            # Пробрасываем HTTP ошибки дальше для правильной обработки
            logger.error(f"HTTP ошибка при добавлении клиента в 3x-UI (API URL: {self.api_url}, Inbound ID: {inbound_id}): {e}")
            raise
        except Exception as e:
            # Другие ошибки тоже пробрасываем
            logger.error(f"Ошибка при добавлении клиента в 3x-UI (API URL: {self.api_url}, Inbound ID: {inbound_id}): {e}", exc_info=True)
            raise
    
    async def delete_client(self, inbound_id: int, email: str) -> bool:
        """
        Удалить клиента из Inbound
        
        Args:
            inbound_id: ID Inbound в 3x-UI
            email: Email/ID клиента для удаления
        
        Returns:
            True если удаление успешно, False иначе
        """
        try:
            # Получаем текущий Inbound
            inbound = await self.get_inbound(inbound_id)
            if not inbound:
                logger.warning(f"Inbound {inbound_id} не найден в 3x-UI (возможно, был удален или изменен ID)")
                return False
            
            # Парсим settings
            settings = json.loads(inbound.get("settings", "{}"))
            clients = settings.get("clients", [])
            
            # Удаляем клиента
            clients = [c for c in clients if c.get("email") != email]
            settings["clients"] = clients
            
            # Согласно документации 3x-UI API, используем endpoint для удаления клиента
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Попробуем стандартный endpoint для удаления клиента
                endpoints = [
                    f"{self.api_url}/inbounds/{inbound_id}/delClient",  # Стандартный endpoint
                    f"{self.api_url}/inbounds/delClient",  # Альтернативный вариант
                ]
                
                # Формат запроса для удаления клиента
                payload = {
                    "id": inbound_id,
                    "email": email,
                }
                
                for endpoint in endpoints:
                    try:
                        response = await client.post(
                            endpoint,
                            json=payload,
                            headers=self._get_auth_headers(),
                        )
                        if response.status_code == 200:
                            result = response.json()
                            if result.get("success"):
                                return True
                    except Exception as e:
                        logger.debug(f"Ошибка при вызове {endpoint}: {e}")
                        continue
                
                # Если не сработало через delClient, пробуем обновить весь Inbound
                endpoints_update = [
                    f"{self.api_url}/inbounds/update/{inbound_id}",
                    f"{self.api_url}/inbound/update/{inbound_id}",
                ]
                
                payload_update = {
                    "id": inbound_id,
                    "settings": json.dumps(settings),
                    "streamSettings": inbound.get("streamSettings", ""),
                    "sniffing": inbound.get("sniffing", ""),
                    "remark": inbound.get("remark", ""),
                    "enable": inbound.get("enable", True),
                    "expiryTime": inbound.get("expiryTime", 0),
                    "listen": inbound.get("listen", ""),
                    "port": inbound.get("port"),
                    "protocol": inbound.get("protocol", "vless"),
                }
                
                for endpoint in endpoints_update:
                    try:
                        response = await client.post(
                            endpoint,
                            json=payload_update,
                            headers=self._get_auth_headers(),
                        )
                        if response.status_code == 200:
                            result = response.json()
                            if result.get("success"):
                                return True
                    except Exception as e:
                        logger.debug(f"Ошибка при вызове {endpoint}: {e}")
                        continue
                
                logger.error(f"Не удалось удалить клиента из 3x-UI через API")
                return False
        except Exception as e:
            logger.error(f"Ошибка при удалении клиента из 3x-UI: {e}")
            return False
    
    async def get_client_config(
        self,
        inbound_id: int,
        email: str,
        server_host: str,
        server_port: int | None = None,
    ) -> str | None:
        """
        Получить конфиг клиента (vless://...) из Inbound
        
        Args:
            inbound_id: ID Inbound в 3x-UI
            email: Email/ID клиента
            server_host: IP или домен сервера
            server_port: Порт сервера (если None, берется из Inbound)
        
        Returns:
            VLESS конфиг или None при ошибке
        """
        try:
            # Получаем Inbound
            inbound = await self.get_inbound(inbound_id)
            if not inbound:
                logger.warning(f"Inbound {inbound_id} не найден в 3x-UI (возможно, был удален или изменен ID)")
                return None
            
            # Парсим settings для поиска клиента
            settings = json.loads(inbound.get("settings", "{}"))
            clients = settings.get("clients", [])
            
            # Ищем клиента по email
            client_data = None
            for client in clients:
                if client.get("email") == email:
                    client_data = client
                    break
            
            if not client_data:
                logger.error(f"Клиент {email} не найден в Inbound {inbound_id}")
                return None
            
            uuid = client_data.get("id")
            if not uuid:
                logger.error(f"UUID не найден для клиента {email}")
                return None
            
            # Парсим streamSettings для получения параметров сети
            stream_settings = json.loads(inbound.get("streamSettings", "{}"))
            network = stream_settings.get("network", "tcp")
            security = stream_settings.get("security", "none")
            
            # Получаем порт
            port = server_port or inbound.get("port")
            if not port:
                logger.error(f"Порт не найден для Inbound {inbound_id}")
                return None
            
            # Параметры для Reality
            reality_public_key = None
            reality_short_id = None
            sni = None
            path = None
            
            if security == "reality":
                reality_settings = stream_settings.get("realitySettings", {})
                reality_inner = reality_settings.get("settings", {})
                reality_public_key = reality_inner.get("publicKey")
                sni = reality_inner.get("serverName") or (reality_settings.get("serverNames", [None])[0] if reality_settings.get("serverNames") else None)
                short_ids = reality_settings.get("shortIds", [])
                if short_ids:
                    reality_short_id = short_ids[0]  # Берем первый Short ID
            
            # Параметры для gRPC/WebSocket
            if network == "grpc":
                grpc_settings = stream_settings.get("grpcSettings", {})
                path = grpc_settings.get("serviceName")
            elif network == "ws":
                ws_settings = stream_settings.get("wsSettings", {})
                path = ws_settings.get("path")
            
            # Генерируем конфиг
            from core.xray import generate_vless_config
            
            return generate_vless_config(
                user_uuid=uuid,
                server_host=server_host,
                server_port=port,
                server_uuid=uuid,
                server_flow=client_data.get("flow") or None,
                server_network=network,
                server_security=security,
                server_sni=sni,
                server_reality_public_key=reality_public_key,
                server_reality_short_id=reality_short_id,
                server_path=path,
                server_host_header=None,
                remark=inbound.get("remark", "fioreVPN"),
            )
        except Exception as e:
            logger.error(f"Ошибка при получении конфига клиента из 3x-UI: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

