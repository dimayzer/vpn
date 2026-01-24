"""
Модуль для работы с Xray-core (VLESS протокол)
Генерация конфигурационных файлов для клиентов
"""
from __future__ import annotations

import uuid
import json
from typing import Optional
from urllib.parse import quote


def generate_uuid() -> str:
    """
    Генерирует UUID для VLESS клиента
    
    Returns:
        str: UUID в формате xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    """
    return str(uuid.uuid4())


def generate_vless_config(
    user_uuid: str,
    server_host: str,
    server_port: int,
    server_uuid: str,
    server_flow: Optional[str] = None,
    server_network: str = "tcp",
    server_security: str = "tls",
    server_sni: Optional[str] = None,
    server_reality_public_key: Optional[str] = None,
    server_reality_short_id: Optional[str] = None,
    server_path: Optional[str] = None,
    server_host_header: Optional[str] = None,
    remark: str = "fioreVPN",
) -> str:
    """
    Генерирует VLESS конфигурацию для клиента (формат v2rayN/v2rayNG)
    
    Args:
        user_uuid: UUID пользователя
        server_host: IP адрес или домен сервера
        server_port: Порт сервера
        server_uuid: UUID сервера (для VLESS)
        server_flow: Flow control (xtls-rprx-vision, xtls-rprx-direct) или None
        server_network: Тип сети (tcp, ws, grpc)
        server_security: Безопасность (none, tls, reality)
        server_sni: SNI для TLS (домен)
        server_reality_public_key: Public key для Reality
        server_reality_short_id: Short ID для Reality
        server_path: Path для WebSocket/gRPC
        server_host_header: Host header для WebSocket
        remark: Название конфигурации
    
    Returns:
        str: VLESS ссылка в формате vless://...
    """
    # Формируем базовую часть ссылки
    # Формат: vless://UUID@HOST:PORT?параметры#REMARK
    
    # Убираем порт из host если он там есть (на случай ошибки в данных)
    clean_host = server_host.split(":")[0] if ":" in server_host else server_host
    
    params = []
    
    # Тип сети — ВСЕГДА указываем
    params.append(f"type={server_network}")
    
    # Encryption — для VLESS всегда none
    params.append("encryption=none")
    
    # Безопасность
    if server_security == "tls":
        params.append("security=tls")
        if server_sni:
            # Берем только первый SNI если их несколько через запятую
            clean_sni = server_sni.split(",")[0].strip()
            params.append(f"sni={clean_sni}")
    elif server_security == "reality":
        params.append("security=reality")
        if server_sni:
            # Берем только первый SNI если их несколько через запятую
            clean_sni = server_sni.split(",")[0].strip()
            params.append(f"sni={clean_sni}")
        if server_reality_public_key:
            params.append(f"pbk={server_reality_public_key}")
        if server_reality_short_id:
            # Берем только первый Short ID если их несколько
            clean_sid = server_reality_short_id.split(",")[0].strip()
            params.append(f"sid={clean_sid}")
        params.append("fp=chrome")  # Fingerprint для Reality
        params.append("spx=%2F")  # SpiderX для Reality
    
    # Flow (только для XTLS)
    if server_flow:
        params.append(f"flow={server_flow}")
    
    # WebSocket настройки
    if server_network == "ws":
        if server_path:
            params.append(f"path={server_path}")
        if server_host_header:
            params.append(f"host={server_host_header}")
    
    # gRPC настройки
    if server_network == "grpc":
        if server_path:
            params.append(f"serviceName={server_path}")
    
    # Собираем параметры
    query_string = "&".join(params) if params else ""
    
    # Формируем ссылку (используем очищенный host)
    vless_url = f"vless://{user_uuid}@{clean_host}:{server_port}"
    if query_string:
        vless_url += f"?{query_string}"
    if remark:
        # URL-кодируем remark (пробелы → %20, спецсимволы и т.д.)
        encoded_remark = quote(remark, safe='')
        vless_url += f"#{encoded_remark}"
    
    return vless_url


def generate_vless_json_config(
    user_uuid: str,
    server_host: str,
    server_port: int,
    server_uuid: str,
    server_flow: Optional[str] = None,
    server_network: str = "tcp",
    server_security: str = "tls",
    server_sni: Optional[str] = None,
    server_reality_public_key: Optional[str] = None,
    server_reality_short_id: Optional[str] = None,
    server_path: Optional[str] = None,
    server_host_header: Optional[str] = None,
    remark: str = "fioreVPN",
) -> dict:
    """
    Генерирует VLESS конфигурацию в формате JSON (для v2ray-core)
    
    Returns:
        dict: Конфигурация в формате JSON
    """
    config = {
        "log": {
            "loglevel": "warning"
        },
        "inbounds": [
            {
                "port": 10808,
                "protocol": "socks",
                "settings": {
                    "udp": True
                }
            }
        ],
        "outbounds": [
            {
                "protocol": "vless",
                "settings": {
                    "vnext": [
                        {
                            "address": server_host,
                            "port": server_port,
                            "users": [
                                {
                                    "id": user_uuid,
                                    "encryption": "none",
                                    "flow": server_flow or ""
                                }
                            ]
                        }
                    ]
                },
                "streamSettings": {
                    "network": server_network,
                    "security": server_security
                }
            }
        ]
    }
    
    # Настройки TLS
    if server_security == "tls":
        config["outbounds"][0]["streamSettings"]["tlsSettings"] = {
            "serverName": server_sni or server_host,
            "allowInsecure": False
        }
    
    # Настройки Reality
    if server_security == "reality":
        reality_settings = {
            "show": False,
            "dest": server_sni or server_host,
            "xver": 0
        }
        if server_reality_public_key:
            reality_settings["publicKey"] = server_reality_public_key
        if server_reality_short_id:
            reality_settings["shortId"] = server_reality_short_id
        config["outbounds"][0]["streamSettings"]["realitySettings"] = reality_settings
    
    # Настройки WebSocket
    if server_network == "ws":
        ws_settings = {}
        if server_path:
            ws_settings["path"] = server_path
        if server_host_header:
            ws_settings["headers"] = {
                "Host": server_host_header
            }
        config["outbounds"][0]["streamSettings"]["wsSettings"] = ws_settings
    
    # Настройки gRPC
    if server_network == "grpc":
        grpc_settings = {}
        if server_path:
            grpc_settings["serviceName"] = server_path
        config["outbounds"][0]["streamSettings"]["grpcSettings"] = grpc_settings
    
    return config


def validate_uuid(uuid_str: str) -> bool:
    """
    Проверяет валидность UUID
    
    Args:
        uuid_str: UUID для проверки
    
    Returns:
        bool: True если UUID валиден
    """
    try:
        uuid.UUID(uuid_str)
        return True
    except (ValueError, TypeError):
        return False

