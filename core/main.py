from __future__ import annotations

from contextlib import asynccontextmanager
import csv
import io
import secrets
import string
from typing import Sequence
from datetime import datetime
import asyncio
import logging
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from fastapi import Depends, FastAPI, HTTPException, Query, Header, Request
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
# Временно отключено из-за проблем с Pydantic
# from slowapi import Limiter, _rate_limit_exceeded_handler
# from slowapi.util import get_remote_address
# from slowapi.errors import RateLimitExceeded
import secrets
import hashlib
import hmac
import time
from datetime import datetime
from sqlalchemy import select, func, text, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from core.config import get_settings
from core.db.session import engine, get_session, SessionLocal, recreate_engine

logger = logging.getLogger(__name__)
from core.db.models import (
    Base,
    User,
    Subscription,
    SubscriptionPlan,
    Payment,
    SubscriptionStatus,
    PaymentStatus,
    BalanceTransaction,
    AuditLog,
    AuditLogAction,
    AdminOverride,
    Ticket,
    TicketMessage,
    TicketStatus,
    MessageDirection,
    SystemSetting,
    ReferralReward,
    PromoCode,
    PromoCodeUsage,
    Server,
    ServerStatus,
    Backup,
    VpnCredential,
    IpLog,
    UserBan,
)
from core.xray import generate_vless_config, generate_uuid
from core.schemas import (
    PaymentCreateIn,
    PaymentWebhookIn,
    UserOut,
    UserUpsertIn,
    SubscriptionStatusOut,
    AdminCreditIn,
    AdminSetActiveIn,
    ReferralInfoOut,
    AuditLogOut,
    PromoCodeValidateIn,
    PromoCodeApplyIn,
    SubscriptionPurchaseIn,
    SubscriptionTrialIn,
    ServerCreateIn,
    ServerUpdateIn,
    ServerOut,
)

settings = get_settings()


def _check_port_sync(host: str, port: int, timeout: int = 10) -> bool:
    """Синхронная проверка доступности порта"""
    import socket
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        # Не используем SO_REUSEADDR для клиентских сокетов, это может вызывать проблемы
        result = sock.connect_ex((host, port))
        return result == 0
    except socket.timeout:
        logger.debug(f"Таймаут при проверке порта {host}:{port}")
        return False
    except Exception as e:
        logger.debug(f"Ошибка при проверке порта {host}:{port}: {e}")
        return False
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


async def _test_connection_speed(server: Server) -> float | None:
    """Тестирует скорость соединения с сервером"""
    import httpx
    import time
    
    try:
        # Используем HTTP запрос для измерения скорости
        # Загружаем тестовые данные (1 МБ)
        test_size_mb = 1.0
        test_size_bytes = int(test_size_mb * 1024 * 1024)
        
        # Генерируем тестовые данные
        test_data = b'0' * test_size_bytes
        
        # Создаем временный эндпоинт для теста скорости
        # Используем простой HTTP запрос к серверу
        url = f"http://{server.host}:{server.xray_port or 80}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            start_time = time.time()
            try:
                # Пытаемся подключиться и измерить время
                response = await client.get(url, follow_redirects=False)
                elapsed = time.time() - start_time
                
                # Если сервер отвечает, вычисляем примерную скорость
                # На основе времени подключения и размера данных
                if elapsed > 0:
                    # Примерная оценка: чем быстрее ответ, тем выше скорость
                    # Используем обратную зависимость от времени отклика
                    speed_mbps = (test_size_mb * 8) / elapsed if elapsed > 0 else None
                    return speed_mbps
            except Exception:
                # Если HTTP не работает, пробуем через socket
                pass
        
        # Альтернативный метод: измерение через socket
        import socket
        loop = asyncio.get_event_loop()
        
        def test_socket_speed():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                start = time.time()
                result = sock.connect_ex((server.host, server.xray_port or 443))
                elapsed = time.time() - start
                sock.close()
                
                if result == 0 and elapsed > 0:
                    # Оценка скорости на основе времени подключения
                    # Чем быстрее подключение, тем выше скорость
                    speed_mbps = (test_size_mb * 8) / (elapsed * 10)  # Умножаем на 10 для более реалистичной оценки
                    return speed_mbps
            except Exception:
                pass
            return None
        
        speed = await loop.run_in_executor(None, test_socket_speed)
        return speed
        
    except Exception as e:
        logger.debug(f"Ошибка при тесте скорости для {server.name}: {e}")
        return None


async def _check_server_status(server: Server) -> dict:
    """Проверяет состояние одного сервера"""
    import socket
    import time
    import httpx
    
    # Если сервер использует 3x-UI API, проверяем доступность API вместо порта
    if server.x3ui_api_url and server.x3ui_username and server.x3ui_password:
        from core.x3ui_api import X3UIAPI
        start_time = time.time()
        x3ui = None
        
        try:
            # Создаем временный клиент для проверки
            x3ui = X3UIAPI(
                api_url=server.x3ui_api_url,
                username=server.x3ui_username,
                password=server.x3ui_password,
            )
            
            # Пытаемся авторизоваться (это проверит доступность API)
            login_success = await x3ui.login()
            response_time_ms = int((time.time() - start_time) * 1000)
            
            if login_success:
                # Если авторизация успешна, сервер точно онлайн
                # Пытаемся получить список Inbounds для дополнительной проверки
                try:
                    inbounds = await x3ui.list_inbounds()
                    inbounds_count = len(inbounds) if inbounds else 0
                    logger.info(f"✅ Сервер {server.name}: онлайн (3x-UI API), время={response_time_ms}ms, inbounds={inbounds_count}")
                except Exception as e:
                    # Если не удалось получить inbounds, но авторизация прошла - сервер онлайн
                    logger.debug(f"Не удалось получить список inbounds для {server.name}, но авторизация успешна: {e}")
                    inbounds_count = 0
                
                return {
                    "is_online": True,
                    "response_time_ms": response_time_ms,
                    "connection_speed_mbps": None,
                    "error_message": None,
                }
            else:
                # Авторизация не удалась - возможно, неправильные учетные данные
                logger.warning(f"❌ Сервер {server.name}: авторизация в 3x-UI API не удалась (время={response_time_ms}ms)")
                return {
                    "is_online": False,
                    "response_time_ms": response_time_ms,
                    "connection_speed_mbps": None,
                    "error_message": "3x-UI API: ошибка авторизации (неправильные учетные данные?)",
                }
                
        except httpx.ConnectError as e:
            # Ошибка подключения - сервер точно оффлайн
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.warning(f"❌ Сервер {server.name}: не удалось подключиться к 3x-UI API (время={response_time_ms}ms): {e}")
            return {
                "is_online": False,
                "response_time_ms": response_time_ms,
                "connection_speed_mbps": None,
                "error_message": f"3x-UI API недоступен: {str(e)[:200]}",
            }
        except Exception as e:
            # Другие ошибки - логируем подробно
            response_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Ошибка при проверке 3x-UI API для сервера {server.name} (время={response_time_ms}ms): {e}", exc_info=True)
            # Если это не ошибка подключения, возможно сервер доступен, но есть проблема с API
            # Для безопасности считаем оффлайн, если не уверены
            return {
                "is_online": False,
                "response_time_ms": response_time_ms,
                "connection_speed_mbps": None,
                "error_message": f"3x-UI API ошибка: {str(e)[:200]}",
            }
        finally:
            if x3ui:
                try:
                    await x3ui.close()
                except:
                    pass
        
        # Если дошли сюда, значит что-то пошло не так, но не используем fallback для 3x-UI серверов
        # Fallback только для серверов без 3x-UI API
        logger.warning(f"Проверка 3x-UI API для {server.name} завершилась неожиданно, не используем fallback")
        return {
            "is_online": False,
            "response_time_ms": int((time.time() - start_time) * 1000),
            "connection_speed_mbps": None,
            "error_message": "Неожиданная ошибка при проверке 3x-UI API",
        }
    
    # Обычная проверка порта VPN-сервера
    port = server.xray_port or 443
    host = server.host
    
    logger.debug(f"Проверка сервера {server.name} ({host}:{port})")
    
    try:
        start_time = time.time()
        loop = asyncio.get_event_loop()
        is_online = await loop.run_in_executor(
            None,
            lambda: _check_port_sync(host, port, timeout=10)
        )
        response_time_ms = int((time.time() - start_time) * 1000)
        
        logger.debug(f"Результат проверки {server.name}: online={is_online}, time={response_time_ms}ms")
        
        # Тестируем скорость соединения, если сервер онлайн
        connection_speed_mbps = None
        if is_online:
            connection_speed_mbps = await _test_connection_speed(server)
            if connection_speed_mbps:
                logger.debug(f"Скорость соединения с {server.name}: {connection_speed_mbps:.2f} Мбит/с")
        
        # Всегда возвращаем response_time_ms для диагностики
        return {
            "is_online": is_online,
            "response_time_ms": response_time_ms,  # Показываем время даже при ошибке
            "connection_speed_mbps": connection_speed_mbps,
            "error_message": None if is_online else f"Port {port} unreachable (timeout: {response_time_ms}ms)",
        }
    except Exception as e:
        logger.error(f"Ошибка при проверке сервера {server.name}: {e}")
        return {
            "is_online": False,
            "response_time_ms": None,
            "connection_speed_mbps": None,
            "error_message": str(e),
        }


async def _close_old_pending_payments():
    """Закрывает платежи со статусом pending, которые созданы больше часа назад"""
    from core.db.session import SessionLocal
    from datetime import timedelta, timezone
    import logging
    
    async with SessionLocal() as session:
        try:
            # Находим все pending платежи старше 1 часа
            now_utc = datetime.now(timezone.utc)
            one_hour_ago = now_utc - timedelta(hours=1)
            logging.info(f"Checking for pending payments older than {one_hour_ago} (UTC)")
            
            old_pending = await session.scalars(
                select(Payment)
                .where(
                    Payment.status == PaymentStatus.pending,
                    Payment.created_at < one_hour_ago
                )
            )
            
            payments_list = old_pending.all()
            logging.info(f"Found {len(payments_list)} old pending payments to close")
            
            closed_count = 0
            for payment in payments_list:
                old_status = payment.status
                payment.status = PaymentStatus.failed
                
                # Логируем закрытие
                user = await session.scalar(select(User).where(User.id == payment.user_id))
                amount_rub = payment.amount_cents / 100
                age_hours = (now_utc - payment.created_at).total_seconds() / 3600
                logging.info(f"Closing payment #{payment.id}: created_at={payment.created_at}, age={age_hours:.2f} hours")
                
                session.add(
                    AuditLog(
                        action=AuditLogAction.payment_status_changed,
                        user_tg_id=user.tg_id if user else None,
                        admin_tg_id=None,
                        details=f"Платеж #{payment.id} автоматически закрыт (pending > 1 часа). Статус: {old_status.value} -> failed. Провайдер: {payment.provider}, сумма: {amount_rub:.2f} RUB ({payment.currency})",
                    )
                )
                closed_count += 1
            
            if closed_count > 0:
                await session.commit()
                logging.info(f"✅ Successfully closed {closed_count} old pending payments")
            else:
                logging.info("No old pending payments to close")
        except Exception as e:
            logging.error(f"Error in _close_old_pending_payments: {e}", exc_info=True)
            await session.rollback()


async def _check_servers_health():
    """Проверка состояния серверов"""
    from core.db.session import SessionLocal
    
    async with SessionLocal() as session:
        servers = await session.scalars(select(Server).where(Server.is_enabled == True))
        
        for server in servers.all():
            try:
                import httpx
                import time
                
                start_time = time.time()
                is_online = False
                response_time_ms = None
                error_message = None
                
                # Простая проверка доступности
                try:
                    # Пытаемся подключиться к серверу
                    host_parts = server.host.split(":")
                    host = host_parts[0]
                    port = int(host_parts[1]) if len(host_parts) > 1 else 80
                    
                    # Проверка через socket
                    import socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, port))
                    sock.close()
                    is_online = result == 0
                    
                    elapsed = (time.time() - start_time) * 1000
                    response_time_ms = int(elapsed)
                except Exception as e:
                    error_message = str(e)[:200]
                    is_online = False
                
                # Получаем количество активных подключений
                active_connections = await session.scalar(
                    select(func.count())
                    .select_from(VpnCredential)
                    .where(VpnCredential.server_id == server.id, VpnCredential.active == True)
                ) or 0
                
                # Создаем запись о статусе
                status = ServerStatus(
                    server_id=server.id,
                    is_online=is_online,
                    response_time_ms=response_time_ms,
                    active_connections=active_connections,
                    error_message=error_message,
                )
                session.add(status)
                await session.commit()
                
            except Exception as e:
                import logging
                logging.error(f"Error checking server {server.id}: {e}")
                continue


async def _create_database_backup(created_by_tg_id: int | None = None) -> Backup | None:
    """Создание резервной копии базы данных"""
    import os
    import subprocess
    from pathlib import Path
    
    try:
        # Создаем директорию для бэкапов, если её нет
        backup_dir = Path("/app/backups")
        backup_dir.mkdir(exist_ok=True)
        
        # Генерируем имя файла с датой и временем
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{timestamp}.dump"
        backup_path = backup_dir / backup_filename
        
        # Получаем параметры подключения к БД
        db_url = settings.db_url
        # Парсим URL: postgresql+asyncpg://user:password@db:5432/vpn
        if "postgresql" in db_url:
            # Извлекаем параметры из URL
            import re
            match = re.match(r"postgresql\+asyncpg://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", db_url)
            if match:
                db_user, db_password, db_host, db_port, db_name = match.groups()
                
                # Используем pg_dump для создания бэкапа
                env = os.environ.copy()
                env["PGPASSWORD"] = db_password
                
                # Создаем бэкап через pg_dump в custom format
                # Custom format более надежен для восстановления данных
                cmd = [
                    "pg_dump",
                    "-h", db_host,
                    "-p", db_port,
                    "-U", db_user,
                    "-d", db_name,
                    "-F", "c",  # Custom format (сжатый, более надежный)
                    "--no-owner",  # Не устанавливать владельца объектов
                    "--no-privileges",  # Не устанавливать привилегии
                    "--no-comments",  # Не включать комментарии (уменьшает размер)
                    "-f", str(backup_path),
                ]
                
                result = subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 минут таймаут
                )
                
                if result.returncode != 0:
                    raise Exception(f"pg_dump failed: {result.stderr}")
                
                # Получаем размер файла
                file_size = backup_path.stat().st_size if backup_path.exists() else 0
                
                # Сохраняем информацию о бэкапе в БД
                async with SessionLocal() as session:
                    backup = Backup(
                        backup_type="database",
                        file_path=str(backup_path),
                        file_size_bytes=file_size,
                        status="completed",
                        created_by_tg_id=created_by_tg_id,
                    )
                    session.add(backup)
                    await session.commit()
                    await session.refresh(backup)
                    
                    # Логируем успешное создание бэкапа
                    if created_by_tg_id:
                        session.add(
                            AuditLog(
                                action=AuditLogAction.backup_action,
                                admin_tg_id=created_by_tg_id,
                                details=f"Создан бэкап #{backup.id} (размер: {file_size / (1024*1024):.2f} MB)",
                            )
                        )
                        await session.commit()
                    
                    # Удаляем старые бэкапы (оставляем последние 10)
                    # Используем новую сессию, чтобы гарантировать, что новый бэкап уже закоммичен
                    import logging
                    import sys
                    print("=== STARTING BACKUP CLEANUP ===", file=sys.stderr, flush=True)
                    logging.info("=== STARTING BACKUP CLEANUP ===")
                    
                    async with SessionLocal() as cleanup_session:
                        print("Cleanup session created", file=sys.stderr, flush=True)
                        logging.info("Starting cleanup of old backups...")
                        
                        # Получаем все бэкапы, отсортированные по дате (новые первые)
                        all_backups_stmt = (
                            select(Backup)
                            .where(Backup.backup_type == "database", Backup.status == "completed")
                            .order_by(Backup.created_at.desc())
                        )
                        all_backups_result = await cleanup_session.scalars(all_backups_stmt)
                        backups_list = list(all_backups_result.all())
                        
                        print(f"Found {len(backups_list)} completed database backups", file=sys.stderr, flush=True)
                        logging.info(f"Found {len(backups_list)} completed database backups")
                        
                        # Если бэкапов больше 10, удаляем все кроме первых 10
                        if len(backups_list) > 10:
                            backups_to_delete = backups_list[10:]  # Все кроме первых 10
                            print(f"Need to delete {len(backups_to_delete)} old backups (keeping 10 newest)", file=sys.stderr, flush=True)
                            logging.info(f"Need to delete {len(backups_to_delete)} old backups (keeping 10 newest)")
                            
                            deleted_count = 0
                            for old_backup in backups_to_delete:
                                try:
                                    old_path = Path(old_backup.file_path)
                                    if old_path.exists():
                                        old_path.unlink()
                                        print(f"Deleted backup file: {old_path}", file=sys.stderr, flush=True)
                                        logging.info(f"Deleted backup file: {old_path}")
                                    await cleanup_session.delete(old_backup)
                                    deleted_count += 1
                                    print(f"Deleted backup record: ID={old_backup.id}, created_at={old_backup.created_at}", file=sys.stderr, flush=True)
                                    logging.info(f"Deleted backup record: ID={old_backup.id}, created_at={old_backup.created_at}")
                                except Exception as e:
                                    print(f"ERROR: Could not delete old backup {old_backup.id}: {e}", file=sys.stderr, flush=True)
                                    logging.error(f"Could not delete old backup {old_backup.id}: {e}", exc_info=True)
                            
                            if deleted_count > 0:
                                await cleanup_session.commit()
                                
                                # Проверяем, что бэкапы действительно удалены
                                verify_stmt = (
                                    select(Backup)
                                    .where(Backup.backup_type == "database", Backup.status == "completed")
                                    .order_by(Backup.created_at.desc())
                                )
                                verify_result = await cleanup_session.scalars(verify_stmt)
                                remaining_backups = list(verify_result.all())
                                print(f"Verification: {len(remaining_backups)} backups remaining after deletion", file=sys.stderr, flush=True)
                                logging.info(f"Verification: {len(remaining_backups)} backups remaining after deletion")
                                
                                print(f"Successfully deleted {deleted_count} old backups", file=sys.stderr, flush=True)
                                logging.info(f"Successfully deleted {deleted_count} old backups")
                            else:
                                print("WARNING: No backups were deleted", file=sys.stderr, flush=True)
                                logging.warning("No backups were deleted")
                        else:
                            print(f"Only {len(backups_list)} backups found, no cleanup needed (keeping all)", file=sys.stderr, flush=True)
                            logging.info(f"Only {len(backups_list)} backups found, no cleanup needed (keeping all)")
                    
                    print("=== BACKUP CLEANUP COMPLETED ===", file=sys.stderr, flush=True)
                    logging.info("=== BACKUP CLEANUP COMPLETED ===")
                    
                    return backup
        else:
            raise Exception("Only PostgreSQL backups are supported")
            
    except Exception as e:
        import logging
        logging.error(f"Error creating backup: {e}")
        
        # Сохраняем информацию об ошибке
        try:
            async with SessionLocal() as session:
                backup = Backup(
                    backup_type="database",
                    file_path="",
                    file_size_bytes=0,
                    status="failed",
                    error_message=str(e)[:500],
                    created_by_tg_id=created_by_tg_id,
                )
                session.add(backup)
                await session.commit()
        except Exception:
            pass
        
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Авто-создание таблиц в dev. Для продакшена — миграции alembic.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
        # Добавляем колонку has_active_subscription, если её нет
        try:
            result = await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='has_active_subscription'")
            )
            exists = result.scalar()
            if not exists:
                await conn.execute(text("ALTER TABLE users ADD COLUMN has_active_subscription BOOLEAN NOT NULL DEFAULT FALSE"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_has_active_subscription ON users(has_active_subscription)"))
                import logging
                logging.info("Added has_active_subscription column to users table")
        except Exception as e:
            import logging
            logging.warning(f"Could not add has_active_subscription column (may already exist): {e}")
        
        # Добавляем колонку subscription_ends_at, если её нет
        try:
            result = await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='subscription_ends_at'")
            )
            exists = result.scalar()
            if not exists:
                await conn.execute(text("ALTER TABLE users ADD COLUMN subscription_ends_at TIMESTAMP WITH TIME ZONE"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_subscription_ends_at ON users(subscription_ends_at)"))
                import logging
                logging.info("Added subscription_ends_at column to users table")
        except Exception as e:
            import logging
            logging.warning(f"Could not add subscription_ends_at column (may already exist): {e}")
        
        # Добавляем колонку selected_server_id, если её нет
        try:
            result = await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='selected_server_id'")
            )
            exists = result.scalar()
            if not exists:
                await conn.execute(text("ALTER TABLE users ADD COLUMN selected_server_id INTEGER"))
                await conn.execute(text("ALTER TABLE users ADD CONSTRAINT fk_users_selected_server_id FOREIGN KEY (selected_server_id) REFERENCES servers(id) ON DELETE SET NULL"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_selected_server_id ON users(selected_server_id)"))
                import logging
                logging.info("Added selected_server_id column to users table")
        except Exception as e:
            import logging
            logging.warning(f"Could not add selected_server_id column (may already exist): {e}")
        
        # ALTER TYPE ... ADD VALUE нельзя выполнять внутри транзакции в PostgreSQL
        # Enum значения определены в models.py и создаются автоматически
        
        # Добавляем колонки для таблицы servers, если их нет
        server_columns = [
            ("xray_port", "INTEGER", "DEFAULT 443"),
            ("xray_uuid", "VARCHAR(36)", ""),
            ("xray_flow", "VARCHAR(16)", ""),
            ("xray_network", "VARCHAR(16)", "DEFAULT 'tcp'"),
            ("xray_security", "VARCHAR(16)", "DEFAULT 'tls'"),
            ("xray_sni", "VARCHAR(255)", ""),
            ("xray_reality_public_key", "VARCHAR(255)", ""),
            ("xray_reality_short_id", "VARCHAR(16)", ""),
            ("xray_path", "VARCHAR(255)", ""),
            ("xray_host", "VARCHAR(255)", ""),
            ("x3ui_api_url", "VARCHAR(255)", ""),
            ("x3ui_username", "VARCHAR(64)", ""),
            ("x3ui_password", "VARCHAR(255)", ""),
            ("x3ui_inbound_id", "INTEGER", ""),
        ]
        
        # Проверяем, существует ли таблица servers
        try:
            table_exists = await conn.execute(
                text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='servers')")
            )
            if not table_exists.scalar():
                import logging
                logging.warning("Table 'servers' does not exist, skipping column migration")
            else:
                # Таблица существует, добавляем колонки
                for column_name, column_type, default in server_columns:
                    try:
                        result = await conn.execute(
                            text(f"SELECT column_name FROM information_schema.columns WHERE table_name='servers' AND column_name='{column_name}'")
                        )
                        exists = result.scalar()
                        if not exists:
                            default_clause = f" {default}" if default else ""
                            await conn.execute(text(f"ALTER TABLE servers ADD COLUMN {column_name} {column_type}{default_clause}"))
                            import logging
                            logging.info(f"Added {column_name} column to servers table")
                    except Exception as e:
                        import logging
                        logging.error(f"Error adding {column_name} column: {e}", exc_info=True)
        except Exception as e:
            import logging
            logging.error(f"Error checking servers table: {e}", exc_info=True)
        
        # Добавляем колонку connection_speed_mbps в server_status, если её нет
        try:
            result = await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='server_status' AND column_name='connection_speed_mbps'")
            )
            exists = result.scalar()
            if not exists:
                await conn.execute(text("ALTER TABLE server_status ADD COLUMN connection_speed_mbps NUMERIC(10, 2)"))
                import logging
                logging.info("Added connection_speed_mbps column to server_status table")
        except Exception as e:
            import logging
            logging.warning(f"Could not add connection_speed_mbps column (may already exist): {e}")
        
        # Добавляем колонку user_uuid в vpn_credentials, если её нет
        try:
            result = await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='vpn_credentials' AND column_name='user_uuid'")
            )
            exists = result.scalar()
            if not exists:
                await conn.execute(text("ALTER TABLE vpn_credentials ADD COLUMN user_uuid VARCHAR(36)"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_vpn_credentials_user_uuid ON vpn_credentials(user_uuid)"))
                import logging
                logging.info("Added user_uuid column to vpn_credentials table")
        except Exception as e:
            import logging
            logging.warning(f"Could not add user_uuid column (may already exist): {e}")
        
    # Таблицы ip_logs и user_bans создаются автоматически через Base.metadata.create_all
    
    # Запускаем фоновую задачу для мониторинга серверов
    async def monitor_servers():
        while True:
            try:
                await asyncio.sleep(60)  # Проверка каждую минуту
                await _check_servers_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                import logging
                logging.error(f"Error in server monitoring: {e}")
    
    monitor_task = asyncio.create_task(monitor_servers())
    
    # Запускаем фоновую задачу для автоматических бэкапов
    async def auto_backup():
        while True:
            try:
                await asyncio.sleep(86400)  # Раз в сутки (24 часа)
                await _create_database_backup(created_by_tg_id=None)
            except asyncio.CancelledError:
                break
            except Exception as e:
                import logging
                logging.error(f"Error in auto backup: {e}")
    
    backup_task = asyncio.create_task(auto_backup())
    
    # Запускаем фоновую задачу для закрытия старых pending платежей
    async def close_old_pending_payments():
        import logging
        # Запускаем сразу при старте
        logging.info("Starting payment cleanup task - checking for old pending payments...")
        await _close_old_pending_payments()
        
        while True:
            try:
                await asyncio.sleep(3600)  # Проверка каждый час
                logging.info("Running scheduled payment cleanup...")
                await _close_old_pending_payments()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error closing old pending payments: {e}", exc_info=True)
    
    payments_cleanup_task = asyncio.create_task(close_old_pending_payments())
    
    # Фоновая задача для проверки истечения подписок
    async def check_expired_subscriptions():
        """Проверяет истечение подписок и обновляет статус пользователей"""
        from core.db.session import SessionLocal
        import logging
        
        while True:
            try:
                await asyncio.sleep(3600)  # Проверка каждый час
                logging.info("Running scheduled subscription status check...")
                
                async with SessionLocal() as session:
                    try:
                        now = datetime.now(timezone.utc)
                        
                        # Находим всех пользователей с активными подписками
                        users_with_subs = await session.scalars(
                            select(User)
                            .where(User.has_active_subscription == True)
                        )
                        
                        updated_count = 0
                        # Получаем всех пользователей и обновляем их статус подписки
                        all_users = await session.scalars(select(User))
                        for user in all_users.all():
                            old_status = user.has_active_subscription
                            await _update_user_subscription_status(user.id, session)
                            await session.flush()
                            if old_status != user.has_active_subscription:
                                updated_count += 1
                        
                        if updated_count > 0:
                            await session.commit()
                            logging.info(f"Updated subscription status for {updated_count} users")
                    except Exception as e:
                        logging.error(f"Error checking expired subscriptions: {e}", exc_info=True)
                        await session.rollback()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in subscription status check task: {e}", exc_info=True)
    
    subscription_check_task = asyncio.create_task(check_expired_subscriptions())
    
    # Фоновая задача для проверки состояния серверов
    async def check_servers_status():
        """Проверяет состояние серверов (доступность портов)"""
        from core.db.session import SessionLocal
        import logging
        import time
        
        while True:
            try:
                await asyncio.sleep(60)  # Проверка каждые 60 секунд
                logging.info("Running scheduled server status check...")
                
                async with SessionLocal() as session:
                    try:
                        # Получаем все активные серверы
                        servers = await session.scalars(
                            select(Server)
                            .where(Server.is_enabled == True)
                        )
                        servers_list = servers.all()
                        
                        checked_count = 0
                        for server in servers_list:
                            try:
                                port = server.xray_port or 443
                                host = server.host
                                
                                # Проверяем, когда была последняя проверка этого сервера
                                last_status = await session.scalar(
                                    select(ServerStatus)
                                    .where(ServerStatus.server_id == server.id)
                                    .order_by(ServerStatus.checked_at.desc())
                                )
                                
                                # Если последняя проверка была менее 20 секунд назад, пропускаем
                                if last_status and last_status.checked_at:
                                    from datetime import datetime, timezone
                                    time_since_check = (datetime.now(timezone.utc) - last_status.checked_at).total_seconds()
                                    if time_since_check < 20:
                                        logger.debug(f"Пропускаем проверку сервера {server.name}, последняя проверка была {time_since_check:.1f} секунд назад")
                                        continue
                                
                                # Проверяем доступность порта
                                status_result = await _check_server_status(server)
                                is_online = status_result["is_online"]
                                response_time_ms = status_result["response_time_ms"]
                                error_message = status_result["error_message"]
                                
                                # Сохраняем статус
                                status = ServerStatus(
                                    server_id=server.id,
                                    is_online=is_online,
                                    response_time_ms=response_time_ms,
                                    connection_speed_mbps=status_result.get("connection_speed_mbps"),
                                    error_message=error_message,
                                )
                                session.add(status)
                                checked_count += 1
                                
                                # Задержка между проверками серверов для избежания rate limiting
                                await asyncio.sleep(1.0)  # Увеличено до 1 секунды
                                
                            except Exception as e:
                                logging.error(f"Error checking server {server.id} ({server.name}): {e}")
                                # Сохраняем статус с ошибкой
                                status = ServerStatus(
                                    server_id=server.id,
                                    is_online=False,
                                    error_message=f"Check error: {str(e)}",
                                )
                                session.add(status)
                        
                        if checked_count > 0:
                            await session.commit()
                            logging.info(f"Checked status for {checked_count} servers")
                            
                            # Удаляем старые записи (оставляем только последние 100 на сервер)
                            for server in servers_list:
                                old_statuses = await session.scalars(
                                    select(ServerStatus)
                                    .where(ServerStatus.server_id == server.id)
                                    .order_by(ServerStatus.checked_at.desc())
                                    .offset(100)
                                )
                                for old_status in old_statuses.all():
                                    await session.delete(old_status)
                            await session.commit()
                            
                    except Exception as e:
                        logging.error(f"Error checking servers status: {e}", exc_info=True)
                        await session.rollback()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in server status check task: {e}", exc_info=True)
    
    server_check_task = asyncio.create_task(check_servers_status())
    
    # Фоновая задача для мониторинга IP и автобана
    async def monitor_client_ips():
        """Мониторит IP адреса клиентов и банит при превышении лимита"""
        from core.db.session import SessionLocal
        from core.db.models import Server, VpnCredential, User, IpLog, UserBan, SystemSetting
        from core.x3ui_api import X3UIAPI
        import logging
        
        # Ждем 2 минуты перед первым запуском
        await asyncio.sleep(120)
        
        while True:
            try:
                async with SessionLocal() as session:
                    try:
                        # Получаем настройки
                        ip_limit_setting = await session.scalar(
                            select(SystemSetting).where(SystemSetting.key == "vpn_limit_ip")
                        )
                        autoban_enabled_setting = await session.scalar(
                            select(SystemSetting).where(SystemSetting.key == "autoban_enabled")
                        )
                        autoban_duration_setting = await session.scalar(
                            select(SystemSetting).where(SystemSetting.key == "autoban_duration_hours")
                        )
                        
                        ip_limit = 1
                        if ip_limit_setting:
                            try:
                                ip_limit = int(ip_limit_setting.value)
                            except (ValueError, TypeError):
                                ip_limit = 1
                        
                        autoban_enabled = True
                        if autoban_enabled_setting:
                            autoban_enabled = autoban_enabled_setting.value.lower() in ("true", "1", "yes")
                        
                        autoban_duration_hours = 24
                        if autoban_duration_setting:
                            try:
                                autoban_duration_hours = int(autoban_duration_setting.value)
                            except (ValueError, TypeError):
                                autoban_duration_hours = 24
                        
                        # Получаем все активные серверы с 3x-UI API
                        servers = await session.scalars(
                            select(Server).where(
                                Server.is_enabled == True,
                                Server.x3ui_api_url.isnot(None),
                                Server.x3ui_username.isnot(None),
                                Server.x3ui_password.isnot(None)
                            )
                        )
                        
                        for server in servers.all():
                            try:
                                x3ui = X3UIAPI(
                                    api_url=server.x3ui_api_url,
                                    username=server.x3ui_username,
                                    password=server.x3ui_password,
                                )
                                
                                try:
                                    # Получаем все активные credentials для этого сервера
                                    credentials = await session.scalars(
                                        select(VpnCredential)
                                        .where(VpnCredential.server_id == server.id)
                                        .where(VpnCredential.active == True)
                                        .options(selectinload(VpnCredential.user))
                                    )
                                    
                                    for cred in credentials.all():
                                        if not cred.user:
                                            continue
                                        
                                        # Формируем email клиента с tg_id
                                        client_email = f"tg_{cred.user.tg_id}_server_{server.id}@fiorevpn"
                                        
                                        # Получаем IP адреса клиента
                                        ips = await x3ui.get_client_ips(client_email)
                                        
                                        if not ips:
                                            continue
                                        
                                        now = datetime.utcnow()
                                        
                                        # Логируем IP адреса
                                        for ip in ips:
                                            if not ip or ip == "No IP Record":
                                                continue
                                            
                                            # Ищем существующую запись
                                            existing_log = await session.scalar(
                                                select(IpLog).where(
                                                    IpLog.user_id == cred.user_id,
                                                    IpLog.server_id == server.id,
                                                    IpLog.ip_address == ip
                                                )
                                            )
                                            
                                            if existing_log:
                                                existing_log.last_seen = now
                                                existing_log.connection_count += 1
                                            else:
                                                session.add(IpLog(
                                                    user_id=cred.user_id,
                                                    server_id=server.id,
                                                    ip_address=ip,
                                                    first_seen=now,
                                                    last_seen=now,
                                                    connection_count=1
                                                ))
                                        
                                        # Проверяем превышение лимита IP
                                        if autoban_enabled and len(ips) > ip_limit:
                                            # Проверяем, не забанен ли уже
                                            existing_ban = await session.scalar(
                                                select(UserBan).where(
                                                    UserBan.user_id == cred.user_id,
                                                    UserBan.is_active == True
                                                )
                                            )
                                            
                                            if not existing_ban:
                                                # Создаем бан
                                                ban = UserBan(
                                                    user_id=cred.user_id,
                                                    reason="ip_limit_exceeded",
                                                    details=f"Обнаружено {len(ips)} IP адресов (лимит: {ip_limit}). IP: {', '.join(ips)}",
                                                    is_active=True,
                                                    auto_ban=True,
                                                    banned_until=now + timedelta(hours=autoban_duration_hours)
                                                )
                                                session.add(ban)
                                                
                                                # Отключаем клиента в 3x-UI
                                                if cred.user_uuid and server.x3ui_inbound_id:
                                                    await x3ui.disable_client(server.x3ui_inbound_id, cred.user_uuid)
                                                
                                                # Уведомляем пользователя
                                                notification_text = (
                                                    "⚠️ <b>Ваш аккаунт временно заблокирован</b>\n\n"
                                                    f"Причина: превышен лимит одновременных подключений ({len(ips)} из {ip_limit})\n"
                                                    f"Блокировка снимется автоматически через {autoban_duration_hours} ч.\n\n"
                                                    "Если вы считаете это ошибкой, обратитесь в поддержку."
                                                )
                                                asyncio.create_task(_send_user_notification(cred.user.tg_id, notification_text))
                                                
                                                logging.warning(
                                                    f"Автобан пользователя {cred.user.tg_id}: "
                                                    f"превышен лимит IP ({len(ips)} > {ip_limit})"
                                                )
                                        
                                        await session.commit()
                                        
                                finally:
                                    await x3ui.close()
                                    
                            except Exception as e:
                                logging.error(f"Error monitoring IPs for server {server.name}: {e}")
                                continue
                                
                    except Exception as e:
                        logging.error(f"Error in IP monitoring task: {e}", exc_info=True)
                        await session.rollback()
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in IP monitoring task: {e}", exc_info=True)
            
            # Проверяем каждые 5 минут
            await asyncio.sleep(300)
    
    ip_monitor_task = asyncio.create_task(monitor_client_ips())
    
    # Фоновая задача для снятия истекших банов
    async def unban_expired_users():
        """Снимает баны с истекшим сроком"""
        from core.db.session import SessionLocal
        from core.db.models import UserBan, VpnCredential, Server
        from core.x3ui_api import X3UIAPI
        import logging
        
        # Ждем 3 минуты перед первым запуском
        await asyncio.sleep(180)
        
        while True:
            try:
                async with SessionLocal() as session:
                    try:
                        now = datetime.utcnow()
                        
                        # Находим истекшие баны
                        expired_bans = await session.scalars(
                            select(UserBan).where(
                                UserBan.is_active == True,
                                UserBan.banned_until.isnot(None),
                                UserBan.banned_until < now
                            )
                        )
                        
                        for ban in expired_bans.all():
                            ban.is_active = False
                            ban.unbanned_at = now
                            
                            # Включаем клиента обратно в 3x-UI
                            credentials = await session.scalars(
                                select(VpnCredential)
                                .where(VpnCredential.user_id == ban.user_id)
                                .where(VpnCredential.active == True)
                                .options(selectinload(VpnCredential.server))
                            )
                            
                            for cred in credentials.all():
                                if not cred.server or not cred.user_uuid:
                                    continue
                                    
                                server = cred.server
                                if server.x3ui_api_url and server.x3ui_username and server.x3ui_password and server.x3ui_inbound_id:
                                    try:
                                        x3ui = X3UIAPI(
                                            api_url=server.x3ui_api_url,
                                            username=server.x3ui_username,
                                            password=server.x3ui_password,
                                        )
                                        try:
                                            await x3ui.enable_client(server.x3ui_inbound_id, cred.user_uuid)
                                        finally:
                                            await x3ui.close()
                                    except Exception as e:
                                        logging.error(f"Error enabling client after unban: {e}")
                            
                            logging.info(f"Автоматически снят бан с пользователя {ban.user_id}")
                        
                        await session.commit()
                        
                    except Exception as e:
                        logging.error(f"Error in unban task: {e}", exc_info=True)
                        await session.rollback()
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in unban task: {e}", exc_info=True)
            
            # Проверяем каждые 10 минут
            await asyncio.sleep(600)
    
    unban_task = asyncio.create_task(unban_expired_users())
    
    yield
    
    monitor_task.cancel()
    backup_task.cancel()
    payments_cleanup_task.cancel()
    subscription_check_task.cancel()
    server_check_task.cancel()
    ip_monitor_task.cancel()
    unban_task.cancel()
    try:
        await monitor_task
        await backup_task
        await payments_cleanup_task
        await subscription_check_task
        await server_check_task
        await ip_monitor_task
        await unban_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="fioreVPN Core API", version="0.1.0", lifespan=lifespan)

# Инициализация rate limiter (временно отключен из-за проблем с Pydantic)
# limiter = Limiter(key_func=get_remote_address)
# app.state.limiter = limiter
# app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
limiter = None  # Временно отключен

# Добавляем middleware для сессий
# Используем SECRET_KEY из env, или BOT_TOKEN, или генерируем случайный
import os
secret_key = os.getenv("SECRET_KEY", "").strip()
if not secret_key:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if bot_token:
        # Используем BOT_TOKEN как основу для secret_key
        import hashlib
        secret_key = hashlib.sha256(bot_token.encode()).digest()
    else:
        # Генерируем случайный ключ
        secret_key = secrets.token_urlsafe(32)
app.add_middleware(SessionMiddleware, secret_key=secret_key)

# Инициализация шаблонов для веб-интерфейса
import os
if os.path.exists("core/templates"):
    templates = Jinja2Templates(directory="core/templates")
else:
    templates = None

# Подключение статических файлов
if os.path.exists("core/static"):
    app.mount("/static", StaticFiles(directory="core/static"), name="static")




# Обработчики для стандартных запросов (чтобы не было 404 в логах)
@app.get("/")
@app.head("/")
async def root():
    """Корневой путь - редирект на админку"""
    return RedirectResponse(url="/admin/login", status_code=302)


@app.get("/privacy", response_class=HTMLResponse)
@app.get("/privacy-policy", response_class=HTMLResponse)
async def privacy_policy(request: Request):
    """Страница политики конфиденциальности"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    from datetime import datetime
    last_updated = datetime.now().strftime("%d.%m.%Y")
    
    return templates.TemplateResponse(
        "privacy.html",
        {
            "request": request,
            "last_updated": last_updated,
        }
    )


@app.get("/robots.txt")
async def robots_txt():
    """Robots.txt для поисковых ботов"""
    return Response(
        content="User-agent: *\nDisallow: /\n",
        media_type="text/plain"
    )


@app.get("/favicon.ico")
async def favicon_ico():
    """Favicon - возвращаем логотип если есть"""
    import os
    favicon_path = "core/static/images/logo.png"
    if os.path.exists(favicon_path):
        from fastapi.responses import FileResponse
        return FileResponse(favicon_path, media_type="image/png")
    return Response(status_code=204)


@app.get("/favicon.png")
async def favicon_png():
    """Favicon PNG - возвращаем 204 No Content"""
    return Response(status_code=204)


def _require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    if settings.admin_token and x_admin_token != settings.admin_token:
        raise HTTPException(status_code=403, detail="admin_forbidden")


def _require_admin_or_web(request: Request, x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> dict:
    """Проверка авторизации через токен (для бота) или веб-сессию (для админки)"""
    # Если токен передан в заголовке
    if x_admin_token:
        # Если токен настроен в settings, проверяем его
        if settings.admin_token:
            if x_admin_token == settings.admin_token:
                return {"tg_id": None, "username": "bot", "first_name": "Bot"}
            # Токен передан, но неверный
            raise HTTPException(status_code=403, detail="invalid_admin_token")
        # Токен передан, но не настроен в settings - разрешаем (для обратной совместимости)
        return {"tg_id": None, "username": "bot", "first_name": "Bot"}
    
    # Если токен не настроен в settings - разрешаем доступ без проверки (для обратной совместимости)
    # Это позволяет работать без токена, если он не настроен
    if not settings.admin_token:
        return {"tg_id": None, "username": "bot", "first_name": "Bot"}
    
    # Если токен настроен, но не передан - проверяем веб-сессию
    return _require_web_admin(request)


def _require_web_admin(request: Request) -> dict:
    """Проверка веб-авторизации через сессию"""
    session_data = request.session.get("admin_user")
    if not session_data:
        raise HTTPException(status_code=403, detail="not_authenticated")
    return session_data


def _get_csrf_token(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


def _require_csrf(request: Request) -> None:
    expected = request.session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token")
    if not expected or not provided or provided != expected:
        raise HTTPException(status_code=403, detail="csrf_forbidden")


def _get_effective_role(tg_id: int, admin_ids: set[int], overrides_map: dict[int, str]) -> str:
    """Возвращает роль: superadmin|admin|moderator|user"""
    if tg_id in admin_ids:
        return "superadmin"
    ov = overrides_map.get(tg_id)
    if ov in {"admin", "moderator", "user"}:
        return ov
    return "user"


def _role_rank(role: str) -> int:
    """Чем выше значение, тем старше роль"""
    ranks = {
        "user": 0,
        "moderator": 1,
        "admin": 2,
        "superadmin": 3,
    }
    return ranks.get(role, 0)


async def _fetch_avatar_url(tg_id: int, bot_token: str) -> str | None:
    """Получаем ссылку на аватар через Telegram getUserProfilePhotos -> getFile"""
    if not bot_token:
        return None
    import httpx
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(
                f"https://api.telegram.org/bot{bot_token}/getUserProfilePhotos",
                params={"user_id": tg_id, "limit": 1},
            )
            data = r.json()
            if not data.get("ok"):
                return None
            photos = data.get("result", {}).get("photos") or []
            if not photos or not photos[0]:
                return None
            file_id = photos[0][0].get("file_id")
            if not file_id:
                return None
            r2 = await client.get(f"https://api.telegram.org/bot{bot_token}/getFile", params={"file_id": file_id})
            d2 = r2.json()
            if not d2.get("ok"):
                return None
            file_path = d2.get("result", {}).get("file_path")
            if not file_path:
                return None
            return f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
    except Exception:
        return None


@app.post("/tickets/create")
async def create_ticket(payload: dict, session: AsyncSession = Depends(get_session)):
    tg_id = int(payload.get("tg_id") or 0)
    topic = (payload.get("topic") or "").strip()
    if not tg_id or not topic:
        raise HTTPException(status_code=400, detail="invalid_payload")

    now = datetime.utcnow()
    ticket = Ticket(
        user_tg_id=tg_id,
        topic=topic,
        status=TicketStatus.new,
        created_at=now,
        updated_at=now,
    )
    session.add(ticket)
    await session.flush()

    session.add(
        TicketMessage(
            ticket_id=ticket.id,
            user_tg_id=tg_id,
            direction=MessageDirection.system,
            admin_tg_id=None,
            text=f"[Тема] {topic}",
        )
    )
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=tg_id,
            details=f"Ticket #{ticket.id} created. Topic: {topic}",
        )
    )
    await session.commit()

    # Планируем автозакрытие, если пользователь не напишет в поддержку в течение 5 минут
    async def autoclose(ticket_id: int):
        await asyncio.sleep(300)
        async with SessionLocal() as s:
            t = await s.scalar(select(Ticket).where(Ticket.id == ticket_id))
            if not t or t.status != TicketStatus.open:
                return
            # есть ли входящие сообщения (пользователь) позже создания
            has_incoming = await s.scalar(
                select(func.count())
                .select_from(TicketMessage)
                .where(
                    TicketMessage.ticket_id == ticket_id,
                    TicketMessage.direction == MessageDirection.incoming,
                    TicketMessage.created_at > t.created_at,
                )
            )
            if has_incoming and has_incoming > 0:
                return
            t.status = TicketStatus.closed
            t.closed_at = datetime.utcnow()
            t.updated_at = t.closed_at
            s.add(
                AuditLog(
                    action=AuditLogAction.admin_action,
                    user_tg_id=t.user_tg_id,
                    details=f"Ticket #{t.id} auto-closed (no user response in 5 minutes)",
                )
            )
            await s.commit()

    asyncio.create_task(autoclose(ticket.id))
    return {"ticket_id": ticket.id}


def _get_csrf_token(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


def _require_csrf(request: Request) -> None:
    expected = request.session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token")
    if not expected or not provided or provided != expected:
        raise HTTPException(status_code=403, detail="csrf_forbidden")


def _get_csrf_token(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


def _require_csrf(request: Request) -> None:
    expected = request.session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token")
    if not expected or not provided or provided != expected:
        raise HTTPException(status_code=403, detail="csrf_forbidden")


@app.exception_handler(StarletteHTTPException)
async def starlette_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Обработка HTTP исключений от Starlette (включая 404 и 405)"""
    import logging
    # Для 405 (Method Not Allowed) - тихо возвращаем ответ без логирования ошибки
    # Это обычно сканирование портов или атаки извне
    if exc.status_code == 405:
        return JSONResponse(
            status_code=405,
            content={"detail": "Method Not Allowed"}
        )
    
    # Для 404 ошибок
    if exc.status_code == 404:
        path = request.url.path
        logging.info(f"404 handler: path={path}")
        
        # Для API endpoints возвращаем JSON (только чистые API пути, без /admin/web)
        is_api_path = (
            path.startswith("/api/") or 
            path.startswith("/subscriptions/") or 
            path.startswith("/payments/") or 
            (path.startswith("/users/") and not path.startswith("/admin/web")) or 
            path.startswith("/promo-codes/") or
            (path.startswith("/tickets/") and not path.startswith("/admin/web")) or
            path.startswith("/health") or
            path.startswith("/support/")
        )
        
        logging.info(f"404 handler: is_api_path={is_api_path}, templates={templates is not None}")
        
        if is_api_path:
            return JSONResponse(
                status_code=404,
                content={"detail": "Not found"}
            )
        
        # Для всех остальных путей (включая /admin/web/*) показываем красивую 404
        if templates:
            logging.info(f"404 handler: returning 404.html template")
            return templates.TemplateResponse(
                "404.html",
                {"request": request},
                status_code=404
            )
        # Если шаблоны не загружены, возвращаем простой текст
        logging.info(f"404 handler: templates not loaded, returning simple HTML")
        return HTMLResponse(
            content="<h1>404 - Страница не найдена</h1><p><a href='/admin/login'>Перейти в админ-панель</a></p>",
            status_code=404
        )
    
    # Для других HTTP ошибок от Starlette пробрасываем дальше (кроме 405, который уже обработан)
    if exc.status_code != 405:
        raise exc


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Обработка HTTP исключений от FastAPI"""
    if exc.status_code == 403:
        # Для веб-интерфейса - редирект на логин
        if request.url.path.startswith("/admin/web"):
            return RedirectResponse(url="/admin/login", status_code=303)
        # Для API - возвращаем JSON
        return JSONResponse(
            status_code=403,
            content={"detail": exc.detail}
        )
    
    # Для 405 (Method Not Allowed) - тихо возвращаем ответ без логирования ошибки
    if exc.status_code == 405:
        # Это обычно сканирование портов или атаки, не логируем как ошибку
        return JSONResponse(
            status_code=405,
            content={"detail": "Method Not Allowed"}
        )
    
    # Для всех остальных HTTPException возвращаем JSON для API endpoints
    if request.url.path.startswith("/api/") or request.url.path.startswith("/subscriptions/") or request.url.path.startswith("/payments/") or request.url.path.startswith("/users/"):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail}
        )
    # Для веб-страниц пробрасываем исключение дальше
    raise exc


def _gen_ref_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


async def _ensure_ref_code_unique(session: AsyncSession) -> str:
    for _ in range(20):
        code = _gen_ref_code()
        exists = await session.scalar(select(User.id).where(User.referral_code == code))
        if not exists:
            return code
    raise RuntimeError("failed_to_generate_ref_code")


async def _get_referral_reward_referrer_amount(session: AsyncSession) -> int:
    """Получает сумму реферальной награды для пригласившего из настроек (в копейках)"""
    setting = await session.scalar(select(SystemSetting).where(SystemSetting.key == "referral_reward_referrer_cents"))
    if setting:
        try:
            return int(setting.value)
        except (ValueError, TypeError):
            pass
    # Значение по умолчанию: 10000 копеек = 100 RUB
    return 10000


async def _get_referral_reward_referred_amount(session: AsyncSession) -> int:
    """Получает сумму реферальной награды для приглашенного из настроек (в копейках)"""
    setting = await session.scalar(select(SystemSetting).where(SystemSetting.key == "referral_reward_referred_cents"))
    if setting:
        try:
            return int(setting.value)
        except (ValueError, TypeError):
            pass
    # Значение по умолчанию: 10000 копеек = 100 RUB
    return 10000


async def _update_user_subscription_status(user_id: int, session: AsyncSession) -> None:
    """Обновляет поля has_active_subscription и subscription_ends_at у пользователя на основе активных подписок"""
    from datetime import datetime, timezone
    
    user = await session.scalar(select(User).where(User.id == user_id))
    if not user:
        return
    
    # Проверяем, есть ли активная подписка, которая еще не истекла
    now = datetime.now(timezone.utc)
    active_sub = await session.scalar(
        select(Subscription)
        .where(Subscription.user_id == user_id)
        .where(Subscription.status == SubscriptionStatus.active)
        .where((Subscription.ends_at.is_(None)) | (Subscription.ends_at > now))
        .order_by(Subscription.ends_at.desc().nullslast())
        .limit(1)
    )
    
    # Обновляем поля
    if active_sub:
        user.has_active_subscription = True
        user.subscription_ends_at = active_sub.ends_at
    else:
        user.has_active_subscription = False
        user.subscription_ends_at = None
        # Очищаем выбранный сервер, если подписка не активна
        user.selected_server_id = None


async def _validate_promo_code(code: str, user_id: int, amount_cents: int, session: AsyncSession, check_percent_usage: bool = False) -> tuple[bool, str, int]:
    """
    Проверяет промокод и возвращает (is_valid, error_message, discount_cents)
    check_percent_usage: если True, проверяет, не использовал ли пользователь уже промокод на скидку
    """
    promo = await session.scalar(select(PromoCode).where(PromoCode.code == code.upper().strip()))
    if not promo:
        return False, "Промокод не найден", 0
    if not promo.is_active:
        return False, "Промокод неактивен", 0
    
    now = datetime.utcnow()
    if promo.valid_from and now < promo.valid_from:
        return False, "Промокод еще не действителен", 0
    if promo.valid_until and now > promo.valid_until:
        return False, "Промокод истек", 0
    
    if promo.max_uses and promo.used_count >= promo.max_uses:
        return False, "Промокод исчерпан", 0
    
    # Проверяем, не использовал ли пользователь уже этот промокод
    existing_usage = await session.scalar(
        select(PromoCodeUsage)
        .where(PromoCodeUsage.promo_code_id == promo.id)
        .where(PromoCodeUsage.user_id == user_id)
    )
    if existing_usage:
        return False, "Вы уже использовали этот промокод", 0
    
    # Если это промокод на скидку (процент), проверяем, не использовал ли пользователь уже другой промокод на скидку
    if check_percent_usage and promo.discount_percent:
        other_percent_usage = await session.scalar(
            select(PromoCodeUsage)
            .join(PromoCode)
            .where(PromoCode.discount_percent.isnot(None))
            .where(PromoCodeUsage.user_id == user_id)
            .where(PromoCodeUsage.promo_code_id != promo.id)
        )
        if other_percent_usage:
            return False, "Вы уже использовали промокод на скидку. Нельзя применить несколько промокодов на скидку", 0
    
    # Вычисляем скидку
    discount_cents = 0
    if promo.discount_percent:
        discount_cents = int(amount_cents * promo.discount_percent / 100)
    elif promo.discount_amount_cents:
        discount_cents = promo.discount_amount_cents
        # Для фикс суммы не ограничиваем суммой платежа - это бонус на баланс
    
    return True, "", discount_cents


@app.post("/promo-codes/validate")
async def validate_promo_code(
    payload: PromoCodeValidateIn,
    session: AsyncSession = Depends(get_session),
):
    """Проверка промокода"""
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    is_valid, error_msg, discount_cents = await _validate_promo_code(
        payload.code, user.id, payload.amount_cents, session
    )
    
    # Получаем информацию о промокоде для определения типа
    promo = None
    promo_type = None
    discount_percent = None
    discount_amount_cents = None
    if is_valid:
        promo = await session.scalar(select(PromoCode).where(PromoCode.code == payload.code.upper().strip()))
        if promo:
            if promo.discount_percent:
                promo_type = "percent"
                discount_percent = promo.discount_percent
            elif promo.discount_amount_cents:
                promo_type = "fixed"
                discount_amount_cents = promo.discount_amount_cents
    
    return {
        "valid": is_valid,
        "error": error_msg if not is_valid else None,
        "discount_cents": discount_cents,
        "promo_type": promo_type,
        "discount_percent": discount_percent,
        "discount_amount_cents": discount_amount_cents,
    }


@app.post("/promo-codes/apply")
async def apply_promo_code(
    payload: PromoCodeApplyIn,
    session: AsyncSession = Depends(get_session),
    admin_user: dict | None = Depends(_require_admin_or_web),
):
    """Применение промокода"""
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    # Находим промокод
    promo = await session.scalar(select(PromoCode).where(PromoCode.code == payload.code.upper().strip()))
    if not promo:
        return {"success": False, "error": "Промокод не найден"}
    
    # Проверяем промокод (для промокодов на скидку проверяем, не использовал ли уже другой промокод на скидку)
    is_valid, error_msg, discount_cents = await _validate_promo_code(
        payload.code, user.id, payload.amount_cents, session, check_percent_usage=bool(promo.discount_percent)
    )
    
    if not is_valid:
        return {"success": False, "error": error_msg}
    
    # Если это промокод на фикс сумму, начисляем баланс сразу
    if promo.discount_amount_cents and not promo.discount_percent:
        user.balance += promo.discount_amount_cents
        session.add(
            BalanceTransaction(
                user_id=user.id,
                admin_tg_id=None,
                amount=promo.discount_amount_cents,
                reason=f"Промокод {promo.code}",
            )
        )
        # Логируем в админке
        admin_tg_id = admin_user.get("tg_id") if admin_user else None
        session.add(
            AuditLog(
                action=AuditLogAction.admin_action,
                user_tg_id=user.tg_id,
                admin_tg_id=admin_tg_id,
                details=f"Применен промокод {promo.code} (фикс сумма: {promo.discount_amount_cents / 100:.2f} RUB). Баланс пополнен.",
            )
        )
    
    # Создаем запись об использовании
    usage = PromoCodeUsage(
        promo_code_id=promo.id,
        user_id=user.id,
        discount_amount_cents=discount_cents,
    )
    session.add(usage)
    
    # Увеличиваем счетчик использований
    promo.used_count += 1
    
    await session.commit()
    
    return {
        "success": True,
        "discount_cents": discount_cents,
        "promo_type": "fixed" if promo.discount_amount_cents and not promo.discount_percent else "percent" if promo.discount_percent else None,
    }


@app.get("/health")
async def health_check(session: AsyncSession = Depends(get_session)):
    """Проверка работоспособности API и подключения к БД"""
    try:
        # Проверка подключения к БД
        await session.execute(select(1))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}


@app.get("/users")
async def list_users(
    session: AsyncSession = Depends(get_session),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> list[UserOut]:
    stmt = select(User).options(selectinload(User.referred_by)).order_by(User.created_at.desc()).limit(limit).offset(offset)
    result = await session.scalars(stmt)
    users: Sequence[User] = result.all()
    out: list[UserOut] = []
    for u in users:
        referred_by_tg_id = u.referred_by.tg_id if u.referred_by else None
        out.append(
            UserOut(
                id=u.id,
                tg_id=u.tg_id,
                username=u.username,
                first_name=u.first_name,
                last_name=u.last_name,
                is_active=u.is_active,
                balance=u.balance,
                referral_code=u.referral_code,
                referred_by_tg_id=referred_by_tg_id,
                trial_used=u.trial_used,
                has_active_subscription=u.has_active_subscription,
                subscription_ends_at=u.subscription_ends_at,
                selected_server_id=u.selected_server_id,
                created_at=u.created_at,
            )
        )
    return out


@app.get("/users/count")
async def users_count(session: AsyncSession = Depends(get_session)) -> dict[str, int]:
    total = await session.scalar(select(func.count()).select_from(User))
    return {"total": int(total or 0)}


@app.get("/users/by_tg/{tg_id}")
async def get_user_by_tg(tg_id: int, session: AsyncSession = Depends(get_session)) -> UserOut:
    stmt = select(User).options(selectinload(User.referred_by)).where(User.tg_id == tg_id)
    user = await session.scalar(stmt)
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    referred_by_tg_id = user.referred_by.tg_id if user.referred_by else None
    return UserOut(
        id=user.id,
        tg_id=user.tg_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        is_active=user.is_active,
        balance=user.balance,
        referral_code=user.referral_code,
        referred_by_tg_id=referred_by_tg_id,
        trial_used=user.trial_used,
        has_active_subscription=user.has_active_subscription,
        subscription_ends_at=user.subscription_ends_at,
        selected_server_id=user.selected_server_id,
        created_at=user.created_at,
    )


@app.post("/users/upsert")
async def upsert_user(payload: UserUpsertIn, session: AsyncSession = Depends(get_session)) -> UserOut:
    stmt = select(User).options(selectinload(User.referred_by)).where(User.tg_id == payload.tg_id)
    user = await session.scalar(stmt)
    is_new = False
    if not user:
        is_new = True
        user = User(tg_id=payload.tg_id, is_active=True, balance=0)
        user.referral_code = await _ensure_ref_code_unique(session)

        if payload.referral_code:
            ref = await session.scalar(select(User).where(User.referral_code == payload.referral_code))
            if ref and ref.tg_id != payload.tg_id:
                user.referred_by = ref

        session.add(user)
        await session.flush()  # Получаем ID пользователя
        session.add(
            AuditLog(
                action=AuditLogAction.user_registered,
                user_tg_id=payload.tg_id,
                details=f"Пользователь зарегистрирован. Реферальный код: {user.referral_code}",
            )
        )
        
        # Начисляем реферальную награду пригласившему и приглашенному
        if user.referred_by:
            referrer_reward_cents = await _get_referral_reward_referrer_amount(session)
            referred_reward_cents = await _get_referral_reward_referred_amount(session)
            
            # Награда для пригласившего
            if referrer_reward_cents > 0:
                user.referred_by.balance += referrer_reward_cents
                session.add(
                    ReferralReward(
                        referrer_user_id=user.referred_by.id,
                        referred_user_id=user.id,
                        amount_cents=referrer_reward_cents,
                        is_for_referrer=True,
                    )
                )
                session.add(
                    BalanceTransaction(
                        user_id=user.referred_by.id,
                        amount=referrer_reward_cents,
                        reason=f"Реферальная награда за приглашение пользователя {payload.tg_id}",
                    )
                )
                session.add(
                    AuditLog(
                        action=AuditLogAction.balance_credited,
                        user_tg_id=user.referred_by.tg_id,
                        details=f"Реферальная награда: {referrer_reward_cents / 100:.2f} RUB за приглашение пользователя {payload.tg_id}",
                    )
                )
                # Отправляем уведомление пригласившему (если включено)
                notify_on_referral = await session.scalar(select(SystemSetting).where(SystemSetting.key == "notify_on_referral"))
                if not notify_on_referral or notify_on_referral.value != "false":
                    notification_text = (
                        f"🎁 <b>Реферальная награда!</b>\n\n"
                        f"Вы получили <b>{referrer_reward_cents / 100:.2f} RUB</b> за приглашение нового пользователя.\n"
                        f"Ваш баланс: <b>{user.referred_by.balance / 100:.2f} RUB</b>"
                    )
                    asyncio.create_task(_send_user_notification(user.referred_by.tg_id, notification_text))
            
            # Награда для приглашенного
            if referred_reward_cents > 0:
                user.balance += referred_reward_cents
                session.add(
                    ReferralReward(
                        referrer_user_id=user.referred_by.id,
                        referred_user_id=user.id,
                        amount_cents=referred_reward_cents,
                        is_for_referrer=False,
                    )
                )
                session.add(
                    BalanceTransaction(
                        user_id=user.id,
                        amount=referred_reward_cents,
                        reason=f"Реферальная награда за регистрацию по приглашению",
                    )
                )
                session.add(
                    AuditLog(
                        action=AuditLogAction.balance_credited,
                        user_tg_id=payload.tg_id,
                        details=f"Реферальная награда: {referred_reward_cents / 100:.2f} RUB за регистрацию по приглашению",
                    )
                )
                # Отправляем уведомление приглашенному (если включено)
                notify_on_referral = await session.scalar(select(SystemSetting).where(SystemSetting.key == "notify_on_referral"))
                if not notify_on_referral or notify_on_referral.value != "false":
                    notification_text = (
                        f"🎁 <b>Добро пожаловать!</b>\n\n"
                        f"Вы получили <b>{referred_reward_cents / 100:.2f} RUB</b> за регистрацию по реферальной ссылке.\n"
                        f"Ваш баланс: <b>{user.balance / 100:.2f} RUB</b>"
                    )
                    asyncio.create_task(_send_user_notification(payload.tg_id, notification_text))
    # Обновляем данные пользователя (username может измениться)
    if payload.username is not None:
        user.username = payload.username
    if payload.first_name is not None:
        user.first_name = payload.first_name
    if payload.last_name is not None:
        user.last_name = payload.last_name
    
    if is_new:
        await session.commit()
    else:
        await session.commit()
    await session.refresh(user)
    # Перезагружаем с relationship после refresh
    user = await session.scalar(stmt)
    referred_by_tg_id = user.referred_by.tg_id if user.referred_by else None
    return UserOut(
        id=user.id,
        tg_id=user.tg_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        is_active=user.is_active,
        balance=user.balance,
        referral_code=user.referral_code,
        referred_by_tg_id=referred_by_tg_id,
        trial_used=user.trial_used,
        has_active_subscription=user.has_active_subscription,
        subscription_ends_at=user.subscription_ends_at,
        selected_server_id=user.selected_server_id,
        created_at=user.created_at,
    )


@app.get("/subscriptions")
async def list_subscriptions(session: AsyncSession = Depends(get_session)) -> list[dict]:
    result = await session.scalars(select(Subscription))
    subs: Sequence[Subscription] = result.all()
    return [
        {
            "id": s.id,
            "user_id": s.user_id,
            "plan_name": s.plan_name,
            "status": s.status,
            "starts_at": s.starts_at,
            "ends_at": s.ends_at,
        }
        for s in subs
    ]


@app.get("/users/{tg_id}/referral/rewards")
async def get_user_referral_rewards(
    tg_id: int,
    limit: int = Query(default=10, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
):
    """Получить историю реферальных наград пользователя"""
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    # Получаем награды, где пользователь был пригласившим
    rewards_result = await session.scalars(
        select(ReferralReward)
        .where(ReferralReward.referrer_user_id == user.id)
        .order_by(ReferralReward.created_at.desc())
        .limit(limit)
    )
    rewards = rewards_result.all()
    
    # Получаем информацию о приглашенных пользователях
    referred_ids = [r.referred_user_id for r in rewards]
    referred_users = {}
    if referred_ids:
        users_result = await session.scalars(select(User).where(User.id.in_(referred_ids)))
        for u in users_result.all():
            referred_users[u.id] = u
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    return [
        {
            "id": r.id,
            "amount": r.amount_cents / 100,
            "referred_user_tg_id": referred_users.get(r.referred_user_id, User(tg_id=0)).tg_id if r.referred_user_id in referred_users else 0,
            "is_for_referrer": r.is_for_referrer,
            "created_at": fmt(r.created_at),
        }
        for r in rewards
    ]


@app.get("/users/{tg_id}/payments")
async def get_user_payments(
    tg_id: int,
    limit: int = Query(default=10, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    """Получить историю платежей пользователя"""
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    payments_result = await session.scalars(
        select(Payment)
        .where(Payment.user_id == user.id)
        .order_by(Payment.created_at.desc())
        .limit(limit)
    )
    payments = payments_result.all()
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    # amount_cents уже хранится в рублях (копейках)
    return [
        {
            "id": p.id,
            "provider": p.provider,
            "amount": p.amount_cents / 100,  # Конвертируем копейки в рубли
            "amount_cents": p.amount_cents,
            "currency": p.currency,
            "status": p.status.value if hasattr(p.status, "value") else str(p.status),
            "created_at": fmt(p.created_at),
        }
        for p in payments
    ]


@app.get("/api/payments/{payment_id}")
async def get_payment_detail(
    payment_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Получить детали платежа по ID"""
    payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
    if not payment:
        raise HTTPException(status_code=404, detail="payment_not_found")
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    return {
        "id": payment.id,
        "provider": payment.provider,
        "amount_cents": payment.amount_cents,
        "currency": payment.currency,
        "status": payment.status.value if hasattr(payment.status, "value") else str(payment.status),
        "external_id": payment.external_id,
        "raw_response": payment.raw_response,
        "created_at": fmt(payment.created_at),
    }


@app.get("/payments")
async def list_payments(session: AsyncSession = Depends(get_session)) -> list[dict]:
    result = await session.scalars(select(Payment))
    pays: Sequence[Payment] = result.all()
    return [
        {
            "id": p.id,
            "user_id": p.user_id,
            "provider": p.provider,
            "amount_cents": p.amount_cents,
            "currency": p.currency,
            "status": p.status,
            "created_at": p.created_at,
        }
        for p in pays
    ]


@app.get("/admin/payments")
async def admin_list_payments(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    provider: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    admin_user: dict | None = Depends(_require_admin_or_web),
) -> dict:
    """Получить список платежей для админки"""
    from sqlalchemy import or_
    
    stmt = select(Payment).options(selectinload(Payment.user))
    
    # Фильтры
    if status:
        try:
            status_enum = PaymentStatus[status]
            stmt = stmt.where(Payment.status == status_enum)
        except (KeyError, ValueError):
            pass
    
    if provider:
        stmt = stmt.where(Payment.provider == provider)
    
    # Подсчет общего количества
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = await session.scalar(count_stmt)
    
    # Получаем платежи с пагинацией
    stmt = stmt.order_by(Payment.created_at.desc()).limit(limit).offset(offset)
    payments_result = await session.scalars(stmt)
    payments = payments_result.all()
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    payments_data = []
    for p in payments:
        user_tg_id = p.user.tg_id if p.user else None
        payments_data.append({
            "id": p.id,
            "user_tg_id": user_tg_id,
            "provider": p.provider,
            "amount": p.amount_cents / 100,  # Конвертируем копейки в рубли
            "amount_cents": p.amount_cents,
            "currency": p.currency,
            "status": p.status.value if hasattr(p.status, "value") else str(p.status),
            "created_at": fmt(p.created_at),
        })
    
    return {
        "payments": payments_data,
        "total": total or 0,
        "limit": limit,
        "offset": offset,
    }


@app.post("/payments/create")
async def create_payment(
    payload: PaymentCreateIn,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Создание платежа для пополнения баланса"""
    from core.config import get_settings
    settings = get_settings()
    
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    # Создаем запись о платеже
    payment = Payment(
        user_id=user.id,
        provider=payload.provider,
        amount_cents=payload.amount_cents,
        currency=payload.currency,
        status=PaymentStatus.pending,
    )
    session.add(payment)
    await session.commit()
    await session.refresh(payment)
    
    # Логируем создание платежа
    amount_rub = payload.amount_cents / 100
    session.add(
        AuditLog(
            action=AuditLogAction.payment_created,
            user_tg_id=user.tg_id,
            admin_tg_id=None,
            details=f"Создан платеж #{payment.id} через {payload.provider}. Сумма: {amount_rub:.2f} RUB ({payload.currency}). Статус: {payment.status.value}",
        )
    )
    await session.commit()
    
    result = {
        "payment_id": payment.id,
        "provider": payment.provider,
        "amount_cents": payment.amount_cents,
        "currency": payment.currency,
        "status": payment.status.value,
    }
    
    # Если это CryptoBot, создаем инвойс
    if payload.provider == "cryptobot" and settings.cryptobot_token:
        try:
            from core.cryptobot import CryptoBotAPI
            cryptobot = CryptoBotAPI(settings.cryptobot_token)
            
            # Конвертируем рубли в USD для CryptoBot
            # payload.amount_cents уже в рублях (копейках)
            amount_rub = payload.amount_cents / 100
            
            # Получаем курс USD/RUB для конвертации
            from core.currency import get_usd_to_rub_rate
            usd_rate = await get_usd_to_rub_rate()
            amount_usd = amount_rub / usd_rate
            
            # CryptoBot требует минимум 0.01 USD для инвойса
            MIN_INVOICE_AMOUNT_USD = 0.01
            min_rub = MIN_INVOICE_AMOUNT_USD * usd_rate
            if amount_usd < MIN_INVOICE_AMOUNT_USD:
                raise HTTPException(
                    status_code=400,
                    detail=f"Минимальная сумма пополнения: {min_rub:.0f} RUB (эквивалент {MIN_INVOICE_AMOUNT_USD} USD)"
                )
            
            # Получаем курс криптовалюты к USD через CryptoBot API
            try:
                exchange_rates = await cryptobot.get_exchange_rates()
                if exchange_rates.get("ok") and exchange_rates.get("result"):
                    rates = exchange_rates["result"]
                    # Ищем курс для выбранной валюты
                    currency_rate = None
                    for rate in rates:
                        if isinstance(rate, dict):
                            source = rate.get("source")
                            target = rate.get("target")
                            rate_value = rate.get("rate")
                            
                            # Проверяем типы перед сравнением
                            if (isinstance(source, str) and isinstance(target, str) and 
                                source == payload.currency and target == "USD"):
                                # Преобразуем rate в число, если это строка
                                try:
                                    currency_rate = float(rate_value) if rate_value else None
                                except (ValueError, TypeError):
                                    currency_rate = None
                                break
                    
                    if currency_rate and isinstance(currency_rate, (int, float)) and currency_rate > 0:
                        # Конвертируем: USD -> криптовалюта
                        crypto_amount = amount_usd / currency_rate
                    else:
                        # Fallback: упрощенная конвертация
                        crypto_amount = amount_usd
                else:
                    # Fallback: упрощенная конвертация
                    crypto_amount = amount_usd
            except Exception as e:
                import logging
                logging.warning(f"Failed to get exchange rates, using fallback: {e}")
                # Fallback: для USDT используем курс 1:1, для других валют нужен курс
                # Но лучше использовать упрощенный курс, чем неправильную сумму
                if payload.currency == "USDT":
                    # USDT привязан к USD, поэтому 1 USD ≈ 1 USDT
                    crypto_amount = amount_usd
                else:
                    # Для других валют нужен курс, используем консервативный fallback
                    logging.error(f"Cannot convert to {payload.currency} without exchange rate")
                    crypto_amount = amount_usd  # Временное решение
            
            invoice = await cryptobot.create_invoice(
                amount=crypto_amount,
                currency=payload.currency,
                description=f"Пополнение баланса fioreVPN на {amount_rub:.2f} RUB",
                paid_btn_name="callback",
                # paid_btn_url не передаем, если не нужен
                payload=f"payment_{payment.id}",
            )
            
            if invoice.get("ok") and invoice.get("result"):
                invoice_data = invoice["result"]
                invoice_id = invoice_data.get("invoice_id")
                payment.external_id = str(invoice_id) if invoice_id else None
                await session.commit()
                
                result["invoice_url"] = invoice_data.get("pay_url")
                result["invoice_id"] = invoice_id
                
                import logging
                logging.info("=" * 80)
                logging.info(f"=== PAYMENT CREATED ===")
                logging.info(f"Payment ID: #{payment.id}")
                logging.info(f"Invoice ID (external_id): {invoice_id} (type: {type(invoice_id).__name__})")
                logging.info(f"Invoice URL: {result.get('invoice_url')}")
                logging.info(f"User ID: {user.id}, User TG ID: {user.tg_id}")
                logging.info(f"Amount: {payment.amount_cents} cents ({payment.amount_cents / 100:.2f} RUB)")
                logging.info(f"Provider: {payment.provider}, Currency: {payment.currency}")
                logging.info("=" * 80)
        except httpx.ReadTimeout as e:
            import logging
            logging.error(f"CryptoBot API timeout when creating invoice: {e}")
            raise HTTPException(
                status_code=504,
                detail="Превышено время ожидания ответа от CryptoBot API. Пожалуйста, попробуйте позже."
            )
        except httpx.ConnectTimeout as e:
            import logging
            logging.error(f"CryptoBot API connection timeout: {e}")
            raise HTTPException(
                status_code=504,
                detail="Не удалось подключиться к CryptoBot API. Проверьте интернет-соединение."
            )
        except Exception as e:
            import logging
            logging.error(f"Error creating CryptoBot invoice: {e}", exc_info=True)
            # Продолжаем без инвойса, платеж все равно создан
    
    return result


@app.post("/payments/webhook")
async def payment_webhook(
    payload: PaymentWebhookIn,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Webhook для обработки уведомлений от платежных систем"""
    import json
    
    # Находим платеж по external_id или payment_id
    if payload.payment_id:
        payment = await session.scalar(select(Payment).where(Payment.id == payload.payment_id))
    else:
        payment = await session.scalar(select(Payment).where(Payment.external_id == payload.external_id))
    
    if not payment:
        raise HTTPException(status_code=404, detail="payment_not_found")
    
    # Обновляем статус платежа
    old_status = payment.status
    if payload.status == "succeeded":
        payment.status = PaymentStatus.succeeded
    elif payload.status == "failed":
        payment.status = PaymentStatus.failed
    else:
        payment.status = PaymentStatus.pending
    
    # Сохраняем external_id если его еще нет
    if not payment.external_id and payload.external_id:
        payment.external_id = payload.external_id
    
    # Сохраняем raw_response
    if payload.raw_data:
        payment.raw_response = json.dumps(payload.raw_data)
    
    await session.commit()
    
    # Логируем изменение статуса платежа
    if old_status != payment.status:
        user = await session.scalar(select(User).where(User.id == payment.user_id))
        amount_rub = payment.amount_cents / 100
        session.add(
            AuditLog(
                action=AuditLogAction.payment_status_changed,
                user_tg_id=user.tg_id if user else None,
                admin_tg_id=None,
                details=f"Статус платежа #{payment.id} изменен: {old_status.value} -> {payment.status.value}. Провайдер: {payment.provider}, сумма: {amount_rub:.2f} RUB ({payment.currency})",
            )
        )
        await session.commit()
    
    # Логируем получение webhook
    user = await session.scalar(select(User).where(User.id == payment.user_id))
    session.add(
        AuditLog(
            action=AuditLogAction.payment_webhook_received,
            user_tg_id=user.tg_id if user else None,
            admin_tg_id=None,
            details=f"Получен webhook для платежа #{payment.id}. Новый статус: {payment.status.value}, провайдер: {payment.provider}",
        )
    )
    await session.commit()
    
    # Если платеж успешен, начисляем баланс
    if payment.status == PaymentStatus.succeeded and old_status != PaymentStatus.succeeded:
        user = await session.scalar(select(User).where(User.id == payment.user_id))
        if user:
            old_balance = user.balance
            user.balance += payment.amount_cents
            
            # Создаем транзакцию баланса
            session.add(
                BalanceTransaction(
                    user_id=user.id,
                    admin_tg_id=None,  # Автоматическое пополнение
                    amount=payment.amount_cents,
                    reason=f"Пополнение баланса через {payment.provider}",
                )
            )
            
            # Логируем
            session.add(
                AuditLog(
                    action=AuditLogAction.payment_processed,
                    user_tg_id=user.tg_id,
                    admin_tg_id=None,
                    details=f"Платеж #{payment.id} обработан. Баланс: {old_balance} -> {user.balance} центов",
                )
            )
            
            await session.commit()
    
    return {"success": True, "payment_id": payment.id, "status": payment.status.value}


@app.get("/payments/cryptobot/info")
async def cryptobot_info() -> dict:
    """Получение информации о приложении CryptoBot"""
    from core.config import get_settings
    settings = get_settings()
    
    if not settings.cryptobot_token:
        raise HTTPException(status_code=400, detail="CRYPTOBOT_TOKEN не настроен")
    
    try:
        from core.cryptobot import CryptoBotAPI
        cryptobot = CryptoBotAPI(settings.cryptobot_token)
        
        result = await cryptobot.get_me()
        
        if result.get("ok"):
            return {
                "success": True,
                "app_info": result.get("result"),
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Неизвестная ошибка"),
            }
    except Exception as e:
        import logging
        logging.error(f"Error getting CryptoBot info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка получения информации: {str(e)}")


@app.post("/payments/cryptobot/setup")
async def cryptobot_setup_webhook(
    webhook_url: str = Query(..., description="Полный URL для webhook (например: https://yourdomain.com/payments/webhook/cryptobot)"),
) -> dict:
    """Настройка webhook для CryptoBot"""
    from core.config import get_settings
    settings = get_settings()
    
    if not settings.cryptobot_token:
        raise HTTPException(status_code=400, detail="CRYPTOBOT_TOKEN не настроен")
    
    try:
        from core.cryptobot import CryptoBotAPI
        cryptobot = CryptoBotAPI(settings.cryptobot_token)
        
        result = await cryptobot.set_webhook(webhook_url)
        
        if result.get("ok"):
            return {
                "success": True,
                "message": "Webhook успешно настроен",
                "webhook_url": webhook_url,
                "result": result.get("result"),
            }
        else:
            # Если метод не поддерживается, сообщаем что нужно настраивать через бота
            error_code = result.get("error_code")
            if error_code == 405:
                return {
                    "success": False,
                    "error": "CryptoBot API не поддерживает настройку webhook через API. Пожалуйста, настройте webhook через интерфейс бота @CryptoBot.",
                    "webhook_url": webhook_url,
                    "instruction": f"Откройте @CryptoBot в Telegram и введите URL: {webhook_url}",
                }
            return {
                "success": False,
                "error": result.get("error", "Неизвестная ошибка"),
            }
    except Exception as e:
        import logging
        logging.error(f"Error setting CryptoBot webhook: {e}", exc_info=True)
        # Не выбрасываем исключение, а возвращаем информацию
        return {
            "success": False,
            "error": f"Ошибка настройки webhook: {str(e)}",
            "webhook_url": webhook_url,
            "instruction": "Пожалуйста, настройте webhook через интерфейс бота @CryptoBot",
        }


@app.post("/payments/webhook/cryptobot")
async def cryptobot_webhook(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Webhook от CryptoBot для обработки платежей"""
    import json
    import logging
    import sys
    
    # Логируем сразу при получении запроса (принудительно в stdout)
    print("=" * 80, file=sys.stderr, flush=True)
    print("=== CRYPTOBOT WEBHOOK RECEIVED ===", file=sys.stderr, flush=True)
    logging.info("=" * 80)
    logging.info("=== CRYPTOBOT WEBHOOK RECEIVED ===")
    
    try:
        # Логируем весь запрос для отладки
        body = await request.body()
        body_str = body.decode('utf-8')
        print(f"Raw body length: {len(body_str)}", file=sys.stderr, flush=True)
        print(f"Raw body (first 500): {body_str[:500]}", file=sys.stderr, flush=True)
        logging.info(f"Raw body length: {len(body_str)}")
        logging.info(f"Raw body: {body_str[:500]}")
        
        data = await request.json()
        print(f"CryptoBot webhook data: {json.dumps(data, indent=2)}", file=sys.stderr, flush=True)
        logging.info(f"CryptoBot webhook data: {json.dumps(data, indent=2)}")
        
        # CryptoBot отправляет update с информацией о платеже
        update_type = data.get("update_type")
        print(f"Update type: {update_type}", file=sys.stderr, flush=True)
        print(f"Data keys: {list(data.keys())}", file=sys.stderr, flush=True)
        logging.info(f"Update type: {update_type}")
        logging.info(f"Data keys: {list(data.keys())}")
        
        if update_type == "invoice_paid":
            # Согласно документации CryptoBot API, данные инвойса находятся в поле "payload"
            # Структура: { "update_type": "invoice_paid", "payload": Invoice }
            invoice_data = data.get("payload", {})
            
            # Fallback: если payload пустой, пробуем invoice (для обратной совместимости)
            if not invoice_data or (isinstance(invoice_data, dict) and len(invoice_data) == 0):
                invoice_data = data.get("invoice", {})
                if invoice_data:
                    print("Using data['invoice'] as fallback", file=sys.stderr, flush=True)
            
            # Если invoice пустой, возможно данные находятся на верхнем уровне или в result
            if not invoice_data or (isinstance(invoice_data, dict) and len(invoice_data) == 0):
                print("WARNING: invoice_data is empty, trying alternative locations", file=sys.stderr, flush=True)
                # Пробуем result
                if "result" in data and isinstance(data["result"], dict) and len(data["result"]) > 0:
                    invoice_data = data["result"]
                    print(f"Using data['result'] as invoice_data (keys: {list(invoice_data.keys())})", file=sys.stderr, flush=True)
                # Если все еще пусто, пробуем использовать весь data как invoice_data
                elif "payload" in data or "invoice_id" in data or "status" in data:
                    invoice_data = data
                    print(f"Using top-level data as invoice_data (keys: {list(invoice_data.keys())})", file=sys.stderr, flush=True)
            # Логируем полную структуру данных для отладки
            import sys
            print("=" * 80, file=sys.stderr, flush=True)
            print("=== CRYPTOBOT WEBHOOK: INVOICE PAID ===", file=sys.stderr, flush=True)
            print(f"Full data structure: {json.dumps(data, indent=2)}", file=sys.stderr, flush=True)
            print(f"Invoice data type: {type(invoice_data).__name__}", file=sys.stderr, flush=True)
            print(f"Invoice data keys: {list(invoice_data.keys()) if isinstance(invoice_data, dict) else 'NOT A DICT'}", file=sys.stderr, flush=True)
            logging.info("=" * 80)
            logging.info("=== CRYPTOBOT WEBHOOK: INVOICE PAID ===")
            logging.info(f"Full data structure: {json.dumps(data, indent=2)}")
            
            # Пробуем разные варианты получения invoice_id и payload
            # invoice_id может быть строкой (например, "IVZhvKKyl5Ce") или числом
            invoice_id = None
            payload_str = ""
            
            if isinstance(invoice_data, dict):
                invoice_id = invoice_data.get("invoice_id") or invoice_data.get("id")
                payload_str = invoice_data.get("payload", "") or invoice_data.get("payload_str", "")
            else:
                # Если invoice_data не словарь, возможно данные в другом формате
                print(f"WARNING: invoice_data is not a dict: {type(invoice_data)}", file=sys.stderr, flush=True)
                # Пробуем получить напрямую из data
                invoice_id = data.get("invoice_id") or data.get("id")
                payload_str = data.get("payload", "") or data.get("payload_str", "")
            
            # Если invoice_id не найден, пробуем получить из других полей
            if not invoice_id:
                # Иногда invoice_id может быть в другом формате
                invoice_id = data.get("invoice_id") or data.get("id")
            
            print(f"Invoice ID: {invoice_id} (type: {type(invoice_id).__name__})", file=sys.stderr, flush=True)
            print(f"Payload: '{payload_str}' (type: {type(payload_str).__name__}, length: {len(payload_str) if payload_str else 0})", file=sys.stderr, flush=True)
            if isinstance(invoice_data, dict):
                print(f"Full invoice data: {json.dumps(invoice_data, indent=2)}", file=sys.stderr, flush=True)
            logging.info(f"Invoice ID: {invoice_id} (type: {type(invoice_id).__name__})")
            logging.info(f"Payload: '{payload_str}' (type: {type(payload_str).__name__})")
            if isinstance(invoice_data, dict):
                logging.info(f"Full invoice data: {json.dumps(invoice_data, indent=2)}")
            print("=" * 80, file=sys.stderr, flush=True)
            
            # Извлекаем payment_id из payload
            payment_id = None
            if payload_str:
                print(f"Processing payload: '{payload_str}'", file=sys.stderr, flush=True)
                if payload_str.startswith("payment_"):
                    try:
                        payment_id = int(payload_str.split("_")[1])
                        print(f"✅✅✅ Extracted payment_id from payload: {payment_id}", file=sys.stderr, flush=True)
                        logging.info(f"Extracted payment_id from payload: {payment_id}")
                    except (ValueError, IndexError) as e:
                        print(f"ERROR: Failed to extract payment_id from payload '{payload_str}': {e}", file=sys.stderr, flush=True)
                        logging.error(f"Failed to extract payment_id from payload '{payload_str}': {e}")
                else:
                    print(f"WARNING: Payload does not start with 'payment_': '{payload_str}'", file=sys.stderr, flush=True)
            else:
                print(f"WARNING: payload_str is empty or None", file=sys.stderr, flush=True)
            
            # Если payload пустой, пытаемся найти платеж по external_id (invoice_id)
            if not payment_id and invoice_id:
                logging.info(f"Payload empty, searching payment by external_id (invoice_id): {invoice_id}")
                
                # Пробуем найти по точному совпадению
                payment_by_external = await session.scalar(
                    select(Payment).where(Payment.external_id == str(invoice_id))
                )
                
                # Если не нашли, пробуем найти по числовому значению
                if not payment_by_external:
                    try:
                        invoice_id_int = int(invoice_id)
                        payment_by_external = await session.scalar(
                            select(Payment).where(Payment.external_id.cast(Integer) == invoice_id_int)
                        )
                    except (ValueError, TypeError):
                        pass
                
                # Если все еще не нашли, пробуем найти все платежи CryptoBot и сравнить
                if not payment_by_external:
                    logging.info(f"🔍 Searching in all recent cryptobot payments...")
                    all_recent = await session.scalars(
                        select(Payment)
                        .where(Payment.provider == "cryptobot")
                        .order_by(Payment.created_at.desc())
                        .limit(20)
                    )
                    for p in all_recent:
                        logging.info(f"  Checking Payment #{p.id}: external_id='{p.external_id}' (type: {type(p.external_id)}), invoice_id='{invoice_id}' (type: {type(invoice_id)})")
                        if p.external_id and str(p.external_id) == str(invoice_id):
                            payment_by_external = p
                            logging.info(f"  ✅ Found match! Payment #{p.id}")
                            break
                
                if payment_by_external:
                    payment_id = payment_by_external.id
                    logging.info(f"✅ Found payment by external_id: payment_id={payment_id}, external_id={payment_by_external.external_id}")
                else:
                    logging.warning(f"❌ Payment not found by external_id={invoice_id}")
                    # Логируем все последние платежи для отладки
                    recent_payments = await session.scalars(
                        select(Payment)
                        .where(Payment.provider == "cryptobot")
                        .order_by(Payment.created_at.desc())
                        .limit(10)
                    )
                    logging.info(f"📋 Recent cryptobot payments (last 10):")
                    for p in recent_payments:
                        logging.info(f"  Payment #{p.id}: external_id='{p.external_id}' (type: {type(p.external_id)}), status={p.status}, created_at={p.created_at}, user_id={p.user_id}")
            
            if payment_id:
                payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
                if payment:
                    logging.info(f"Found payment #{payment_id}, current status: {payment.status}")
                    # Обновляем статус платежа
                    old_status = payment.status
                    payment.status = PaymentStatus.succeeded
                    payment.external_id = str(invoice_id)
                    payment.raw_response = json.dumps(data)
                    await session.commit()
                    logging.info(f"Payment #{payment_id} status updated to succeeded")
                    
                    # Начисляем баланс если еще не начислен
                    if old_status != PaymentStatus.succeeded:
                        logging.info(f"Processing balance credit for payment #{payment_id}")
                        user = await session.scalar(select(User).where(User.id == payment.user_id))
                        if user:
                            old_balance = user.balance
                            user.balance += payment.amount_cents
                            
                            logging.info(f"User {user.tg_id}: balance {old_balance} -> {user.balance} cents")
                            
                            session.add(
                                BalanceTransaction(
                                    user_id=user.id,
                                    admin_tg_id=None,
                                    amount=payment.amount_cents,
                                    reason=f"Пополнение баланса через {payment.provider}",
                                )
                            )
                            
                            session.add(
                                AuditLog(
                                    action=AuditLogAction.payment_processed,
                                    user_tg_id=user.tg_id,
                                    admin_tg_id=None,
                                    details=f"Платеж #{payment.id} обработан через CryptoBot. Баланс: {old_balance} -> {user.balance} центов",
                                )
                            )
                            
                            await session.commit()
                            logging.info(f"Balance credited successfully for user {user.tg_id}")
                            
                            # Отправляем уведомление пользователю через бота (если включено)
                            notify_on_payment = await session.scalar(select(SystemSetting).where(SystemSetting.key == "notify_on_payment"))
                            if not notify_on_payment or notify_on_payment.value != "false":
                                try:
                                    # amount_cents и balance уже в рублях (копейках)
                                    amount_rub = payment.amount_cents / 100
                                    new_balance_rub = user.balance / 100
                                    
                                    notification_text = (
                                        f"✅ <b>Платеж успешно обработан!</b>\n\n"
                                        f"💰 Пополнено: <b>{amount_rub:.2f} RUB</b>\n"
                                        f"💵 Текущий баланс: <b>{new_balance_rub:.2f} RUB</b>"
                                    )
                                    
                                    # Используем BOT_TOKEN из окружения
                                    bot_token = os.getenv("BOT_TOKEN")
                                    if bot_token:
                                        import asyncio
                                        asyncio.create_task(_send_user_notification(
                                            user.tg_id,
                                            notification_text,
                                            bot_token
                                        ))
                                        logging.info(f"Notification task created for user {user.tg_id}")
                                    else:
                                        logging.warning(f"BOT_TOKEN not found, cannot send notification to user {user.tg_id}")
                                except Exception as e:
                                    logging.error(f"Failed to send notification to user {user.tg_id}: {e}", exc_info=True)
                        else:
                            logging.error(f"User not found for payment #{payment_id}, user_id={payment.user_id}")
                else:
                    logging.error(f"Payment #{payment_id} not found")
            else:
                import sys
                print(f"WARNING: Could not extract payment_id from payload: '{payload_str}'", file=sys.stderr, flush=True)
                print(f"INFO: invoice_id from webhook: {invoice_id}", file=sys.stderr, flush=True)
                logging.warning(f"Could not extract payment_id from payload: {payload_str}")
                logging.info(f"invoice_id from webhook: {invoice_id}")
                # Пробуем найти платеж по invoice_id напрямую, если payload пустой
                if invoice_id:
                    print(f"🔍 Trying to find payment by invoice_id: {invoice_id}", file=sys.stderr, flush=True)
                    logging.info(f"🔍 Trying to find payment by invoice_id directly: {invoice_id} (type: {type(invoice_id)})")
                    # Пробуем разные варианты поиска
                    payment_by_invoice = await session.scalar(
                        select(Payment).where(
                            Payment.external_id == str(invoice_id)
                        ).order_by(Payment.created_at.desc())
                    )
                    
                    # Если не нашли, пробуем найти среди всех платежей CryptoBot
                    if not payment_by_invoice:
                        logging.info(f"🔍 Searching in all cryptobot payments...")
                        all_payments = await session.scalars(
                            select(Payment)
                            .where(Payment.provider == "cryptobot")
                            .order_by(Payment.created_at.desc())
                            .limit(50)
                        )
                        for p in all_payments:
                            p_external_str = str(p.external_id) if p.external_id else "None"
                            invoice_str = str(invoice_id) if invoice_id else "None"
                            if p.external_id and str(p.external_id) == str(invoice_id):
                                payment_by_invoice = p
                                logging.info(f"✅✅✅ FOUND PAYMENT #{p.id} by invoice_id in all payments: external_id='{p_external_str}' == invoice_id='{invoice_str}'")
                                break
                            else:
                                logging.info(f"  Payment #{p.id}: external_id='{p_external_str}' != invoice_id='{invoice_str}'")
                    if payment_by_invoice:
                        logging.info(f"Found payment #{payment_by_invoice.id} by invoice_id, updating status")
                        old_status = payment_by_invoice.status
                        payment_by_invoice.status = PaymentStatus.succeeded
                        payment_by_invoice.raw_response = json.dumps(data)
                        await session.commit()
                        logging.info(f"Payment #{payment_by_invoice.id} status updated from {old_status} to succeeded")
                        
                        # Начисляем баланс если еще не начислен
                        if old_status != PaymentStatus.succeeded:
                            user = await session.scalar(select(User).where(User.id == payment_by_invoice.user_id))
                            if user:
                                old_balance = user.balance
                                user.balance += payment_by_invoice.amount_cents
                                session.add(
                                    BalanceTransaction(
                                        user_id=user.id,
                                        admin_tg_id=None,
                                        amount=payment_by_invoice.amount_cents,
                                        reason=f"Пополнение баланса через {payment_by_invoice.provider}",
                                    )
                                )
                                session.add(
                                    AuditLog(
                                        action=AuditLogAction.payment_processed,
                                        user_tg_id=user.tg_id,
                                        admin_tg_id=None,
                                        details=f"Платеж #{payment_by_invoice.id} обработан через CryptoBot. Баланс: {old_balance} -> {user.balance} центов",
                                    )
                                )
                                await session.commit()
                                
                                # Отправляем уведомление
                                try:
                                    amount_rub = payment_by_invoice.amount_cents / 100
                                    new_balance_rub = user.balance / 100
                                    notification_text = (
                                        f"✅ <b>Платеж успешно обработан!</b>\n\n"
                                        f"💰 Пополнено: <b>{amount_rub:.2f} RUB</b>\n"
                                        f"💵 Текущий баланс: <b>{new_balance_rub:.2f} RUB</b>"
                                    )
                                    bot_token = os.getenv("BOT_TOKEN")
                                    if bot_token:
                                        import asyncio
                                        asyncio.create_task(_send_user_notification(
                                            user.tg_id,
                                            notification_text,
                                            bot_token
                                        ))
                                except Exception as e:
                                    logging.error(f"Failed to send notification: {e}")
                    else:
                        logging.warning(f"Payment not found by invoice_id: {invoice_id}")
        else:
            logging.info(f"Update type '{update_type}' is not invoice_paid, ignoring")
        
        return {"ok": True}
    except Exception as e:
        import logging
        logging.error(f"Error processing CryptoBot webhook: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}


@app.post("/payments/cryptobot/check/{payment_id}")
async def cryptobot_check_payment(
    payment_id: int,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Ручная проверка статуса платежа через CryptoBot API"""
    from core.config import get_settings
    settings = get_settings()
    
    if not settings.cryptobot_token:
        raise HTTPException(status_code=400, detail="CRYPTOBOT_TOKEN не настроен")
    
    try:
        payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
        if not payment:
            raise HTTPException(status_code=404, detail="Payment not found")
        
        if not payment.external_id:
            return {
                "success": False,
                "error": "Payment has no external_id (invoice_id)",
                "payment_id": payment_id,
            }
        
        from core.cryptobot import CryptoBotAPI
        cryptobot = CryptoBotAPI(settings.cryptobot_token)
        
        invoice_id = int(payment.external_id)
        invoice_status = await cryptobot.get_invoice_status(invoice_id)
        
        if invoice_status.get("ok") and invoice_status.get("result"):
            invoices = invoice_status["result"]
            if invoices and len(invoices) > 0:
                invoice = invoices[0]
                status = invoice.get("status")
                
                # Если инвойс оплачен, обрабатываем платеж
                if status == "paid" and payment.status != PaymentStatus.succeeded:
                    import json
                    # Обновляем статус платежа
                    payment.status = PaymentStatus.succeeded
                    payment.raw_response = json.dumps(invoice)
                    await session.commit()
                    
                    # Начисляем баланс
                    user = await session.scalar(select(User).where(User.id == payment.user_id))
                    if user:
                        old_balance = user.balance
                        user.balance += payment.amount_cents
                        
                        session.add(
                            BalanceTransaction(
                                user_id=user.id,
                                admin_tg_id=None,
                                amount=payment.amount_cents,
                                reason=f"Пополнение баланса через {payment.provider}",
                            )
                        )
                        
                        session.add(
                            AuditLog(
                                action=AuditLogAction.payment_processed,
                                user_tg_id=user.tg_id,
                                admin_tg_id=None,
                                details=f"Платеж #{payment.id} обработан через CryptoBot (ручная проверка). Баланс: {old_balance} -> {user.balance} центов",
                            )
                        )
                        
                        await session.commit()
                        
                        return {
                            "success": True,
                            "message": "Payment processed successfully",
                            "payment_id": payment_id,
                            "status": "succeeded",
                            "balance_credited": True,
                        }
                
                return {
                    "success": True,
                    "payment_id": payment_id,
                    "invoice_status": status,
                    "payment_status": payment.status.value,
                }
        
        return {
            "success": False,
            "error": "Failed to get invoice status",
            "response": invoice_status,
        }
    except Exception as e:
        import logging
        logging.error(f"Error checking CryptoBot payment: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка проверки платежа: {str(e)}")


@app.get("/subscriptions/status/by_tg/{tg_id}")
async def subscription_status_by_tg(tg_id: int, session: AsyncSession = Depends(get_session)) -> SubscriptionStatusOut:
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return SubscriptionStatusOut(has_active=False)

    sub = await session.scalar(
        select(Subscription)
        .where(Subscription.user_id == user.id)
        .where(Subscription.status == SubscriptionStatus.active)
        .order_by(Subscription.ends_at.desc().nullslast())
    )

    if not sub:
        return SubscriptionStatusOut(has_active=False)

    return SubscriptionStatusOut(has_active=True, plan_name=sub.plan_name, ends_at=sub.ends_at)


async def _get_subscription_plans_from_db(session: AsyncSession) -> dict[int, dict]:
    """Получить тарифы из БД"""
    plans = await session.scalars(
        select(SubscriptionPlan)
        .where(SubscriptionPlan.is_active == True)
        .order_by(SubscriptionPlan.display_order, SubscriptionPlan.months)
    )
    return {
        plan.months: {
            "name": plan.name,
            "price_cents": plan.price_cents,
            "description": plan.description or "",
        }
        for plan in plans.all()
    }


async def _ensure_default_plans(session: AsyncSession) -> None:
    """Создать тарифы по умолчанию, если их нет"""
    existing = await session.scalar(select(func.count()).select_from(SubscriptionPlan))
    if existing and existing > 0:
        return
    
    default_plans = [
        SubscriptionPlan(months=1, name="1 месяц", price_cents=10000, description="Месячная подписка", display_order=1),
        SubscriptionPlan(months=3, name="3 месяца", price_cents=27000, description="Трехмесячная подписка со скидкой 10%", display_order=2),
        SubscriptionPlan(months=6, name="6 месяцев", price_cents=48000, description="Полугодовая подписка со скидкой 20%", display_order=3),
        SubscriptionPlan(months=12, name="12 месяцев", price_cents=84000, description="Годовая подписка со скидкой 30%", display_order=4),
    ]
    for plan in default_plans:
        session.add(plan)
    await session.commit()


@app.get("/subscriptions/plans")
async def get_subscription_plans(session: AsyncSession = Depends(get_session)) -> dict:
    """Получить список доступных тарифов подписки"""
    await _ensure_default_plans(session)
    plans = await session.scalars(
        select(SubscriptionPlan)
        .where(SubscriptionPlan.is_active == True)
        .order_by(SubscriptionPlan.display_order, SubscriptionPlan.months)
    )
    return {
        "plans": [
            {
                "id": plan.id,
                "months": plan.months,
                "name": plan.name,
                "description": plan.description or "",
                "price_cents": plan.price_cents,
                "price_rub": plan.price_cents / 100,
                "is_active": plan.is_active,
                "display_order": plan.display_order,
            }
            for plan in plans.all()
        ]
    }


@app.post("/subscriptions/purchase")
async def purchase_subscription(
    payload: SubscriptionPurchaseIn,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Покупка подписки через баланс"""
    from datetime import datetime, timedelta, timezone
    
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    await _ensure_default_plans(session)
    
    # Получаем план из БД
    plan_db = await session.scalar(
        select(SubscriptionPlan)
        .where(SubscriptionPlan.months == payload.plan_months)
        .where(SubscriptionPlan.is_active == True)
    )
    
    if not plan_db:
        raise HTTPException(status_code=400, detail="invalid_plan")
    
    plan_name = plan_db.name
    price_cents = plan_db.price_cents
    
    # Проверяем, есть ли активный промокод на скидку у пользователя
    promo_discount_cents = 0
    promo_code_used = None
    if payload.promo_code:
        # Проверяем промокод на скидку (процент)
        is_valid, error_msg, discount_cents = await _validate_promo_code(
            payload.promo_code, user.id, price_cents, session, check_percent_usage=True
        )
        if is_valid:
            promo = await session.scalar(select(PromoCode).where(PromoCode.code == payload.promo_code.upper().strip()))
            if promo and promo.discount_percent:
                promo_discount_cents = discount_cents
                # Применяем промокод
                usage = PromoCodeUsage(
                    promo_code_id=promo.id,
                    user_id=user.id,
                    discount_amount_cents=promo_discount_cents,
                )
                session.add(usage)
                promo.used_count += 1
                promo_code_used = promo.code
                # Логируем в админке
                session.add(
                    AuditLog(
                        action=AuditLogAction.admin_action,
                        user_tg_id=user.tg_id,
                        admin_tg_id=None,
                        details=f"Применен промокод {promo.code} (скидка {promo.discount_percent}%) при покупке подписки. Скидка: {promo_discount_cents / 100:.2f} RUB.",
                    )
                )
    
    # Применяем скидку
    final_price_cents = price_cents - promo_discount_cents
    if final_price_cents < 0:
        final_price_cents = 0
    
    # Проверяем баланс
    if user.balance < final_price_cents:
        raise HTTPException(
            status_code=400,
            detail=f"insufficient_balance. Required: {final_price_cents / 100:.2f} RUB, Available: {user.balance / 100:.2f} RUB"
        )
    
    # Проверяем, есть ли активная подписка
    active_sub = await session.scalar(
        select(Subscription)
        .where(Subscription.user_id == user.id)
        .where(Subscription.status == SubscriptionStatus.active)
        .order_by(Subscription.ends_at.desc().nullslast())
    )
    
    # Если есть активная подписка, продлеваем её
    now = datetime.now(timezone.utc)
    if active_sub and active_sub.ends_at and active_sub.ends_at > now:
        # Продлеваем от текущей даты окончания
        starts_at = active_sub.ends_at
        ends_at = starts_at + timedelta(days=payload.plan_months * 30)
    else:
        # Создаем новую подписку
        starts_at = now
        ends_at = now + timedelta(days=payload.plan_months * 30)
    
    # Списываем баланс (с учетом скидки)
    user.balance -= final_price_cents
    
    # Создаем транзакцию баланса
    reason = f"Покупка подписки: {plan_name}"
    if promo_code_used:
        reason += f" (промокод {promo_code_used}, скидка {promo_discount_cents / 100:.2f} RUB)"
    session.add(
        BalanceTransaction(
            user_id=user.id,
            admin_tg_id=None,
            amount=-final_price_cents,  # Отрицательное значение = списание
            reason=reason,
        )
    )
    
    # Создаем или обновляем подписку
    if active_sub and active_sub.status == SubscriptionStatus.active:
        # Продлеваем существующую
        active_sub.ends_at = ends_at
        active_sub.price_cents = final_price_cents  # Сохраняем финальную цену с учетом скидки
        subscription = active_sub
    else:
        # Создаем новую подписку
        subscription = Subscription(
            user_id=user.id,
            plan_name=plan_name,
            price_cents=final_price_cents,  # Сохраняем финальную цену с учетом скидки
            currency="RUB",
            status=SubscriptionStatus.active,
            starts_at=starts_at,
            ends_at=ends_at,
        )
        session.add(subscription)
    
    # Логируем покупку
    log_details = f"Покупка подписки: {plan_name}. Цена: {final_price_cents / 100:.2f} RUB"
    if promo_code_used:
        log_details += f" (промокод {promo_code_used}, скидка {promo_discount_cents / 100:.2f} RUB)"
    log_details += f". Действует до: {ends_at.strftime('%d.%m.%Y %H:%M')} (UTC)"
    session.add(
        AuditLog(
            action=AuditLogAction.subscription_created,
            user_tg_id=user.tg_id,
            admin_tg_id=None,
            details=log_details,
        )
    )
    
    await session.commit()
    
    # Обновляем статус подписки у пользователя после коммита
    await _update_user_subscription_status(user.id, session)
    await session.commit()
    await session.refresh(user)  # Обновляем данные пользователя в сессии
    
    # Не генерируем VPN конфиги автоматически - пользователь выберет сервер и сгенерирует ключ сам
    
    # Отправляем уведомление пользователю (если включено)
    notify_on_subscription = await session.scalar(select(SystemSetting).where(SystemSetting.key == "notify_on_subscription"))
    if not notify_on_subscription or notify_on_subscription.value != "false":
        try:
            ends_at_moscow = ends_at.astimezone(ZoneInfo("Europe/Moscow"))
            ends_str = ends_at_moscow.strftime("%d.%m.%Y %H:%M")
            notification_text = (
                f"✅ <b>Подписка успешно активирована!</b>\n\n"
                f"📦 Тариф: <b>{plan_name}</b>\n"
                f"💰 Стоимость: {final_price_cents / 100:.2f} RUB"
            )
            if promo_code_used:
                notification_text += f"\n🎟️ Промокод: {promo_code_used} (скидка {promo_discount_cents / 100:.2f} RUB)"
            notification_text += (
                f"\n📅 Действует до: {ends_str} МСК\n"
                f"💵 Остаток баланса: {user.balance / 100:.2f} RUB"
            )
            asyncio.create_task(_send_user_notification(user.tg_id, notification_text))
        except Exception:
            pass  # Игнорируем ошибки отправки уведомлений
    
    return {
        "subscription_id": subscription.id,
        "plan_name": plan_name,
        "price_cents": final_price_cents,
        "price_rub": final_price_cents / 100,
        "original_price_cents": price_cents,
        "original_price_rub": price_cents / 100,
        "discount_cents": promo_discount_cents,
        "promo_code": promo_code_used,
        "starts_at": starts_at.isoformat(),
        "ends_at": ends_at.isoformat(),
        "balance_remaining": user.balance / 100,
    }


@app.post("/subscriptions/trial")
async def activate_trial(
    payload: SubscriptionTrialIn,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Активация бесплатного пробного периода на 7 дней (единоразово)"""
    from datetime import datetime, timedelta, timezone
    
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    # Проверяем, использован ли уже пробный период
    if user.trial_used:
        raise HTTPException(status_code=400, detail="trial_already_used")
    
    # Проверяем, нет ли активной подписки
    active_sub = await session.scalar(
        select(Subscription)
        .where(Subscription.user_id == user.id)
        .where(Subscription.status == SubscriptionStatus.active)
    )
    
    if active_sub:
        raise HTTPException(status_code=400, detail="active_subscription_exists")
    
    # Активируем пробный период
    now = datetime.now(timezone.utc)
    starts_at = now
    ends_at = now + timedelta(days=7)
    
    subscription = Subscription(
        user_id=user.id,
        plan_name="Пробный период (7 дней)",
        price_cents=0,
        currency="RUB",
        status=SubscriptionStatus.active,
        starts_at=starts_at,
        ends_at=ends_at,
    )
    session.add(subscription)
    
    # Отмечаем, что пробный период использован
    user.trial_used = True
    
    # Логируем активацию пробного периода
    session.add(
        AuditLog(
            action=AuditLogAction.subscription_activated,
            user_tg_id=user.tg_id,
            admin_tg_id=None,
            details=f"Активирован пробный период на 7 дней. Действует до: {ends_at.strftime('%d.%m.%Y %H:%M')} (UTC)",
        )
    )
    
    await session.commit()
    
    # Обновляем статус подписки у пользователя после коммита
    await _update_user_subscription_status(user.id, session)
    await session.commit()
    await session.refresh(user)  # Обновляем данные пользователя в сессии
    
    # Отправляем уведомление пользователю (если включено)
    notify_on_subscription = await session.scalar(select(SystemSetting).where(SystemSetting.key == "notify_on_subscription"))
    if not notify_on_subscription or notify_on_subscription.value != "false":
        try:
            ends_at_moscow = ends_at.astimezone(ZoneInfo("Europe/Moscow"))
            ends_str = ends_at_moscow.strftime("%d.%m.%Y %H:%M")
            notification_text = (
                f"🎁 <b>Пробный период активирован!</b>\n\n"
                f"📦 Тариф: <b>Пробный период (7 дней)</b>\n"
                f"📅 Действует до: {ends_str} МСК\n\n"
                f"После окончания пробного периода вы сможете приобрести подписку."
            )
            asyncio.create_task(_send_user_notification(user.tg_id, notification_text))
        except Exception:
            pass  # Игнорируем ошибки отправки уведомлений
    
    return {
        "subscription_id": subscription.id,
        "plan_name": "Пробный период (7 дней)",
        "starts_at": starts_at.isoformat(),
        "ends_at": ends_at.isoformat(),
    }


@app.get("/users/referral/by_tg/{tg_id}")
async def referral_info_by_tg(tg_id: int, session: AsyncSession = Depends(get_session)) -> ReferralInfoOut:
    stmt = select(User).options(selectinload(User.referred_by)).where(User.tg_id == tg_id)
    user = await session.scalar(stmt)
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    if not user.referral_code:
        user.referral_code = await _ensure_ref_code_unique(session)
        await session.commit()
        await session.refresh(user)
        # Перезагружаем с relationship после refresh
        user = await session.scalar(stmt)

    referrals_count = await session.scalar(select(func.count()).select_from(User).where(User.referred_by_user_id == user.id))
    referred_by_tg_id = user.referred_by.tg_id if user.referred_by else None
    
    # Подсчитываем общую сумму реферальных наград (только для пригласившего)
    total_rewards_result = await session.scalar(
        select(func.sum(ReferralReward.amount_cents))
        .select_from(ReferralReward)
        .where(
            ReferralReward.referrer_user_id == user.id,
            ReferralReward.is_for_referrer == True
        )
    )
    total_rewards_cents = int(total_rewards_result or 0)
    
    return ReferralInfoOut(
        tg_id=user.tg_id,
        referral_code=user.referral_code,
        referred_by_tg_id=referred_by_tg_id,
        referrals_count=int(referrals_count or 0),
        total_rewards_cents=total_rewards_cents,
    )


# --- Admin actions (optionally protected by ADMIN_TOKEN) ---
async def _send_user_notification(tg_id: int, text: str, bot_token: str | None = None) -> None:
    """Отправляет уведомление пользователю в боте"""
    if not bot_token:
        bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        return
    
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": tg_id, "text": text, "parse_mode": "HTML"},
            )
    except Exception:
        pass  # Игнорируем ошибки отправки уведомлений


@app.post("/admin/users/credit")
async def admin_credit_user(
    payload: AdminCreditIn,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> dict[str, int]:
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    old_balance = user.balance
    # payload.amount уже в рублях, конвертируем в копейки
    amount_cents = int(float(payload.amount) * 100)
    user.balance += amount_cents
    amount_rub = amount_cents / 100
    session.add(
        BalanceTransaction(
            user_id=user.id,
            admin_tg_id=payload.admin_tg_id,
            amount=amount_cents,
            reason=payload.reason,
        )
    )
    session.add(
        AuditLog(
            action=AuditLogAction.balance_credited,
            user_tg_id=payload.tg_id,
            admin_tg_id=payload.admin_tg_id,
            details=f"Баланс изменен: {old_balance} -> {user.balance} копеек (RUB). Причина: {payload.reason or 'не указана'}",
        )
    )
    await session.commit()
    await session.refresh(user)
    
    # Отправляем уведомление пользователю
    if amount_rub > 0:
        notification_text = (
            f"💰 <b>Баланс пополнен</b>\n\n"
            f"Сумма: <b>+{amount_rub:.2f} RUB</b>\n"
            f"Текущий баланс: <b>{user.balance / 100:.2f} RUB</b>"
        )
        if payload.reason:
            notification_text += f"\n\nПричина: {payload.reason}"
    else:
        notification_text = (
            f"💰 <b>Изменение баланса</b>\n\n"
            f"Сумма: <b>{amount_rub:.2f} RUB</b>\n"
            f"Текущий баланс: <b>{user.balance / 100:.2f} RUB</b>"
        )
        if payload.reason:
            notification_text += f"\n\nПричина: {payload.reason}"
    
    asyncio.create_task(_send_user_notification(payload.tg_id, notification_text))
    
    return {"tg_id": user.tg_id, "balance": user.balance, "new_balance_cents": user.balance}


@app.post("/admin/users/set_active")
async def admin_set_active(
    payload: AdminSetActiveIn,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> dict[str, int | bool]:
    user = await session.scalar(select(User).where(User.tg_id == payload.tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    old_status = user.is_active
    user.is_active = bool(payload.is_active)
    action_type = AuditLogAction.user_unblocked if payload.is_active else AuditLogAction.user_blocked
    session.add(
        AuditLog(
            action=action_type,
            user_tg_id=payload.tg_id,
            details=f"Статус изменен: {'заблокирован' if old_status else 'активен'} -> {'активен' if payload.is_active else 'заблокирован'}",
        )
    )
    await session.commit()
    return {"tg_id": user.tg_id, "is_active": user.is_active}


@app.post("/admin/users/block")
async def admin_block_user(
    payload: dict[str, int],
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> dict[str, int | bool]:
    tg_id = payload.get("tg_id")
    if not tg_id:
        raise HTTPException(status_code=400, detail="tg_id required")
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    user.is_active = False
    session.add(
        AuditLog(
            action=AuditLogAction.user_blocked,
            user_tg_id=tg_id,
            details="Пользователь заблокирован",
        )
    )
    await session.commit()
    
    # Отправляем уведомление пользователю
    notification_text = (
        f"❌ <b>Аккаунт заблокирован</b>\n\n"
        f"Ваш аккаунт был заблокирован администратором."
    )
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    return {"tg_id": user.tg_id, "is_active": False}


@app.post("/admin/users/unblock")
async def admin_unblock_user(
    payload: dict[str, int],
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> dict[str, int | bool]:
    tg_id = payload.get("tg_id")
    if not tg_id:
        raise HTTPException(status_code=400, detail="tg_id required")
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    user.is_active = True
    session.add(
        AuditLog(
            action=AuditLogAction.user_unblocked,
            user_tg_id=tg_id,
            details="Пользователь разблокирован",
        )
    )
    await session.commit()
    
    # Отправляем уведомление пользователю
    notification_text = (
        f"✅ <b>Аккаунт разблокирован</b>\n\n"
        f"Ваш аккаунт был разблокирован администратором."
    )
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    return {"tg_id": user.tg_id, "is_active": True}


@app.get("/admin/users/export.csv")
async def admin_export_users_csv(
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> StreamingResponse:
    result = await session.scalars(select(User).order_by(User.created_at.desc()))
    users: Sequence[User] = result.all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "tg_id", "is_active", "balance", "referral_code", "referred_by_tg_id", "created_at"])
    for u in users:
        writer.writerow(
            [
                u.id,
                u.tg_id,
                u.is_active,
                u.balance,
                u.referral_code or "",
                (u.referred_by.tg_id if u.referred_by else ""),
                u.created_at.isoformat(),
            ]
        )
    buf.seek(0)

    headers = {"Content-Disposition": 'attachment; filename="users_export.csv"'}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/admin/users/export.xlsx")
async def admin_export_users_xlsx(
    session: AsyncSession = Depends(get_session),
    _admin: None = Depends(_require_admin),
):
    """Экспорт пользователей в Excel с форматированием"""
    result = await session.scalars(select(User).order_by(User.created_at.desc()))
    users = result.all()
    
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = set(int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()) if admin_ids_str else set()
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Пользователи"
    
    # Заголовки
    headers = ["ID", "Telegram ID", "Имя", "Username", "Баланс (USD)", "Статус", "Роль", "Реферальный код", "Регистрация"]
    ws.append(headers)
    
    # Стили для заголовков
    header_fill = PatternFill(start_color="667eea", end_color="667eea", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_alignment = Alignment(horizontal="center", vertical="center")
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_alignment
        cell.border = border
    
    # Данные
    for user in users:
        balance_rub = user.balance / 100  # Баланс уже в рублях (копейках)
        status = "Активен" if user.is_active else "Заблокирован"
        role = "Админ" if user.tg_id in admin_ids else "Пользователь"
        
        row = [
            user.id,
            user.tg_id,
            f"{user.first_name or ''} {user.last_name or ''}".strip() or "—",
            user.username or "—",
            balance_rub,
            status,
            role,
            user.referral_code or "—",
            user.created_at.strftime("%d.%m.%Y %H:%M") if user.created_at else "—",
        ]
        ws.append(row)
        
        # Стили для строк
        last_row = ws.max_row
        for cell in ws[last_row]:
            cell.border = border
            cell.alignment = Alignment(horizontal="left", vertical="center")
            
            # Цвет для статуса
            if cell.column == 6:  # Статус
                if cell.value == "Активен":
                    cell.fill = PatternFill(start_color="d4edda", end_color="d4edda", fill_type="solid")
                else:
                    cell.fill = PatternFill(start_color="f8d7da", end_color="f8d7da", fill_type="solid")
    
    # Автоподбор ширины колонок
    for column in ws.columns:
        max_length = 0
        column_letter = get_column_letter(column[0].column)
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    # Сохраняем в память
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="users_export.xlsx"'},
    )


@app.get("/admin/web/export/logs.csv")
async def admin_web_export_logs_csv(
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
) -> StreamingResponse:
    """Экспорт логов в CSV"""
    result = await session.scalars(
        select(AuditLog)
        .order_by(AuditLog.created_at.desc())
    )
    logs = result.all()
    
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "action", "user_tg_id", "admin_tg_id", "details", "created_at"])
    for log in logs:
        writer.writerow([
            log.id,
            log.action.value if hasattr(log.action, "value") else str(log.action),
            log.user_tg_id or "",
            log.admin_tg_id or "",
            (log.details or "").replace("\n", " ").replace("\r", " "),
            log.created_at.isoformat(),
        ])
    buf.seek(0)
    
    headers = {"Content-Disposition": 'attachment; filename="logs_export.csv"'}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/admin/web/export/logs.xlsx")
async def admin_web_export_logs_xlsx(
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Экспорт логов в Excel с форматированием"""
    result = await session.scalars(
        select(AuditLog)
        .order_by(AuditLog.created_at.desc())
    )
    logs = result.all()
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Логи"
    
    headers = ["ID", "Действие", "Пользователь", "Админ", "Детали", "Время"]
    ws.append(headers)
    
    # Стили для заголовков
    header_fill = PatternFill(start_color="667eea", end_color="667eea", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_alignment = Alignment(horizontal="center", vertical="center")
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_alignment
        cell.border = border
    
    # Цвета для критических действий
    critical_actions = ['user_blocked', 'user_unblocked', 'balance_credited', 'role_changed']
    warning_actions = ['ticket_created', 'subscription_created']
    
    for log in logs:
        action_str = log.action.value if hasattr(log.action, "value") else str(log.action)
        is_critical = action_str in critical_actions
        is_warning = action_str in warning_actions
        
        row = [
            log.id,
            action_str,
            log.user_tg_id or "—",
            log.admin_tg_id or "—",
            (log.details or "—").replace("\n", " ").replace("\r", " "),
            log.created_at.strftime("%d.%m.%Y %H:%M") if log.created_at else "—",
        ]
        ws.append(row)
        
        last_row = ws.max_row
        for cell in ws[last_row]:
            cell.border = border
            cell.alignment = Alignment(horizontal="left", vertical="center")
            
            # Цвет для критических действий
            if cell.column == 2:  # Действие
                if is_critical:
                    cell.fill = PatternFill(start_color="f8d7da", end_color="f8d7da", fill_type="solid")
                elif is_warning:
                    cell.fill = PatternFill(start_color="fff3cd", end_color="fff3cd", fill_type="solid")
    
    # Автоподбор ширины колонок
    for column in ws.columns:
        max_length = 0
        column_letter = get_column_letter(column[0].column)
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="logs_export.xlsx"'},
    )


@app.get("/admin/web/export/tickets.csv")
async def admin_web_export_tickets_csv(
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
) -> StreamingResponse:
    """Экспорт тикетов в CSV"""
    result = await session.scalars(
        select(Ticket)
        .order_by(Ticket.created_at.desc())
    )
    tickets = result.all()
    
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "user_tg_id", "topic", "status", "created_at", "updated_at", "closed_at"])
    for ticket in tickets:
        writer.writerow([
            ticket.id,
            ticket.user_tg_id,
            (ticket.topic or "").replace("\n", " ").replace("\r", " "),
            ticket.status.value if hasattr(ticket.status, "value") else str(ticket.status),
            ticket.created_at.isoformat() if ticket.created_at else "",
            ticket.updated_at.isoformat() if ticket.updated_at else "",
            ticket.closed_at.isoformat() if ticket.closed_at else "",
        ])
    buf.seek(0)
    
    headers = {"Content-Disposition": 'attachment; filename="tickets_export.csv"'}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/admin/web/export/tickets.xlsx")
async def admin_web_export_tickets_xlsx(
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Экспорт тикетов в Excel с форматированием"""
    result = await session.scalars(
        select(Ticket)
        .order_by(Ticket.created_at.desc())
    )
    tickets = result.all()
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Тикеты"
    
    headers = ["ID", "Пользователь", "Тема", "Статус", "Создан", "Обновлён"]
    ws.append(headers)
    
    # Стили для заголовков
    header_fill = PatternFill(start_color="667eea", end_color="667eea", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_alignment = Alignment(horizontal="center", vertical="center")
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_alignment
        cell.border = border
    
    for ticket in tickets:
        status_str = ticket.status.value if hasattr(ticket.status, "value") else str(ticket.status)
        
        row = [
            ticket.id,
            ticket.user_tg_id,
            ticket.topic or "—",
            status_str,
            ticket.created_at.strftime("%d.%m.%Y %H:%M") if ticket.created_at else "—",
            ticket.updated_at.strftime("%d.%m.%Y %H:%M") if ticket.updated_at else "—",
        ]
        ws.append(row)
        
        last_row = ws.max_row
        for cell in ws[last_row]:
            cell.border = border
            cell.alignment = Alignment(horizontal="left", vertical="center")
            
            # Цвет для статуса
            if cell.column == 4:  # Статус
                if status_str == "closed":
                    cell.fill = PatternFill(start_color="e9ecef", end_color="e9ecef", fill_type="solid")
                elif status_str == "in_progress":
                    cell.fill = PatternFill(start_color="d1ecf1", end_color="d1ecf1", fill_type="solid")
                elif status_str == "new":
                    cell.fill = PatternFill(start_color="fff3cd", end_color="fff3cd", fill_type="solid")
    
    # Автоподбор ширины колонок
    for column in ws.columns:
        max_length = 0
        column_letter = get_column_letter(column[0].column)
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="tickets_export.xlsx"'},
    )


@app.get("/admin/logs")
async def admin_get_logs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    action: AuditLogAction | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> list[AuditLogOut]:
    stmt = select(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit).offset(offset)
    if action:
        stmt = stmt.where(AuditLog.action == action)
    result = await session.scalars(stmt)
    logs: Sequence[AuditLog] = result.all()
    return [
        AuditLogOut(
            id=log.id,
            action=log.action.value,
            user_tg_id=log.user_tg_id,
            admin_tg_id=log.admin_tg_id,
            details=log.details,
            created_at=log.created_at,
        )
        for log in logs
    ]


@app.get("/admin/logs/count")
async def admin_logs_count(
    action: AuditLogAction | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: None = Depends(_require_admin),
) -> dict[str, int]:
    stmt = select(func.count()).select_from(AuditLog)
    if action:
        stmt = stmt.where(AuditLog.action == action)
    total = await session.scalar(stmt)
    return {"total": int(total or 0)}


# --- Web Admin Interface ---
def _require_web_admin(request: Request) -> dict:
    """Проверка веб-авторизации через сессию"""
    session_data = request.session.get("admin_user")
    if not session_data:
        raise HTTPException(status_code=403, detail="not_authenticated")
    return session_data


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    """Страница авторизации через Telegram"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    # Если уже авторизован, редирект на админку
    if request.session.get("admin_user"):
        return RedirectResponse(url="/admin/web", status_code=303)
    
    # Получаем bot_username из переменной окружения
    bot_username_raw = os.getenv("BOT_USERNAME", "").strip()
    bot_username = bot_username_raw
    
    # Если не задан, пытаемся получить через Bot API
    if not bot_username:
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        if bot_token:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r = await client.get(f"https://api.telegram.org/bot{bot_token}/getMe")
                    if r.status_code == 200:
                        data = r.json()
                        if data.get("ok"):
                            bot_username = data.get("result", {}).get("username", "").strip()
            except Exception as e:
                # Логируем ошибку для отладки
                import logging
                logging.warning(f"Failed to get bot username via API: {e}")
                pass
    
    # Нормализуем username
    if bot_username.startswith("@"):
        bot_username = bot_username[1:]
    bot_username = bot_username.strip()

    # Telegram Login Widget принимает username бота (обычно заканчивается на "bot").
    # Если у нас явно задано что-то вроде "fioreVPN" (имя, а не username), лучше показать ошибку.
    if bot_username and not bot_username.lower().endswith("bot"):
        bot_username = ""
    
    return templates.TemplateResponse("login.html", {
        "request": request,
        "bot_username": bot_username or "",
        "bot_username_raw": bot_username_raw or "",
    })


def _verify_telegram_auth(data: dict, bot_token: str) -> bool:
    """Проверка подписи данных от Telegram Login Widget"""
    try:
        # Получаем hash из данных
        received_hash = data.pop("hash", "")
        if not received_hash:
            return False
        
        # Сортируем данные и создаем строку для проверки
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
        
        # Telegram Login Widget:
        # secret_key = SHA256(bot_token)
        secret_key = hashlib.sha256(bot_token.encode()).digest()
        
        # Вычисляем hash
        calculated_hash = hmac.new(
            key=secret_key,
            msg=data_check_string.encode(),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        return calculated_hash == received_hash
    except Exception:
        return False


@app.get("/admin/auth/telegram")
async def admin_auth_telegram(request: Request):
    """Обработка авторизации через Telegram Login Widget"""
    # Берём ровно те параметры, которые прислал Telegram (важно для подписи!)
    qp = dict(request.query_params)
    try:
        tg_id = int(qp.get("id", "0"))
        auth_date = int(qp.get("auth_date", "0"))
        received_hash = qp.get("hash", "")
        first_name = qp.get("first_name", "")
        last_name = qp.get("last_name", "")
        username = qp.get("username", "")
        photo_url = qp.get("photo_url", "")
    except Exception:
        return RedirectResponse(url="/admin/login?error=invalid_request", status_code=303)

    # Проверяем, что пользователь админ
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = set(int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()) if admin_ids_str else set()
    
    if tg_id not in admin_ids:
        return RedirectResponse(url="/admin/login?error=not_admin", status_code=303)
    
    # Проверяем подпись Telegram
    bot_token = os.getenv("BOT_TOKEN", "")
    if not bot_token:
        return RedirectResponse(url="/admin/login?error=server_misconfigured", status_code=303)

    # Проверяем подпись по оригинальным query params
    auth_data = dict(qp)
    auth_data["hash"] = received_hash
    if not _verify_telegram_auth(auth_data.copy(), bot_token):
        return RedirectResponse(url="/admin/login?error=invalid_signature", status_code=303)
    
    # Проверяем, что данные не устарели (не старше 24 часов)
    current_time = int(time.time())
    if current_time - auth_date > 86400:  # 24 часа
        return RedirectResponse(url="/admin/login?error=expired", status_code=303)
    
    # Сохраняем данные в сессию
    request.session["admin_user"] = {
        "tg_id": tg_id,
        "first_name": first_name,
        "last_name": last_name,
        "username": username,
        "photo_url": photo_url,
    }
    _get_csrf_token(request)
    
    return RedirectResponse(url="/admin/web", status_code=303)


@app.get("/admin/logout")
async def admin_logout(request: Request):
    """Выход из админки"""
    request.session.clear()
    resp = RedirectResponse(
        url="/admin/login?force_reauth=1" if "drop_session" in request.query_params else "/admin/login",
        status_code=303,
    )
    # Явно удаляем cookie сессии, чтобы не осталось подписанного состояния
    resp.delete_cookie(key="session")
    return resp


@app.get("/admin/web/dashboard", response_class=HTMLResponse)
async def admin_web_dashboard(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Дашборд со статистикой"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    from datetime import timedelta
    now_utc = datetime.utcnow()
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = now_utc - timedelta(days=7)
    month_start = now_utc - timedelta(days=30)
    
    # Общая статистика
    total_users = await session.scalar(select(func.count()).select_from(User))
    total_active = await session.scalar(select(func.count()).select_from(User).where(User.is_active == True))
    total_blocked = await session.scalar(select(func.count()).select_from(User).where(User.is_active == False))
    total_balance = await session.scalar(select(func.sum(User.balance)).select_from(User)) or 0
    total_balance_rub = total_balance / 100  # Баланс уже в рублях (копейках)
    
    # Подписки
    total_subscriptions = await session.scalar(select(func.count()).select_from(Subscription))
    active_subscriptions = await session.scalar(
        select(func.count()).select_from(Subscription).where(Subscription.status == SubscriptionStatus.active)
    )
    
    # Платежи
    total_payments = await session.scalar(select(func.count()).select_from(Payment))
    succeeded_payments = await session.scalar(
        select(func.count()).select_from(Payment).where(Payment.status == PaymentStatus.succeeded)
    )
    total_revenue = await session.scalar(
        select(func.sum(Payment.amount_cents)).select_from(Payment).where(Payment.status == PaymentStatus.succeeded)
    ) or 0
    total_revenue_rub = total_revenue / 100  # Выручка уже в рублях (копейках)
    
    # Тикеты
    total_tickets = await session.scalar(select(func.count()).select_from(Ticket))
    new_tickets = await session.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.status == TicketStatus.new)
    )
    in_progress_tickets = await session.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.status == TicketStatus.in_progress)
    )
    closed_tickets = await session.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.status == TicketStatus.closed)
    )
    
    # Статистика за периоды
    users_today = await session.scalar(
        select(func.count()).select_from(User).where(User.created_at >= today_start)
    )
    users_week = await session.scalar(
        select(func.count()).select_from(User).where(User.created_at >= week_start)
    )
    users_month = await session.scalar(
        select(func.count()).select_from(User).where(User.created_at >= month_start)
    )
    
    payments_today = await session.scalar(
        select(func.count()).select_from(Payment)
        .where(Payment.created_at >= today_start, Payment.status == PaymentStatus.succeeded)
    )
    payments_week = await session.scalar(
        select(func.count()).select_from(Payment)
        .where(Payment.created_at >= week_start, Payment.status == PaymentStatus.succeeded)
    )
    payments_month = await session.scalar(
        select(func.count()).select_from(Payment)
        .where(Payment.created_at >= month_start, Payment.status == PaymentStatus.succeeded)
    )
    
    revenue_today = await session.scalar(
        select(func.sum(Payment.amount_cents)).select_from(Payment)
        .where(Payment.created_at >= today_start, Payment.status == PaymentStatus.succeeded)
    ) or 0
    revenue_week = await session.scalar(
        select(func.sum(Payment.amount_cents)).select_from(Payment)
        .where(Payment.created_at >= week_start, Payment.status == PaymentStatus.succeeded)
    ) or 0
    revenue_month = await session.scalar(
        select(func.sum(Payment.amount_cents)).select_from(Payment)
        .where(Payment.created_at >= month_start, Payment.status == PaymentStatus.succeeded)
    ) or 0
    
    tickets_today = await session.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.created_at >= today_start)
    )
    tickets_week = await session.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.created_at >= week_start)
    )
    tickets_month = await session.scalar(
        select(func.count()).select_from(Ticket).where(Ticket.created_at >= month_start)
    )
    
    # Топ пользователей по балансу
    top_users_result = await session.scalars(
        select(User)
        .order_by(User.balance.desc())
        .limit(10)
    )
    top_users = [
        {
            "tg_id": u.tg_id,
            "username": u.username or "—",
            "balance": u.balance / 100,
        }
        for u in top_users_result.all()
    ]
    
    # Последние действия
    recent_logs_result = await session.scalars(
        select(AuditLog)
        .order_by(AuditLog.created_at.desc())
        .limit(10)
    )
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    recent_logs = [
        {
            "id": log.id,
            "action": log.action.value if hasattr(log.action, "value") else str(log.action),
            "user_tg_id": log.user_tg_id,
            "admin_tg_id": log.admin_tg_id,
            "details": log.details or "—",
            "created_at": fmt(log.created_at),
        }
        for log in recent_logs_result.all()
    ]
    
    # Получаем алерты
    hour_ago = now_utc - timedelta(hours=1)
    day_ago = now_utc - timedelta(days=1)
    
    dashboard_alerts = []
    
    # Много новых тикетов
    if new_tickets >= 5:
        dashboard_alerts.append({
            "type": "warning",
            "title": "Много новых тикетов",
            "message": f"Создано {new_tickets} новых тикетов. Требуется внимание!",
            "link": "/admin/web/tickets?status=all",
        })
    
    # Зависшие тикеты
    stale_tickets = await session.scalars(
        select(Ticket)
        .where(
            Ticket.status == TicketStatus.in_progress,
            Ticket.updated_at < day_ago
        )
    )
    stale_count = len(stale_tickets.all())
    if stale_count > 0:
        dashboard_alerts.append({
            "type": "warning",
            "title": "Зависшие тикеты",
            "message": f"{stale_count} тикетов в работе не обновлялись более 24 часов",
            "link": "/admin/web/tickets?status=all",
        })
    
    # Отрицательный баланс
    negative_balance_count = await session.scalar(
        select(func.count()).select_from(User).where(User.balance < 0)
    )
    if negative_balance_count and negative_balance_count > 0:
        dashboard_alerts.append({
            "type": "danger",
            "title": "Отрицательный баланс",
            "message": f"{negative_balance_count} пользователей имеют отрицательный баланс",
            "link": "/admin/web/users?balance_max=0",
        })
    
    # Низкий общий баланс (менее $100)
    if total_balance_rub < 100:
        dashboard_alerts.append({
            "type": "info",
            "title": "Низкий общий баланс",
            "message": f"Общий баланс всех пользователей: {total_balance_rub:.2f} RUB",
        })
    
    # Получаем статус серверов
    servers = await session.scalars(select(Server).where(Server.is_enabled == True))
    servers_list = servers.all()
    servers_status = []
    
    for server in servers_list:
        # Получаем последний статус сервера
        latest_status = await session.scalar(
            select(ServerStatus)
            .where(ServerStatus.server_id == server.id)
            .order_by(ServerStatus.checked_at.desc())
            .limit(1)
        )
        
        servers_status.append({
            "id": server.id,
            "name": server.name,
            "host": server.host,
            "location": server.location or "—",
            "is_online": latest_status.is_online if latest_status else False,
            "response_time_ms": latest_status.response_time_ms if latest_status else None,
            "active_connections": latest_status.active_connections if latest_status else 0,
            "capacity": server.capacity,
            "checked_at": latest_status.checked_at if latest_status else None,
            "error_message": latest_status.error_message if latest_status else None,
        })
        
        # Добавляем алерт, если сервер недоступен
        if latest_status and not latest_status.is_online:
            dashboard_alerts.append({
                "type": "danger",
                "title": f"Сервер {server.name} недоступен",
                "message": f"Сервер {server.name} ({server.host}) не отвечает. {latest_status.error_message or 'Проверьте подключение'}",
                "link": "/admin/web/servers",
            })
    
    csrf_token = _get_csrf_token(request)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "admin_user": admin_user,
        "csrf_token": csrf_token,
        "total_users": int(total_users or 0),
        "total_active": int(total_active or 0),
        "total_blocked": int(total_blocked or 0),
        "total_balance_rub": total_balance_rub,
        "total_subscriptions": int(total_subscriptions or 0),
        "active_subscriptions": int(active_subscriptions or 0),
        "total_payments": int(total_payments or 0),
        "succeeded_payments": int(succeeded_payments or 0),
        "total_revenue_rub": total_revenue_rub,
        "total_tickets": int(total_tickets or 0),
        "new_tickets": int(new_tickets or 0),
        "in_progress_tickets": int(in_progress_tickets or 0),
        "closed_tickets": int(closed_tickets or 0),
        "users_today": int(users_today or 0),
        "users_week": int(users_week or 0),
        "users_month": int(users_month or 0),
        "payments_today": int(payments_today or 0),
        "payments_week": int(payments_week or 0),
        "payments_month": int(payments_month or 0),
        "revenue_today": revenue_today / 100,
        "revenue_week": revenue_week / 100,
        "revenue_month": revenue_month / 100,
        "tickets_today": int(tickets_today or 0),
        "tickets_week": int(tickets_week or 0),
        "tickets_month": int(tickets_month or 0),
        "top_users": top_users,
        "recent_logs": recent_logs,
        "alerts": dashboard_alerts,
        "servers_status": servers_status,
    })


@app.get("/admin/web/api/backups/recent")
async def admin_api_recent_backups(
    limit: int = Query(default=5, ge=1, le=10),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API для получения последних бэкапов"""
    backups_result = await session.scalars(
        select(Backup)
        .order_by(Backup.created_at.desc())
        .limit(limit)
    )
    backups = backups_result.all()
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    backups_data = []
    for backup in backups:
        file_size_mb = backup.file_size_bytes / (1024 * 1024) if backup.file_size_bytes > 0 else 0
        
        backups_data.append({
            "id": backup.id,
            "backup_type": backup.backup_type,
            "file_size_mb": file_size_mb,
            "status": backup.status,
            "error_message": backup.error_message,
            "created_at": fmt(backup.created_at),
        })
    
    return {"backups": backups_data}


@app.get("/admin/web/api/dashboard/stats")
async def admin_api_dashboard_stats(
    days: int = Query(default=30, ge=7, le=365),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API для получения статистики для графиков"""
    from datetime import timedelta
    now_utc = datetime.utcnow()
    start_date = now_utc - timedelta(days=days)
    
    # Статистика регистраций по дням
    registrations_by_day = await session.execute(
        select(
            func.date(User.created_at).label('date'),
            func.count(User.id).label('count')
        )
        .where(User.created_at >= start_date)
        .group_by(func.date(User.created_at))
        .order_by(func.date(User.created_at))
    )
    registrations_data = registrations_by_day.all()
    
    # Статистика платежей по дням
    payments_by_day = await session.execute(
        select(
            func.date(Payment.created_at).label('date'),
            func.count(Payment.id).label('count'),
            func.sum(Payment.amount_cents).label('revenue')
        )
        .where(
            Payment.created_at >= start_date,
            Payment.status == PaymentStatus.succeeded
        )
        .group_by(func.date(Payment.created_at))
        .order_by(func.date(Payment.created_at))
    )
    payments_data = payments_by_day.all()
    
    # Статистика тикетов по дням
    tickets_by_day = await session.execute(
        select(
            func.date(Ticket.created_at).label('date'),
            func.count(Ticket.id).label('count')
        )
        .where(Ticket.created_at >= start_date)
        .group_by(func.date(Ticket.created_at))
        .order_by(func.date(Ticket.created_at))
    )
    tickets_data = tickets_by_day.all()
    
    # Форматируем данные
    def format_date(dt):
        if dt:
            # Если это date, преобразуем в строку напрямую
            if isinstance(dt, datetime):
                try:
                    from zoneinfo import ZoneInfo
                    moscow_tz = ZoneInfo("Europe/Moscow")
                    return dt.astimezone(moscow_tz).strftime("%d.%m")
                except Exception:
                    return dt.strftime("%d.%m")
            else:
                # Если это date объект
                return dt.strftime("%d.%m")
        return ""
    
    return {
        "registrations": [
            {"date": format_date(r.date), "count": r.count}
            for r in registrations_data
        ],
        "payments": [
            {"date": format_date(p.date), "count": p.count, "revenue": (p.revenue or 0) / 100}
            for p in payments_data
        ],
        "tickets": [
            {"date": format_date(t.date), "count": t.count}
            for t in tickets_data
        ],
    }


@app.get("/admin/web", response_class=HTMLResponse)
async def admin_web(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Главная страница - перенаправление на дашборд"""
    return RedirectResponse(url="/admin/web/dashboard", status_code=303)


@app.get("/admin/web/api/users")
async def admin_api_users(
    q: str | None = Query(default=None),
    status: str | None = Query(default="all"),
    role: str | None = Query(default="all"),
    reg_period: str | None = Query(default="all"),
    balance_min: str | None = Query(default=None),
    balance_max: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API для получения списка пользователей (для автообновления)"""
    # Валидируем фильтры (та же логика, что и в admin_web_users)
    status_filter = (status or "all").lower()
    if status_filter not in {"all", "active", "blocked"}:
        status_filter = "all"
    
    role_filter = (role or "all").lower()
    if role_filter not in {"all", "superadmin", "admin", "moderator", "user"}:
        role_filter = "all"
    
    reg_filter = (reg_period or "all").lower()
    if reg_filter not in {"all", "today", "7d", "30d"}:
        reg_filter = "all"
    
    # Получаем пользователей (с поиском/фильтрами)
    stmt = select(User).options(selectinload(User.referred_by)).order_by(User.created_at.desc())
    if status_filter == "active":
        stmt = stmt.where(User.is_active == True)
    elif status_filter == "blocked":
        stmt = stmt.where(User.is_active == False)
    
    # Фильтр по периоду регистрации
    if reg_filter != "all":
        now_utc = datetime.utcnow()
        if reg_filter == "today":
            start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        elif reg_filter == "7d":
            start = now_utc - timedelta(days=7)
        else:  # "30d"
            start = now_utc - timedelta(days=30)
        stmt = stmt.where(User.created_at >= start)
    
    if q:
        qq = q.strip()
        if qq.isdigit():
            stmt = stmt.where(User.tg_id == int(qq))
        else:
            like = f"%{qq.lower()}%"
            stmt = stmt.where(
                func.lower(User.username).like(like)
                | func.lower(User.first_name).like(like)
                | func.lower(User.last_name).like(like)
            )
    
    # Фильтр по балансу
    if balance_min:
        try:
            min_balance = int(float(balance_min) * 100)
            stmt = stmt.where(User.balance >= min_balance)
        except (ValueError, TypeError):
            pass
    if balance_max:
        try:
            max_balance = int(float(balance_max) * 100)
            stmt = stmt.where(User.balance <= max_balance)
        except (ValueError, TypeError):
            pass
    
    result = await session.scalars(stmt)
    users: Sequence[User] = result.all()
    
    # Подтягиваем overrides ролей
    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
    
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = set(int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()) if admin_ids_str else set()
    
    # Форматируем данные
    users_data = []
    for u in users:
        referred_by_tg_id = u.referred_by.tg_id if u.referred_by else None
        full_name = " ".join(filter(None, [u.first_name or "", u.last_name or ""])) or "—"
        tag = f"@{u.username}" if u.username else "—"
        eff_role = _get_effective_role(u.tg_id, admin_ids, overrides_map)
        
        # Применяем фильтр по роли
        if role_filter != "all" and eff_role != role_filter:
            continue
        
        role = "Главный админ" if eff_role == "superadmin" else ("Админ" if eff_role == "admin" else ("Модератор" if eff_role == "moderator" else "Пользователь"))
        balance_rub = u.balance / 100  # Баланс уже в рублях (копейках)
        
        try:
            from zoneinfo import ZoneInfo
            moscow_tz = ZoneInfo("Europe/Moscow")
            dt_moscow = u.created_at.astimezone(moscow_tz)
            created_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
        except:
            created_str = str(u.created_at)[:16]
        
        users_data.append({
            "id": u.id,
            "tg_id": u.tg_id,
            "username": u.username or "—",
            "full_name": full_name,
            "tag": tag,
            "role": role,
            "is_active": u.is_active,
            "has_active_subscription": u.has_active_subscription,
            "balance": balance_rub,
            "referral_code": u.referral_code or "—",
            "referred_by_tg_id": referred_by_tg_id,
            "created_at": created_str,
        })
    
    # Счётчики
    total_users = await session.scalar(select(func.count()).select_from(User))
    total_active = await session.scalar(select(func.count()).select_from(User).where(User.is_active == True))
    total_blocked = await session.scalar(select(func.count()).select_from(User).where(User.is_active == False))
    
    return {
        "users": users_data,
        "total_users": int(total_users or 0),
        "total_active": int(total_active or 0),
        "total_blocked": int(total_blocked or 0),
    }


@app.get("/admin/web/api/notifications")
async def admin_api_notifications(
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API для получения уведомлений и алертов"""
    from datetime import timedelta
    now_utc = datetime.utcnow()
    hour_ago = now_utc - timedelta(hours=1)
    day_ago = now_utc - timedelta(days=1)
    
    notifications = []
    alerts = []
    
    # Новые тикеты (за последний час)
    new_tickets_count = await session.scalar(
        select(func.count()).select_from(Ticket)
        .where(Ticket.status == TicketStatus.new, Ticket.created_at >= hour_ago)
    )
    if new_tickets_count and new_tickets_count > 0:
        notifications.append({
            "type": "ticket",
            "severity": "info",
            "title": f"Новых тикетов: {new_tickets_count}",
            "message": f"За последний час создано {new_tickets_count} новых тикетов",
            "link": "/admin/web/tickets?status=all",
            "count": new_tickets_count,
        })
        if new_tickets_count >= 5:
            alerts.append({
                "type": "warning",
                "title": "Много новых тикетов",
                "message": f"Создано {new_tickets_count} новых тикетов за последний час. Требуется внимание!",
            })
    
    # Тикеты в работе без обновления более 24 часов
    stale_tickets = await session.scalars(
        select(Ticket)
        .where(
            Ticket.status == TicketStatus.in_progress,
            Ticket.updated_at < day_ago
        )
    )
    stale_count = len(stale_tickets.all())
    if stale_count > 0:
        alerts.append({
            "type": "warning",
            "title": "Зависшие тикеты",
            "message": f"{stale_count} тикетов в работе не обновлялись более 24 часов",
        })
    
    # Пользователи с отрицательным балансом
    negative_balance_count = await session.scalar(
        select(func.count()).select_from(User).where(User.balance < 0)
    )
    if negative_balance_count and negative_balance_count > 0:
        alerts.append({
            "type": "danger",
            "title": "Отрицательный баланс",
            "message": f"{negative_balance_count} пользователей имеют отрицательный баланс",
        })
    
    # Новые пользователи за последний час
    new_users_count = await session.scalar(
        select(func.count()).select_from(User).where(User.created_at >= hour_ago)
    )
    if new_users_count and new_users_count > 0:
        notifications.append({
            "type": "user",
            "severity": "success",
            "title": f"Новых пользователей: {new_users_count}",
            "message": f"За последний час зарегистрировано {new_users_count} новых пользователей",
            "link": "/admin/web/users?reg_period=today",
            "count": new_users_count,
        })
    
    # Критические действия в логах (блокировки, изменения баланса)
    critical_actions = await session.scalars(
        select(AuditLog)
        .where(
            AuditLog.created_at >= hour_ago,
            AuditLog.action.in_([
                AuditLogAction.user_blocked,
                AuditLogAction.balance_credited,
            ])
        )
        .order_by(AuditLog.created_at.desc())
        .limit(10)
    )
    critical_count = len(critical_actions.all())
    if critical_count > 0:
        notifications.append({
            "type": "action",
            "severity": "warning",
            "title": f"Критических действий: {critical_count}",
            "message": f"За последний час выполнено {critical_count} критических действий",
            "link": "/admin/web/logs",
            "count": critical_count,
        })
    
    # Платежи за последний час
    recent_payments = await session.scalar(
        select(func.count()).select_from(Payment)
        .where(
            Payment.created_at >= hour_ago,
            Payment.status == PaymentStatus.succeeded
        )
    )
    if recent_payments and recent_payments > 0:
        notifications.append({
            "type": "payment",
            "severity": "success",
            "title": f"Новых платежей: {recent_payments}",
            "message": f"За последний час обработано {recent_payments} успешных платежей",
            "link": "/admin/web/logs?action=payment_processed",
            "count": recent_payments,
        })
    
    return {
        "notifications": notifications,
        "alerts": alerts,
        "unread_count": len(notifications) + len(alerts),
    }


@app.get("/admin/web/users", response_class=HTMLResponse)
async def admin_web_users(
    request: Request,
    q: str | None = Query(default=None),
    status: str | None = Query(default="all"),
    role: str | None = Query(default="all"),
    reg_period: str | None = Query(default="all"),
    balance_min: str | None = Query(default=None),
    balance_max: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Веб-интерфейс админки для просмотра пользователей"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    # Валидируем фильтр статуса
    status_filter = (status or "all").lower()
    if status_filter not in {"all", "active", "blocked"}:
        status_filter = "all"

    # Валидируем фильтр роли
    role_filter = (role or "all").lower()
    if role_filter not in {"all", "superadmin", "admin", "moderator", "user"}:
        role_filter = "all"

    # Валидируем период регистрации
    reg_filter = (reg_period or "all").lower()
    if reg_filter not in {"all", "today", "7d", "30d"}:
        reg_filter = "all"

    # Счётчики
    total_users = await session.scalar(select(func.count()).select_from(User))
    total_active = await session.scalar(select(func.count()).select_from(User).where(User.is_active == True))
    total_blocked = await session.scalar(select(func.count()).select_from(User).where(User.is_active == False))

    # Получаем пользователей (с поиском/фильтрами)
    stmt = select(User).options(selectinload(User.referred_by)).order_by(User.created_at.desc())
    if status_filter == "active":
        stmt = stmt.where(User.is_active == True)
    elif status_filter == "blocked":
        stmt = stmt.where(User.is_active == False)

    # Фильтр по периоду регистрации (по created_at, в UTC)
    if reg_filter != "all":
        now_utc = datetime.utcnow()
        if reg_filter == "today":
            start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        elif reg_filter == "7d":
            start = now_utc - timedelta(days=7)
        else:  # "30d"
            start = now_utc - timedelta(days=30)
        stmt = stmt.where(User.created_at >= start)

    if q:
        qq = q.strip()
        if qq.isdigit():
            stmt = stmt.where(User.tg_id == int(qq))
        else:
            # поиск по username/first/last
            like = f"%{qq.lower()}%"
            stmt = stmt.where(
                func.lower(User.username).like(like)
                | func.lower(User.first_name).like(like)
                | func.lower(User.last_name).like(like)
            )
    
    # Фильтр по балансу
    if balance_min:
        try:
            min_balance = int(float(balance_min) * 100)  # конвертируем USD в центы
            stmt = stmt.where(User.balance >= min_balance)
        except (ValueError, TypeError):
            pass
    if balance_max:
        try:
            max_balance = int(float(balance_max) * 100)  # конвертируем RUB в копейки
            stmt = stmt.where(User.balance <= max_balance)
        except (ValueError, TypeError):
            pass
    
    result = await session.scalars(stmt)
    users: Sequence[User] = result.all()

    # Подтягиваем overrides ролей
    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
    
    # Получаем admin_ids из переменной окружения
    import os
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = set(int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()) if admin_ids_str else set()
    
    # Форматируем данные для шаблона и фильтруем по ролям
    users_data = []
    for u in users:
        referred_by_tg_id = u.referred_by.tg_id if u.referred_by else None
        full_name = " ".join(filter(None, [u.first_name or "", u.last_name or ""])) or "—"
        tag = f"@{u.username}" if u.username else "—"
        eff_role = _get_effective_role(u.tg_id, admin_ids, overrides_map)
        
        # Применяем фильтр по роли (если задан)
        if role_filter != "all" and eff_role != role_filter:
            continue
        
        role = "Главный админ" if eff_role == "superadmin" else ("Админ" if eff_role == "admin" else ("Модератор" if eff_role == "moderator" else "Пользователь"))
        balance_rub = u.balance / 100  # Баланс уже в рублях (копейках)
        
        # Форматируем время в МСК
        try:
            from zoneinfo import ZoneInfo
            moscow_tz = ZoneInfo("Europe/Moscow")
            dt_moscow = u.created_at.astimezone(moscow_tz)
            created_str = dt_moscow.strftime("%d.%m.%Y %H:%M")
        except:
            created_str = str(u.created_at)[:16]
        
        users_data.append({
            "id": u.id,
            "tg_id": u.tg_id,
            "username": u.username or "—",
            "full_name": full_name,
            "tag": tag,
            "role": role,
            "is_active": u.is_active,
            "has_active_subscription": u.has_active_subscription,
            "balance": balance_rub,
            "referral_code": u.referral_code or "—",
            "referred_by_tg_id": referred_by_tg_id,
            "created_at": created_str,
        })
    
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "users": users_data,
        "total_users": int(total_users or 0),
        "total_active": int(total_active or 0),
        "total_blocked": int(total_blocked or 0),
        "q": q or "",
        "status": status_filter,
        "role_filter": role_filter,
        "reg_filter": reg_filter,
        "balance_min": balance_min or "",
        "balance_max": balance_max or "",
        "admin_user": admin_user,
        "csrf_token": _get_csrf_token(request),
    })


@app.get("/admin/web/users/{tg_id}", response_class=HTMLResponse)
async def admin_web_user_detail(
    tg_id: int,
    request: Request,
    logs_page: int = Query(default=1, ge=1),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)

    stmt = select(User).options(selectinload(User.referred_by)).where(User.tg_id == tg_id)
    user = await session.scalar(stmt)
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
    override_role = overrides_map.get(tg_id)
    eff_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
    role = "Главный админ" if eff_role == "superadmin" else ("Админ" if eff_role == "admin" else ("Модератор" if eff_role == "moderator" else "Пользователь"))
    balance_rub = user.balance / 100  # Баланс уже в рублях (копейках)

    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        created_str = user.created_at.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M")
    except Exception:
        created_str = str(user.created_at)[:16]

    referrals_count = await session.scalar(select(func.count()).select_from(User).where(User.referred_by_user_id == user.id))

    # --- Логи с пагинацией
    logs_page_size = 20
    logs_stmt = (
        select(AuditLog)
        .where(AuditLog.user_tg_id == tg_id)
        .order_by(AuditLog.created_at.desc())
    )
    total_logs = await session.scalar(select(func.count()).select_from(logs_stmt.subquery()))
    logs_result = await session.scalars(
        logs_stmt
        .limit(logs_page_size)
        .offset((logs_page - 1) * logs_page_size)
    )
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    logs = [
        {
            "id": log.id,
            "action": log.action.value if hasattr(log.action, "value") else str(log.action),
            "admin_tg_id": log.admin_tg_id,
            "details": log.details,
            "created_at": fmt(log.created_at),
        }
        for log in logs_result.all()
    ]
    logs_has_next = (logs_page * logs_page_size) < (total_logs or 0)
    logs_has_prev = logs_page > 1

    # --- Все тикеты пользователя
    tickets_result = await session.scalars(
        select(Ticket)
        .where(Ticket.user_tg_id == tg_id)
        .order_by(Ticket.updated_at.desc())
    )
    tickets_list = tickets_result.all()
    
    tickets_data = []
    for t in tickets_list:
        tickets_data.append({
            "id": t.id,
            "topic": t.topic or "—",
            "status": t.status.value if hasattr(t.status, "value") else str(t.status),
            "created_at": fmt(t.created_at),
            "updated_at": fmt(t.updated_at),
        })

    # Получаем информацию о выбранном сервере
    selected_server_name = None
    if user.selected_server_id:
        selected_server = await session.get(Server, user.selected_server_id)
        if selected_server:
            selected_server_name = selected_server.name
    
    user_data = {
        "id": user.id,
        "tg_id": user.tg_id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "role": role,
        "is_admin_env": user.tg_id in admin_ids,
        "role_override": override_role,
        "is_active": user.is_active,
        "has_active_subscription": user.has_active_subscription,
        "subscription_ends_at": fmt(user.subscription_ends_at) if user.subscription_ends_at else "—",
        "selected_server_name": selected_server_name or "—",
        "balance": balance_rub,
        "referral_code": user.referral_code or "—",
        "referred_by_tg_id": user.referred_by.tg_id if user.referred_by else None,
        "created_at": created_str,
    }

    photo_url = await _fetch_avatar_url(tg_id, os.getenv("BOT_TOKEN", ""))
    
    # Получаем информацию о банах
    active_ban = await session.scalar(
        select(UserBan)
        .where(UserBan.user_id == user.id)
        .where(UserBan.is_active == True)
        .order_by(UserBan.banned_at.desc())
    )
    
    ban_info = None
    if active_ban:
        ban_info = {
            "id": active_ban.id,
            "reason": active_ban.reason,
            "details": active_ban.details,
            "banned_at": fmt(active_ban.banned_at),
            "banned_until": fmt(active_ban.banned_until) if active_ban.banned_until else "Перманентно",
            "auto_ban": active_ban.auto_ban,
        }
    
    # Получаем историю IP адресов
    ip_logs = await session.scalars(
        select(IpLog)
        .where(IpLog.user_id == user.id)
        .order_by(IpLog.last_seen.desc())
        .limit(20)
    )
    ip_history = []
    for ip_log in ip_logs.all():
        server = await session.get(Server, ip_log.server_id)
        ip_history.append({
            "ip": ip_log.ip_address,
            "server": server.name if server else "—",
            "first_seen": fmt(ip_log.first_seen),
            "last_seen": fmt(ip_log.last_seen),
            "count": ip_log.connection_count,
        })

    return templates.TemplateResponse("user_detail.html", {
        "request": request,
        "user": user_data,
        "referrals_count": int(referrals_count or 0),
        "logs": logs,
        "logs_page": logs_page,
        "logs_has_next": logs_has_next,
        "logs_has_prev": logs_has_prev,
        "tickets": tickets_data,
        "admin_user": admin_user,
        "csrf_token": _get_csrf_token(request),
        "can_toggle_role": (user.tg_id not in admin_ids),  # нельзя менять роль для env-админа (главный)
        "is_protected_admin": (user.tg_id in admin_ids),
        "role_value": ("superadmin" if user.tg_id in admin_ids else (override_role if override_role else "user")),
        "role_options": ["admin", "moderator", "user"],
        "avatar_url": photo_url,
        "ban_info": ban_info,
        "ip_history": ip_history,
    })


@app.get("/admin/web/api/users/{tg_id}/logs")
async def admin_api_user_logs(
    tg_id: int,
    page: int = Query(default=1, ge=1),
    action: str = Query(default="all"),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API для получения логов пользователя с пагинацией."""
    logs_page_size = 20
    logs_stmt = (
        select(AuditLog)
        .where(AuditLog.user_tg_id == tg_id)
        .order_by(AuditLog.created_at.desc())
    )
    
    if action != "all":
        try:
            action_enum = AuditLogAction(action)
            logs_stmt = logs_stmt.where(AuditLog.action == action_enum)
        except ValueError:
            pass  # Игнорируем неверное значение
    total_logs = await session.scalar(select(func.count()).select_from(logs_stmt.subquery()))
    logs_result = await session.scalars(
        logs_stmt
        .limit(logs_page_size)
        .offset((page - 1) * logs_page_size)
    )
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    logs = [
        {
            "id": log.id,
            "action": log.action.value if hasattr(log.action, "value") else str(log.action),
            "admin_tg_id": log.admin_tg_id,
            "details": log.details,
            "created_at": fmt(log.created_at),
        }
        for log in logs_result.all()
    ]
    logs_has_next = (page * logs_page_size) < (total_logs or 0)
    logs_has_prev = page > 1
    
    return {
        "logs": logs,
        "page": page,
        "has_next": logs_has_next,
        "has_prev": logs_has_prev,
        "total": total_logs or 0,
    }


@app.get("/admin/web/payments", response_class=HTMLResponse)
async def admin_web_payments(
    request: Request,
    page: int = Query(default=1, ge=1),
    status: str | None = Query(default=None),
    provider: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Страница платежей в админке"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    page_size = 50
    offset = (page - 1) * page_size
    
    result = await admin_list_payments(
        limit=page_size,
        offset=offset,
        status=status,
        provider=provider,
        session=session,
        admin_user=admin_user,
    )
    
    payments = result.get("payments", [])
    total = result.get("total", 0)
    has_next = (page * page_size) < total
    has_prev = page > 1
    
    return templates.TemplateResponse("payments.html", {
        "request": request,
        "admin_user": admin_user,
        "payments": payments,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
        "total": total,
        "status_filter": status or "all",
        "provider_filter": provider or "all",
        "csrf_token": _get_csrf_token(request),
    })


@app.get("/admin/web/user/{tg_id}", response_class=HTMLResponse)
async def admin_web_user(
    tg_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)

    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = set(int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()) if admin_ids_str else set()

    stmt = (
        select(User)
        .options(
            selectinload(User.referred_by),
            selectinload(User.referrals),
        )
        .where(User.tg_id == tg_id)
    )
    user = await session.scalar(stmt)
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    referred_by_tg_id = user.referred_by.tg_id if user.referred_by else None
    referrals_count = len(user.referrals or [])
    role = "Админ" if user.tg_id in admin_ids else "Пользователь"
    balance_rub = user.balance / 100  # Баланс уже в рублях (копейках)

    # Подписка
    sub = await session.scalar(
        select(Subscription)
        .where(Subscription.user_id == user.id, Subscription.status == SubscriptionStatus.active)
        .order_by(Subscription.ends_at.desc().nullslast())
    )
    sub_info = None
    if sub:
        try:
            from zoneinfo import ZoneInfo
            moscow_tz = ZoneInfo("Europe/Moscow")
            ends_msk = sub.ends_at.astimezone(moscow_tz) if sub.ends_at else None
            ends_str = ends_msk.strftime("%d.%m.%Y %H:%M") if ends_msk else "—"
        except Exception:
            ends_str = sub.ends_at.isoformat() if sub and sub.ends_at else "—"
        sub_info = {
            "plan": sub.plan_name or "—",
            "ends_at": ends_str,
            "status": sub.status.value if hasattr(sub.status, "value") else str(sub.status),
        }

    # Время регистрации в МСК
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        created_msk = user.created_at.astimezone(moscow_tz)
        created_str = created_msk.strftime("%d.%m.%Y %H:%M")
    except Exception:
        created_str = str(user.created_at)[:16]

    user_data = {
        "id": user.id,
        "tg_id": user.tg_id,
        "username": user.username or "—",
        "full_name": " ".join(filter(None, [user.first_name or "", user.last_name or ""])) or "—",
        "tag": f"@{user.username}" if user.username else "—",
        "role": role,
        "is_active": user.is_active,
        "balance": balance_rub,
        "referral_code": user.referral_code or "—",
        "referred_by_tg_id": referred_by_tg_id,
        "referrals_count": referrals_count,
        "created_at": created_str,
    }

    # --- Все тикеты пользователя
    tickets_result = await session.scalars(
        select(Ticket)
        .where(Ticket.user_tg_id == tg_id)
        .order_by(Ticket.updated_at.desc())
    )
    tickets_list = tickets_result.all()
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    tickets_data = []
    for t in tickets_list:
        tickets_data.append({
            "id": t.id,
            "topic": t.topic or "—",
            "status": t.status.value if hasattr(t.status, "value") else str(t.status),
            "created_at": fmt(t.created_at),
            "updated_at": fmt(t.updated_at),
        })

    return templates.TemplateResponse(
        "user_detail.html",
        {
            "request": request,
            "user": user_data,
            "sub": sub_info,
            "csrf_token": _get_csrf_token(request),
            "admin_user": admin_user,
            "tickets": tickets_data,
            "open_modal": request.query_params.get("open"),
        },
    )


@app.post("/admin/web/users/block")
async def admin_web_block_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    ticket_id = form.get("ticket_id")
    reason = str(form.get("reason", "")).strip()
    if not reason:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=reason_required", status_code=303)
    admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}

    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    actor_tg = admin_user.get("tg_id")
    actor_role = _get_effective_role(actor_tg, admin_ids, overrides_map)
    target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
    # Самого себя блокировать нельзя
    if actor_tg == user.tg_id:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_self", status_code=303)
    # Иерархия для чужих
    if _role_rank(actor_role) <= _role_rank(target_role):
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_rank", status_code=303)
    user.is_active = False
    session.add(
        AuditLog(
            action=AuditLogAction.user_blocked,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Пользователь заблокирован (web). Причина: {reason}",
        )
    )
    await session.commit()
    
    # Отправляем уведомление пользователю
    notification_text = (
        f"❌ <b>Аккаунт заблокирован</b>\n\n"
        f"Ваш аккаунт был заблокирован администратором."
    )
    if reason:
        notification_text += f"\n\nПричина: {reason}"
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/support/webhook")
async def support_webhook(
    update: dict,
    session: AsyncSession = Depends(get_session),
    x_admin_token: str | None = Header(default=None),
):
    # Убираем обязательную авторизацию заголовком — Telegram не шлёт его на вебхуке
    settings = get_settings()

    # Поддерживаем message и edited_message
    message = update.get("message") or update.get("edited_message") or {}
    chat = message.get("chat") or {}
    text = message.get("text") or message.get("caption") or ""
    tg_id = chat.get("id")
    if not tg_id or not text:
        return {"status": "ignored"}

    # Создаём/обновляем тикет
    ticket = await session.scalar(
        select(Ticket).where(Ticket.user_tg_id == tg_id).order_by(Ticket.updated_at.desc())
    )
    now = datetime.utcnow()
    if not ticket:
        ticket = Ticket(user_tg_id=tg_id, status=TicketStatus.open, created_at=now, updated_at=now)
        session.add(ticket)
        await session.flush()
    else:
        ticket.updated_at = now

    session.add(
        TicketMessage(
            ticket_id=ticket.id,
            user_tg_id=tg_id,
            direction=MessageDirection.incoming,
            admin_tg_id=None,
            text=text,
        )
    )
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=tg_id,
            details=f"User message to support: {text}",
        )
    )
    await session.commit()
    return {"status": "ok"}


@app.post("/admin/web/users/unblock")
async def admin_web_unblock_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    reason = str(form.get("reason", "")).strip()
    if not reason:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=reason_required", status_code=303)
    admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}

    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    actor_role = _get_effective_role(admin_user.get("tg_id"), admin_ids, overrides_map)
    target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
    if admin_user.get("tg_id") == user.tg_id:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_self", status_code=303)
    if _role_rank(actor_role) <= _role_rank(target_role):
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_rank", status_code=303)
    user.is_active = True
    session.add(
        AuditLog(
            action=AuditLogAction.user_unblocked,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Пользователь разблокирован (web). Причина: {reason}",
        )
    )
    await session.commit()
    
    # Отправляем уведомление пользователю
    notification_text = (
        f"✅ <b>Аккаунт разблокирован</b>\n\n"
        f"Ваш аккаунт был разблокирован администратором."
    )
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/web/users/vpn-ban")
async def admin_web_vpn_ban_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Забанить пользователя в VPN (отключить клиента в 3x-UI)"""
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    reason = str(form.get("reason", "")).strip()
    duration_hours = int(str(form.get("duration_hours", "24")))
    
    if not reason:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=reason_required", status_code=303)
    
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)
    
    # Проверяем, нет ли уже активного бана
    existing_ban = await session.scalar(
        select(UserBan).where(UserBan.user_id == user.id, UserBan.is_active == True)
    )
    
    if existing_ban:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=already_banned", status_code=303)
    
    now = datetime.utcnow()
    banned_until = now + timedelta(hours=duration_hours) if duration_hours > 0 else None
    
    # Создаем бан
    ban = UserBan(
        user_id=user.id,
        reason="manual",
        details=reason,
        is_active=True,
        auto_ban=False,
        banned_until=banned_until,
    )
    session.add(ban)
    
    # Отключаем клиента в 3x-UI
    credentials = await session.scalars(
        select(VpnCredential)
        .where(VpnCredential.user_id == user.id)
        .where(VpnCredential.active == True)
        .options(selectinload(VpnCredential.server))
    )
    
    for cred in credentials.all():
        if not cred.server or not cred.user_uuid:
            continue
        
        server = cred.server
        if server.x3ui_api_url and server.x3ui_username and server.x3ui_password and server.x3ui_inbound_id:
            try:
                from core.x3ui_api import X3UIAPI
                x3ui = X3UIAPI(
                    api_url=server.x3ui_api_url,
                    username=server.x3ui_username,
                    password=server.x3ui_password,
                )
                try:
                    await x3ui.disable_client(server.x3ui_inbound_id, cred.user_uuid)
                finally:
                    await x3ui.close()
            except Exception as e:
                logger.error(f"Error disabling client for ban: {e}")
    
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"VPN бан пользователя. Причина: {reason}. Срок: {duration_hours}ч" if duration_hours > 0 else f"VPN бан пользователя (перманентный). Причина: {reason}",
        )
    )
    await session.commit()
    
    # Уведомляем пользователя
    duration_text = f"на {duration_hours} часов" if duration_hours > 0 else "на неопределенный срок"
    notification_text = (
        f"⛔ <b>Ваш VPN доступ заблокирован</b>\n\n"
        f"Причина: {reason}\n"
        f"Срок: {duration_text}\n\n"
        "Если вы считаете это ошибкой, обратитесь в поддержку."
    )
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/web/users/vpn-unban")
async def admin_web_vpn_unban_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Разбанить пользователя в VPN"""
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)
    
    # Находим активный бан
    active_ban = await session.scalar(
        select(UserBan).where(UserBan.user_id == user.id, UserBan.is_active == True)
    )
    
    if not active_ban:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=not_banned", status_code=303)
    
    # Снимаем бан
    active_ban.is_active = False
    active_ban.unbanned_at = datetime.utcnow()
    active_ban.unbanned_by_tg_id = admin_user.get("tg_id")
    
    # Включаем клиента обратно в 3x-UI
    credentials = await session.scalars(
        select(VpnCredential)
        .where(VpnCredential.user_id == user.id)
        .where(VpnCredential.active == True)
        .options(selectinload(VpnCredential.server))
    )
    
    for cred in credentials.all():
        if not cred.server or not cred.user_uuid:
            continue
        
        server = cred.server
        if server.x3ui_api_url and server.x3ui_username and server.x3ui_password and server.x3ui_inbound_id:
            try:
                from core.x3ui_api import X3UIAPI
                x3ui = X3UIAPI(
                    api_url=server.x3ui_api_url,
                    username=server.x3ui_username,
                    password=server.x3ui_password,
                )
                try:
                    await x3ui.enable_client(server.x3ui_inbound_id, cred.user_uuid)
                finally:
                    await x3ui.close()
            except Exception as e:
                logger.error(f"Error enabling client for unban: {e}")
    
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"VPN разбан пользователя",
        )
    )
    await session.commit()
    
    # Уведомляем пользователя
    notification_text = (
        f"✅ <b>Ваш VPN доступ восстановлен</b>\n\n"
        "Вы снова можете пользоваться VPN."
    )
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/web/users/credit")
async def admin_web_credit_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    amount_str = str(form.get("amount", "0")).replace(",", ".").strip()
    reason = str(form.get("reason", "")).strip()
    if not reason:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=reason_required", status_code=303)
    try:
        amount_rub = float(amount_str)
    except Exception:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=bad_amount", status_code=303)
    if amount_rub == 0:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=bad_amount", status_code=303)
    amount_cents = int(round(amount_rub * 100))

    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    # Проверка иерархии ролей: нельзя менять баланс равным/старшим
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = set(int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()) if admin_ids_str else set()
    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
    actor_tg = admin_user.get("tg_id")
    actor_role = _get_effective_role(actor_tg, admin_ids, overrides_map)
    target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
    # Самому себе всегда можно; для чужих — проверка ранга
    if actor_tg != user.tg_id and _role_rank(actor_role) <= _role_rank(target_role):
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_rank", status_code=303)
    # Запрет менять баланс главному админу другими
    admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
    if tg_id in admin_ids and actor_tg != user.tg_id:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=role_protected", status_code=303)

    old_balance = user.balance
    user.balance += amount_cents
    session.add(
        BalanceTransaction(
            user_id=user.id,
            admin_tg_id=admin_user.get("tg_id"),
            amount=amount_cents,
            reason=reason,
        )
    )
    session.add(
        AuditLog(
            action=AuditLogAction.balance_credited,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Баланс изменен (web): {old_balance} -> {user.balance} центов. Причина: {reason}",
        )
    )
    await session.commit()
    
    # Отправляем уведомление пользователю
    amount_rub = amount_cents / 100
    if amount_rub > 0:
        notification_text = (
            f"💰 <b>Баланс пополнен</b>\n\n"
            f"Сумма: <b>+{amount_rub:.2f} RUB</b>\n"
            f"Текущий баланс: <b>{user.balance / 100:.2f} RUB</b>"
        )
        if reason:
            notification_text += f"\n\nПричина: {reason}"
    else:
        notification_text = (
            f"💰 <b>Изменение баланса</b>\n\n"
            f"Сумма: <b>{amount_rub:.2f} RUB</b>\n"
            f"Текущий баланс: <b>{user.balance / 100:.2f} RUB</b>"
        )
        if reason:
            notification_text += f"\n\nПричина: {reason}"
    
    asyncio.create_task(_send_user_notification(tg_id, notification_text))
    
    back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/web/users/subscription")
async def admin_web_manage_subscription(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
) -> JSONResponse:
    """Управление подпиской пользователя через админ-панель"""
    from datetime import timedelta, timezone
    
    _require_csrf(request)
    try:
        data = await request.json()
        tg_id = data.get("tg_id")
        action = data.get("action")  # "add", "extend", "remove"
        days = data.get("days")
        reason = data.get("reason", "").strip()
        
        if not tg_id:
            return JSONResponse({"success": False, "error": "Не указан tg_id"})
        if not action or action not in ["add", "extend", "remove"]:
            return JSONResponse({"success": False, "error": "Неверное действие"})
        if action != "remove" and (not days or days < 1 or days > 9999):
            return JSONResponse({"success": False, "error": "Неверное количество дней (от 1 до 9999 дней)"})
        if not reason:
            return JSONResponse({"success": False, "error": "Нужно указать причину"})
        
        user = await session.scalar(select(User).where(User.tg_id == tg_id))
        if not user:
            return JSONResponse({"success": False, "error": "Пользователь не найден"})
        
        actor_tg = admin_user.get("tg_id")
        now = datetime.now(timezone.utc)
        
        if action == "remove":
            # Отменяем все активные подписки
            active_subs = await session.scalars(
                select(Subscription)
                .where(Subscription.user_id == user.id)
                .where(Subscription.status == SubscriptionStatus.active)
            )
            canceled_count = 0
            for sub in active_subs.all():
                sub.status = SubscriptionStatus.canceled
                canceled_count += 1
            
            # Логируем
            session.add(
                AuditLog(
                    action=AuditLogAction.admin_action,
                    user_tg_id=tg_id,
                    admin_tg_id=actor_tg,
                    details=f"Отменена подписка администратором. Причина: {reason}",
                )
            )
            
            await session.commit()
            
            # Обновляем статус пользователя после коммита отмены подписки
            await _update_user_subscription_status(user.id, session)
            await session.commit()
            await session.refresh(user)  # Обновляем данные пользователя в сессии
            
            # Отправляем уведомление пользователю
            notification_text = (
                f"📋 <b>Подписка отменена</b>\n\n"
                f"Ваша подписка была отменена администратором.\n\n"
                f"Причина: {reason}"
            )
            asyncio.create_task(_send_user_notification(tg_id, notification_text))
            
            return JSONResponse({"success": True, "message": f"Отменено подписок: {canceled_count}"})
        
        elif action == "add":
            # Выдаем новую подписку
            starts_at = now
            ends_at = now + timedelta(days=days)
            
            subscription = Subscription(
                user_id=user.id,
                plan_name=f"Подписка на {days} дней (выдана админом)",
                price_cents=0,  # Бесплатная подписка от админа
                currency="RUB",
                status=SubscriptionStatus.active,
                starts_at=starts_at,
                ends_at=ends_at,
            )
            session.add(subscription)
            
            # Логируем
            session.add(
                AuditLog(
                    action=AuditLogAction.subscription_created,
                    user_tg_id=tg_id,
                    admin_tg_id=actor_tg,
                    details=f"Выдана подписка на {days} дней администратором. Действует до: {ends_at.strftime('%d.%m.%Y %H:%M')} (UTC). Причина: {reason}",
                )
            )
            
            await session.commit()
            
            # Обновляем статус пользователя после коммита подписки
            await _update_user_subscription_status(user.id, session)
            await session.commit()
            await session.refresh(user)  # Обновляем данные пользователя в сессии
            
            # Отправляем уведомление пользователю
            try:
                ends_at_moscow = ends_at.astimezone(ZoneInfo("Europe/Moscow"))
                ends_str = ends_at_moscow.strftime("%d.%m.%Y %H:%M")
            except:
                ends_str = ends_at.strftime("%d.%m.%Y %H:%M")
            
            notification_text = (
                f"✅ <b>Подписка выдана</b>\n\n"
                f"Администратор выдал вам подписку на <b>{days} дней</b>.\n\n"
                f"📅 Подписка действует до: <b>{ends_str} МСК</b>\n\n"
                f"Причина: {reason}"
            )
            asyncio.create_task(_send_user_notification(tg_id, notification_text))
            
            return JSONResponse({"success": True, "message": f"Подписка выдана на {days} дней"})
        
        elif action == "extend":
            # Продлеваем существующую подписку или создаем новую
            active_sub = await session.scalar(
                select(Subscription)
                .where(Subscription.user_id == user.id)
                .where(Subscription.status == SubscriptionStatus.active)
                .order_by(Subscription.ends_at.desc().nullslast())
            )
            
            if active_sub and active_sub.ends_at and active_sub.ends_at > now:
                # Продлеваем от текущей даты окончания
                new_ends_at = active_sub.ends_at + timedelta(days=days)
                active_sub.ends_at = new_ends_at
                subscription = active_sub
            else:
                # Создаем новую подписку
                starts_at = now
                new_ends_at = now + timedelta(days=days)
                subscription = Subscription(
                    user_id=user.id,
                    plan_name=f"Подписка на {days} дней (продлена админом)",
                    price_cents=0,
                    currency="RUB",
                    status=SubscriptionStatus.active,
                    starts_at=starts_at,
                    ends_at=new_ends_at,
                )
                session.add(subscription)
            
            # Логируем
            session.add(
                AuditLog(
                    action=AuditLogAction.subscription_created,
                    user_tg_id=tg_id,
                    admin_tg_id=actor_tg,
                    details=f"Продлена подписка на {days} дней администратором. Действует до: {new_ends_at.strftime('%d.%m.%Y %H:%M')} (UTC). Причина: {reason}",
                )
            )
            
            await session.commit()
            
            # Обновляем статус пользователя после коммита подписки
            await _update_user_subscription_status(user.id, session)
            await session.commit()
            await session.refresh(user)  # Обновляем данные пользователя в сессии
            
            # Отправляем уведомление пользователю
            try:
                new_ends_at_moscow = new_ends_at.astimezone(ZoneInfo("Europe/Moscow"))
                ends_str = new_ends_at_moscow.strftime("%d.%m.%Y %H:%M")
            except:
                ends_str = new_ends_at.strftime("%d.%m.%Y %H:%M")
            
            notification_text = (
                f"🔄 <b>Подписка продлена</b>\n\n"
                f"Администратор продлил вашу подписку на <b>{days} дней</b>.\n\n"
                f"📅 Подписка теперь действует до: <b>{ends_str} МСК</b>\n\n"
                f"Причина: {reason}"
            )
            asyncio.create_task(_send_user_notification(tg_id, notification_text))
            
            return JSONResponse({"success": True, "message": f"Подписка продлена на {days} дней"})
        
    except Exception as e:
        import logging
        logging.error(f"Error managing subscription: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)})


@app.post("/admin/web/users/message")
async def admin_web_send_message(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    ticket_id = form.get("ticket_id")
    text = str(form.get("text", "")).strip()
    if not text:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=reason_required", status_code=303)

    settings = get_settings()
    # Для сообщений в тикете используем support-бот, иначе основной
    bot_token = None
    if ticket_id:
        bot_token = os.getenv("SUPPORT_BOT_TOKEN", "") or settings.support_bot_token or settings.admin_token
    if not bot_token:
        bot_token = os.getenv("BOT_TOKEN", "") or settings.admin_token
    if not bot_token:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=bot_token_missing", status_code=303)

    # Проверка, что пользователь существует
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    # Отправляем сообщение от имени бота
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": tg_id, "text": text},
            )
            if r.status_code != 200 or not r.json().get("ok"):
                back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
                return RedirectResponse(url=f"{back}?error=send_failed", status_code=303)
    except Exception:
        back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
        return RedirectResponse(url=f"{back}?error=send_failed", status_code=303)

    # Создаем или обновляем тикет
    ticket = None
    if ticket_id:
        try:
            ticket = await session.scalar(select(Ticket).where(Ticket.id == int(ticket_id)))
        except Exception:
            ticket = None
    if not ticket:
        ticket = await session.scalar(
            select(Ticket).where(Ticket.user_tg_id == tg_id).order_by(Ticket.updated_at.desc())
        )
    now = datetime.utcnow()
    if not ticket:
        ticket = Ticket(user_tg_id=tg_id, status=TicketStatus.open, created_at=now, updated_at=now)
        session.add(ticket)
        await session.flush()
    else:
        ticket.updated_at = now

    # Сохраняем сообщение в тикете
    session.add(
        TicketMessage(
            ticket_id=ticket.id,
            user_tg_id=tg_id,
            direction=MessageDirection.outgoing,
            admin_tg_id=admin_user.get("tg_id"),
            text=text,
        )
    )

    # Логируем как admin_action
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Admin sent message to user: {text}",
        )
    )
    await session.commit()
    back = request.headers.get("referer") or f"/admin/web/users/{tg_id}"
    return RedirectResponse(url=back, status_code=303)


@app.post("/admin/web/tickets/{ticket_id}/status")
async def admin_web_ticket_status(
    ticket_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    # Для статуса тикета убираем строгий CSRF, чтобы кнопки работали без ajax
    form = await request.form()
    action = str(form.get("action", "")).strip()
    if action not in {"close", "take"}:
        return RedirectResponse(url=f"/admin/web/tickets/{ticket_id}?error=bad_action", status_code=303)

    ticket = await session.scalar(select(Ticket).where(Ticket.id == ticket_id))
    if not ticket:
        return RedirectResponse(url="/admin/web/tickets?error=ticket_not_found", status_code=303)
    # Закрытые не открываем/не берем
    if ticket.status == TicketStatus.closed:
        return RedirectResponse(url=f"/admin/web/tickets/{ticket_id}?error=already_closed", status_code=303)

    now = datetime.utcnow()
    system_text = None
    if action == "close":
        ticket.status = TicketStatus.closed
        ticket.closed_at = now
        system_text = f"Тикет закрыт админом {admin_user.get('tg_id')}"
    elif action == "take":
        ticket.status = TicketStatus.in_progress
        system_text = f"Тикет взял админ {admin_user.get('tg_id')}"
    ticket.updated_at = now

    if system_text:
        session.add(
            TicketMessage(
                ticket_id=ticket.id,
                user_tg_id=ticket.user_tg_id,
                direction=MessageDirection.system,
                admin_tg_id=admin_user.get("tg_id"),
                text=system_text,
            )
        )

    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=ticket.user_tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Ticket {ticket.id} action: {action}",
        )
    )
    await session.commit()

    # Пытаемся уведомить пользователя через support-бот
    try:
        bot_token = os.getenv("SUPPORT_BOT_TOKEN", "") or get_settings().support_bot_token
        if bot_token:
            import httpx
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": ticket.user_tg_id, "text": system_text},
                )
    except Exception:
        pass

    return RedirectResponse(url=f"/admin/web/tickets/{ticket_id}", status_code=303)


@app.get("/admin/web/api/tickets/{ticket_id}/messages")
async def admin_api_ticket_messages(
    ticket_id: int,
    since_id: int | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: вернуть сообщения тикета (для автообновления чата в админке)."""
    ticket = await session.scalar(select(Ticket).where(Ticket.id == ticket_id))
    if not ticket:
        raise HTTPException(status_code=404, detail="ticket_not_found")

    stmt = select(TicketMessage).where(TicketMessage.ticket_id == ticket.id)
    if since_id is not None:
        stmt = stmt.where(TicketMessage.id > since_id)
    stmt = stmt.order_by(TicketMessage.id.asc())
    result = await session.scalars(stmt)
    items = []
    for m in result.all():
        items.append(
            {
                "id": m.id,
                "created_at": m.created_at.isoformat(),
                "direction": m.direction.value if hasattr(m.direction, "value") else str(m.direction),
                "user_tg_id": m.user_tg_id,
                "admin_tg_id": m.admin_tg_id,
                "text": m.text,
            }
        )
    return {"messages": items}

@app.post("/tickets/create")
async def create_ticket_endpoint(payload: dict, session: AsyncSession = Depends(get_session)):
    """Создание тикета по запросу от основного бота."""
    tg_id = int(payload.get("tg_id", 0))
    topic = (payload.get("topic") or "").strip()
    if not tg_id or not topic:
        raise HTTPException(status_code=400, detail="tg_id_and_topic_required")

    now = datetime.utcnow()
    ticket = Ticket(
        user_tg_id=tg_id,
        topic=topic,
        status=TicketStatus.new,
        created_at=now,
        updated_at=now,
    )
    session.add(ticket)
    await session.commit()
    await session.refresh(ticket)
    return {"ticket_id": ticket.id}


@app.get("/admin/web/tickets", response_class=HTMLResponse)
async def admin_web_tickets(
    request: Request,
    status: str = Query(default="all"),
    page: int = Query(default=1, ge=1),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)

    status_filter = status.lower()
    if status_filter not in {"all", "open", "closed"}:
        status_filter = "all"

    page_size = 20
    stmt = select(Ticket).order_by(Ticket.updated_at.desc())
    if status_filter == "open":
        stmt = stmt.where(Ticket.status != TicketStatus.closed)
    elif status_filter == "closed":
        stmt = stmt.where(Ticket.status == TicketStatus.closed)

    total = await session.scalar(select(func.count()).select_from(stmt.subquery()))
    result = await session.scalars(stmt.limit(page_size).offset((page - 1) * page_size))
    tickets = result.all()

    has_next = (page * page_size) < (total or 0)
    has_prev = page > 1

    return templates.TemplateResponse(
        "tickets.html",
        {
            "request": request,
            "tickets": tickets,
            "status": status_filter,
            "page": page,
            "has_next": has_next,
            "has_prev": has_prev,
            "admin_user": admin_user,
        },
    )


@app.get("/admin/web/api/tickets")
async def admin_api_tickets(
    status: str = Query(default="all"),
    page: int = Query(default=1, ge=1),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """JSON-API для списка тикетов (используется для автообновления интерфейса)."""
    status_filter = status.lower()
    if status_filter not in {"all", "open", "closed"}:
        status_filter = "all"

    page_size = 20
    stmt = select(Ticket).order_by(Ticket.updated_at.desc())
    if status_filter == "open":
        stmt = stmt.where(Ticket.status != TicketStatus.closed)
    elif status_filter == "closed":
        stmt = stmt.where(Ticket.status == TicketStatus.closed)

    total = await session.scalar(select(func.count()).select_from(stmt.subquery()))
    result = await session.scalars(stmt.limit(page_size).offset((page - 1) * page_size))
    tickets = result.all()

    items: list[dict[str, object]] = []
    # Приводим время к Москве (UTC+3)
    try:
        from zoneinfo import ZoneInfo
        msk_tz = ZoneInfo("Europe/Moscow")
    except Exception:
        msk_tz = None

    for t in tickets:
        if t.updated_at and msk_tz:
            msk_time = t.updated_at.astimezone(msk_tz)
            updated_str = msk_time.strftime("%d.%m.%Y %H:%M")
        else:
            updated_str = t.updated_at.isoformat() if t.updated_at else None
        items.append(
            {
                "id": t.id,
                "user_tg_id": t.user_tg_id,
                "topic": t.topic,
                "status": t.status.value if hasattr(t.status, "value") else str(t.status),
                "updated_at": updated_str,
            }
        )

    has_next = (page * page_size) < (total or 0)
    has_prev = page > 1

    return {
        "tickets": items,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
        "status": status_filter,
    }


@app.get("/admin/web/logs", response_class=HTMLResponse)
async def admin_web_logs(
    request: Request,
    q: str | None = Query(default=None),
    action: str | None = Query(default="all"),
    page: int = Query(default=1, ge=1),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Веб-страница логов с фильтрами."""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)

    action_filter = (action or "all")
    valid_actions = {
        "user_registered",
        "balance_credited",
        "user_blocked",
        "user_unblocked",
        "subscription_created",
        "subscription_activated",
        "payment_processed",
        "payment_created",
        "payment_status_changed",
        "payment_webhook_received",
        "admin_action",
        "backup_action",
    }
    if action_filter not in valid_actions and action_filter != "all":
        action_filter = "all"

    page_size = 50
    stmt = select(AuditLog).order_by(AuditLog.created_at.desc())
    if action_filter != "all":
        stmt = stmt.where(AuditLog.action == AuditLogAction(action_filter))

    if q:
        qq = q.strip()
        conds = []
        if qq.isdigit():
            v = int(qq)
            conds.append(AuditLog.user_tg_id == v)
            conds.append(AuditLog.admin_tg_id == v)
        like = f"%{qq.lower()}%"
        conds.append(func.lower(AuditLog.details).like(like))
        stmt = stmt.where(or_(*conds))

    total = await session.scalar(select(func.count()).select_from(stmt.subquery()))
    result = await session.scalars(stmt.limit(page_size).offset((page - 1) * page_size))
    rows = result.all()

    # Форматируем время в МСК
    try:
        from zoneinfo import ZoneInfo
        msk_tz = ZoneInfo("Europe/Moscow")
    except Exception:
        msk_tz = None

    logs = []
    for log in rows:
        if msk_tz:
            created_str = log.created_at.astimezone(msk_tz).strftime("%d.%m.%Y %H:%M")
        else:
            created_str = log.created_at.isoformat()
        logs.append(
            {
                "id": log.id,
                "action": log.action.value if hasattr(log.action, "value") else str(log.action),
                "user_tg_id": log.user_tg_id,
                "admin_tg_id": log.admin_tg_id,
                "details": log.details,
                "created_at": created_str,
            }
        )

    has_next = (page * page_size) < (total or 0)
    has_prev = page > 1

    return templates.TemplateResponse(
        "logs.html",
        {
            "request": request,
            "logs": logs,
            "page": page,
            "has_next": has_next,
            "has_prev": has_prev,
            "action_filter": action_filter,
            "q": q or "",
            "admin_user": admin_user,
        },
    )

@app.get("/admin/web/tickets/{ticket_id}", response_class=HTMLResponse)
async def admin_web_ticket_detail(
    request: Request,
    ticket_id: int,
    page: int = Query(default=1, ge=1),
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Подробный просмотр тикета с историей сообщений."""
    ticket = await session.scalar(select(Ticket).where(Ticket.id == ticket_id))
    if not ticket:
        return RedirectResponse(url="/admin/web/tickets?error=ticket_not_found", status_code=303)

    user = await session.scalar(select(User).where(User.tg_id == ticket.user_tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/tickets?error=user_not_found", status_code=303)

    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"

    info = {
        "id": ticket.id,
        "user_tg_id": ticket.user_tg_id,
        "topic": ticket.topic,
        "status": ticket.status.value if hasattr(ticket.status, "value") else str(ticket.status),
        "created_at": fmt(ticket.created_at),
        "updated_at": fmt(ticket.updated_at),
        "closed_at": fmt(ticket.closed_at),
    }

    page_size = 30
    total_messages = await session.scalar(
        select(func.count()).select_from(TicketMessage).where(TicketMessage.ticket_id == ticket.id)
    )
    # Если явно не передали page, показываем последнюю страницу, чтобы видеть новые сообщения
    page_param = request.query_params.get("page")
    if not page_param:
        page = max(((total_messages or 0) - 1) // page_size + 1, 1)

    result = await session.scalars(
        select(TicketMessage)
        .where(TicketMessage.ticket_id == ticket.id)
        .order_by(TicketMessage.created_at.asc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    )
    messages_raw = result.all()
    
    # Преобразуем messages в словари с правильным direction (строка вместо enum)
    messages = []
    for m in messages_raw:
        messages.append({
            "id": m.id,
            "created_at": fmt(m.created_at),
            "direction": m.direction.value if hasattr(m.direction, "value") else str(m.direction),
            "user_tg_id": m.user_tg_id,
            "admin_tg_id": m.admin_tg_id,
            "text": m.text,
        })
    
    has_next = (page * page_size) < (total_messages or 0)
    has_prev = page > 1

    return templates.TemplateResponse(
        "ticket_detail.html",
        {
            "request": request,
            "ticket": info,
            "messages": messages,
            "page": page,
            "has_next": has_next,
            "has_prev": has_prev,
            "csrf_token": _get_csrf_token(request),
            "admin_user": admin_user,
            "user": user,
        },
    )


@app.post("/admin/web/users/role")
async def admin_web_set_role(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    _require_csrf(request)
    form = await request.form()
    tg_id = int(str(form.get("tg_id", "0")))
    role = str(form.get("role", "user")).strip()
    reason = str(form.get("reason", "")).strip()
    if not reason:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=reason_required", status_code=303)

    allowed_roles = {"admin", "moderator", "user"}
    admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()

    if tg_id in admin_ids:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=role_protected", status_code=303)
    if role not in allowed_roles:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=bad_role", status_code=303)

    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        return RedirectResponse(url="/admin/web/users?error=user_not_found", status_code=303)

    overrides_result = await session.scalars(select(AdminOverride))
    overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}

    actor_role = _get_effective_role(admin_user.get("tg_id"), admin_ids, overrides_map)
    target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
    if admin_user.get("tg_id") == user.tg_id:
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_self", status_code=303)
    if _role_rank(actor_role) <= _role_rank(target_role):
        return RedirectResponse(url=f"/admin/web/users/{tg_id}?error=forbidden_rank", status_code=303)

    override_obj = await session.scalar(select(AdminOverride).where(AdminOverride.tg_id == tg_id))
    if not override_obj:
        override_obj = AdminOverride(tg_id=tg_id, role=role)
        session.add(override_obj)
    else:
        override_obj.role = role

    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            user_tg_id=tg_id,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Role set to {role} (web). Причина: {reason}",
        )
    )
    await session.commit()
    return RedirectResponse(url=f"/admin/web/users/{tg_id}", status_code=303)


@app.post("/admin/web/users/bulk/block")
async def admin_web_bulk_block_users(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Массовая блокировка пользователей"""
    _require_csrf(request)
    try:
        data = await request.json()
        user_ids = data.get("user_ids", [])
        reason = data.get("reason", "").strip()
        
        if not user_ids:
            return {"success": False, "error": "Не выбраны пользователи"}
        if not reason:
            return {"success": False, "error": "Нужно указать причину"}
        
        admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
        overrides_result = await session.scalars(select(AdminOverride))
        overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
        
        actor_tg = admin_user.get("tg_id")
        actor_role = _get_effective_role(actor_tg, admin_ids, overrides_map)
        
        processed = 0
        for user_id in user_ids:
            user = await session.scalar(select(User).where(User.tg_id == user_id))
            if not user:
                continue
            if actor_tg == user.tg_id:
                continue  # Не блокируем себя
            target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
            if _role_rank(actor_role) <= _role_rank(target_role):
                continue  # Пропускаем если недостаточно прав
            user.is_active = False
            session.add(
                AuditLog(
                    action=AuditLogAction.user_blocked,
                    user_tg_id=user_id,
                    admin_tg_id=actor_tg,
                    details=f"Массовая блокировка (web). Причина: {reason}",
                )
            )
            processed += 1
        
        await session.commit()
        return {"success": True, "processed": processed}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/admin/web/users/bulk/unblock")
async def admin_web_bulk_unblock_users(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Массовая разблокировка пользователей"""
    _require_csrf(request)
    try:
        data = await request.json()
        user_ids = data.get("user_ids", [])
        reason = data.get("reason", "").strip()
        
        if not user_ids:
            return {"success": False, "error": "Не выбраны пользователи"}
        if not reason:
            return {"success": False, "error": "Нужно указать причину"}
        
        admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
        overrides_result = await session.scalars(select(AdminOverride))
        overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
        
        actor_tg = admin_user.get("tg_id")
        actor_role = _get_effective_role(actor_tg, admin_ids, overrides_map)
        
        processed = 0
        for user_id in user_ids:
            user = await session.scalar(select(User).where(User.tg_id == user_id))
            if not user:
                continue
            if actor_tg == user.tg_id:
                continue
            target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
            if _role_rank(actor_role) <= _role_rank(target_role):
                continue
            user.is_active = True
            session.add(
                AuditLog(
                    action=AuditLogAction.user_unblocked,
                    user_tg_id=user_id,
                    admin_tg_id=actor_tg,
                    details=f"Массовая разблокировка (web). Причина: {reason}",
                )
            )
            processed += 1
        
        await session.commit()
        return {"success": True, "processed": processed}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/admin/web/users/bulk/credit")
async def admin_web_bulk_credit_users(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Массовое изменение баланса пользователей"""
    _require_csrf(request)
    try:
        data = await request.json()
        user_ids = data.get("user_ids", [])
        amount = data.get("amount")
        reason = data.get("reason", "").strip()
        
        if not user_ids:
            return {"success": False, "error": "Не выбраны пользователи"}
        if amount is None:
            return {"success": False, "error": "Не указана сумма"}
        if not reason:
            return {"success": False, "error": "Нужно указать причину"}
        
        amount_cents = int(float(amount) * 100)
        admin_ids = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()) if os.getenv("ADMIN_IDS") else set()
        overrides_result = await session.scalars(select(AdminOverride))
        overrides_map = {ov.tg_id: ov.role for ov in overrides_result.all()}
        
        actor_tg = admin_user.get("tg_id")
        actor_role = _get_effective_role(actor_tg, admin_ids, overrides_map)
        
        processed = 0
        for user_id in user_ids:
            user = await session.scalar(select(User).where(User.tg_id == user_id))
            if not user:
                continue
            target_role = _get_effective_role(user.tg_id, admin_ids, overrides_map)
            if actor_tg != user.tg_id and _role_rank(actor_role) <= _role_rank(target_role):
                continue
            old_balance = user.balance
            user.balance += amount_cents
            session.add(
                BalanceTransaction(
                    user_id=user.id,
                    amount=amount_cents,
                    balance_before=old_balance,
                    balance_after=user.balance,
                    reason=f"Массовое изменение (web). Причина: {reason}",
                )
            )
            session.add(
                AuditLog(
                    action=AuditLogAction.balance_credited,
                    user_tg_id=user_id,
                    admin_tg_id=actor_tg,
                    details=f"Массовое изменение баланса: {amount} USD. Причина: {reason}",
                )
            )
            processed += 1
        
        await session.commit()
        return {"success": True, "processed": processed}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/admin/web/settings", response_class=HTMLResponse)
async def admin_web_settings(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Страница настроек системы"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    settings_result = await session.scalars(select(SystemSetting).order_by(SystemSetting.key))
    settings_list = settings_result.all()
    settings_dict = {s.key: s.value for s in settings_list}
    
    csrf_token = _get_csrf_token(request)
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "admin_user": admin_user,
            "settings": settings_dict,
            "csrf_token": csrf_token,
        },
    )


@app.post("/admin/web/settings")
async def admin_web_update_settings(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Обновление настроек системы"""
    _require_csrf(request)
    try:
        form_data = await request.form()
        actor_tg = admin_user.get("tg_id")
        
        for key, value in form_data.items():
            if key == "csrf_token":
                continue
            
            # Нормализация ключей настроек
            if key == "trial_days":
                key = "trial_period_days"
            elif key == "auto_extend_subscription":
                key = "auto_renew_subscription"
            
            # Специальная обработка для сумм в RUB - конвертируем в копейки
            # В форме используются ключи с _cents, но значения вводятся в RUB
            if key in ["referral_reward_referrer_cents", "referral_reward_referred_cents", "min_topup_amount_cents", "max_topup_amount_cents"]:
                try:
                    amount_rub = float(value)
                    value = str(int(amount_rub * 100))
                except (ValueError, TypeError):
                    pass
            
            # Для max_topup_amount_cents - если пусто, удаляем настройку
            if key == "max_topup_amount_cents" and (not value or value.strip() == ""):
                setting = await session.scalar(select(SystemSetting).where(SystemSetting.key == key))
                if setting:
                    await session.delete(setting)
                continue
            
            # Пропускаем пустые значения для текстовых полей
            if not value or (isinstance(value, str) and not value.strip()):
                continue
            
            setting = await session.scalar(select(SystemSetting).where(SystemSetting.key == key))
            if setting:
                setting.value = str(value)
                setting.updated_by_tg_id = actor_tg
                setting.updated_at = datetime.utcnow()
            else:
                setting = SystemSetting(
                    key=key,
                    value=str(value),
                    updated_by_tg_id=actor_tg,
                    updated_at=datetime.utcnow(),
                )
                session.add(setting)
            
            session.add(
                AuditLog(
                    action=AuditLogAction.admin_action,
                    admin_tg_id=actor_tg,
                    details=f"Изменена настройка: {key} = {value}",
                )
            )
        
        await session.commit()
        return RedirectResponse(url="/admin/web/settings?success=1", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin/web/settings?error={str(e)}", status_code=303)


@app.get("/settings/bot")
async def get_bot_settings(
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Получить настройки бота (публичный endpoint)"""
    settings_result = await session.scalars(
        select(SystemSetting).where(
            SystemSetting.key.in_([
                "bot_welcome_message",
                "bot_help_message",
                "min_topup_amount_cents",
                "max_topup_amount_cents",
            ])
        )
    )
    settings_list = settings_result.all()
    settings_dict = {s.key: s.value for s in settings_list}
    
    # Конвертируем суммы из копеек в рубли для удобства
    result = {}
    if "bot_welcome_message" in settings_dict:
        result["welcome_message"] = settings_dict["bot_welcome_message"]
    if "bot_help_message" in settings_dict:
        result["help_message"] = settings_dict["bot_help_message"]
    if "min_topup_amount_cents" in settings_dict:
        try:
            result["min_topup_amount_rub"] = float(settings_dict["min_topup_amount_cents"]) / 100
        except (ValueError, TypeError):
            result["min_topup_amount_rub"] = 1.0
    if "max_topup_amount_cents" in settings_dict:
        try:
            result["max_topup_amount_rub"] = float(settings_dict["max_topup_amount_cents"]) / 100
        except (ValueError, TypeError):
            pass
    
    return result


@app.get("/admin/web/subscription-plans", response_class=HTMLResponse)
async def admin_web_subscription_plans(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Страница управления тарифами подписки"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    await _ensure_default_plans(session)
    
    plans_result = await session.scalars(
        select(SubscriptionPlan)
        .order_by(SubscriptionPlan.display_order, SubscriptionPlan.months)
    )
    plans = plans_result.all()
    
    csrf_token = _get_csrf_token(request)
    return templates.TemplateResponse(
        "subscription_plans.html",
        {
            "request": request,
            "admin_user": admin_user,
            "plans": plans,
            "csrf_token": csrf_token,
        },
    )


@app.post("/admin/web/subscription-plans/update")
async def admin_web_update_subscription_plan(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Обновление тарифа подписки"""
    _require_csrf(request)
    try:
        form_data = await request.form()
        plan_id = int(form_data.get("plan_id"))
        name = form_data.get("name", "").strip()
        description = form_data.get("description", "").strip()
        price_rub = float(form_data.get("price_rub", 0))
        is_active = form_data.get("is_active") == "on"
        display_order = int(form_data.get("display_order", 0))
        
        plan = await session.scalar(select(SubscriptionPlan).where(SubscriptionPlan.id == plan_id))
        if not plan:
            return RedirectResponse(url="/admin/web/subscription-plans?error=plan_not_found", status_code=303)
        
        old_price = plan.price_cents
        plan.name = name
        plan.description = description
        plan.price_cents = int(price_rub * 100)
        plan.is_active = is_active
        plan.display_order = display_order
        plan.updated_at = datetime.now(timezone.utc)
        
        session.add(
            AuditLog(
                action=AuditLogAction.admin_action,
                admin_tg_id=admin_user.get("tg_id"),
                details=f"Обновлен тариф подписки: {plan.name} (ID: {plan_id}). Цена: {old_price / 100:.2f} RUB -> {price_rub:.2f} RUB",
            )
        )
        
        await session.commit()
        return RedirectResponse(url="/admin/web/subscription-plans?success=updated", status_code=303)
    except Exception as e:
        import logging
        logging.error(f"Error updating subscription plan: {e}", exc_info=True)
        return RedirectResponse(url=f"/admin/web/subscription-plans?error={str(e)}", status_code=303)


@app.get("/admin/web/promo-codes", response_class=HTMLResponse)
async def admin_web_promo_codes(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Страница управления промокодами"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    promo_codes_result = await session.scalars(
        select(PromoCode).order_by(PromoCode.created_at.desc())
    )
    promo_codes = promo_codes_result.all()
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    promo_codes_data = []
    for p in promo_codes:
        promo_codes_data.append({
            "id": p.id,
            "code": p.code,
            "discount_percent": p.discount_percent,
            "discount_amount_cents": p.discount_amount_cents,
            "max_uses": p.max_uses,
            "used_count": p.used_count,
            "is_active": p.is_active,
            "valid_from": fmt(p.valid_from) if p.valid_from else "—",
            "valid_until": fmt(p.valid_until) if p.valid_until else "—",
            "created_at": fmt(p.created_at),
        })
    
    csrf_token = _get_csrf_token(request)
    return templates.TemplateResponse(
        "promo_codes.html",
        {
            "request": request,
            "admin_user": admin_user,
            "promo_codes": promo_codes_data,
            "csrf_token": csrf_token,
        },
    )


@app.post("/admin/web/promo-codes/create")
async def admin_web_create_promo_code(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Создание нового промокода"""
    _require_csrf(request)
    try:
        form = await request.form()
        
        code = str(form.get("code", "")).strip().upper()
        discount_type = str(form.get("discount_type", ""))
        discount_value = str(form.get("discount_value", ""))
        max_uses = form.get("max_uses")
        valid_from = form.get("valid_from")
        valid_until = form.get("valid_until")
        description = str(form.get("description", "")).strip()
        
        if not code:
            return RedirectResponse(url="/admin/web/promo-codes?error=code_required", status_code=303)
        
        # Проверяем уникальность кода
        existing = await session.scalar(select(PromoCode).where(PromoCode.code == code))
        if existing:
            return RedirectResponse(url="/admin/web/promo-codes?error=code_exists", status_code=303)
        
        discount_percent = None
        discount_amount_cents = None
        
        if discount_type == "percent":
            try:
                discount_percent = int(float(discount_value))
                if discount_percent < 0 or discount_percent > 100:
                    return RedirectResponse(url="/admin/web/promo-codes?error=invalid_percent", status_code=303)
            except (ValueError, TypeError):
                return RedirectResponse(url="/admin/web/promo-codes?error=invalid_percent", status_code=303)
        elif discount_type == "amount":
            try:
                discount_amount_cents = int(float(discount_value) * 100)
                if discount_amount_cents <= 0:
                    return RedirectResponse(url="/admin/web/promo-codes?error=invalid_amount", status_code=303)
            except (ValueError, TypeError):
                return RedirectResponse(url="/admin/web/promo-codes?error=invalid_amount", status_code=303)
        else:
            return RedirectResponse(url="/admin/web/promo-codes?error=invalid_discount_type", status_code=303)
        
        max_uses_int = None
        if max_uses:
            try:
                max_uses_int = int(max_uses)
                if max_uses_int <= 0:
                    max_uses_int = None
            except (ValueError, TypeError):
                max_uses_int = None
        
        valid_from_dt = None
        if valid_from:
            try:
                from zoneinfo import ZoneInfo
                moscow_tz = ZoneInfo("Europe/Moscow")
                valid_from_dt = datetime.strptime(valid_from, "%Y-%m-%dT%H:%M")
                valid_from_dt = moscow_tz.localize(valid_from_dt).astimezone(ZoneInfo("UTC"))
            except Exception:
                pass
        
        valid_until_dt = None
        if valid_until:
            try:
                from zoneinfo import ZoneInfo
                moscow_tz = ZoneInfo("Europe/Moscow")
                valid_until_dt = datetime.strptime(valid_until, "%Y-%m-%dT%H:%M")
                valid_until_dt = moscow_tz.localize(valid_until_dt).astimezone(ZoneInfo("UTC"))
            except Exception:
                pass
        
        promo = PromoCode(
            code=code,
            discount_percent=discount_percent,
            discount_amount_cents=discount_amount_cents,
            max_uses=max_uses_int,
            used_count=0,
            is_active=True,
            valid_from=valid_from_dt,
            valid_until=valid_until_dt,
            created_by_tg_id=admin_user.get("tg_id"),
            description=description or None,
        )
        session.add(promo)
        
        session.add(
            AuditLog(
                action=AuditLogAction.admin_action,
                admin_tg_id=admin_user.get("tg_id"),
                details=f"Создан промокод {code}",
            )
        )
        
        await session.commit()
        return RedirectResponse(url="/admin/web/promo-codes?success=created", status_code=303)
    except Exception as e:
        import traceback
        error_msg = str(e)[:100]  # Ограничиваем длину сообщения об ошибке
        return RedirectResponse(url=f"/admin/web/promo-codes?error={error_msg}", status_code=303)


@app.get("/admin/web/api/promo-codes/{promo_id}")
async def admin_api_promo_code_detail(
    promo_id: int,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Получить детальную информацию о промокоде"""
    promo = await session.scalar(select(PromoCode).where(PromoCode.id == promo_id))
    if not promo:
        raise HTTPException(status_code=404, detail="promo_code_not_found")
    
    # Получаем список использований
    usages_result = await session.scalars(
        select(PromoCodeUsage)
        .where(PromoCodeUsage.promo_code_id == promo_id)
        .order_by(PromoCodeUsage.used_at.desc())
        .limit(50)
    )
    usages = usages_result.all()
    
    # Получаем информацию о пользователях
    user_ids = [u.user_id for u in usages]
    users_map = {}
    if user_ids:
        users_result = await session.scalars(select(User).where(User.id.in_(user_ids)))
        for u in users_result.all():
            users_map[u.id] = u
    
    # Получаем информацию о создателе
    creator = None
    if promo.created_by_tg_id:
        creator = await session.scalar(select(User).where(User.tg_id == promo.created_by_tg_id))
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    usages_data = []
    for u in usages:
        user = users_map.get(u.user_id)
        usages_data.append({
            "user_tg_id": user.tg_id if user else 0,
            "username": user.username if user else "—",
            "first_name": user.first_name if user else "—",
            "discount_amount": u.discount_amount_cents / 100,
            "used_at": fmt(u.used_at),
        })
    
    return {
        "id": promo.id,
        "code": promo.code,
        "discount_percent": promo.discount_percent,
        "discount_amount_cents": promo.discount_amount_cents,
        "max_uses": promo.max_uses,
        "used_count": promo.used_count,
        "is_active": promo.is_active,
        "valid_from": fmt(promo.valid_from) if promo.valid_from else None,
        "valid_until": fmt(promo.valid_until) if promo.valid_until else None,
        "created_at": fmt(promo.created_at),
        "created_by": {
            "tg_id": creator.tg_id if creator else None,
            "username": creator.username if creator else None,
            "first_name": creator.first_name if creator else None,
        } if creator else None,
        "description": promo.description,
        "usages": usages_data,
    }


@app.post("/admin/web/promo-codes/{promo_id}/toggle")
async def admin_web_toggle_promo_code(
    promo_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Активация/деактивация промокода"""
    _require_csrf(request)
    promo = await session.scalar(select(PromoCode).where(PromoCode.id == promo_id))
    if not promo:
        return RedirectResponse(url="/admin/web/promo-codes?error=not_found", status_code=303)
    
    promo.is_active = not promo.is_active
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Промокод {promo.code} {'активирован' if promo.is_active else 'деактивирован'}",
        )
    )
    await session.commit()
    return RedirectResponse(url="/admin/web/promo-codes?success=toggled", status_code=303)


@app.get("/admin/web/backups", response_class=HTMLResponse)
async def admin_web_backups(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Страница управления резервными копиями"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    # Получаем только завершенные бэкапы (failed и in_progress тоже показываем, но они не удаляются)
    backups_result = await session.scalars(
        select(Backup)
        .where(Backup.backup_type == "database")  # Фильтруем только database бэкапы
        .order_by(Backup.created_at.desc())
        .limit(50)
    )
    backups = backups_result.all()
    
    try:
        from zoneinfo import ZoneInfo
        moscow_tz = ZoneInfo("Europe/Moscow")
        fmt = lambda dt: dt.astimezone(moscow_tz).strftime("%d.%m.%Y %H:%M") if dt else "—"
    except Exception:
        fmt = lambda dt: dt.isoformat()[:16] if dt else "—"
    
    backups_data = []
    for backup in backups:
        file_size_mb = backup.file_size_bytes / (1024 * 1024) if backup.file_size_bytes > 0 else 0
        
        backups_data.append({
            "id": backup.id,
            "backup_type": backup.backup_type,
            "file_path": backup.file_path,
            "file_size_mb": file_size_mb,
            "status": backup.status,
            "error_message": backup.error_message,
            "created_at": fmt(backup.created_at),
            "created_by_tg_id": backup.created_by_tg_id,
        })
    
    return templates.TemplateResponse("backups.html", {
        "request": request,
        "admin_user": admin_user,
        "backups": backups_data,
        "csrf_token": _get_csrf_token(request),
    })


@app.post("/admin/web/backups/create")
async def admin_web_create_backup(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Создание резервной копии вручную"""
    # Проверяем CSRF токен из заголовка или формы
    expected = request.session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token")
    if not provided:
        # Пробуем получить из формы
        try:
            form = await request.form()
            provided = form.get("csrf_token")
        except Exception:
            pass
    
    if not expected or not provided or provided != expected:
        return RedirectResponse(url="/admin/web/backups?error=csrf_forbidden", status_code=303)
    
    # Создаем бэкап в фоне
    asyncio.create_task(_create_database_backup(created_by_tg_id=admin_user.get("tg_id")))
    
    # Логируем действие
    session.add(
        AuditLog(
            action=AuditLogAction.backup_action,
            admin_tg_id=admin_user.get("tg_id"),
            details="Запущено создание резервной копии",
        )
    )
    await session.commit()
    
    return RedirectResponse(url="/admin/web/backups?success=backup_started", status_code=303)


@app.get("/admin/web/backups/{backup_id}/download")
async def admin_web_download_backup(
    backup_id: int,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Скачать резервную копию"""
    backup = await session.scalar(select(Backup).where(Backup.id == backup_id))
    if not backup:
        raise HTTPException(status_code=404, detail="backup_not_found")
    
    if backup.status != "completed":
        raise HTTPException(status_code=400, detail="backup_not_ready")
    
    from pathlib import Path
    backup_path = Path(backup.file_path)
    
    if not backup_path.exists():
        raise HTTPException(status_code=404, detail="backup_file_not_found")
    
    # Логируем скачивание
    session.add(
        AuditLog(
            action=AuditLogAction.backup_action,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Скачан бэкап #{backup_id}",
        )
    )
    await session.commit()
    
    def file_generator():
        with open(backup_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                yield chunk
    
    return StreamingResponse(
        file_generator(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{backup_path.name}"',
        },
    )


@app.post("/admin/web/backups/{backup_id}/delete")
async def admin_web_delete_backup(
    backup_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Удаление резервной копии"""
    # Проверяем CSRF токен
    expected = request.session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token")
    if not provided:
        try:
            form = await request.form()
            provided = form.get("csrf_token")
        except Exception:
            pass
    
    if not expected or not provided or provided != expected:
        return JSONResponse({"success": False, "error": "csrf_forbidden"}, status_code=403)
    
    # Проверяем, что бэкап существует
    backup = await session.scalar(select(Backup).where(Backup.id == backup_id))
    if not backup:
        return JSONResponse({"success": False, "error": "backup_not_found"}, status_code=404)
    
    # Удаляем файл бэкапа, если он существует и это валидный файл
    from pathlib import Path
    if backup.file_path and backup.file_path.strip():
        backup_path = Path(backup.file_path)
        # Проверяем, что путь валидный, существует и это файл (не директория)
        if backup_path.exists() and backup_path.is_file():
            try:
                backup_path.unlink()
                import logging
                logging.info(f"Deleted backup file: {backup_path}")
            except Exception as e:
                import logging
                logging.warning(f"Could not delete backup file {backup_path}: {e}")
        elif backup_path.exists() and backup_path.is_dir():
            import logging
            logging.warning(f"Backup path is a directory, not a file: {backup_path}")
        elif not backup_path.exists():
            import logging
            logging.info(f"Backup file does not exist (may have been deleted already): {backup_path}")
    
    # Удаляем запись из базы данных
    await session.delete(backup)
    
    # Логируем удаление
    session.add(
        AuditLog(
            action=AuditLogAction.backup_action,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Удален бэкап #{backup_id}",
        )
    )
    await session.commit()
    
    return JSONResponse({"success": True, "message": "Backup deleted successfully"})


async def _restore_database_backup(backup_id: int, restored_by_tg_id: int | None = None) -> dict:
    """Восстановление базы данных из резервной копии"""
    import os
    import subprocess
    from pathlib import Path
    
    try:
        # Получаем информацию о бэкапе ДО закрытия соединений
        async with SessionLocal() as session:
            backup = await session.scalar(select(Backup).where(Backup.id == backup_id))
            if not backup:
                return {"success": False, "error": "backup_not_found"}
            
            if backup.status != "completed":
                return {"success": False, "error": "backup_not_ready"}
            
            backup_path = Path(backup.file_path)
            if not backup_path.exists():
                return {"success": False, "error": "backup_file_not_found"}
        
        # Закрываем все активные соединения перед восстановлением
        # Это важно, чтобы избежать конфликтов с типами данных
        await engine.dispose()
        
        # Небольшая задержка для завершения всех операций
        await asyncio.sleep(1)
        
        # Получаем параметры подключения к БД
        db_url = settings.db_url
        if "postgresql" not in db_url:
            # Пересоздаем engine после ошибки
            await recreate_engine()
            return {"success": False, "error": "only_postgresql_supported"}
        
        # Извлекаем параметры из URL
        import re
        match = re.match(r"postgresql\+asyncpg://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", db_url)
        if not match:
            # Пересоздаем engine после ошибки
            await recreate_engine()
            return {"success": False, "error": "invalid_db_url"}
        
        db_user, db_password, db_host, db_port, db_name = match.groups()
        
        env = os.environ.copy()
        env["PGPASSWORD"] = db_password
        
        # Используем pg_restore для восстановления из custom format
        # --clean удалит и пересоздаст объекты схемы, затем восстановит данные в правильном порядке
        import logging
        
        # Отключаем проверку внешних ключей перед восстановлением
        # Это позволит восстановить данные в любом порядке
        try:
            disable_fk_cmd = [
                "psql",
                "-h", db_host,
                "-p", db_port,
                "-U", db_user,
                "-d", db_name,
                "-c", "SET session_replication_role = 'replica';"
            ]
            subprocess.run(disable_fk_cmd, env=env, capture_output=True, timeout=10)
        except Exception as e:
            logging.warning(f"Could not disable FK checks: {e}")
        
        # Используем pg_restore с --clean для полного восстановления
        # --clean удалит и пересоздаст таблицы, затем восстановит данные в правильном порядке
        cmd = [
            "pg_restore",
            "-h", db_host,
            "-p", db_port,
            "-U", db_user,
            "-d", db_name,
            "--clean",  # Очистить объекты перед созданием (удалит таблицы и пересоздаст)
            "--if-exists",  # Не выдавать ошибку если объект не существует
            "--no-owner",  # Не устанавливать владельца объектов
            "--no-privileges",  # Не устанавливать привилегии
            "--verbose",  # Подробный вывод для отладки
            str(backup_path),
        ]
        
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=600  # 10 минут таймаут
        )
        
        # Включаем проверку внешних ключей обратно
        try:
            enable_fk_cmd = [
                "psql",
                "-h", db_host,
                "-p", db_port,
                "-U", db_user,
                "-d", db_name,
                "-c", "SET session_replication_role = 'origin';"
            ]
            subprocess.run(enable_fk_cmd, env=env, capture_output=True, timeout=10)
        except Exception as e:
            logging.warning(f"Could not enable FK checks: {e}")
        
        if result.returncode != 0:
            error_msg = result.stderr[:500] if result.stderr else result.stdout[:500] if result.stdout else "Unknown error"
            # Пересоздаем engine после ошибки
            await recreate_engine()
            return {"success": False, "error": f"pg_restore failed: {error_msg}"}
        
        # Пересоздаем engine и SessionLocal после восстановления
        # Это критически важно для обновления метаданных типов данных
        await recreate_engine()
        
        # Небольшая задержка для стабилизации соединений
        await asyncio.sleep(0.5)
        
        # Логируем восстановление с новым соединением
        # Используем прямой доступ к модулю для получения обновленного SessionLocal
        import core.db.session as session_module
        async with session_module.SessionLocal() as session:
            session.add(
                AuditLog(
                    action=AuditLogAction.backup_action,
                    admin_tg_id=restored_by_tg_id,
                    details=f"Восстановлена база данных из бэкапа #{backup_id}",
                )
            )
            await session.commit()
        
        return {"success": True, "message": "Database restored successfully. Please refresh the page."}
        
    except Exception as e:
        import logging
        logging.error(f"Error restoring backup: {e}")
        
        # Пересоздаем engine после ошибки
        try:
            await recreate_engine()
        except Exception as restore_error:
            logging.error(f"Error recreating engine: {restore_error}")
        
        return {"success": False, "error": str(e)[:500]}


@app.post("/admin/web/backups/{backup_id}/restore")
async def admin_web_restore_backup(
    backup_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Восстановление базы данных из резервной копии"""
    # Проверяем CSRF токен
    expected = request.session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token")
    if not provided:
        try:
            form = await request.form()
            provided = form.get("csrf_token")
        except Exception:
            pass
    
    if not expected or not provided or provided != expected:
        raise HTTPException(status_code=403, detail="csrf_forbidden")
    
    # Проверяем, что бэкап существует и готов
    backup = await session.scalar(select(Backup).where(Backup.id == backup_id))
    if not backup:
        raise HTTPException(status_code=404, detail="backup_not_found")
    
    if backup.status != "completed":
        raise HTTPException(status_code=400, detail="backup_not_ready")
    
    # Запускаем восстановление в фоне (это долгая операция)
    asyncio.create_task(_restore_database_backup(backup_id, restored_by_tg_id=admin_user.get("tg_id")))
    
    return JSONResponse({"success": True, "message": "Восстановление запущено. Это может занять несколько минут."})


async def _generate_vpn_config_for_user_server(user_id: int, server_id: int, session: AsyncSession, expires_at: datetime):
    """Генерирует VPN конфиг для пользователя на указанном сервере"""
    import json
    # Получаем конкретный сервер
    server = await session.scalar(
        select(Server)
        .where(Server.id == server_id)
        .where(Server.is_enabled == True)
        .where(
            (Server.x3ui_api_url.isnot(None)) | (Server.xray_uuid.isnot(None))
        )
    )
    
    if not server:
        raise ValueError(f"Сервер {server_id} не найден или не активен")
    
    user = await session.get(User, user_id)
    if not user:
        return
    
    # Создаем конфиг для сервера
    # Проверяем, нет ли уже активного конфига для этого сервера
    existing = await session.scalar(
        select(VpnCredential)
        .where(VpnCredential.user_id == user_id)
        .where(VpnCredential.server_id == server.id)
        .where(VpnCredential.active == True)
    )
    
    config_text = None
    user_uuid = None
    
    # Если сервер использует API 3x-UI - создаем клиента автоматически
    if server.x3ui_api_url and server.x3ui_username and server.x3ui_password:
        from core.x3ui_api import X3UIAPI
        from core.xray import generate_uuid
        
        config_text = None
        user_uuid = None
        x3ui = None
        
        try:
            x3ui = X3UIAPI(
                api_url=server.x3ui_api_url,
                username=server.x3ui_username,
                password=server.x3ui_password,
            )
            
            # Определяем ID инбаунда (простой подход - требуем указания ID)
            inbound_id = server.x3ui_inbound_id
            
            if not inbound_id:
                # Если ID не указан, пробуем найти автоматически (fallback)
                logger.warning(f"Inbound ID не указан для сервера {server.name}, пытаемся найти автоматически")
                found_inbound = await x3ui.find_first_vless_inbound()
                
                if found_inbound:
                    inbound_id = found_inbound.get("id")
                    logger.info(f"Автоматически найден VLESS Inbound ID {inbound_id} для сервера {server.name}")
                    # Сохраняем найденный ID в базу данных
                    server.x3ui_inbound_id = inbound_id
                    await session.commit()
                else:
                    # Логируем все доступные Inbounds для отладки
                    all_inbounds = await x3ui.list_inbounds()
                    if all_inbounds:
                        inbound_list = []
                        for inb in all_inbounds:
                            inbound_list.append({
                                "id": inb.get("id"),
                                "port": inb.get("port"),
                                "protocol": inb.get("protocol"),
                                "remark": inb.get("remark", ""),
                                "enable": inb.get("enable", False)
                            })
                        logger.error(f"Доступные Inbounds в 3x-UI для сервера {server.name}: {inbound_list}")
                        inbound_info = ", ".join([f"ID:{inb['id']} ({inb['protocol']}, порт:{inb['port']})" for inb in inbound_list])
                        raise ValueError(
                            f"Inbound ID не указан для сервера {server.name} и не найден VLESS Inbound автоматически. "
                            f"Доступные Inbounds: {inbound_info}. "
                            f"Укажите правильный Inbound ID в настройках сервера."
                        )
                    else:
                        logger.error(f"Не удалось получить список Inbounds из 3x-UI для сервера {server.name}")
                        raise ValueError(
                            f"Inbound ID не указан для сервера {server.name} и не удалось получить список Inbounds. "
                            f"Проверьте настройки API 3x-UI (URL: {server.x3ui_api_url}, username: {server.x3ui_username})."
                        )
            
            # Генерируем уникальный email для клиента с tg_id
            client_email = f"tg_{user.tg_id}_server_{server.id}@fiorevpn"
            
            # ВАЖНО: Удаляем существующего клиента перед созданием нового (для regenerate и duplicate email)
            # Игнорируем ошибки, так как клиент может не существовать (например, при первой генерации)
            try:
                deleted = await x3ui.delete_client(inbound_id, client_email)
                if deleted:
                    logger.info(f"Удален существующий клиент {client_email} из Inbound {inbound_id}")
                else:
                    logger.debug(f"Клиент {client_email} не найден в Inbound {inbound_id} (это нормально для новой генерации)")
            except Exception as del_err:
                # Не критичная ошибка - клиент может не существовать
                logger.debug(f"Не удалось удалить клиента {client_email} (возможно, его нет): {del_err}")
            
            # Получаем настройки лимитов из SystemSetting
            limit_ip_setting = await session.scalar(select(SystemSetting).where(SystemSetting.key == "vpn_limit_ip"))
            
            limit_ip = 1  # По умолчанию 1 IP
            if limit_ip_setting:
                try:
                    limit_ip = int(limit_ip_setting.value)
                except (ValueError, TypeError):
                    limit_ip = 1
            
            # Создаем клиента в 3x-UI
            expire_timestamp = int(expires_at.timestamp() * 1000) if expires_at else 0  # 3x-UI использует миллисекунды
            logger.info(
                f"Создание клиента через 3x-UI API для пользователя {user_id} (tg_id: {user.tg_id}) на сервере {server.name} (ID: {server.id}): "
                f"API URL={server.x3ui_api_url}, Inbound ID={inbound_id}, email={client_email}"
            )
            
            try:
                client_data = await x3ui.add_client(
                    inbound_id=inbound_id,
                    email=client_email,
                    uuid=None,  # Автоматическая генерация
                    flow=server.xray_flow or "",
                    expire=expire_timestamp,
                    limit_ip=limit_ip,
                    total_gb=0,  # Без ограничений трафика
                )
            except ConnectionError as e:
                # Специальная обработка ошибок подключения
                error_msg = str(e)
                if "localhost" in server.x3ui_api_url or "127.0.0.1" in server.x3ui_api_url:
                    error_msg = f"Не удалось подключиться к 3x-UI через SSH-туннель. Проверьте, что SSH-туннель запущен и порт доступен. Ошибка: {e}"
                logger.error(f"Ошибка подключения к 3x-UI для сервера {server.name}: {error_msg}")
                raise ValueError(error_msg) from e
            except Exception as e:
                logger.error(f"Ошибка при создании клиента в 3x-UI для сервера {server.name}: {e}")
                raise
            
            if client_data:
                # Получаем UUID созданного клиента
                user_uuid = client_data.get("id") or client_data.get("uuid")
                
                if not user_uuid:
                    logger.warning(f"Не удалось получить UUID для клиента {client_email} из ответа API 3x-UI")
                    raise ValueError(f"Не удалось получить UUID для клиента на сервере {server.name}")
                
                # Генерируем конфиг напрямую из параметров сервера (как в примере ChatGPT)
                from core.xray import generate_vless_config
                config_text = generate_vless_config(
                    user_uuid=user_uuid,
                    server_host=server.host,
                    server_port=server.xray_port or 443,
                    server_uuid=user_uuid,  # Используем UUID пользователя
                    server_flow=server.xray_flow,
                    server_network=server.xray_network or "tcp",
                    server_security=server.xray_security or "tls",
                    server_sni=server.xray_sni,
                    server_reality_public_key=server.xray_reality_public_key,
                    server_reality_short_id=server.xray_reality_short_id,
                    server_path=server.xray_path,
                    server_host_header=server.xray_host,
                    remark=f"{server.name}",
                )
                
                if not config_text:
                    logger.warning(f"Не удалось сгенерировать конфиг для клиента {client_email} на сервере {server.name}")
                    raise ValueError(f"Не удалось сгенерировать конфиг для клиента на сервере {server.name}")
            else:
                # Клиент не был создан (инбаунд не найден или другая ошибка)
                logger.warning(f"Не удалось создать клиента через API 3x-UI для сервера {server.name} (ID: {server.id}, Inbound ID: {server.x3ui_inbound_id})")
                # Если есть UUID, используем fallback
                if server.xray_uuid:
                    logger.info(f"Используем fallback на UUID для сервера {server.name} (клиент не создан)")
                    raise ValueError("INBOUND_NOT_FOUND_FALLBACK_TO_UUID")
                raise ValueError(f"Не удалось создать клиента на сервере {server.name}")
        except ValueError as e:
            error_msg = str(e)
            # Если это специальный сигнал для fallback на UUID
            if error_msg == "INBOUND_NOT_FOUND_FALLBACK_TO_UUID" and server.xray_uuid:
                logger.info(f"Переходим на fallback с UUID для сервера {server.name}")
                # Не устанавливаем config_text и user_uuid, чтобы код перешел к elif server.xray_uuid
                config_text = None
                user_uuid = None
            elif "Inbound не найден" in error_msg and server.xray_uuid:
                # Если Inbound не найден, но есть UUID, используем fallback
                logger.info(f"Переходим на fallback с UUID для сервера {server.name} (Inbound не найден)")
                config_text = None
                user_uuid = None
            else:
                logger.warning(f"Ошибка при создании клиента через API 3x-UI для сервера {server.name}: {e}")
                raise
        except ConnectionError as e:
            # Ошибка подключения (например, SSH-туннель не работает)
            error_msg = str(e)
            logger.error(f"Ошибка подключения к 3x-UI для сервера {server.name}: {error_msg}")
            # Если есть UUID, используем fallback
            if server.xray_uuid:
                logger.info(f"Используем fallback на UUID для сервера {server.name} после ошибки подключения")
                config_text = None
                user_uuid = None
            else:
                # Пробрасываем ошибку с понятным сообщением
                raise ValueError(f"Не удалось подключиться к 3x-UI для сервера {server.name}. {error_msg}")
        except Exception as e:
            import httpx
            # Если это HTTP ошибка от 3x-UI API, пробрасываем её дальше для правильной обработки
            if isinstance(e, (httpx.HTTPStatusError, httpx.RequestError)):
                logger.warning(f"HTTP ошибка при создании клиента через API 3x-UI для сервера {server.name}: {e}")
                raise
            # Если есть UUID, пытаемся использовать fallback
            if server.xray_uuid:
                logger.info(f"Используем fallback на UUID для сервера {server.name} после ошибки API")
                config_text = None
                user_uuid = None
            else:
                logger.warning(f"Ошибка при создании клиента через API 3x-UI для сервера {server.name}: {e}")
                raise
        finally:
            # Закрываем сессию 3x-UI API
            if x3ui:
                try:
                    await x3ui.close()
                except:
                    pass
        
        # Если конфиг не был создан через API, но есть UUID, используем fallback
        if not config_text or not user_uuid:
            if server.xray_uuid:
                logger.info(f"Используем fallback на UUID для сервера {server.name}")
                # Переходим к генерации через UUID
                pass
            else:
                raise ValueError(f"Не удалось сгенерировать конфиг для сервера {server.name}")
    
    # Если API 3x-UI не настроен или не удалось создать через API, используем старый способ с UUID
    if (not config_text or not user_uuid) and server.xray_uuid:
        user_uuid = server.xray_uuid
        config_text = generate_vless_config(
            user_uuid=user_uuid,
            server_host=server.host,
            server_port=server.xray_port or 443,
            server_uuid=server.xray_uuid,
            server_flow=server.xray_flow,
            server_network=server.xray_network or "tcp",
            server_security=server.xray_security or "tls",
            server_sni=server.xray_sni,
            server_reality_public_key=server.xray_reality_public_key,
            server_reality_short_id=server.xray_reality_short_id,
            server_path=server.xray_path,
            server_host_header=server.xray_host,
            remark=f"{server.name}",
        )
    elif not config_text or not user_uuid:
        # Пропускаем серверы без настроек
        raise ValueError(f"Сервер {server.name} не настроен (нет API 3x-UI и UUID)")
        
    if not config_text:
        raise ValueError(f"Не удалось сгенерировать конфиг для сервера {server.name}")
    
    if existing:
        # Обновляем существующий конфиг
        existing.expires_at = expires_at
        existing.config_text = config_text
        if user_uuid:
            existing.user_uuid = user_uuid
    else:
        # Создаем новый конфиг
        credential = VpnCredential(
            user_id=user_id,
            server_id=server.id,
            user_uuid=user_uuid,
            config_text=config_text,
            active=True,
            expires_at=expires_at,
        )
        session.add(credential)
    
    await session.commit()


# API endpoints для управления серверами
@app.get("/admin/web/servers", response_class=HTMLResponse)
async def admin_web_servers(
    request: Request,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """Страница управления серверами"""
    if not templates:
        return HTMLResponse(content="<h1>Шаблоны не настроены</h1>", status_code=500)
    
    servers_result = await session.scalars(
        select(Server)
        .order_by(Server.created_at.desc())
    )
    servers = servers_result.all()
    
    # Получаем последние статусы серверов
    servers_with_status = []
    for server in servers:
        last_status = await session.scalar(
            select(ServerStatus)
            .where(ServerStatus.server_id == server.id)
            .order_by(ServerStatus.checked_at.desc())
        )
        servers_with_status.append({
            "server": server,
            "status": last_status,
        })
    
    csrf_token = _get_csrf_token(request)
    return templates.TemplateResponse(
        "servers.html",
        {
            "request": request,
            "admin_user": admin_user,
            "servers": servers_with_status,
            "csrf_token": csrf_token,
        },
    )


@app.get("/admin/web/api/servers")
async def admin_api_servers(
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: Получить список серверов"""
    servers_result = await session.scalars(
        select(Server)
        .order_by(Server.created_at.desc())
    )
    servers = servers_result.all()
    
    # Получаем последние статусы
    servers_list = []
    for server in servers:
        last_status = await session.scalar(
            select(ServerStatus)
            .where(ServerStatus.server_id == server.id)
            .order_by(ServerStatus.checked_at.desc())
        )
        server_dict = {
            "id": server.id,
            "name": server.name,
            "host": server.host,
            "location": server.location,
            "is_enabled": server.is_enabled,
            "capacity": server.capacity,
            "created_at": server.created_at.isoformat(),
            "xray_port": server.xray_port,
            "xray_uuid": server.xray_uuid,
            "xray_flow": server.xray_flow,
            "xray_network": server.xray_network,
            "xray_security": server.xray_security,
            "xray_sni": server.xray_sni,
            "xray_reality_public_key": server.xray_reality_public_key,
            "xray_reality_short_id": server.xray_reality_short_id,
            "xray_path": server.xray_path,
            "xray_host": server.xray_host,
            "x3ui_api_url": server.x3ui_api_url,
            "x3ui_username": server.x3ui_username,
            "x3ui_password": server.x3ui_password,
            "x3ui_inbound_id": server.x3ui_inbound_id,
        }
        if last_status:
            # Время уже в UTC, на клиенте добавим +3 часа через JavaScript
            server_dict["status"] = {
                "is_online": last_status.is_online,
                "response_time_ms": last_status.response_time_ms,
                "connection_speed_mbps": float(last_status.connection_speed_mbps) if last_status.connection_speed_mbps else None,
                "checked_at": last_status.checked_at.isoformat() if last_status.checked_at else None,
            }
        servers_list.append(server_dict)
    
    return {"servers": servers_list}


@app.get("/admin/web/api/servers/{server_id}/inbounds")
async def admin_api_get_server_inbounds(
    server_id: int,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: Получить список Inbounds из 3x-UI для сервера"""
    server = await session.get(Server, server_id)
    if not server:
        raise HTTPException(status_code=404, detail="server_not_found")
    
    if not server.x3ui_api_url or not server.x3ui_username or not server.x3ui_password:
        raise HTTPException(status_code=400, detail="3x_ui_api_not_configured")
    
    x3ui = None
    try:
        from core.x3ui_api import X3UIAPI
        x3ui = X3UIAPI(
            api_url=server.x3ui_api_url,
            username=server.x3ui_username,
            password=server.x3ui_password,
        )
        
        inbounds = await x3ui.list_inbounds()
        if not inbounds:
            return {"inbounds": [], "error": "Не удалось получить список Inbounds. Проверьте настройки API 3x-UI."}
        
        # Форматируем список Inbounds
        inbounds_list = []
        for inbound in inbounds:
            inbounds_list.append({
                "id": inbound.get("id"),
                "port": inbound.get("port"),
                "protocol": inbound.get("protocol", "").lower(),
                "remark": inbound.get("remark", ""),
                "enable": inbound.get("enable", False),
                "is_vless": inbound.get("protocol", "").lower() == "vless",
            })
        
        return {"inbounds": inbounds_list}
    except Exception as e:
        import logging
        logging.error(f"Ошибка при получении списка Inbounds для сервера {server_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении списка Inbounds: {str(e)}")
    finally:
        if x3ui:
            try:
                await x3ui.close()
            except:
                pass


@app.post("/admin/web/api/servers")
async def admin_api_create_server(
    payload: ServerCreateIn,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: Создать сервер"""
    # Проверяем уникальность имени
    existing = await session.scalar(select(Server).where(Server.name == payload.name))
    if existing:
        raise HTTPException(status_code=400, detail="server_name_exists")
    
    server = Server(
        name=payload.name,
        host=payload.host,
        location=payload.location,
        is_enabled=payload.is_enabled,
        capacity=payload.capacity,
        xray_port=payload.xray_port,
        xray_uuid=payload.xray_uuid,
        xray_flow=payload.xray_flow,
        xray_network=payload.xray_network,
        xray_security=payload.xray_security,
        xray_sni=payload.xray_sni,
        xray_reality_public_key=payload.xray_reality_public_key,
        xray_reality_short_id=payload.xray_reality_short_id,
        xray_path=payload.xray_path,
        xray_host=payload.xray_host,
        x3ui_api_url=payload.x3ui_api_url,
        x3ui_username=payload.x3ui_username,
        x3ui_password=payload.x3ui_password,
        x3ui_inbound_id=payload.x3ui_inbound_id,
    )
    session.add(server)
    await session.commit()
    await session.refresh(server)
    
    # Логируем создание
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Создан сервер: {server.name} ({server.host})",
        )
    )
    await session.commit()
    
    return {"id": server.id, "name": server.name}


@app.put("/admin/web/api/servers/{server_id}")
async def admin_api_update_server(
    server_id: int,
    payload: ServerUpdateIn,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: Обновить сервер"""
    server = await session.get(Server, server_id)
    if not server:
        raise HTTPException(status_code=404, detail="server_not_found")
    
    # Обновляем поля
    if payload.name is not None:
        # Проверяем уникальность имени
        existing = await session.scalar(select(Server).where(Server.name == payload.name).where(Server.id != server_id))
        if existing:
            raise HTTPException(status_code=400, detail="server_name_exists")
        server.name = payload.name
    
    if payload.host is not None:
        server.host = payload.host
    if payload.location is not None:
        server.location = payload.location
    if payload.is_enabled is not None:
        server.is_enabled = payload.is_enabled
    if payload.capacity is not None:
        server.capacity = payload.capacity
    if payload.xray_port is not None:
        server.xray_port = payload.xray_port
    if payload.xray_uuid is not None:
        server.xray_uuid = payload.xray_uuid
    if payload.xray_flow is not None:
        server.xray_flow = payload.xray_flow
    if payload.xray_network is not None:
        server.xray_network = payload.xray_network
    if payload.xray_security is not None:
        server.xray_security = payload.xray_security
    if payload.xray_sni is not None:
        server.xray_sni = payload.xray_sni
    if payload.xray_reality_public_key is not None:
        server.xray_reality_public_key = payload.xray_reality_public_key
    if payload.xray_reality_short_id is not None:
        server.xray_reality_short_id = payload.xray_reality_short_id
    if payload.xray_path is not None:
        server.xray_path = payload.xray_path
    if payload.xray_host is not None:
        server.xray_host = payload.xray_host
    if payload.x3ui_api_url is not None:
        server.x3ui_api_url = payload.x3ui_api_url
    if payload.x3ui_username is not None:
        server.x3ui_username = payload.x3ui_username
    if payload.x3ui_password is not None:
        server.x3ui_password = payload.x3ui_password
    if payload.x3ui_inbound_id is not None:
        server.x3ui_inbound_id = payload.x3ui_inbound_id
    
    await session.commit()
    
    # Логируем обновление
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Обновлен сервер: {server.name} (ID: {server_id})",
        )
    )
    await session.commit()
    
    return {"id": server.id, "name": server.name}


@app.delete("/admin/web/api/servers/{server_id}")
async def admin_api_delete_server(
    server_id: int,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: Удалить сервер"""
    server = await session.get(Server, server_id)
    if not server:
        raise HTTPException(status_code=404, detail="server_not_found")
    
    server_name = server.name
    await session.delete(server)
    await session.commit()
    
    # Логируем удаление
    session.add(
        AuditLog(
            action=AuditLogAction.admin_action,
            admin_tg_id=admin_user.get("tg_id"),
            details=f"Удален сервер: {server_name} (ID: {server_id})",
        )
    )
    await session.commit()
    
    return {"success": True}


@app.post("/admin/web/api/servers/{server_id}/check")
async def admin_api_check_server(
    server_id: int,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
):
    """API: Проверить состояние сервера вручную"""
    try:
        server = await session.get(Server, server_id)
        if not server:
            raise HTTPException(status_code=404, detail="server_not_found")
        
        logger.info(f"Ручная проверка сервера {server.name} (ID: {server_id})")
        
        # Проверяем состояние (используем тот же метод, что и автопроверка)
        status_result = await _check_server_status(server)
        
        logger.info(f"Результат ручной проверки {server.name}: online={status_result['is_online']}, time={status_result.get('response_time_ms')}ms")
        
        # Сохраняем статус
        status = ServerStatus(
            server_id=server.id,
            is_online=status_result["is_online"],
            response_time_ms=status_result["response_time_ms"],
            error_message=status_result["error_message"],
        )
        session.add(status)
        await session.commit()
        
        # Время уже в UTC, на клиенте добавим +3 часа через JavaScript
        return {
            "server_id": server.id,
            "server_name": server.name,
            "status": {
                "is_online": status_result["is_online"],
                "response_time_ms": status_result["response_time_ms"],
                "error_message": status_result["error_message"],
                "checked_at": status.checked_at.isoformat() if status.checked_at else None,
            }
        }
    except Exception as e:
        logger.error(f"Ошибка при ручной проверке сервера {server_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка проверки: {str(e)}")


@app.get("/admin/web/api/servers/{server_id}/history")
async def admin_api_server_history(
    server_id: int,
    session: AsyncSession = Depends(get_session),
    admin_user: dict = Depends(_require_web_admin),
    limit: int = 100,
):
    """API: Получить историю статусов сервера для графика"""
    server = await session.get(Server, server_id)
    if not server:
        raise HTTPException(status_code=404, detail="server_not_found")
    
    # Получаем последние N записей статусов
    statuses_result = await session.scalars(
        select(ServerStatus)
        .where(ServerStatus.server_id == server.id)
        .order_by(ServerStatus.checked_at.desc())
        .limit(limit)
    )
    statuses = statuses_result.all()
    
    # Формируем данные для графика (в обратном порядке - от старых к новым)
    history = []
    for status in reversed(statuses):
        history.append({
            "checked_at": status.checked_at.isoformat() if status.checked_at else None,
            "is_online": status.is_online,
            "response_time_ms": status.response_time_ms,
            "error_message": status.error_message,
        })
    
    return {
        "server_id": server.id,
        "server_name": server.name,
        "history": history,
    }


@app.get("/servers/available")
async def get_available_servers(
    session: AsyncSession = Depends(get_session),
):
    """Получить список доступных серверов для пользователей"""
    servers_result = await session.scalars(
        select(Server)
        .where(Server.is_enabled == True)
        .where(
            (Server.x3ui_api_url.isnot(None)) | (Server.xray_uuid.isnot(None))
        )  # Сервер должен иметь либо API 3x-UI, либо UUID
        .order_by(Server.name)
    )
    servers = servers_result.all()
    
    # Получаем последние статусы
    servers_list = []
    for server in servers:
        last_status = await session.scalar(
            select(ServerStatus)
            .where(ServerStatus.server_id == server.id)
            .order_by(ServerStatus.checked_at.desc())
        )
        server_dict = {
            "id": server.id,
            "name": server.name,
            "host": server.host,
            "location": server.location,
            "capacity": server.capacity,
        }
        if last_status:
            server_dict["status"] = {
                "is_online": last_status.is_online,
                "response_time_ms": last_status.response_time_ms,
            }
        else:
            server_dict["status"] = {
                "is_online": False,
                "response_time_ms": None,
            }
        servers_list.append(server_dict)
    
    return {"servers": servers_list}


@app.post("/users/{tg_id}/select-server")
async def select_server_for_user(
    tg_id: int,
    payload: dict,
    session: AsyncSession = Depends(get_session),
):
    """Установить выбранный сервер для пользователя"""
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    server_id = payload.get("server_id")
    if not server_id:
        raise HTTPException(status_code=400, detail="server_id_required")
    
    # Проверяем, что сервер существует и активен
    server = await session.scalar(
        select(Server)
        .where(Server.id == server_id)
        .where(Server.is_enabled == True)
    )
    if not server:
        raise HTTPException(status_code=404, detail="server_not_found")
    
    old_server_id = user.selected_server_id
    
    # Если меняем сервер — удаляем клиента со старого сервера
    # ВАЖНО: Это не должно блокировать смену сервера, поэтому все ошибки логируются, но не прерывают процесс
    if old_server_id and old_server_id != server_id:
        old_server = await session.get(Server, old_server_id)
        if old_server and old_server.x3ui_api_url and old_server.x3ui_username and old_server.x3ui_password:
            try:
                from core.x3ui_api import X3UIAPI
                import asyncio
                
                # Устанавливаем таймаут для удаления клиента со старого сервера (5 секунд)
                async def delete_old_client():
                    x3ui = None
                    try:
                        x3ui = X3UIAPI(
                            api_url=old_server.x3ui_api_url,
                            username=old_server.x3ui_username,
                            password=old_server.x3ui_password,
                        )
                        client_email = f"tg_{user.tg_id}_server_{old_server.id}@fiorevpn"
                        inbound_id = old_server.x3ui_inbound_id
                        if inbound_id:
                            # Пытаемся удалить с таймаутом
                            deleted = await asyncio.wait_for(
                                x3ui.delete_client(inbound_id, client_email),
                                timeout=5.0
                            )
                            if deleted:
                                logger.info(f"Удален клиент {client_email} со старого сервера {old_server.name}")
                            else:
                                logger.info(f"Клиент {client_email} не найден на старом сервере {old_server.name} (возможно, уже удален)")
                    except asyncio.TimeoutError:
                        logger.warning(f"Таймаут при удалении клиента со старого сервера {old_server.name}")
                    except Exception as e:
                        logger.warning(f"Не удалось удалить клиента со старого сервера {old_server.name}: {e}")
                    finally:
                        if x3ui:
                            try:
                                await x3ui.close()
                            except:
                                pass
                
                # Запускаем удаление в фоне, не блокируя смену сервера
                asyncio.create_task(delete_old_client())
                logger.info(f"Запущена фоновая задача удаления клиента со старого сервера {old_server.name}")
            except Exception as e:
                logger.warning(f"Ошибка при инициализации удаления клиента со старого сервера {old_server.name}: {e}")
        
        # Деактивируем старые VPN credentials для старого сервера
        old_credentials = await session.scalars(
            select(VpnCredential)
            .where(VpnCredential.user_id == user.id)
            .where(VpnCredential.server_id == old_server_id)
            .where(VpnCredential.active == True)
        )
        for cred in old_credentials:
            cred.active = False
        logger.info(f"Деактивированы старые VPN credentials пользователя {user.tg_id} для сервера {old_server_id}")
    
    # Устанавливаем выбранный сервер
    user.selected_server_id = server_id
    await session.commit()
    
    return {"success": True, "server_id": server_id, "server_name": server.name}


@app.get("/users/{tg_id}/vpn-key")
async def get_user_vpn_key(
    tg_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Получить VPN ключ пользователя"""
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    if not user.selected_server_id:
        return {"key": None, "server_name": None}
    
    # Получаем активный ключ для выбранного сервера
    credential = await session.scalar(
        select(VpnCredential)
        .where(VpnCredential.user_id == user.id)
        .where(VpnCredential.server_id == user.selected_server_id)
        .where(VpnCredential.active == True)
        .order_by(VpnCredential.created_at.desc())
    )
    
    server = await session.get(Server, user.selected_server_id)
    server_name = server.name if server else None
    
    if credential and credential.config_text:
        return {"key": credential.config_text, "server_name": server_name}
    
    return {"key": None, "server_name": server_name}


@app.post("/users/{tg_id}/vpn-key/generate")
async def generate_user_vpn_key(
    tg_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Сгенерировать VPN ключ для пользователя"""
    # Получаем payload из запроса
    try:
        payload = await request.json() if request.headers.get("content-type") == "application/json" else {}
    except:
        payload = {}
    
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    if not user.selected_server_id:
        raise HTTPException(status_code=400, detail="server_not_selected")
    
    # Проверяем активную подписку
    if not user.has_active_subscription or not user.subscription_ends_at:
        raise HTTPException(status_code=403, detail="no_active_subscription")
    
    from datetime import datetime, timezone
    if user.subscription_ends_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=403, detail="subscription_expired")
    
    # Получаем сервер
    server = await session.get(Server, user.selected_server_id)
    if not server or not server.is_enabled:
        raise HTTPException(status_code=404, detail="server_not_found")
    
    # Проверяем, есть ли уже активный ключ
    existing_active = await session.scalar(
        select(VpnCredential)
        .where(VpnCredential.user_id == user.id)
        .where(VpnCredential.server_id == user.selected_server_id)
        .where(VpnCredential.active == True)
        .order_by(VpnCredential.created_at.desc())
    )
    
    # Проверяем параметр regenerate (для "Сменить ключ")
    regenerate = payload.get("regenerate", False) if payload else False
    
    # Если пользователь уже имеет активный ключ и не запрошена регенерация, возвращаем 400
    if existing_active and existing_active.config_text and not regenerate:
        raise HTTPException(status_code=400, detail="user_already_has_key")
    
    # Если запрошена регенерация, деактивируем старый ключ
    if existing_active and regenerate:
        existing_active.active = False
        await session.commit()
    
    # Генерируем новый ключ
    try:
        import httpx
        await _generate_vpn_config_for_user_server(user.id, user.selected_server_id, session, user.subscription_ends_at)
    except ValueError as e:
        error_msg = str(e)
        # 503 - 3x-UI недоступен
        if "не удалось получить список Inbounds" in error_msg or "не удалось получить" in error_msg.lower() or "не удалось подключиться" in error_msg.lower():
            raise HTTPException(status_code=503, detail=f"3x_ui_unavailable: {error_msg}")
        # 400 - ошибка конфигурации сервера
        if "Inbound не найден" in error_msg or "не настроен" in error_msg:
            raise HTTPException(status_code=400, detail=f"server_configuration_error: {error_msg}")
        # 500 - реальный баг
        import logging
        logging.error(f"Ошибка при генерации ключа для пользователя {user.tg_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"key_generation_failed: {error_msg}")
    except httpx.HTTPStatusError as e:
        # 503 - 3x-UI недоступен (HTTP ошибки от API)
        if e.response.status_code in (404, 503, 502, 504):
            raise HTTPException(status_code=503, detail=f"3x_ui_unavailable: HTTP {e.response.status_code}")
        # 500 - другие HTTP ошибки
        import logging
        logging.error(f"HTTP ошибка при генерации ключа для пользователя {user.tg_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="key_generation_failed")
    except httpx.RequestError as e:
        # 503 - 3x-UI недоступен (сетевые ошибки)
        import logging
        logging.warning(f"Сетевая ошибка при подключении к 3x-UI для пользователя {user.tg_id}: {e}")
        raise HTTPException(status_code=503, detail="3x_ui_unavailable: Не удалось подключиться к 3x-UI")
    except Exception as e:
        # 500 - только реальные баги
        import logging
        logging.error(f"Неожиданная ошибка при генерации ключа для пользователя {user.tg_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="key_generation_failed")
    
    # Получаем созданный ключ
    credential = await session.scalar(
        select(VpnCredential)
        .where(VpnCredential.user_id == user.id)
        .where(VpnCredential.server_id == user.selected_server_id)
        .where(VpnCredential.active == True)
        .order_by(VpnCredential.created_at.desc())
    )
    
    if not credential or not credential.config_text:
        import logging
        logging.error(f"Ключ не был создан для пользователя {user.tg_id}, хотя ошибок не было")
        raise HTTPException(status_code=500, detail="key_generation_failed")
    
    return {"key": credential.config_text, "server_name": server.name}


@app.get("/users/{tg_id}/vpn-configs")
async def get_user_vpn_configs(
    tg_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Получить VPN конфиги пользователя"""
    user = await session.scalar(select(User).where(User.tg_id == tg_id))
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    
    # Проверяем, есть ли активная подписка
    if not user.has_active_subscription:
        raise HTTPException(status_code=403, detail="no_active_subscription")
    
    # Получаем активные конфиги
    credentials = await session.scalars(
        select(VpnCredential)
        .where(VpnCredential.user_id == user.id)
        .where(VpnCredential.active == True)
        .options(selectinload(VpnCredential.server))
    )
    configs_list = credentials.all()
    
    configs = []
    for cred in configs_list:
        configs.append({
            "id": cred.id,
            "server_name": cred.server.name if cred.server else None,
            "config": cred.config_text,
            "expires_at": cred.expires_at.isoformat() if cred.expires_at else None,
        })
    
    return {"configs": configs}


# Catch-all роут для всех необработанных путей (должен быть последним)
@app.get("/{path:path}")
async def catch_all(request: Request, path: str):
    """Перехватывает все необработанные GET запросы и показывает 404"""
    url_path = request.url.path
    
    # Для API endpoints возвращаем JSON (исключаем /admin/web пути)
    is_api_path = (
        url_path.startswith("/api/") or 
        url_path.startswith("/subscriptions/") or 
        url_path.startswith("/payments/") or 
        (url_path.startswith("/users/") and not url_path.startswith("/admin/web")) or 
        url_path.startswith("/promo-codes/") or
        (url_path.startswith("/tickets/") and not url_path.startswith("/admin/web")) or
        url_path.startswith("/health") or
        url_path.startswith("/support/")
    )
    
    if is_api_path:
        raise HTTPException(status_code=404, detail="Not found")
    
    # Для всех остальных путей (включая /admin/web/*) показываем красивую 404
    if templates:
        return templates.TemplateResponse(
            "404.html",
            {"request": request},
            status_code=404
        )
    # Если шаблоны не загружены, возвращаем простой текст
    return HTMLResponse(
        content="<h1>404 - Страница не найдена</h1><p><a href='/admin/login'>Перейти в админ-панель</a></p>",
        status_code=404
    )

