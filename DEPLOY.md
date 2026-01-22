# Деплой fioreVPN на SprintHost

## 1. Подготовка сервера

```bash
# Установка Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
sudo usermod -aG docker $USER

# Установка Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Выйдите и войдите снова
exit
```

## 2. Загрузка проекта на сервер

**Вариант A: Через Git (рекомендуется)**

```bash
# Создайте директорию
mkdir -p ~/fiorevpn
cd ~/fiorevpn

# Клонируйте репозиторий
git clone <your-repo-url> .
```

**Вариант B: Через SCP/SFTP**

```bash
# Создайте директорию
mkdir -p ~/fiorevpn
cd ~/fiorevpn

# Загрузите файлы через scp с вашего компьютера:
# scp -r "C:\Users\79161\Documents\vpn bot\*" username@server-ip:~/fiorevpn/
```

## 3. Настройка переменных окружения

```bash
cd ~/fiorevpn
cp env.sample .env
nano .env  # заполните все переменные
```

**Обязательно укажите:**
- `BOT_TOKEN` - токен Telegram бота
- `ADMIN_IDS` - ID администраторов (через запятую)
- `POSTGRES_PASSWORD` - сильный пароль для БД
- `ADMIN_TOKEN` - секретный токен
- `CRYPTOBOT_TOKEN` - токен CryptoBot
- `BOT_USERNAME` - username бота
- `SECRET_KEY` - секретный ключ (сгенерируйте: `openssl rand -hex 32`)

## 4. Запуск проекта

```bash
cd ~/fiorevpn

# Создайте директорию для бэкапов
mkdir -p backups

# Запустите проект
docker-compose -f docker-compose.prod.yml up -d --build

# Проверьте статус
docker-compose -f docker-compose.prod.yml ps

# Проверьте логи
docker-compose -f docker-compose.prod.yml logs -f
```

## 5. Проверка работоспособности

```bash
# Проверка API
curl http://localhost:8000/health

# Проверка бота
# Отправьте /start боту в Telegram

# Просмотр логов
docker-compose -f docker-compose.prod.yml logs -f core
docker-compose -f docker-compose.prod.yml logs -f bot
```

## Полезные команды

```bash
# Просмотр логов
docker-compose -f docker-compose.prod.yml logs -f

# Перезапуск сервисов
docker-compose -f docker-compose.prod.yml restart core
docker-compose -f docker-compose.prod.yml restart bot

# Остановка
docker-compose -f docker-compose.prod.yml down

# Обновление проекта
docker-compose -f docker-compose.prod.yml down
# обновите код
docker-compose -f docker-compose.prod.yml up -d --build

# Бэкап БД
docker-compose -f docker-compose.prod.yml exec db pg_dump -U user vpn > backup_$(date +%Y%m%d_%H%M%S).sql
```

## Настройка домена (опционально)

Если нужно настроить домен для админ-панели:

```bash
# Установите Nginx
sudo apt update
sudo apt install nginx

# Создайте конфигурацию
sudo nano /etc/nginx/sites-available/fiorevpn
```

Вставьте:
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

```bash
# Активируйте конфигурацию
sudo ln -s /etc/nginx/sites-available/fiorevpn /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Настройте SSL
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

## Решение проблем

**Бот не отвечает:**
```bash
docker-compose -f docker-compose.prod.yml logs bot
# Проверьте BOT_TOKEN в .env
```

**Ошибки БД:**
```bash
docker-compose -f docker-compose.prod.yml logs db
# Проверьте DB_URL и POSTGRES_PASSWORD в .env
```

**API не работает:**
```bash
docker-compose -f docker-compose.prod.yml logs core
curl http://localhost:8000/health
```

