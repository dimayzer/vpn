#!/bin/bash
# Автоматическая настройка SSH-туннелей из конфигурационного файла

set -e

CONFIG_FILE="${1:-ssh-tunnels.conf}"
PROJECT_DIR="/root/fiorevpn"
SYSTEMD_DIR="/etc/systemd/system"

echo "🔐 Автоматическая настройка SSH-туннелей из $CONFIG_FILE..."

# Проверяем, что мы root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Пожалуйста, запустите скрипт от root: sudo $0 [config_file]"
    exit 1
fi

# Проверяем наличие конфигурационного файла
if [ ! -f "$PROJECT_DIR/$CONFIG_FILE" ]; then
    echo "❌ Конфигурационный файл не найден: $PROJECT_DIR/$CONFIG_FILE"
    echo "ℹ️ Создайте файл $CONFIG_FILE с конфигурацией туннелей"
    exit 1
fi

# Читаем конфигурацию
tunnel_count=0
while IFS='|' read -r server_name local_port remote_host remote_port ssh_user ssh_key ssh_host || [ -n "$server_name" ]; do
    # Пропускаем пустые строки и комментарии
    [[ -z "$server_name" || "$server_name" =~ ^# ]] && continue
    
    tunnel_count=$((tunnel_count + 1)
    service_name="x3ui-tunnel-${server_name,,}"  # lowercase
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📋 Настройка туннеля для $server_name"
    echo "   Локальный порт: $local_port"
    echo "   Удаленный: $remote_host:$remote_port на $ssh_host"
    echo "   SSH ключ: $ssh_key"
    
    # Проверяем наличие SSH-ключа
    if [ ! -f "$ssh_key" ]; then
        echo "⚠️ SSH-ключ не найден: $ssh_key"
        echo "   Создайте ключ: ssh-keygen -t ed25519 -f $ssh_key -N \"\""
        echo "   Скопируйте на сервер: ssh-copy-id -i ${ssh_key}.pub $ssh_user@$ssh_host"
        read -p "Продолжить без этого туннеля? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
        continue
    fi
    
    # Устанавливаем права на ключ
    chmod 600 "$ssh_key"
    
    # Создаем systemd сервис
    cat > "$SYSTEMD_DIR/$service_name.service" << EOF
[Unit]
Description=SSH Tunnel to 3x-UI $server_name
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/ssh -N -L 0.0.0.0:$local_port:$remote_host:$remote_port -i $ssh_key -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=3 $ssh_user@$ssh_host
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    
    echo "✅ Создан systemd сервис: $service_name.service"
    
done < "$PROJECT_DIR/$CONFIG_FILE"

if [ $tunnel_count -eq 0 ]; then
    echo "❌ Не найдено ни одного туннеля в конфигурации"
    exit 1
fi

# Перезагружаем systemd
echo ""
echo "🔄 Перезагрузка systemd..."
systemctl daemon-reload

# Включаем и запускаем все сервисы
echo "🚀 Запуск туннелей..."
for service_file in "$SYSTEMD_DIR"/x3ui-tunnel-*.service; do
    if [ -f "$service_file" ]; then
        service_name=$(basename "$service_file" .service)
        echo "   Включение $service_name..."
        systemctl enable "$service_name" 2>/dev/null || true
        systemctl restart "$service_name" 2>/dev/null || true
    fi
done

# Ждем немного для запуска
sleep 2

# Проверяем статус
echo ""
echo "📊 Статус туннелей:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for service_file in "$SYSTEMD_DIR"/x3ui-tunnel-*.service; do
    if [ -f "$service_file" ]; then
        service_name=$(basename "$service_file" .service)
        echo ""
        systemctl status "$service_name" --no-pager -l | head -n 10
    fi
done

echo ""
echo "✅ Настройка завершена! Настроено туннелей: $tunnel_count"
echo ""
echo "📋 Полезные команды:"
echo "   systemctl status x3ui-tunnel-*    # Статус всех туннелей"
echo "   systemctl restart x3ui-tunnel-*  # Перезапуск всех туннелей"
echo "   ss -tulpn | grep -E '38868|38869|38870'  # Проверка портов"

