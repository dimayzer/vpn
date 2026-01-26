#!/bin/bash
# Скрипт для настройки SSH-туннелей для 3x-UI

set -e

echo "🔐 Настройка SSH-туннелей для 3x-UI..."

# Проверяем, что мы root
if [ "$EUID" -ne 0 ]; then 
    echo "❌ Пожалуйста, запустите скрипт от root: sudo $0"
    exit 1
fi

# Директория проекта
PROJECT_DIR="/root/fiorevpn"
SYSTEMD_DIR="/etc/systemd/system"

# Проверяем наличие SSH-ключей
if [ ! -f "$PROJECT_DIR/ssh/x3ui_key" ]; then
    echo "❌ SSH-ключ не найден: $PROJECT_DIR/ssh/x3ui_key"
    echo "ℹ️ Создайте ключ: ssh-keygen -t ed25519 -f $PROJECT_DIR/ssh/x3ui_key -N \"\""
    exit 1
fi

# Устанавливаем права на ключ
chmod 600 "$PROJECT_DIR/ssh/x3ui_key"

# Копируем systemd сервисы
echo "📋 Копирование systemd сервисов..."

# Сервис для первого туннеля
if [ -f "$PROJECT_DIR/systemd/x3ui-tunnel-1.service" ]; then
    cp "$PROJECT_DIR/systemd/x3ui-tunnel-1.service" "$SYSTEMD_DIR/"
    echo "✅ Скопирован x3ui-tunnel-1.service"
else
    echo "⚠️ Файл x3ui-tunnel-1.service не найден, создаем базовый..."
    cat > "$SYSTEMD_DIR/x3ui-tunnel-1.service" << 'EOF'
[Unit]
Description=SSH Tunnel to 3x-UI Server 1
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/ssh -N -L 0.0.0.0:38868:127.0.0.1:38868 -i /root/fiorevpn/ssh/x3ui_key -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=3 root@62.133.60.47
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
fi

# Сервис для второго туннеля (если нужен)
if [ -f "$PROJECT_DIR/systemd/x3ui-tunnel-2.service" ]; then
    echo "⚠️ Обнаружен x3ui-tunnel-2.service"
    echo "ℹ️ Убедитесь, что в файле указаны правильные параметры:"
    echo "   - IP адрес второго сервера"
    echo "   - Порт (например, 38869)"
    echo "   - Путь к SSH-ключу (например, x3ui_key_2)"
    read -p "Установить второй туннель? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cp "$PROJECT_DIR/systemd/x3ui-tunnel-2.service" "$SYSTEMD_DIR/"
        echo "✅ Скопирован x3ui-tunnel-2.service"
    fi
fi

# Перезагружаем systemd
echo "🔄 Перезагрузка systemd..."
systemctl daemon-reload

# Включаем и запускаем сервисы
echo "🚀 Запуск туннелей..."

systemctl enable x3ui-tunnel-1
systemctl start x3ui-tunnel-1

if systemctl list-unit-files | grep -q x3ui-tunnel-2.service; then
    systemctl enable x3ui-tunnel-2
    systemctl start x3ui-tunnel-2
fi

# Проверяем статус
echo ""
echo "📊 Статус туннелей:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
systemctl status x3ui-tunnel-1 --no-pager -l

if systemctl list-units | grep -q x3ui-tunnel-2.service; then
    echo ""
    systemctl status x3ui-tunnel-2 --no-pager -l
fi

echo ""
echo "✅ Настройка завершена!"
echo ""
echo "📋 Полезные команды:"
echo "   systemctl status x3ui-tunnel-1    # Статус первого туннеля"
echo "   systemctl status x3ui-tunnel-2     # Статус второго туннеля"
echo "   systemctl restart x3ui-tunnel-1   # Перезапуск первого туннеля"
echo "   systemctl restart x3ui-tunnel-2   # Перезапуск второго туннеля"
echo "   journalctl -u x3ui-tunnel-1 -f    # Логи первого туннеля"
echo "   journalctl -u x3ui-tunnel-2 -f    # Логи второго туннеля"


