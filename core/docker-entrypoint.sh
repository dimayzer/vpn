#!/bin/bash
set -e

# SSH-туннели должны быть запущены на хосте, а не в контейнере
# Проверяем доступность всех туннелей через host.docker.internal

# Список портов для проверки (можно расширить через переменные окружения)
TUNNEL_PORTS="${TUNNEL_PORTS:-38868 38869}"

echo "🔍 Проверка доступности SSH-туннелей через host.docker.internal..."

check_port() {
    local port=$1
    local available=false
    
    # Способ 1: Проверка через curl
    if curl -s --connect-timeout 2 --max-time 3 -o /dev/null -w "%{http_code}" http://host.docker.internal:$port > /dev/null 2>&1; then
        available=true
    fi
    
    # Способ 2: Проверка через nc (netcat)
    if ! $available && command -v nc >/dev/null 2>&1; then
        if nc -z -w 2 host.docker.internal $port 2>/dev/null; then
            available=true
        fi
    fi
    
    # Способ 3: Проверка через /dev/tcp (bash builtin)
    if ! $available; then
        if timeout 2 bash -c "echo > /dev/tcp/host.docker.internal/$port" 2>/dev/null; then
            available=true
        fi
    fi
    
    if $available; then
        echo "   ✅ Порт $port доступен"
        return 0
    else
        echo "   ⚠️ Порт $port недоступен"
        return 1
    fi
}

# Проверяем все порты
all_available=true
for port in $TUNNEL_PORTS; do
    if ! check_port $port; then
        all_available=false
    fi
done

if $all_available; then
    echo "✅ Все SSH-туннели доступны"
else
    echo "⚠️ Предупреждение: Некоторые туннели недоступны"
    echo "ℹ️ Убедитесь, что SSH-туннели запущены на хосте и слушают на 0.0.0.0"
    echo "ℹ️ Проверка на хосте: ss -tulpn | grep -E '$(echo $TUNNEL_PORTS | tr ' ' '|')'"
    echo "ℹ️ Используйте скрипт: sudo ~/fiorevpn/setup-ssh-tunnels-auto.sh"
fi

# Запускаем основное приложение
exec "$@"

