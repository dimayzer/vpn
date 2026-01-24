#!/bin/bash
set -e

# Если указаны переменные для SSH-туннеля, запускаем его
if [ -n "$X3UI_SSH_HOST" ] && [ -n "$X3UI_SSH_USER" ] && [ -n "$X3UI_SSH_KEY" ]; then
    echo "🔐 Настройка SSH-туннеля к 3x-UI..."
    
    # Устанавливаем права на ключ
    chmod 600 "$X3UI_SSH_KEY"
    
    # Запускаем autossh в фоне
    autossh -M 0 \
        -N \
        -f \
        -i "$X3UI_SSH_KEY" \
        -o StrictHostKeyChecking=no \
        -o ServerAliveInterval=30 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        -L 127.0.0.1:38868:127.0.0.1:38868 \
        "${X3UI_SSH_USER}@${X3UI_SSH_HOST}" \
        > /dev/null 2>&1 &
    
    echo "✅ SSH-туннель запущен: localhost:38868 -> ${X3UI_SSH_USER}@${X3UI_SSH_HOST}:38868"
    
    # Ждем немного, чтобы туннель установился
    sleep 2
else
    echo "ℹ️ SSH-туннель не настроен (X3UI_SSH_HOST, X3UI_SSH_USER, X3UI_SSH_KEY не указаны)"
fi

# Запускаем основное приложение
exec "$@"

