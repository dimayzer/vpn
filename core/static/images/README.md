# Изображения для брендинга fioreVPN

Поместите сюда следующий файл:

1. **logo.png** - Логотип сервиса (аватарка с щитом и замком)
   - Рекомендуемый размер: 512x512px или больше
   - Формат: PNG с прозрачным фоном
   - Будет использоваться на всех страницах админки и как favicon

После добавления файла перезапустите контейнер `core`:
```bash
docker compose -f docker-compose.prod.yml restart core
```

Или пересоберите образ:
```bash
docker compose -f docker-compose.prod.yml up -d --build core
```

