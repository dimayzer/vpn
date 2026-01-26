-- Миграция для переименования колонки months в days в таблице subscription_plans
-- Выполните этот скрипт в базе данных после обновления кода

-- Проверяем, существует ли таблица subscription_plans
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'subscription_plans') THEN
        RAISE NOTICE 'Таблица subscription_plans не существует. Создайте её сначала через SQLAlchemy.';
        RETURN;
    END IF;
END $$;

-- Проверяем, существует ли колонка months
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='subscription_plans' AND column_name='months') THEN
        RAISE NOTICE 'Колонка months не найдена. Возможно, миграция уже выполнена или таблица создана с колонкой days.';
        RETURN;
    END IF;
    
    -- Проверяем, существует ли уже колонка days
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='subscription_plans' AND column_name='days') THEN
        RAISE NOTICE 'Колонка days уже существует. Удаляем старую колонку months.';
        -- Удаляем уникальное ограничение на months, если оно есть
        ALTER TABLE subscription_plans DROP CONSTRAINT IF EXISTS subscription_plans_months_key;
        -- Удаляем колонку months
        ALTER TABLE subscription_plans DROP COLUMN IF EXISTS months;
        RAISE NOTICE 'Колонка months удалена.';
        RETURN;
    END IF;
    
    -- Добавляем новую колонку days
    ALTER TABLE subscription_plans ADD COLUMN days INTEGER;
    RAISE NOTICE 'Добавлена колонка days';
    
    -- Конвертируем значения: months * 30 = days (приблизительно)
    -- Для точности: 1 месяц = 30 дней, 3 месяца = 90 дней, 6 месяцев = 180 дней, 12 месяцев = 365 дней
    UPDATE subscription_plans SET days = months * 30 WHERE days IS NULL;
    RAISE NOTICE 'Значения конвертированы из months в days (months * 30)';
    
    -- Делаем колонку days NOT NULL
    ALTER TABLE subscription_plans ALTER COLUMN days SET NOT NULL;
    RAISE NOTICE 'Колонка days установлена как NOT NULL';
    
    -- Добавляем уникальное ограничение на days
    ALTER TABLE subscription_plans ADD CONSTRAINT subscription_plans_days_key UNIQUE (days);
    RAISE NOTICE 'Добавлено уникальное ограничение на days';
    
    -- Удаляем уникальное ограничение на months, если оно есть
    ALTER TABLE subscription_plans DROP CONSTRAINT IF EXISTS subscription_plans_months_key;
    RAISE NOTICE 'Удалено уникальное ограничение на months';
    
    -- Удаляем старую колонку months
    ALTER TABLE subscription_plans DROP COLUMN months;
    RAISE NOTICE 'Колонка months удалена';
    
    RAISE NOTICE 'Миграция завершена успешно!';
END $$;

-- Проверяем результат
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'subscription_plans' 
    AND column_name IN ('days', 'months')
ORDER BY column_name;


