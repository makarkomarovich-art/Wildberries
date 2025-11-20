-- ========================================================================
-- Исправление CASCADE для week_rows и week_stats
-- ========================================================================
-- Проверяем и исправляем внешние ключи, чтобы при удалении из week_reports
-- автоматически удалялись связанные записи из week_rows и week_stats
-- ========================================================================

-- Удаляем старые внешние ключи (если они есть без CASCADE)
DO $$
BEGIN
    -- Удаляем FK для week_rows, если он существует
    IF EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'week_rows_realizationreport_id_fkey'
    ) THEN
        ALTER TABLE week_rows 
        DROP CONSTRAINT week_rows_realizationreport_id_fkey;
    END IF;
    
    -- Удаляем FK для week_stats, если он существует
    IF EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'week_stats_realizationreport_id_fkey'
    ) THEN
        ALTER TABLE week_stats 
        DROP CONSTRAINT week_stats_realizationreport_id_fkey;
    END IF;
END $$;

-- Создаем правильные внешние ключи с CASCADE
ALTER TABLE week_rows 
ADD CONSTRAINT week_rows_realizationreport_id_fkey 
FOREIGN KEY (realizationreport_id) 
REFERENCES week_reports(realizationreport_id) 
ON DELETE CASCADE;

ALTER TABLE week_stats 
ADD CONSTRAINT week_stats_realizationreport_id_fkey 
FOREIGN KEY (realizationreport_id) 
REFERENCES week_reports(realizationreport_id) 
ON DELETE CASCADE;

-- Проверяем, что CASCADE настроен правильно
DO $$
DECLARE
    fk_cascade_rows TEXT;
    fk_cascade_stats TEXT;
BEGIN
    -- Проверяем week_rows
    SELECT confdeltype INTO fk_cascade_rows
    FROM pg_constraint 
    WHERE conname = 'week_rows_realizationreport_id_fkey';
    
    -- Проверяем week_stats
    SELECT confdeltype INTO fk_cascade_stats
    FROM pg_constraint 
    WHERE conname = 'week_stats_realizationreport_id_fkey';
    
    IF fk_cascade_rows = 'c' AND fk_cascade_stats = 'c' THEN
        RAISE NOTICE '✅ CASCADE настроен правильно для обеих таблиц';
    ELSE
        RAISE WARNING '⚠️  Проблема с настройкой CASCADE: week_rows=%, week_stats=%', 
            fk_cascade_rows, fk_cascade_stats;
    END IF;
END $$;


