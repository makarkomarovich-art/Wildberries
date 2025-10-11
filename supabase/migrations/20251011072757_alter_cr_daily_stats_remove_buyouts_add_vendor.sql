-- Добавляем vendor_code
ALTER TABLE cr_daily_stats 
ADD COLUMN vendor_code TEXT;

-- Удаляем buyout поля
ALTER TABLE cr_daily_stats 
DROP COLUMN IF EXISTS buyouts_count,
DROP COLUMN IF EXISTS buyouts_sum_rub,
DROP COLUMN IF EXISTS buyout_price,
DROP COLUMN IF EXISTS buyouts_percent;

-- Индекс на vendor_code
CREATE INDEX idx_cr_vendor_code ON cr_daily_stats(vendor_code);

-- Заполняем vendor_code для существующих записей из таблицы products
UPDATE cr_daily_stats 
SET vendor_code = products.vendor_code
FROM products
WHERE cr_daily_stats.nm_id = products.nm_id
  AND cr_daily_stats.vendor_code IS NULL;

-- Делаем NOT NULL после заполнения
ALTER TABLE cr_daily_stats ALTER COLUMN vendor_code SET NOT NULL;