-- ========================================================================
-- Создание таблицы supplies_to_warehouses для хранения информации о поставках
-- ========================================================================
-- Включает:
-- 1. Таблицу supplies_to_warehouses
-- 2. Индексы
-- 3. Триггер для автоматического обновления updated_at
-- ========================================================================

-- ========================================================================
-- 1. Создание таблицы supplies_to_warehouses
-- ========================================================================
CREATE TABLE supplies_to_warehouses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Связи
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  
  -- Основные данные из API /incomes
  income_id INTEGER NOT NULL,
  nm_id BIGINT NOT NULL,
  supplier_article TEXT NOT NULL,
  barcode TEXT NOT NULL,
  tech_size TEXT,
  quantity INTEGER NOT NULL,
  warehouse_name TEXT NOT NULL,
  
  -- Даты
  date TIMESTAMPTZ NOT NULL,
  last_change_date TIMESTAMPTZ NOT NULL,
  
  -- Дополнительные поля
  number TEXT,  -- номер УПД (может быть пустым)
  
  -- Коэффициент логистики (из curl supplyDetails)
  delivery_and_storage_expr NUMERIC,
  
  -- Технические метки
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Уникальность: одна запись на пару (income_id, nm_id)
CREATE UNIQUE INDEX ux_supplies_income_nm ON supplies_to_warehouses (income_id, nm_id);

-- Индексы для быстрого поиска
CREATE INDEX idx_supplies_income_id ON supplies_to_warehouses (income_id);
CREATE INDEX idx_supplies_nm_id ON supplies_to_warehouses (nm_id);
CREATE INDEX idx_supplies_product_id ON supplies_to_warehouses (product_id);

-- ========================================================================
-- 2. Создание триггера для автоматического обновления updated_at
-- ========================================================================
-- Используем существующую функцию update_updated_at_column()

DROP TRIGGER IF EXISTS update_supplies_to_warehouses_updated_at ON supplies_to_warehouses;

CREATE TRIGGER update_supplies_to_warehouses_updated_at
    BEFORE UPDATE ON supplies_to_warehouses
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ========================================================================
-- 3. Комментарии
-- ========================================================================
COMMENT ON TABLE supplies_to_warehouses IS 'Информация о принятых товарах на складах WB';
COMMENT ON COLUMN supplies_to_warehouses.income_id IS 'Номер поставки из API /incomes';
COMMENT ON COLUMN supplies_to_warehouses.delivery_and_storage_expr IS 'Коэффициент логистики из supplyDetails';
COMMENT ON COLUMN supplies_to_warehouses.number IS 'Номер УПД (может быть пустым)';
COMMENT ON COLUMN supplies_to_warehouses.tech_size IS 'Размер товара (строка)';

COMMENT ON TRIGGER update_supplies_to_warehouses_updated_at ON supplies_to_warehouses 
IS 'Автоматически обновляет updated_at при UPDATE записи';