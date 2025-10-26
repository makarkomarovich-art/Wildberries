-- ========================================================================
-- Создание таблицы orders для хранения информации о заказах
-- ========================================================================
-- Включает:
-- 1. Таблицу orders
-- 2. Индексы
-- 3. Триггер для автоматического обновления updated_at
-- ========================================================================

-- ========================================================================
-- 1. Создание таблицы orders
-- ========================================================================
CREATE TABLE orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Связи
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  
  -- Уникальный ID заказа из API
  srid TEXT NOT NULL UNIQUE,
  
  -- Основные данные из API /orders
  date TIMESTAMPTZ NOT NULL,
  last_change_date TIMESTAMPTZ NOT NULL,
  
  -- Адреса и склады
  warehouse_name TEXT NOT NULL,
  warehouse_type TEXT,
  country_name TEXT,
  oblast_okrug_name TEXT,
  region_name TEXT,
  
  -- Информация о товаре
  nm_id BIGINT NOT NULL,
  barcode TEXT NOT NULL,
  supplier_article TEXT NOT NULL,
  tech_size TEXT,
  category TEXT,
  subject TEXT,
  brand TEXT,
  
  -- Информация о поставке
  income_id INTEGER NOT NULL,
  delivery_and_storage_expr NUMERIC,
  
  -- Флаги
  is_supply BOOLEAN NOT NULL DEFAULT false,
  is_realization BOOLEAN NOT NULL DEFAULT false,
  is_cancel BOOLEAN NOT NULL DEFAULT false,
  
  -- Цены
  total_price NUMERIC NOT NULL,
  discount_percent NUMERIC NOT NULL,
  spp NUMERIC NOT NULL,
  finished_price NUMERIC NOT NULL,
  price_with_disc NUMERIC NOT NULL,
  
  -- Дополнительные поля
  cancel_date TIMESTAMPTZ,
  sticker TEXT,
  g_number TEXT,
  
  -- Технические метки
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Индексы для быстрого поиска
CREATE INDEX idx_orders_product_id ON orders (product_id);
CREATE INDEX idx_orders_income_id ON orders (income_id);
CREATE INDEX idx_orders_nm_id ON orders (nm_id);
CREATE INDEX idx_orders_date ON orders (date);
CREATE INDEX idx_orders_last_change_date ON orders (last_change_date);
CREATE INDEX idx_orders_srid ON orders (srid);

-- ========================================================================
-- 2. Создание триггера для автоматического обновления updated_at
-- ========================================================================
DROP TRIGGER IF EXISTS update_orders_updated_at ON orders;

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ========================================================================
-- 3. Комментарии
-- ========================================================================
COMMENT ON TABLE orders IS 'Информация о заказах Wildberries';
COMMENT ON COLUMN orders.srid IS 'Уникальный ID заказа (srid из API)';
COMMENT ON COLUMN orders.delivery_and_storage_expr IS 'Коэффициент логистики (NULL по умолчанию если не найден в supplies)';
COMMENT ON COLUMN orders.income_id IS 'FK на supplies_to_warehouses по income_id';

COMMENT ON TRIGGER update_orders_updated_at ON orders 
IS 'Автоматически обновляет updated_at при UPDATE записи';
