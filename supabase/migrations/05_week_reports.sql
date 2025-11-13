-- ========================================================================
-- Создание таблицы week_reports для хранения агрегированных данных weekly reports
-- ========================================================================
-- Включает:
-- 1. Таблицу week_reports
-- 2. Индексы
-- 3. Триггер для автоматического обновления updated_at
-- ========================================================================


-- ========================================================================
-- 1. Создание последовательности для serial_id
-- ========================================================================
CREATE SEQUENCE IF NOT EXISTS week_reports_serial_id_seq START 1;

-- ========================================================================
-- 2. Создание таблицы week_reports
-- ========================================================================
CREATE TABLE week_reports (
  -- PK
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Базовые атрибуты отчета
  serial_id INTEGER DEFAULT nextval('week_reports_serial_id_seq'),
  report_type TEXT,
  realizationreport_id INTEGER NOT NULL UNIQUE,
  date_from DATE NOT NULL,
  date_to DATE NOT NULL,
  
  -- Агрегированные метрики по количеству
  quantity_sells_total INTEGER,           -- Продажи (Продажа + Возврат с минусом)
  quantity_return_total INTEGER,          -- Возвраты
  cancels_total INTEGER,                  -- Отмены (return_amount Логистика - quantity_return_total)
  
  -- Агрегированные метрики по ценам
  retail_price_total NUMERIC(14,2),       -- Продажи (Продажа + Возврат с минусом)
  retail_amount_total NUMERIC(14,2),      -- Продажи (Продажа + Возврат с минусом)
  rub_discountWB_both_total NUMERIC(14,2), -- Скидка WB (retail_price_total - retail_amount_total)
  perc_discountWB_both_total NUMERIC(5,2), -- % скидки WB (rub_discountWB_both_total / retail_price_total)
  
  -- Агрегированные метрики по выплатам
  ppvz_for_pay_total NUMERIC(14,2),       -- К перечислению (Продажа + Возврат с минусом)
  rub_commision_both_total NUMERIC(14,2), -- Комиссия WB (retail_price_total - ppvz_for_pay_total)
  perc_commisian_both_total NUMERIC(5,2), -- % комиссии WB (rub_commision_both_total / retail_price_total)
  
  -- Агрегированные метрики по логистике
  delivery_amount_total NUMERIC(14,2),    -- Сумма доставки (Логистика)
  return_amount_total NUMERIC(14,2),      -- Сумма возвратов (Логистика)
  delivery_rub_total NUMERIC(14,2),       -- Стоимость доставки (Логистика)
  
  -- Агрегированные метрики по доп. услугам
  penalty_total NUMERIC(14,2),            -- Штрафы
  storage_fee_total NUMERIC(14,2),        -- Хранение
  deduction_total NUMERIC(14,2),          -- Удержания
  acceptance_total NUMERIC(14,2),         -- Платная приемка
  
  -- Технические метки (управляются PostgreSQL)
  created_at TIMESTAMPTZ DEFAULT NOW(),   -- когда создана
  updated_at TIMESTAMPTZ DEFAULT NOW()    -- когда обновлена
);

-- Индексы для быстрого поиска
CREATE INDEX idx_week_reports_realizationreport_id ON week_reports (realizationreport_id);

-- Связываем последовательность с полем serial_id
ALTER TABLE week_reports ALTER COLUMN serial_id SET DEFAULT nextval('week_reports_serial_id_seq');
