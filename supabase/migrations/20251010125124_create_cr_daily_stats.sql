-- Создание таблицы cr_daily_stats для статистики CR (Conversion Rate)
-- Объединенная миграция: создание с финальной структурой

CREATE TABLE cr_daily_stats (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Связи и ключи
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  nm_id BIGINT NOT NULL,
  vendor_code TEXT NOT NULL,

  -- Бизнес-дата: к какому дню относятся метрики
  -- Источник: Python вычисляет today/yesterday по Europe/Moscow
  -- selectedPeriod → today, previousPeriod → yesterday
  date_of_period DATE NOT NULL,

  -- Метрики: счетчики (все допускают NULL)
  open_card_count INTEGER,
  add_to_cart_count INTEGER,
  orders_count INTEGER,
  cancel_count INTEGER,

  -- Метрики: суммы
  orders_sum_rub NUMERIC(14,2),

  -- Остатки на складах
  stocks_mp INTEGER,
  stocks_wb INTEGER,

  -- Конверсии
  add_to_cart_percent NUMERIC(5,2),
  cart_to_order_percent NUMERIC(5,2),

  -- Агрегаты (вычисляются в Python)
  order_price NUMERIC(14,2),   -- orders_sum_rub / orders_count (NULL если count=0/NULL)

  -- Технические метки (управляются PostgreSQL)
  created_at TIMESTAMPTZ DEFAULT NOW(),  -- когда создана
  updated_at TIMESTAMPTZ DEFAULT NOW()   -- когда обновлена
);

-- Уникальность: одна запись на артикул в день
CREATE UNIQUE INDEX ux_cr_daily ON cr_daily_stats (nm_id, date_of_period);

-- Индексы для быстрого поиска
CREATE INDEX idx_cr_nm_date ON cr_daily_stats (nm_id, date_of_period);
CREATE INDEX idx_cr_date ON cr_daily_stats (date_of_period);
CREATE INDEX idx_cr_product_id ON cr_daily_stats (product_id);
CREATE INDEX idx_cr_vendor_code ON cr_daily_stats (vendor_code);
