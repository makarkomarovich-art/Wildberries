CREATE TYPE cr_period AS ENUM ('current', 'previous');

CREATE TABLE cr_daily_stats (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- связи и ключи
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  nm_id BIGINT NOT NULL,

  -- Бизнес-дата: к какому дню относятся метрики
  -- Источник: Python вычисляет today/yesterday по Europe/Moscow
  -- selectedPeriod → today, previousPeriod → yesterday
  date_of_period DATE NOT NULL,

  -- метрики (все допускают NULL по ТЗ)
  open_card_count INTEGER,
  add_to_cart_count INTEGER,
  orders_count INTEGER,
  orders_sum_rub NUMERIC(14,2),
  buyouts_count INTEGER,
  buyouts_sum_rub NUMERIC(14,2),
  cancel_count INTEGER,
  stocks_mp INTEGER,
  stocks_wb INTEGER,

  add_to_cart_percent NUMERIC(5,2),
  cart_to_order_percent NUMERIC(5,2),
  buyouts_percent NUMERIC(5,2),

  -- агрегаты
  order_price NUMERIC(14,2),   -- orders_sum_rub / orders_count (NULL если count=0/NULL)
  buyout_price NUMERIC(14,2),  -- buyouts_sum_rub / buyouts_count (NULL если count=0/NULL)

  -- Технические метки (управляются PostgreSQL)
  created_at TIMESTAMPTZ DEFAULT NOW(),  -- когда создана
  updated_at TIMESTAMPTZ DEFAULT NOW()   -- когда обновлена (нужен триггер)
);

-- Уникальность: одна запись на артикул в день
CREATE UNIQUE INDEX ux_cr_daily ON cr_daily_stats (nm_id, date_of_period);

-- Индексы для поиска
CREATE INDEX idx_cr_nm_date ON cr_daily_stats (nm_id, date_of_period);
CREATE INDEX idx_cr_date ON cr_daily_stats (date_of_period);
CREATE INDEX idx_cr_product_id ON cr_daily_stats (product_id);