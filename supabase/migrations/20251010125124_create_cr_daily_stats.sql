CREATE TYPE cr_period AS ENUM ('current', 'previous');

CREATE TABLE cr_daily_stats (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- связи и ключи
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  nm_id BIGINT NOT NULL,

  -- бизнес-дата метрики и период
  date_of_period DATE NOT NULL,          -- дата по Europe/Moscow (сегодня или вчера)

  -- служебная метка, когда снят замер
  snapshot_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),

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

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Уникальность по (nm_id, date_of_period) - одна запись на артикул в день
CREATE UNIQUE INDEX ux_cr_daily ON cr_daily_stats (nm_id, date_of_period);

-- Индексы для быстрого поиска
CREATE INDEX idx_cr_nm_date ON cr_daily_stats (nm_id, date_of_period);
CREATE INDEX idx_cr_date ON cr_daily_stats (date_of_period);
CREATE INDEX idx_cr_product_id ON cr_daily_stats (product_id);