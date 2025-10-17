-- ========================================================================
-- Создание таблиц и функций для рекламной статистики (adv_params)
-- ========================================================================
-- Включает:
-- 1. Таблицу adv_campaign_daily_stats (детальная статистика)
-- 2. Таблицу adv_params (агрегированная статистика)
-- 3. Триггеры для автоматического обновления updated_at
-- 4. RPC функцию для агрегации данных
-- ========================================================================


-- ========================================================================
-- 1. Создание таблицы adv_campaign_daily_stats
-- ========================================================================
CREATE TABLE adv_campaign_daily_stats (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Ключи
  advert_id BIGINT NOT NULL,            -- ID рекламной кампании из WB
  nm_id BIGINT NOT NULL,                -- Артикул WB
  vendor_code TEXT NOT NULL,            -- Артикул продавца (из products)
  date DATE NOT NULL,                   -- Дата показателей
  
  -- Метрики артикула (из nms[], агрегат по всем платформам)
  views INTEGER DEFAULT 0,              -- Показы (сумма по apps[].nms[])
  clicks INTEGER DEFAULT 0,             -- Клики (сумма по apps[].nms[])
  cpc NUMERIC(10,2),                    -- Средняя стоимость клика
  ctr NUMERIC(5,2),                     -- CTR (%)
  sum NUMERIC(14,2) DEFAULT 0,          -- Затраты (₽) (сумма по apps[].nms[])
  
  -- Заказы (из days[], включая склейку)
  orders INTEGER DEFAULT 0,             -- Количество заказов (из days[].orders)
  orders_sum NUMERIC(14,2) DEFAULT 0,   -- Сумма заказов (₽) (из days[].sum_price)
  
  -- Вычисляемые метрики
  cpm NUMERIC(10,2),                    -- CPM = (sum / views) * 1000
  
  -- Технические метки
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Уникальность: одна кампания + один артикул + одна дата
CREATE UNIQUE INDEX ux_adv_daily_stats ON adv_campaign_daily_stats(advert_id, nm_id, date);

-- Индексы для быстрого поиска
CREATE INDEX idx_adv_daily_advert_id ON adv_campaign_daily_stats(advert_id);
CREATE INDEX idx_adv_daily_nm_id ON adv_campaign_daily_stats(nm_id);
CREATE INDEX idx_adv_daily_date ON adv_campaign_daily_stats(date);
CREATE INDEX idx_adv_daily_nm_date ON adv_campaign_daily_stats(nm_id, date);
CREATE INDEX idx_adv_daily_vendor_code ON adv_campaign_daily_stats(vendor_code);

-- Комментарии
COMMENT ON TABLE adv_campaign_daily_stats IS 'Детальная статистика по рекламным кампаниям: каждая строка = один артикул в одной кампании за один день';
COMMENT ON COLUMN adv_campaign_daily_stats.views IS 'Показы артикула (сумма по всем платформам из nms[])';
COMMENT ON COLUMN adv_campaign_daily_stats.orders IS 'Заказы из days[] - включает склейку (ассоциированные артикулы)';
COMMENT ON COLUMN adv_campaign_daily_stats.cpm IS 'CPM (Cost Per Mille) = затраты на 1000 показов';


-- ========================================================================
-- 2. Создание таблицы adv_params
-- ========================================================================
CREATE TABLE adv_params (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Ключи
  nm_id BIGINT NOT NULL,                -- Артикул WB
  vendor_code TEXT NOT NULL,            -- Артикул продавца
  date DATE NOT NULL,                   -- Дата показателей
  
  -- Агрегированные метрики (сумма из всех кампаний артикула)
  views INTEGER DEFAULT 0,              -- Показы (сумма)
  clicks INTEGER DEFAULT 0,             -- Клики (сумма)
  sum NUMERIC(14,2) DEFAULT 0,          -- Затраты (₽) (сумма)
  
  -- Вычисляемые метрики
  cpc NUMERIC(10,2),                    -- Средняя стоимость клика (sum / clicks)
  cpm NUMERIC(10,2),                    -- CPM = (sum / views) * 1000
  ctr NUMERIC(5,2),                     -- CTR = (clicks / views) * 100
  
  -- Заказы (сумма из всех кампаний)
  orders INTEGER DEFAULT 0,             -- Количество заказов
  orders_sum NUMERIC(14,2) DEFAULT 0,   -- Сумма заказов (₽)
  
  -- Технические метки
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  
  -- Связь с products
  FOREIGN KEY (nm_id) REFERENCES products(nm_id) ON DELETE CASCADE
);

-- Уникальность: один артикул - одна дата
CREATE UNIQUE INDEX ux_adv_params_nm_date ON adv_params(nm_id, date);

-- Индексы для быстрого поиска
CREATE INDEX idx_adv_params_nm_id ON adv_params(nm_id);
CREATE INDEX idx_adv_params_date ON adv_params(date);
CREATE INDEX idx_adv_params_vendor_code ON adv_params(vendor_code);

-- Комментарии
COMMENT ON TABLE adv_params IS 'Агрегированная рекламная статистика: каждая строка = один артикул за один день (суммируем все кампании)';
COMMENT ON COLUMN adv_params.views IS 'Суммарные показы артикула во всех кампаниях';
COMMENT ON COLUMN adv_params.orders IS 'Суммарные заказы артикула во всех кампаниях (включая склейку)';
COMMENT ON COLUMN adv_params.cpm IS 'CPM = (sum / views) * 1000, NULL если views = 0';


-- ========================================================================
-- 3. Создание функции для автоматического обновления updated_at
-- ========================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_updated_at_column IS 'Автоматически обновляет поле updated_at при UPDATE';


-- ========================================================================
-- 4. Создание триггеров для updated_at
-- ========================================================================

-- Триггер для adv_campaign_daily_stats
CREATE TRIGGER update_adv_campaign_daily_stats_updated_at
    BEFORE UPDATE ON adv_campaign_daily_stats
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Триггер для adv_params (ОТКЛЮЧЕН - updated_at управляется в aggregate_adv_params)
-- Причина: триггер перезаписывает логику условного обновления в ON CONFLICT
-- CREATE TRIGGER update_adv_params_updated_at
--     BEFORE UPDATE ON adv_params
--     FOR EACH ROW
--     EXECUTE FUNCTION update_updated_at_column();


-- ========================================================================
-- 5. Создание RPC функции для агрегации
-- ========================================================================
CREATE OR REPLACE FUNCTION aggregate_adv_params(
  p_date_from DATE DEFAULT NULL,
  p_date_to DATE DEFAULT NULL
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count INTEGER;
BEGIN
  INSERT INTO adv_params (
    nm_id,
    vendor_code,
    date,
    views,
    clicks,
    sum,
    cpc,
    cpm,
    ctr,
    orders,
    orders_sum
  )
  SELECT
    s.nm_id,
    s.vendor_code,
    s.date,
    SUM(s.views)::INTEGER AS views,
    SUM(s.clicks)::INTEGER AS clicks,
    SUM(s.sum) AS sum,
    -- CPC: средняя стоимость клика
    CASE
      WHEN SUM(s.clicks) > 0 THEN ROUND(SUM(s.sum) / SUM(s.clicks), 2)
      ELSE NULL
    END AS cpc,
    -- CPM: стоимость 1000 показов
    CASE
      WHEN SUM(s.views) > 0 THEN ROUND((SUM(s.sum) / SUM(s.views)) * 1000, 2)
      ELSE NULL
    END AS cpm,
    -- CTR: процент кликов от показов
    CASE
      WHEN SUM(s.views) > 0 THEN ROUND((SUM(s.clicks)::NUMERIC / SUM(s.views)) * 100, 2)
      ELSE NULL
    END AS ctr,
    SUM(s.orders)::INTEGER AS orders,
    SUM(s.orders_sum) AS orders_sum
  FROM
    adv_campaign_daily_stats s
  WHERE
    (p_date_from IS NULL OR s.date >= p_date_from)
    AND (p_date_to IS NULL OR s.date <= p_date_to)
  GROUP BY
    s.nm_id,
    s.vendor_code,
    s.date
  ON CONFLICT (nm_id, date) DO UPDATE SET
    vendor_code = EXCLUDED.vendor_code,
    views = EXCLUDED.views,
    clicks = EXCLUDED.clicks,
    sum = EXCLUDED.sum,
    cpc = EXCLUDED.cpc,
    cpm = EXCLUDED.cpm,
    ctr = EXCLUDED.ctr,
    orders = EXCLUDED.orders,
    orders_sum = EXCLUDED.orders_sum,
    -- Обновляем updated_at ТОЛЬКО если данные реально изменились
    updated_at = CASE 
      WHEN (
        adv_params.views IS DISTINCT FROM EXCLUDED.views OR
        adv_params.clicks IS DISTINCT FROM EXCLUDED.clicks OR
        adv_params.sum IS DISTINCT FROM EXCLUDED.sum OR
        adv_params.orders IS DISTINCT FROM EXCLUDED.orders OR
        adv_params.orders_sum IS DISTINCT FROM EXCLUDED.orders_sum
      )
      THEN NOW()
      ELSE adv_params.updated_at
    END;
  
  -- Возвращаем количество обработанных записей
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;

COMMENT ON FUNCTION aggregate_adv_params IS 'Агрегирует данные из adv_campaign_daily_stats в adv_params, группируя по nm_id и date';

