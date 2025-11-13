-- week_stats: aggregated per nm_id per realizationreport_id
CREATE TABLE week_stats (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  report_type TEXT,
  realizationreport_id INTEGER NOT NULL REFERENCES week_reports(realizationreport_id) ON DELETE CASCADE,
  product_id UUID REFERENCES products(id) ON DELETE CASCADE,
  date_from DATE NOT NULL,
  date_to DATE NOT NULL,
  nm_id BIGINT NOT NULL,
  sa_name TEXT,
  quantity_sells_nm INTEGER,
  quantity_return_nm INTEGER,
  cancels_nm INTEGER,
  retail_price_nm NUMERIC(12, 2),
  retail_amount_nm NUMERIC(12, 2),
  rub_discountwb_both_nm NUMERIC(12, 2),
  perc_discountwb_both_nm NUMERIC(12, 2),
  rub_spp_nm NUMERIC(12, 2),
  perc_spp_nm NUMERIC(12, 2),
  perc_wallet_discount_nm NUMERIC(12, 2),
  ppvz_for_pay_nm NUMERIC(12, 2),
  rub_commision_both_nm NUMERIC(12, 2),
  perc_commisian_both_nm NUMERIC(12, 2),
  rub_commission_nm NUMERIC(12, 2),
  perc_commission_nm NUMERIC(12, 2),
  rub_excess_comission_nm NUMERIC(12, 2),
  perc_excess_comission_nm NUMERIC(12, 2),
  delivery_amount_nm NUMERIC(12, 2),
  return_amount_nm NUMERIC(12, 2),
  delivery_rub_nm NUMERIC(12, 2),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Uniqueness
CREATE UNIQUE INDEX idx_week_stats_unique ON week_stats (realizationreport_id, nm_id);

-- Indexes
CREATE INDEX idx_week_stats_realizationreport_id ON week_stats (realizationreport_id);
CREATE INDEX idx_week_stats_nm_id ON week_stats (nm_id);
