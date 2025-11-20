-- Create week_rows table
CREATE TABLE week_rows (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Мягкая связь на конкретный размер товара
  product_size_id UUID REFERENCES product_sizes(id) ON DELETE CASCADE,
  realizationreport_id INTEGER NOT NULL REFERENCES week_reports(realizationreport_id) ON DELETE CASCADE,
  report_type TEXT,
  rr_id BIGINT,
  gi_id BIGINT,
  order_dt DATE,
  sale_dt DATE,
  srid TEXT,
  barcode BIGINT,
  ts_name TEXT,
  nm_id BIGINT,
  sa_name TEXT,
  doc_type_name TEXT,
  quantity INTEGER,
  retail_price NUMERIC(12, 2),
  retail_amount NUMERIC(12, 2),
  rub_discountwb_both NUMERIC(12, 2),
  perc_discountwb_both NUMERIC(12, 2),
  ppvz_spp_prc NUMERIC(12, 2),
  rub_spp NUMERIC(12, 2),
  rub_wallet_dicount NUMERIC(12, 2),
  perc_wallet_discount NUMERIC(12, 2),
  ppvz_for_pay NUMERIC(12, 2),
  rub_commision_both NUMERIC(12, 2),
  perc_commisian_both NUMERIC(12, 2),
  commission_percent NUMERIC(12, 2),
  rub_commission NUMERIC(12, 2),
  rub_excess_comission NUMERIC(12, 2),
  perc_excess_comission NUMERIC(12, 2),
  supplier_oper_name TEXT,
  bonus_type_name TEXT,
  delivery_amount NUMERIC(12, 2),
  return_amount NUMERIC(12, 2),
  delivery_rub NUMERIC(12, 2),
  site_country TEXT,
  office_name TEXT,
  penalty NUMERIC(12, 2),
  storage_fee NUMERIC(12, 2),
  deduction NUMERIC(12, 2),
  acceptance NUMERIC(12, 2),
  kiz TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes
CREATE INDEX idx_week_rows_product_size_id ON week_rows (product_size_id);
CREATE INDEX idx_week_rows_realizationreport_id ON week_rows (realizationreport_id);
CREATE INDEX idx_week_rows_nm_id ON week_rows (nm_id);
CREATE INDEX idx_week_rows_sale_dt ON week_rows (sale_dt);

-- Create UNIQUE constraint to prevent duplicates
CREATE UNIQUE INDEX idx_week_rows_unique_rr_id ON week_rows (realizationreport_id, rr_id);
