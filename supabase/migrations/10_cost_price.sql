-- ========================================================================
-- Create cost_price table for product cost prices from Google Sheets
-- ========================================================================

CREATE TABLE IF NOT EXISTS cost_price (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Relations
  product_id UUID REFERENCES products(id) ON DELETE SET NULL,

  -- Business keys
  nm_id BIGINT NOT NULL,
  vendor_code TEXT NOT NULL,
  date DATE NOT NULL,

  -- Cost price value from Google Sheets
  cost_price NUMERIC(14,2) NOT NULL
);

-- Uniqueness: one row per (nm_id, date)
CREATE UNIQUE INDEX IF NOT EXISTS ux_cost_price_nm_date ON cost_price (nm_id, date);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_cost_price_nm_id ON cost_price (nm_id);
CREATE INDEX IF NOT EXISTS idx_cost_price_date ON cost_price (date);
