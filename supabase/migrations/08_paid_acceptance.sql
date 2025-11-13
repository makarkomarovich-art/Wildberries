-- ========================================================================
-- Create paid_acceptance table for Seller Analytics Acceptance report
-- ========================================================================

-- Enable pgcrypto for gen_random_uuid if not enabled (safe to re-run)
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS paid_acceptance (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Relations
  product_id UUID REFERENCES products(id) ON DELETE SET NULL,

  -- Business keys
  nm_id BIGINT NOT NULL,
  vendor_code TEXT NOT NULL,
  shk_create_date DATE NOT NULL,

  -- Aggregated metrics
  count INTEGER NOT NULL,
  total NUMERIC(14,2) NOT NULL,

  -- Timestamps
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Uniqueness: one row per (nm_id, shk_create_date)
CREATE UNIQUE INDEX IF NOT EXISTS ux_paid_acceptance_nm_date ON paid_acceptance (nm_id, shk_create_date);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_paid_acceptance_nm_id ON paid_acceptance (nm_id);
CREATE INDEX IF NOT EXISTS idx_paid_acceptance_date ON paid_acceptance (shk_create_date);

-- Update updated_at automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_paid_acceptance_updated_at ON paid_acceptance;
CREATE TRIGGER trg_paid_acceptance_updated_at
  BEFORE UPDATE ON paid_acceptance
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE paid_acceptance IS 'Daily paid acceptance per nm_id aggregated by shk_create_date';
COMMENT ON COLUMN paid_acceptance.total IS 'Sum of total acceptance cost for the day & article';


