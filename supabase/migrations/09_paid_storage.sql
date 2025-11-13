-- ========================================================================
-- Create paid_storage table for Seller Analytics Paid Storage report
-- ========================================================================

-- Enable pgcrypto for gen_random_uuid if not enabled (safe to re-run)
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS paid_storage (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Relations
  product_id UUID REFERENCES products(id) ON DELETE SET NULL,

  -- Business keys
  nm_id BIGINT NOT NULL,
  vendor_code TEXT NOT NULL,
  date DATE NOT NULL,

  -- Aggregated metrics
  warehouse_price NUMERIC(14,2) NOT NULL,

  -- Optional details (multiple supplies per article per day)
  gi_ids BIGINT[],

  -- Timestamps
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Uniqueness: one row per (nm_id, date)
CREATE UNIQUE INDEX IF NOT EXISTS ux_paid_storage_nm_date ON paid_storage (nm_id, date);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_paid_storage_nm_id ON paid_storage (nm_id);
CREATE INDEX IF NOT EXISTS idx_paid_storage_date ON paid_storage (date);

-- Update updated_at automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_paid_storage_updated_at ON paid_storage;
CREATE TRIGGER trg_paid_storage_updated_at
  BEFORE UPDATE ON paid_storage
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE paid_storage IS 'Daily paid storage cost per nm_id (aggregated by date & nm_id)';
COMMENT ON COLUMN paid_storage.warehouse_price IS 'Sum of warehousePrice for the day & article';
COMMENT ON COLUMN paid_storage.gi_ids IS 'Array of related supply ids (giId) for the aggregated row';


