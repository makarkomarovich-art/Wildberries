-- Draft migration: add nm_id and vendor_code to product_sizes (minimal)
-- NOTE: This draft does NOT perform backfill or set NOT NULL.
-- You plan to recreate entity; apply after removing the old one.

BEGIN;

ALTER TABLE product_sizes
  ADD COLUMN IF NOT EXISTS nm_id BIGINT;

ALTER TABLE product_sizes
  ADD COLUMN IF NOT EXISTS vendor_code TEXT;

CREATE INDEX IF NOT EXISTS idx_product_sizes_nm_id ON product_sizes(nm_id);
CREATE INDEX IF NOT EXISTS idx_product_sizes_vendor_code ON product_sizes(vendor_code);

COMMIT;


