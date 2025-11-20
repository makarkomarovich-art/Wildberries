CREATE TABLE product_sizes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  serial_id BIGSERIAL UNIQUE NOT NULL,
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  nm_id BIGINT NOT NULL,
  vendor_code TEXT NOT NULL,
  barcode TEXT UNIQUE NOT NULL,
  size TEXT
);

-- Helpful indexes
CREATE INDEX idx_product_sizes_product_id ON product_sizes(product_id);
CREATE INDEX idx_product_sizes_barcode ON product_sizes(barcode);
CREATE INDEX idx_product_sizes_nm_id ON product_sizes(nm_id);
CREATE INDEX idx_product_sizes_vendor_code ON product_sizes(vendor_code);