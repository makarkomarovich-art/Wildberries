CREATE TABLE product_sizes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  serial_id BIGSERIAL UNIQUE NOT NULL,
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  barcode BIGINT NOT NULL,
  size TEXT
);

CREATE INDEX idx_product_sizes_product_id ON product_sizes(product_id);
CREATE INDEX idx_product_sizes_barcode ON product_sizes(barcode);