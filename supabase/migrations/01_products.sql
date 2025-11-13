CREATE TABLE products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  serial_id BIGSERIAL UNIQUE NOT NULL,
  vendor_code TEXT UNIQUE NOT NULL,
  nm_id BIGINT UNIQUE NOT NULL,
  imt_id BIGINT NOT NULL,
  category_wb TEXT NOT NULL,
  main_photo_url TEXT,
  title TEXT NOT NULL
);

CREATE INDEX idx_products_category ON products(category_wb);