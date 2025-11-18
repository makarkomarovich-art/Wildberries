-- Create new table cr_daily_stats_new (no stocks, no cancel)
create table if not exists public.cr_daily_stats_new (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references public.products(id) on delete cascade,
  nm_id bigint not null,
  vendor_code text not null,
  date_of_period date not null,
  open_card_count integer,
  add_to_cart_count integer,
  orders_count integer,
  orders_sum_rub integer,
  buyouts_count integer,
  buyouts_sum_rub integer,
  add_to_cart_percent integer,
  cart_to_order_percent integer,
  order_price numeric(12,2),
  buyout_price numeric(12,2),
  created_at timestamptz not null default now()
);

-- Unique composite key
create unique index if not exists uq_cr_new_nm_date on public.cr_daily_stats_new (nm_id, date_of_period);

-- Useful indexes
create index if not exists idx_cr_new_product_id on public.cr_daily_stats_new (product_id);
create index if not exists idx_cr_new_date on public.cr_daily_stats_new (date_of_period);


