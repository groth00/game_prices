INSERT INTO fanatical(
  meta_id, ts, name,
  price, discount_price, discount_percent,
  best_ever, flash_sale, os, release_date,
  available_from, available_until
)
VALUES(
  :meta_id, :ts, :name,
  :price, :discount_price, :discount_percent,
  :best_ever, :flash_sale, :os, :release_date,
  :available_from, :available_until
);
