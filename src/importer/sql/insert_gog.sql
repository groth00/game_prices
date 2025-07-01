INSERT INTO gog(
  meta_id, ts, name,
  price, discount_price, discount_percent,
  release_date, developer, publisher, product_type
)
VALUES(
  :meta_id, :ts, :name,
  :price, :discount_price, :discount_percent,
  :release_date, :developer, :publisher, :product_type
);
