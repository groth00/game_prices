INSERT INTO gamebillet(
  meta_id, ts, name,
  discount_price, discount_percent
)
VALUES(
  :meta_id, :ts, :name,
  :discount_price, :discount_percent
);
