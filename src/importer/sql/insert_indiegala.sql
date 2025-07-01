INSERT INTO indiegala(
  meta_id, ts, name,
  price, discount_price,
  valid_from, valid_until,
  os, release_date, publisher
)
VALUES(
  :meta_id, :ts, :name,
  :price, :discount_price,
  :valid_from, :valid_until,
  :os, :release_date, :publisher
);
