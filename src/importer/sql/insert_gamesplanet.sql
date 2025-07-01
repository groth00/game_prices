INSERT INTO gamesplanet(
  meta_id, ts, name,
  price, discount_price, discount_percent
)
VALUES(
  :meta_id, :ts, :name,
  :price, :discount_price, :discount_percent
);
