INSERT INTO gmg(
  meta_id, ts, appid, name,
  price, discount_price, discount_percent,
  is_dlc, franchise, publisher
)
VALUES(
  :meta_id, :ts, :appid, :name,
  :price, :discount_price, :discount_percent,
  :is_dlc, :franchise, :publisher
);
