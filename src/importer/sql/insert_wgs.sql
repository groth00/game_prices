INSERT INTO wgs(
  meta_id, ts, name,
  discount_price, discount_percent,
  is_dlc, publisher
)
VALUES(
  :meta_id, :ts, :name,
  :discount_price, :discount_percent,
  :is_dlc, :publisher
);
