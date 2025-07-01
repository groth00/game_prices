INSERT INTO steam(
  meta_id, ts, packageid, bundleid, name,
  price, discount_price, discount_percent,
  available_until
)
VALUES(
  :meta_id, :ts, :packageid, :bundleid, :name,
  :price, :discount_price, :discount_percent,
  :available_until
);
