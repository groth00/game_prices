INSERT INTO bundles_steam(
  bundleid, ts, name, type, included_types, included_appids, included_items,
  original_price, discount_price
)
VALUES(
  :bundleid, :ts, :name, :type, :included_types, :included_appids, :included_items,
  :original_price, :discount_price
);
