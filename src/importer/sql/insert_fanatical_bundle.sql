INSERT INTO bundles_fanatical(
  ts, name, products, tiers, bundle_type, valid_from, valid_until
)
VALUES(
  :ts, :name, :products, :tiers, :bundle_type, :valid_from, :valid_until
);
