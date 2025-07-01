-- metadata
CREATE TABLE IF NOT EXISTS drm(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tags(
  id INTEGER PRIMARY KEY,
  tagid INTEGER NOT NULL,
  name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS categories(
  id INTEGER PRIMARY KEY,
  catid INTEGER NOT NULL,
  catcat INTEGER NOT NULL,
  name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metadata(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  cname TEXT NOT NULL,
  appid INTEGER,
  is_dlc INTEGER,
  tags BLOB,
  categories_player BLOB,
  categories_controller BLOB,
  categories_features BLOB,
  review_count INTEGER,
  review_pct_positive INTEGER,
  short_desc TEXT,
  publishers TEXT[],
  developers TEXT[],
  franchises TEXT[],
  release_date INTEGER,
  windows INTEGER,
  mac INTEGER,
  linux INTEGER,
  steam_deck_compat INTEGER,
  CONSTRAINT unique_name_pair UNIQUE(name, cname)
);

CREATE INDEX IF NOT EXISTS metadata_appid ON metadata(appid);
CREATE INDEX IF NOT EXISTS metadata_cover ON metadata(cname, id);

-- prices
CREATE TABLE IF NOT EXISTS fanatical(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  best_ever INTEGER NOT NULL DEFAULT 0,
  flash_sale INTEGER NOT NULL DEFAULT 0,
  os TEXT NOT NULL,
  release_date INTEGER NOT NULL DEFAULT 0,
  available_from INTEGER NOT NULL,
  available_until INTEGER NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS gamebillet(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS gamesplanet(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS gmg(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  appid INTEGER NOT NULL,
  price REAL NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  is_dlc INTEGER NOT NULL DEFAULT 0,
  franchise TEXT NOT NULL,
  publisher TEXT NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS gog(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  release_date INTEGER NOT NULL DEFAULT 0,
  developer TEXT NOT NULL,
  publisher TEXT NOT NULL,
  product_type TEXT NOT NULL,
  drm INTEGER DEFAULT 2,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS indiegala(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  discount_price REAL NOT NULL,
  valid_from INTEGER NOT NULL,
  valid_until INTEGER NOT NULL,
  os TEXT NOT NULL,
  release_date INTEGER NOT NULL DEFAULT 0,
  publisher TEXT NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS steam(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  packageid INTEGER,
  bundleid INTEGER,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  available_until INTEGER NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);
CREATE TABLE IF NOT EXISTS wgs(
  id INTEGER PRIMARY KEY,
  meta_id INTEGER,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  discount_price REAL NOT NULL,
  discount_percent INTEGER NOT NULL,
  is_dlc INTEGER NOT NULL,
  publisher TEXT NOT NULL,
  drm INTEGER DEFAULT 1,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);

-- bundles
CREATE TABLE IF NOT EXISTS bundles_fanatical(
  id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  products TEXT NOT NULL,
  tiers TEXT NOT NULL,
  bundle_type TEXT NOT NULL,
  valid_from TEXT NOT NULL,
  valid_until TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bundles_steam(
  bundleid INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  type INTEGER,
  included_types TEXT,
  included_appids TEXT,
  included_items TEXT,
  original_price REAL NOT NULL,
  discount_price REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS bundles_indiegala(
  id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL,
  name TEXT NOT NULL,
  price REAL NOT NULL,
  products TEXT NOT NULL,
  valid_until TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fanatical_cover ON fanatical(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS gamebillet_cover ON gamebillet(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS gamesplanet_cover ON gamesplanet(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS gmg_cover ON gmg(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS gog_cover ON gog(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS indiegala_cover ON indiegala(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS steam_cover ON steam(name, ts DESC, meta_id);
CREATE INDEX IF NOT EXISTS wgs_cover ON wgs(name, ts DESC, meta_id);
