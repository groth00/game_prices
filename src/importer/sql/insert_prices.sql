DROP TABLE IF EXISTS prices;

CREATE TABLE IF NOT EXISTS prices(
  meta_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  fanatical_price REAL,
  gamebillet_price REAL,
  gamesplanet_price REAL,
  gmg_price REAL,
  gog_price REAL,
  indiegala_price REAL,
  steam_price REAL,
  wgs_price REAL,
  is_dlc INTEGER,
  tags TEXT,
  categories_player TEXT,
  categories_controller TEXT,
  categories_features TEXT,
  review_count INTEGER,
  review_pct_positive INTEGER,
  release_date INTEGER,
  windows INTEGER,
  mac INTEGER,
  linux INTEGER,
  steam_deck_compat INTEGER,
  FOREIGN KEY(meta_id) REFERENCES metadata(id)
);

INSERT INTO prices(
  meta_id, name, fanatical_price, gamebillet_price, gamesplanet_price,
  gmg_price, gog_price, indiegala_price, steam_price, wgs_price,
  is_dlc, review_count, review_pct_positive, release_date,
  windows, mac, linux, steam_deck_compat
)
SELECT metadata.id, metadata.name,
  s1.discount_price AS fanatical_price,
  s2.discount_price AS gamebillet_price,
  s3.discount_price AS gamesplanet_price,
  s4.discount_price AS gmg_price,
  s5.discount_price AS gog_price,
  s6.discount_price AS indiegala_price,
  s7.discount_price AS steam_price,
  s8.discount_price AS wgs_price,
  metadata.is_dlc, metadata.review_count, metadata.review_pct_positive, metadata.release_date,
  metadata.windows, metadata.mac, metadata.linux, metadata.steam_deck_compat
FROM
  metadata LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM fanatical
    )
    WHERE rn = 1
  ) s1 ON metadata.id = s1.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM gamebillet
    )
    WHERE rn = 1
  ) s2 ON metadata.id = s2.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM gamesplanet
    )
    WHERE rn = 1
  ) s3 ON metadata.id = s3.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM gmg
    )
    WHERE rn = 1
  ) s4 ON metadata.id = s4.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM gog
    )
    WHERE rn = 1
  ) s5 ON metadata.id = s5.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM indiegala
    )
    WHERE rn = 1
  ) s6 ON metadata.id = s6.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM steam
    )
    WHERE rn = 1
  ) s7 ON metadata.id = s7.meta_id
  LEFT JOIN (
    SELECT meta_id, discount_price
    FROM (
      SELECT meta_id, discount_price, ROW_NUMBER() OVER(PARTITION BY meta_id ORDER BY ts DESC, rowid DESC) AS rn
      FROM wgs
    )
    WHERE rn = 1
  ) s8 ON metadata.id = s8.meta_id
