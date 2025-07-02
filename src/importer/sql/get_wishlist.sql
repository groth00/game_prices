SELECT
  name,
  fanatical_price, gamebillet_price, gamesplanet_price,
  gmg_price, gog_price, indiegala_price, steam_price, wgs_price,
  is_dlc, tags, categories_player, categories_controller, categories_features,
  review_count, review_pct_positive, release_date,
  windows, mac, linux, steam_deck_compat
FROM prices WHERE prices.meta_id
IN (SELECT metadata.id FROM metadata INNER JOIN steam_wishlist ON steam_wishlist.appid = metadata.appid);
