UPDATE metadata
SET
  is_dlc = ?1, tags = ?2, categories_player = ?3, categories_controller = ?4, categories_features = ?5,
  review_count = ?6, review_pct_positive = ?7, short_desc = ?8, publishers = ?9, developers = ?10,
  franchises = ?11, release_date = ?12, windows = ?13, mac = ?14, linux = ?15, steam_deck_compat = ?16
WHERE appid = ?17;
