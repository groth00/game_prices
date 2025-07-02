use std::{collections::HashMap, fs};

use anyhow::Error;
use bincode::config;
use games_proto::generated::CWishlistGetWishlistSortedFilteredResponse;
use log::info;
use rusqlite::{Connection, params};
use serde::Deserialize;

use crate::utils::{execute_sql, rows_inserted};

pub fn update_search_table(conn: &mut Connection) -> Result<(), Error> {
    let tags_raw = fs::read_to_string("output/steam/tags.json")?;
    let categories_raw = fs::read_to_string("output/steam/categories.json")?;

    let tags_list: Tags = serde_json::from_str(&tags_raw)?;
    let mut tags_map: HashMap<u32, String> = HashMap::new();
    for tag in tags_list.response.tags {
        tags_map.insert(tag.tagid, tag.name);
    }
    info!("created tags map");

    let categories_list: Vec<Category> = serde_json::from_str(&categories_raw)?;
    let mut categories_map: HashMap<u32, String> = HashMap::new();
    for category in categories_list {
        categories_map.insert(category.categoryid, category.display_name);
    }
    info!("created categories map");

    execute_sql("src/importer/sql/insert_prices.sql")?;
    info!("inserted into prices: {}", rows_inserted(conn, "prices")?);

    let select_query = "SELECT rowid, tags, categories_player, categories_controller, categories_features FROM metadata WHERE tags IS NOT NULL AND categories_player IS NOT NULL AND categories_controller IS NOT NULL AND categories_features IS NOT NULL";
    let update_query = "UPDATE prices SET tags = ?1, categories_player = ?2, categories_controller = ?3, categories_features = ?4 WHERE meta_id = ?5";

    let tx = conn.transaction()?;
    {
        let mut select_stmt = tx.prepare(select_query)?;
        let mut update_stmt = tx.prepare(update_query)?;

        let mut rows = select_stmt.query([])?;
        while let Ok(Some(r)) = rows.next() {
            let rowid = r.get::<usize, i64>(0)?;
            let mut ret: [String; 4] = [const { String::new() }; 4];
            for i in 0..=3 {
                let raw = r.get::<usize, Vec<u8>>(i + 1)?;
                if raw.is_empty() {
                    ret[i] = "None".into();
                } else {
                    let (data, _bytes): (Vec<u32>, usize) =
                        bincode::decode_from_slice(&raw, config::standard())
                            .expect("failed to deserialize tags");
                    if i == 0 {
                        ret[i] = data
                            .into_iter()
                            .map(|id| tags_map.get(&id).map_or("Unknown", |v| v))
                            .collect::<Vec<_>>()
                            .join(",");
                    } else {
                        ret[i] = data
                            .into_iter()
                            .map(|id| categories_map.get(&id).map_or("Unknown", |v| v))
                            .collect::<Vec<_>>()
                            .join(",");
                    }
                }
            }
            update_stmt.execute(params![ret[0], ret[1], ret[2], ret[3], rowid])?;
        }
    }
    tx.commit()?;
    info!("updated tags and categories in prices table");

    Ok(())
}

pub fn insert_wishlist(conn: &mut Connection) -> Result<(), Error> {
    let wishlist = fs::read_to_string("output/steam/wishlist.json")?;
    let wishlist: CWishlistGetWishlistSortedFilteredResponse = serde_json::from_str(&wishlist)?;

    let appids = wishlist
        .items
        .iter()
        .map(|i| i.appid.expect("missing appid"))
        .collect::<Vec<_>>();

    let insert_wishlist = "INSERT INTO steam_wishlist(appid) VALUES(?1)";

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(insert_wishlist)?;
        tx.execute("DELETE FROM steam_wishlist", [])?;
        for appid in appids {
            stmt.execute([&appid])?;
        }
    }
    tx.commit()?;

    Ok(())
}

pub fn read_wishlist(conn: &mut Connection) -> Result<Vec<WishlistItem>, Error> {
    let query = include_str!("../sql/get_wishlist.sql");

    let mut stmt = conn.prepare(query)?;
    let items = stmt
        .query_map([], |r| {
            let name = r.get::<&str, String>("name").unwrap();
            let fanatical_price = r
                .get::<&str, Option<f64>>("fanatical_price")?
                .unwrap_or_default();
            let gamebillet_price = r
                .get::<&str, Option<f64>>("gamebillet_price")?
                .unwrap_or_default();
            let gamesplanet_price = r
                .get::<&str, Option<f64>>("gamesplanet_price")?
                .unwrap_or_default();
            let gmg_price = r.get::<&str, Option<f64>>("gmg_price")?.unwrap_or_default();
            let gog_price = r.get::<&str, Option<f64>>("gog_price")?.unwrap_or_default();
            let indiegala_price = r
                .get::<&str, Option<f64>>("indiegala_price")?
                .unwrap_or_default();
            let steam_price = r
                .get::<&str, Option<f64>>("steam_price")?
                .unwrap_or_default();
            let wgs_price = r.get::<&str, Option<f64>>("wgs_price")?.unwrap_or_default();
            let is_dlc = r.get::<&str, Option<bool>>("is_dlc")?.unwrap_or_default();
            let tags = r.get::<&str, Option<String>>("tags")?.unwrap_or_default();
            let categories_player = r
                .get::<&str, Option<String>>("categories_player")?
                .unwrap_or_default();
            let categories_controller = r
                .get::<&str, Option<String>>("categories_controller")?
                .unwrap_or_default();
            let categories_features = r
                .get::<&str, Option<String>>("categories_features")?
                .unwrap_or_default();
            let review_count = r
                .get::<&str, Option<u32>>("review_count")?
                .unwrap_or_default();
            let review_pct_positive = r
                .get::<&str, Option<u8>>("review_pct_positive")?
                .unwrap_or_default();
            let release_date = r
                .get::<&str, Option<u32>>("release_date")?
                .unwrap_or_default();
            let windows = r.get::<&str, Option<bool>>("windows")?.unwrap_or_default();
            let mac = r.get::<&str, Option<bool>>("mac")?.unwrap_or_default();
            let linux = r.get::<&str, Option<bool>>("linux")?.unwrap_or_default();
            let steam_deck = r
                .get::<&str, Option<u8>>("steam_deck_compat")?
                .unwrap_or_default();

            Ok(WishlistItem {
                name,
                fanatical_price,
                gamebillet_price,
                gamesplanet_price,
                gmg_price,
                gog_price,
                indiegala_price,
                steam_price,
                wgs_price,
                is_dlc,
                tags,
                categories_player,
                categories_controller,
                categories_features,
                review_count,
                review_pct_positive,
                release_date,
                windows,
                mac,
                linux,
                steam_deck,
            })
        })?
        .map(Result::unwrap)
        .collect::<Vec<_>>();

    Ok(items)
}

#[derive(Debug)]
pub struct WishlistItem {
    name: String,
    fanatical_price: f64,
    gamebillet_price: f64,
    gamesplanet_price: f64,
    gmg_price: f64,
    gog_price: f64,
    indiegala_price: f64,
    steam_price: f64,
    wgs_price: f64,
    is_dlc: bool,
    tags: String,
    categories_player: String,
    categories_controller: String,
    categories_features: String,
    review_count: u32,
    review_pct_positive: u8,
    release_date: u32,
    windows: bool,
    mac: bool,
    linux: bool,
    steam_deck: u8,
}

#[derive(Deserialize)]
struct Tags {
    response: TagResponse,
}

#[derive(Deserialize)]
struct TagResponse {
    // version_hash: String,
    tags: Vec<Tag>,
}

#[derive(Deserialize)]
struct Tag {
    tagid: u32,
    name: String,
}

#[derive(Deserialize)]
struct Category {
    categoryid: u32,
    // 0: type, 1: player, 2: features, 3: controller
    // category_type: u32,
    display_name: String,
}
