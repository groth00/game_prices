use std::{
    collections::{BTreeSet, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use anyhow::Error;
use chrono::{DateTime, NaiveDateTime};
use log::{error, info};
use rusqlite::{Connection, Result, named_params};
use serde::{Deserialize, Serialize};

use crate::{
    insert::MetadataResponse,
    rows_inserted,
    utils::{all_files, move_file},
};

pub fn import_bundles(conn: &mut Connection) -> Result<(), Error> {
    let sources: [(
        &'static str,
        &'static str,
        &'static str,
        &dyn Fn(&mut Connection, PathBuf) -> Result<i64, Error>,
    ); 3] = [
        (
            "fanatical",
            "output/fanatical",
            "bundles",
            &import_fanatical_bundle,
        ),
        ("steam", "output/steam", "pbundles", &import_steam_bundle),
        (
            "indiegala",
            "output/indiegala",
            "bundles",
            &import_indiegala_bundle,
        ),
    ];

    // parse steam bundles separately
    for file in all_files("output/steam", "bundles")? {
        let path = file.path();
        info!("parse steam bundle: {:?}", &path);
        parse_steam_bundle(&path)?;
        move_file(&path, "steam")?;
    }

    for (store, dir, prefix, import_fn) in sources {
        for file in all_files(dir, prefix)? {
            info!("insert {} bundle: {}", store, import_fn(conn, file.path())?);
            move_file(&file.path(), store)?;
        }
    }

    Ok(())
}

#[derive(Deserialize, Serialize)]
struct FanaticalBundles {
    pickandmix: Vec<FanaticalBundle>,
}

#[derive(Deserialize, Serialize)]
struct FanaticalBundle {
    name: String,
    products: Vec<FanaticalBundleProduct>,
    tiers: Vec<FanaticalBundleTier>,
    bundle_type: FanaticalBundleType,
    valid_from: String,
    valid_until: String,
}

#[derive(Deserialize, Serialize)]
struct FanaticalBundleProduct {
    name: String,
}

#[derive(Deserialize, Serialize)]
struct FanaticalBundleTier {
    quantity: u32,
    price: f64,
}

#[derive(Deserialize, Serialize)]
enum FanaticalBundleType {
    #[serde(alias = "bundle")]
    GameBundle,
    #[serde(alias = "book-bundle")]
    BookBundle,
    #[serde(alias = "elearning-bundle")]
    ELearningBundle,
    #[serde(alias = "software-bundle")]
    SoftwareBundle,
    #[serde(alias = "comic-bundle")]
    ComicBundle,
}

impl From<&str> for FanaticalBundleType {
    fn from(value: &str) -> Self {
        match value {
            "bundle" => Self::GameBundle,
            "book-bundle" => Self::BookBundle,
            "elearning-bundle" => Self::ELearningBundle,
            "software-bundle" => Self::SoftwareBundle,
            "comic-bundle" => Self::ComicBundle,
            _ => unreachable!(),
        }
    }
}

impl From<FanaticalBundleType> for &'static str {
    fn from(value: FanaticalBundleType) -> &'static str {
        match value {
            FanaticalBundleType::GameBundle => "bundle",
            FanaticalBundleType::BookBundle => "book-bundle",
            FanaticalBundleType::ELearningBundle => "elarning-bundle",
            FanaticalBundleType::SoftwareBundle => "software-bundle",
            FanaticalBundleType::ComicBundle => "comic-bundle",
        }
    }
}

fn import_fanatical_bundle<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let mut file = File::open(&path)?;
    let ts = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let mut buf = String::with_capacity(1 << 20);
    let _ = file.read_to_string(&mut buf)?;
    let bundles: FanaticalBundles = serde_json::from_str(&buf)?;

    let insert_sql = include_str!("../sql/insert_fanatical_bundle.sql");

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(insert_sql)?;

        // NOTE: bundles can include duplicate items, go figure
        for bundle in bundles.pickandmix {
            let mut seen: HashSet<&str> = HashSet::new();
            let filtered = bundle
                .products
                .iter()
                .filter(|x| seen.insert(&x.name))
                .collect::<Vec<_>>();

            let products = serde_json::to_value(filtered)?;
            let tiers = serde_json::to_value(bundle.tiers)?;
            let bundle_type: &'static str = bundle.bundle_type.into();
            let valid_from = DateTime::parse_from_rfc3339(&bundle.valid_from)?;
            let valid_until = DateTime::parse_from_rfc3339(&bundle.valid_until)?;

            let params = named_params! {
                ":ts": ts,
                ":name": bundle.name,
                ":products": products,
                ":tiers": tiers,
                ":bundle_type": bundle_type,
                ":valid_from": valid_from,
                ":valid_until": valid_until,
            };
            stmt.execute(params)?;
        }
    }
    tx.commit()?;
    Ok(rows_inserted(conn, "bundles_fanatical")?)
}

#[derive(Default, Deserialize, Serialize)]
struct BundleInfo {
    bundleid: u32,
    name: String,
    r#type: i32,
    included_types: Vec<i32>,
    included_appids: Vec<u32>,
    included_items: Vec<BundleItem>,
    original_price: f64,
    discount_price: f64,
}

#[derive(Default, Deserialize, Serialize)]
struct BundleItem {
    item_type: i32, // general type: game, package, bundle, software, etc.
    id: u32,
    name: String,
    appid: u32,
    r#type: i32, // game or dlc
    original_price: f64,
    final_price: f64,
}

fn parse_steam_bundle<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    let file = File::open(&path)?;
    let ts = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let reader = BufReader::new(file);

    let mut seen: BTreeSet<u32> = BTreeSet::new();
    let mut bundles: Vec<BundleInfo> = Vec::with_capacity(2 << 14);

    for line in reader.lines() {
        let line: MetadataResponse = serde_json::from_str(&line?)?;
        for item in line.store_items {
            if item.visible.is_none_or(|b| b == false) {
                continue;
            }

            let mut info = BundleInfo::default();
            if let Some(id) = item.id {
                if !seen.insert(id) {
                    continue;
                }
                info.bundleid = id;
            } else {
                continue;
            }

            if let Some(name) = item.name {
                info.name = name;
            } else {
                continue;
            }

            if let Some(r#type) = item.r#type {
                info.r#type = r#type;
            } else {
                continue;
            }
            info.included_types = item.included_types;
            info.included_appids = item.included_appids;

            // some bundles have null best_purchase_option; get individual prices first
            let mut bundle_cost = 0f64;
            for subitem in item
                .included_items
                .expect("missing included_items")
                .included_apps
            {
                let mut bundle_item = BundleItem::default();
                if subitem.visible.is_none_or(|b| b == false) {
                    continue;
                }
                bundle_item.item_type = subitem.item_type.expect("missing item_type");
                bundle_item.id = subitem.id.expect("missing id");
                bundle_item.name = subitem.name.clone().expect("missing name");
                bundle_item.appid = subitem.appid.expect("missing appid");
                bundle_item.r#type = subitem.r#type.expect("missing type in inner item");

                // best_purchase_option and purchase_options are both present or not present
                let item_cost = if let Some(purchase) = subitem.best_purchase_option {
                    let price = (purchase
                        .final_price_in_cents
                        .expect("missing final_price_in_cents"))
                        as f64
                        / 100f64;
                    bundle_item.final_price = price;

                    if let Some(original_price) = purchase.original_price_in_cents {
                        bundle_item.original_price = original_price as f64 / 100f64;
                    } else {
                        bundle_item.original_price = price;
                    }
                    price
                } else {
                    if subitem.is_free != Some(true) {
                        error!(
                            "missing best_purchase_option: {:?} {:?}",
                            subitem.name.clone().unwrap(),
                            subitem.store_url_path.clone().unwrap(),
                        );
                    }
                    bundle_item.final_price = 0f64;
                    bundle_item.original_price = 0f64;
                    0f64
                };
                bundle_cost += item_cost;
                info.included_items.push(bundle_item);
            }

            // finalize bundle cost
            if let Some(purchase) = item.best_purchase_option {
                info.original_price = (purchase
                    .price_before_bundle_discount
                    .expect("missing price_before_bundle_discount"))
                    as f64
                    / 100f64;
                info.discount_price = (purchase
                    .final_price_in_cents
                    .expect("missing final_price_in_cents"))
                    as f64
                    / 100f64;
            } else {
                info.original_price = bundle_cost;
                info.discount_price = info.original_price;
            }
            bundles.push(info);
        }
    }

    let serialized = serde_json::to_string_pretty(&bundles)?;
    let output_path = format!("output/steam/pbundles_{}.json", &ts);
    fs::write(output_path, serialized)?;

    Ok(())
}

fn import_steam_bundle<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let mut file = File::open(&path)?;
    let ts = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let mut buf = String::with_capacity(1 << 24);
    let _ = file.read_to_string(&mut buf)?;
    let items: Vec<BundleInfo> = serde_json::from_str(&mut buf)?;

    let insert_query = include_str!("../sql/insert_steam_bundle.sql");

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(insert_query)?;

        for item in items {
            let included_types = serde_json::to_value(item.included_types)?;
            let included_appids = serde_json::to_value(item.included_appids)?;
            let included_items = serde_json::to_value(item.included_items)?;

            let params = named_params! {
                ":bundleid": item.bundleid,
                ":ts": ts,
                ":name": item.name,
                ":type": item.r#type,
                ":included_types": included_types,
                ":included_appids": included_appids,
                ":included_items": included_items,
                ":original_price": item.original_price,
                ":discount_price": item.discount_price,
            };

            stmt.execute(params)?;
        }
    }
    tx.commit()?;
    Ok(rows_inserted(&conn, "bundles_steam")?)
}

#[derive(Deserialize, Serialize)]
struct IndiegalaBundle {
    price: f64,
    name: String,
    games: Vec<IndiegalaBundleGame>,
    active_until: String,
}

#[derive(Deserialize, Serialize)]
struct IndiegalaBundleGame {
    name: String,
    developer: String,
}

fn import_indiegala_bundle<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let mut file = File::open(&path)?;
    let ts = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let mut buf = Vec::with_capacity(8192);
    let _ = file.read_to_end(&mut buf)?;

    let items: Vec<IndiegalaBundle> = serde_json::from_slice(&buf)?;

    let insert_query = include_str!("../sql/insert_indiegala_bundle.sql");

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(insert_query)?;
        for item in items {
            let products = serde_json::to_value(item.games)?;
            let valid_until =
                NaiveDateTime::parse_from_str(&item.active_until, "%Y/%m/%d %H:%M:%S")?;

            stmt.execute(named_params! {
                ":ts": ts,
                ":name": &item.name,
                ":price": &item.price,
                ":products": products,
                ":valid_until": valid_until,
            })?;
        }
    }
    tx.commit()?;

    Ok(rows_inserted(&conn, "bundles_indiegala")?)
}
