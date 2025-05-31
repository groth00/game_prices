use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::Command,
    time::UNIX_EPOCH,
};

use anyhow::Error;
use bincode::{config, encode_to_vec};
use clap::{Parser, Subcommand};
use env_logger::{Builder, Env};
use log::{error, info};
use once_cell::sync::Lazy;
use regex::Regex;
use rusqlite::{Connection, Result, Statement, ToSql, config::DbConfig, named_params, params};
use serde::{Deserialize, Serialize};

use games_core::{
    algolia, fanatical, gmg, gog,
    indiegala::{self, DrmInfo},
    wgs,
};
use games_proto::generated::{CStoreBrowseGetItemsResponse, CStoreQueryQueryResponse, StoreItem};

mod gamebillet {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct PriceInfo {
        pub name: String,
        pub price: f64,
        pub percent_discount: u64,
    }
}

mod gamesplanet {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct PriceInfo {
        pub name: String,
        pub original_price: f64,
        pub discount: u64,
        pub price: f64,
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Import {
        #[command(subcommand)]
        action: ImportCmd,
    },
}

#[derive(Subcommand)]
enum ImportCmd {
    /// create metadata and price tables and indexes
    CreateTables,
    /// init drm, tags, categories tables
    InitMetadata,
    /// init map of name to steam appids from appinfo.jsonl
    InitSteamMap,
    /// insert pairs of unique names and normalized names for all stores
    UpdateNames,
    /// init steam metadata from appinfo.jsonl
    ImportMetadata,

    /// update map of steam name to appid set
    UpdateSteamMap,
    /// update steam metadata from appinfo_latest.jsonl
    UpdateMetadata,
    /// insert store prices
    ImportPrices,
    /// import bundles from steam, fanatical, indiegala
    ImportBundles,
    /// merge all price tables with latest information
    UpdateSearchTable,

    /// drop all tables
    DropTables,
    /// drop all tables and load initial metadata and prices
    Reset,
}

fn main() -> Result<(), Error> {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let mut conn = Connection::open("games.db")?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)?;

    let args = Args::parse();

    match &args.command {
        Commands::Import { action } => match action {
            ImportCmd::CreateTables => create_tables(&mut conn),
            ImportCmd::InitSteamMap => init_steam_map(),
            ImportCmd::UpdateSteamMap => update_steam_map(),
            ImportCmd::UpdateNames => do_upsert_names(&mut conn),
            ImportCmd::InitMetadata => init_metadata(&mut conn),
            ImportCmd::ImportMetadata => {
                update_steam_metadata(&mut conn, "output/steam/appinfo.jsonl")
            }
            ImportCmd::UpdateMetadata => {
                update_steam_metadata(&mut conn, "output/steam/appinfo_latest.jsonl")
            }
            ImportCmd::ImportPrices => import_prices(&mut conn),
            ImportCmd::ImportBundles => import_bundles(&mut conn),
            ImportCmd::UpdateSearchTable => update_search_table(&mut conn),
            ImportCmd::DropTables => drop_tables(&mut conn),
            ImportCmd::Reset => {
                drop_tables(&mut conn)?;
                create_tables(&mut conn)?;
                init_metadata(&mut conn)?;
                init_steam_map()?;
                update_steam_map()?;
                do_upsert_names(&mut conn)?;
                update_steam_metadata(&mut conn, "output/steam/appinfo.jsonl")?;
                import_prices(&mut conn)?;
                Ok(())
            }
        },
    }?;

    Ok(())
}

fn create_tables(conn: &mut Connection) -> Result<(), Error> {
    // metadata tables
    let create_drm_table =
        "CREATE TABLE IF NOT EXISTS drm(id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    let create_tags_table = "CREATE TABLE IF NOT EXISTS tags(id INTEGER PRIMARY KEY, tagid INTEGER NOT NULL, name TEXT NOT NULL)";
    let create_categories_table = "CREATE TABLE IF NOT EXISTS categories(id INTEGER PRIMARY KEY, catid INTEGER NOT NULL, catcat INTEGER NOT NULL, name TEXT NOT NULL)";
    let create_metadata_table = "CREATE TABLE IF NOT EXISTS metadata(id INTEGER PRIMARY KEY,
        name TEXT NOT NULL, cname TEXT NOT NULL, appid INTEGER, is_dlc INTEGER, tags INTEGER[],
        categories_player INTEGER[], categories_controller INTEGER[], categories_features INTEGER[],
        review_count INTEGER, review_pct_positive INTEGER,
        short_desc TEXT, publishers TEXT[], developers TEXT[], franchises TEXT[],
        release_date INTEGER, windows INTEGER, mac INTEGER, linux INTEGER, steam_deck_compat INTEGER,
        CONSTRAINT unique_name_pair UNIQUE(name, cname))";

    let create_metadata_appid_idx = "CREATE INDEX IF NOT EXISTS metadata_appid ON metadata(appid)";
    let create_metadata_cover_idx =
        "CREATE INDEX IF NOT EXISTS metadata_cover ON metadata(cname, id)";

    // price tables
    let create_fanatical_table = "CREATE TABLE IF NOT EXISTS fanatical(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, price REAL NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, available_from INTEGER NOT NULL, available_until INTEGER NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_gamebillet_table = "CREATE TABLE IF NOT EXISTS gamebillet(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_gamesplanet_table = "CREATE TABLE IF NOT EXISTS gamesplanet(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, price REAL NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_gmg_table = "CREATE TABLE IF NOT EXISTS gmg(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, appid INTEGER NOT NULL, price REAL NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_gog_table = "CREATE TABLE IF NOT EXISTS gog(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, price REAL NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, drm INTEGER DEFAULT 2, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_indiegala_table = "CREATE TABLE IF NOT EXISTS indiegala(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, price REAL NOT NULL, discount_price REAL NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_steam_table = "CREATE TABLE IF NOT EXISTS steam(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, packageid INTEGER, bundleid INTEGER, name TEXT NOT NULL, price REAL NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, available_until INTEGER NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";
    let create_wgs_table = "CREATE TABLE IF NOT EXISTS wgs(id INTEGER PRIMARY KEY, meta_id INTEGER, ts INTEGER NOT NULL, name TEXT NOT NULL, discount_price REAL NOT NULL, discount_percent INTEGER NOT NULL, is_dlc INTEGER NOT NULL, drm INTEGER DEFAULT 1, FOREIGN KEY(meta_id) REFERENCES metadata(id))";

    let tx = conn.transaction()?;
    {
        tx.execute(create_drm_table, [])?;
        tx.execute(create_tags_table, [])?;
        tx.execute(create_categories_table, [])?;
        tx.execute(create_metadata_table, [])?;

        tx.execute(create_metadata_appid_idx, [])?;
        tx.execute(create_metadata_cover_idx, [])?;

        tx.execute(create_fanatical_table, [])?;
        tx.execute(create_gamebillet_table, [])?;
        tx.execute(create_gamesplanet_table, [])?;
        tx.execute(create_gmg_table, [])?;
        tx.execute(create_gog_table, [])?;
        tx.execute(create_indiegala_table, [])?;
        tx.execute(create_steam_table, [])?;
        tx.execute(create_wgs_table, [])?;

        for table in [
            "fanatical",
            "gamebillet",
            "gamesplanet",
            "gmg",
            "gog",
            "indiegala",
            "steam",
            "wgs",
        ] {
            tx.execute(
                &format!(
                    "CREATE INDEX IF NOT EXISTS {}_cover ON {}(name, ts DESC, meta_id)",
                    table, table
                ),
                [],
            )?;
        }
    }
    tx.commit()?;

    info!("created all tables");
    Ok(())
}

fn drop_tables(conn: &mut Connection) -> Result<(), Error> {
    let drop_fanatical_table = "DROP TABLE IF EXISTS fanatical";
    let drop_gamebillet_table = "DROP TABLE IF EXISTS gamebillet";
    let drop_gamesplanet_table = "DROP TABLE IF EXISTS gamesplanet";
    let drop_gmg_table = "DROP TABLE IF EXISTS gmg";
    let drop_gog_table = "DROP TABLE IF EXISTS gog";
    let drop_indiegala_table = "DROP TABLE IF EXISTS indiegala";
    let drop_steam_table = "DROP TABLE IF EXISTS steam";
    let drop_wgs_table = "DROP TABLE IF EXISTS wgs";

    let drop_drm_table = "DROP TABLE IF EXISTS drm";
    let drop_tags_table = "DROP TABLE IF EXISTS tags";
    let drop_categories_table = "DROP TABLE IF EXISTS categories";
    let drop_metadata_table = "DROP TABLE IF EXISTS metadata";

    let tx = conn.transaction()?;

    {
        tx.execute(drop_fanatical_table, [])?;
        tx.execute(drop_gamebillet_table, [])?;
        tx.execute(drop_gamesplanet_table, [])?;
        tx.execute(drop_gmg_table, [])?;
        tx.execute(drop_gog_table, [])?;
        tx.execute(drop_indiegala_table, [])?;
        tx.execute(drop_steam_table, [])?;
        tx.execute(drop_wgs_table, [])?;

        tx.execute(drop_drm_table, [])?;
        tx.execute(drop_tags_table, [])?;
        tx.execute(drop_categories_table, [])?;
        tx.execute(drop_metadata_table, [])?;
    }
    info!("dropped all tables");

    tx.commit()?;

    Ok(())
}

fn init_metadata(conn: &mut Connection) -> Result<(), Error> {
    conn.execute(
        "INSERT INTO drm(name) VALUES('steam'), ('gog'), ('unknown')",
        [],
    )?;

    let tags_raw = fs::read_to_string("output/steam/tags.json")?;
    let tags: Tags = serde_json::from_str(&tags_raw)?;

    let insert_tags = "INSERT INTO tags(tagid, name) VALUES(:tagid, :name)";

    let tx = conn.transaction()?;

    {
        let mut stmt = tx.prepare(insert_tags)?;

        for tag in tags.response.tags {
            stmt.execute(named_params! {
            ":tagid": tag.tagid,
            ":name": tag.name})?;
        }
    }

    tx.commit()?;

    info!("insert into tags: {}", rows_inserted(conn, "tags")?);

    let categories_raw = fs::read_to_string("output/steam/categories.json")?;
    let categories: Vec<Category> = serde_json::from_str(&categories_raw)?;

    let insert_categories =
        "INSERT INTO categories(catid, catcat, name) VALUES(:catid, :catcat, :name)";

    let tx = conn.transaction()?;

    {
        let mut stmt = tx.prepare(insert_categories)?;

        for category in categories {
            stmt.execute(named_params! {
                ":catid": category.categoryid,
                ":catcat": category.category_type,
                ":name": category.display_name,
            })?;
        }
    }

    tx.commit()?;

    info!(
        "insert into categories: {}",
        rows_inserted(conn, "categories")?
    );

    Ok(())
}

/// create mapping of app names and nested purchase option names to appid
fn init_steam_map() -> Result<(), Error> {
    let input = File::open("output/steam/appinfo.jsonl")?;
    let reader = BufReader::new(input);

    let mut steam_items: Vec<SteamMapItem> = Vec::with_capacity(2 << 18);
    for line in reader.lines() {
        let line: CStoreBrowseGetItemsResponse = serde_json::from_str(&line?)?;
        for item in line.store_items {
            let mut metadata = SteamMapItem::default();

            if item.name.is_none() {
                continue;
            }

            if let Some(appid) = item.appid {
                metadata.appid = appid;
            } else {
                continue;
            }

            if let Some(info) = item.basic_info {
                metadata.publishers = info
                    .publishers
                    .iter()
                    .map(|x| x.name.as_deref().unwrap_or("".into()))
                    .collect::<Vec<&str>>()
                    .join(",");
                metadata.developers = info
                    .developers
                    .iter()
                    .map(|x| x.name.as_deref().unwrap_or("".into()))
                    .collect::<Vec<&str>>()
                    .join(",");
            }

            if let Some(release) = item.release {
                metadata.release_date = release.steam_release_date;
            }

            metadata.purchase_options = item
                .purchase_options
                .iter()
                .map(|o| PurchaseOption {
                    purchase_option_name: o.purchase_option_name.clone(),
                    packageid: o.packageid,
                    bundleid: o.bundleid,
                })
                .collect::<Vec<_>>();
            steam_items.push(metadata);
        }
    }

    // let input_path = "output/steam/appinfo_jaq.json";
    let output_path = "output/steam/map.json";

    let mut map: SteamMap = HashMap::new();
    for item in steam_items.into_iter() {
        let name = normalize(&item.name);
        map.entry(name).or_default().insert(item.appid);

        for subitem in item.purchase_options {
            match subitem.purchase_option_name {
                Some(n) => map.entry(normalize(&n)).or_default().insert(item.appid),
                _ => false,
            };
        }
    }

    let serialized = serde_json::to_string_pretty(&map)?;
    fs::write(output_path, serialized)?;

    Ok(())
}

/// fetch_appids -> fetch_appinfo -> update_steam_map
/// update mapping with info from IStoreService/GetItems
fn update_steam_map() -> Result<(), Error> {
    let map_path = "output/steam/map.json";
    let info_path = "output/steam/appinfo_latest.jsonl";

    let map_raw = fs::read(map_path)?;
    let mut map: SteamMap = serde_json::from_slice(&map_raw)?;
    info!("loaded map.json");

    let info = fs::read_to_string(info_path)?;

    for line in info.lines() {
        let items: MetadataResponse = serde_json::from_str(&line)?;
        for item in items.store_items {
            if item.name.is_none() || item.appid.is_none() {
                continue;
            }
            let name = normalize(&item.name.unwrap());
            map.entry(name)
                .or_default()
                .insert(item.appid.expect("missing appid"));

            for subitem in item.purchase_options {
                match subitem.purchase_option_name {
                    Some(n) => map
                        .entry(normalize(&n))
                        .or_default()
                        .insert(item.appid.unwrap()),
                    _ => false,
                };
            }
        }
    }

    let serialized = serde_json::to_string_pretty(&map)?;
    fs::write(map_path, serialized)?;

    Ok(())
}

fn do_upsert_names(mut conn: &mut Connection) -> Result<(), Error> {
    let mut paths: HashMap<&str, PathBuf> = HashMap::new();
    paths.insert("fanatical", latest_file("output/fanatical", "on_sale")?);
    paths.insert("gamebillet", latest_file("output/gamebillet", "on_sale")?);
    paths.insert("gamesplanet", latest_file("output/gamesplanet", "on_sale")?);
    paths.insert("gmg", latest_file("output/gmg", "on_sale")?);
    paths.insert("gog", latest_file("output/gog", "on_sale")?);
    paths.insert("indiegala", latest_file("output/indiegala", "on_sale")?);
    paths.insert("steam", PathBuf::from("output/steam/map.json"));
    paths.insert("wgs", latest_file("output/wingamestore", "on_sale")?);
    upsert_names(&mut conn, paths)?;
    Ok(())
}

/// insert unique (name, cname) pairs into metadata table from steam map
fn upsert_names<'a>(
    conn: &mut Connection,
    paths: HashMap<&'static str, PathBuf>,
) -> Result<(), Error> {
    let insert_name_only = "INSERT INTO metadata(name, cname) VALUES(:name, :cname) ON CONFLICT(name, cname) DO NOTHING";
    let insert_with_appid = "INSERT INTO metadata(name, cname, appid) VALUES(:name, :cname, :appid) ON CONFLICT(name, cname) DO NOTHING";

    let mut seen_names: HashSet<String> = HashSet::new();
    let mut raw_str = String::new();
    let mut raw_bytes = Vec::new();

    let mut steam = File::open(paths.get("steam").unwrap())?;
    steam.read_to_string(&mut raw_str)?;
    let steam_items: SteamMap = serde_json::from_str(&raw_str)?;

    let tx = conn.transaction()?;
    {
        let mut insert_with_appid_stmt = tx.prepare(insert_with_appid)?;
        for (name, appid_set) in steam_items.iter() {
            let cname = normalize(name);
            seen_names.insert(cname.clone());
            for appid in appid_set {
                insert_with_appid_stmt.execute(named_params! {
                    ":name": name,
                    ":cname": cname,
                    ":appid": appid,
                })?;
            }
        }
    }
    tx.commit()?;
    info!("insert steam names: {}", rows_inserted(conn, "metadata")?);
    raw_str.clear();

    let mut fanatical = File::open(paths.get("fanatical").unwrap())?;
    fanatical.read_to_string(&mut raw_str)?;

    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for line in raw_str.lines() {
            let row: algolia::AlgoliaResponse<fanatical::AlgoliaHit> = serde_json::from_str(line)?;
            for subrow in row.results {
                for hit in subrow.hits {
                    let cname = normalize(&hit.name);
                    if seen_names.insert(cname.clone()) {
                        insert_stmt
                            .execute(named_params! { ":name": &hit.name, ":cname": cname })?;
                    }
                }
            }
        }
    }
    tx.commit()?;
    info!(
        "insert fanatical names: {}",
        rows_inserted(conn, "metadata")?
    );
    raw_str.clear();

    let mut gamebillet = File::open(paths.get("gamebillet").unwrap())?;
    gamebillet.read_to_end(&mut raw_bytes)?;
    let rows: Vec<gamebillet::PriceInfo> = serde_json::from_slice(&raw_bytes)?;

    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for row in rows {
            let cname = normalize(&row.name);
            if seen_names.insert(cname.clone()) {
                insert_stmt.execute(named_params! { ":name": &row.name, ":cname": cname })?;
            }
        }
    }
    tx.commit()?;
    info!(
        "insert gamebillet names: {}",
        rows_inserted(conn, "metadata")?
    );
    raw_bytes.clear();

    let mut gamesplanet = File::open(paths.get("gamesplanet").unwrap())?;
    gamesplanet.read_to_string(&mut raw_str)?;

    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for line in raw_str.lines() {
            let items: Vec<gamesplanet::PriceInfo> = serde_json::from_str(line)?;
            for row in items {
                let cname = normalize(&row.name);
                if seen_names.insert(cname.clone()) {
                    insert_stmt.execute(named_params! { ":name": &row.name, ":cname": cname })?;
                }
            }
        }
    }
    tx.commit()?;
    info!(
        "insert gamesplanet names: {}",
        rows_inserted(conn, "metadata")?
    );
    raw_str.clear();

    let mut gmg = File::open(paths.get("gmg").unwrap())?;
    gmg.read_to_string(&mut raw_str)?;

    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for line in raw_str.lines() {
            let row: algolia::AlgoliaResponse<gmg::AlgoliaHit> = serde_json::from_str(line)?;
            for subrow in row.results {
                for hit in subrow.hits {
                    let cname = normalize(&hit.name);
                    if seen_names.insert(cname.clone()) {
                        insert_stmt
                            .execute(named_params! { ":name": &hit.name, ":cname": cname })?;
                    }
                }
            }
        }
    }
    tx.commit()?;
    info!("insert gmg names: {}", rows_inserted(conn, "metadata")?);
    raw_str.clear();

    let mut gog = File::open(paths.get("gog").unwrap())?;
    gog.read_to_end(&mut raw_bytes)?;

    let rows: Vec<gog::PriceInfo> = serde_json::from_slice(&raw_bytes)?;
    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for row in rows {
            let cname = normalize(&row.name);
            if seen_names.insert(cname.clone()) {
                insert_stmt.execute(named_params! { ":name": &row.name, ":cname": cname })?;
            }
        }
    }
    tx.commit()?;
    info!("insert gog names: {}", rows_inserted(conn, "metadata")?);
    raw_bytes.clear();

    let mut indiegala = File::open(paths.get("indiegala").unwrap())?;
    indiegala.read_to_end(&mut raw_bytes)?;
    let rows: Vec<indiegala::PriceInfo> = serde_json::from_slice(&raw_bytes)?;
    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for row in rows {
            if row.drm_info != DrmInfo::SteamKey {
                continue;
            }
            let cname = normalize(&row.title);
            if seen_names.insert(cname.clone()) {
                insert_stmt.execute(named_params! { ":name": &row.title, ":cname": cname })?;
            }
        }
    }
    tx.commit()?;
    info!(
        "insert indiegala names: {}",
        rows_inserted(conn, "metadata")?
    );
    raw_bytes.clear();

    let mut wgs = File::open(paths.get("wgs").unwrap())?;
    wgs.read_to_end(&mut raw_bytes)?;
    let rows: Vec<wgs::PriceInfo> = serde_json::from_slice(&raw_bytes)?;

    let tx = conn.transaction()?;
    {
        let mut insert_stmt = tx.prepare(insert_name_only)?;
        for row in rows {
            let cname = normalize(&row.title);
            if seen_names.insert(cname.clone()) {
                insert_stmt.execute(named_params! { ":name": &row.title, ":cname": cname })?;
            }
        }
    }
    tx.commit()?;
    info!("insert wgs names: {}", rows_inserted(conn, "metadata")?);
    raw_bytes.clear();

    Ok(())
}

fn update_steam_metadata<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<(), Error> {
    let insert_metadata = "UPDATE metadata SET is_dlc = ?1, tags = ?2, categories_player = ?3, categories_controller = ?4, categories_features = ?5, review_count = ?6, review_pct_positive = ?7, short_desc = ?8, publishers = ?9, developers = ?10, franchises = ?11, release_date = ?12, windows = ?13, mac = ?14, linux = ?15, steam_deck_compat = ?16 WHERE appid = ?17";

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut appids: BTreeSet<u32> = BTreeSet::new();

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(insert_metadata)?;

        for line in reader.lines() {
            let row: MetadataResponse = serde_json::from_str(&line?)?;

            for item in row.store_items {
                let mut metadata = Metadata::default();

                if item.name.is_none() {
                    continue;
                }

                if let Some(appid) = item.appid {
                    if appids.insert(appid) == false {
                        continue;
                    } else {
                        metadata.appid = appid;
                    }
                } else {
                    continue;
                }

                if let Some(item_type) = item.r#type {
                    if item_type == 0 {
                        metadata.is_dlc = false;
                    } else if item_type == 4 {
                        metadata.is_dlc = true;
                    }
                } else {
                    metadata.is_dlc = false;
                }

                let tagids = encode_to_vec(item.tagids, config::standard())?;
                metadata.tags = tagids;

                if let Some(cats) = item.categories {
                    let categories_player =
                        encode_to_vec(cats.supported_player_categoryids, config::standard())?;
                    metadata.categories_player = categories_player;

                    let categories_controller =
                        encode_to_vec(cats.controller_categoryids, config::standard())?;
                    metadata.categories_controller = categories_controller;

                    let categories_features =
                        encode_to_vec(cats.feature_categoryids, config::standard())?;
                    metadata.categories_features = categories_features;
                }

                if let Some(review) = item.reviews {
                    if let Some(summary) = review.summary_filtered {
                        metadata.review_count = summary.review_count;
                        metadata.review_pct_positive = summary.percent_positive;
                    }
                }

                if let Some(info) = item.basic_info {
                    metadata.short_desc = info.short_description.unwrap_or("".into());
                    metadata.publishers = info
                        .publishers
                        .iter()
                        .map(|x| x.name.as_deref().unwrap_or("".into()))
                        .collect::<Vec<&str>>()
                        .join(",");
                    metadata.developers = info
                        .developers
                        .iter()
                        .map(|x| x.name.as_deref().unwrap_or("".into()))
                        .collect::<Vec<&str>>()
                        .join(",");
                    metadata.franchises = info
                        .franchises
                        .iter()
                        .map(|x| x.name.as_deref().unwrap_or("".into()))
                        .collect::<Vec<&str>>()
                        .join(",");
                }

                if let Some(release) = item.release {
                    metadata.release_date = release.steam_release_date;
                }

                if let Some(platforms) = item.platforms {
                    metadata.windows = platforms.windows.unwrap_or(false);
                    metadata.mac = platforms.mac.unwrap_or(false);
                    metadata.linux = platforms.linux.unwrap_or(false);
                    metadata.steam_deck_compat = platforms.steam_deck_compat_category;
                }

                stmt.execute(params![
                    metadata.is_dlc,
                    metadata.tags,
                    metadata.categories_player,
                    metadata.categories_controller,
                    metadata.categories_features,
                    metadata.review_count,
                    metadata.review_pct_positive,
                    metadata.short_desc,
                    metadata.developers,
                    metadata.publishers,
                    metadata.franchises,
                    metadata.release_date,
                    metadata.windows,
                    metadata.mac,
                    metadata.linux,
                    metadata.steam_deck_compat,
                    metadata.appid,
                ])?;
            }
        }
    }
    tx.commit()?;
    info!("insert into metadata: {}", rows_inserted(conn, "metadata")?);

    Ok(())
}

fn import_prices(mut conn: &mut Connection) -> Result<(), Error> {
    let path_fanatical = latest_file("output/fanatical", "on_sale")?;
    let path_gamebillet = latest_file("output/gamebillet", "on_sale")?;
    let path_gamesplanet = latest_file("output/gamesplanet", "on_sale")?;
    let path_gmg = latest_file("output/gmg", "on_sale")?;
    let path_gog = latest_file("output/gog", "on_sale")?;
    let path_indiegala = latest_file("output/indiegala", "on_sale")?;
    let path_steam = latest_file("output/steam", "on_sale")?;
    let path_wgs = latest_file("output/wingamestore", "on_sale")?;

    info!(
        "insert prices fanatical: {}",
        insert_fanatical(&mut conn, path_fanatical)?
    );
    info!(
        "insert prices gamebillet: {}",
        insert_gamebillet(&mut conn, path_gamebillet)?
    );
    info!(
        "insert prices gamesplanet: {}",
        insert_gamesplanet(&mut conn, path_gamesplanet)?
    );
    info!("insert prices gmg: {}", insert_gmg(&mut conn, path_gmg)?);
    info!("insert prices gog: {}", insert_gog(&mut conn, path_gog)?);
    info!(
        "insert prices indiegala: {}",
        insert_indiegala(&mut conn, path_indiegala)?
    );
    info!(
        "insert prices steam: {}",
        insert_steam(&mut conn, path_steam)?
    );
    info!("insert prices wgs: {}", insert_wgs(&mut conn, path_wgs)?);

    Ok(())
}

fn insert_fanatical<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Fanatical.queries();

    let mut file = File::open(&path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;
    let mut raw = String::with_capacity(2 * 2 << 20);
    file.read_to_string(&mut raw)?;

    let tx = conn.transaction()?;
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "fanatical",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for line in raw.lines() {
            let row: algolia::AlgoliaResponse<fanatical::AlgoliaHit> = serde_json::from_str(&line)?;
            // game and dlc arrays; each has hits array
            for subrow in row.results {
                for hit in subrow.hits {
                    let cname = normalize(&hit.name);
                    let params: [(&'static str, &dyn ToSql); 8] = [
                        (":meta_id", &0),
                        (":ts", &modified),
                        (":name", &cname),
                        (":price", &hit.full_price.usd),
                        (":discount_price", &hit.price.usd),
                        (":discount_percent", &hit.discount_percent),
                        (":available_from", &hit.available_valid_from),
                        (":available_until", &hit.available_valid_until),
                    ];
                    ins.insert_checked(
                        &cname,
                        &[&cname],
                        &[&cname],
                        &[&hit.name, &cname],
                        &params,
                    )?;
                }
            }
        }
    }
    tx.commit()?;
    Ok(rows_inserted(conn, "fanatical")?)
}

fn insert_gamebillet<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Gamebillet.queries();

    let mut file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let mut raw = Vec::with_capacity(2 << 20);
    file.read_to_end(&mut raw)?;

    let rows: Vec<gamebillet::PriceInfo> = serde_json::from_slice(&raw)?;

    let tx = conn.transaction()?;

    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "gamebillet",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for row in rows {
            let cname = normalize(&row.name);
            let params: [(&'static str, &dyn ToSql); 5] = [
                (":meta_id", &0),
                (":ts", &modified),
                (":name", &cname),
                (":discount_price", &row.price),
                (":discount_percent", &row.percent_discount),
            ];

            ins.insert_checked(&cname, &[&cname], &[&cname], &[&row.name, &cname], &params)?;
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "gamebillet")?)
}

fn insert_gamesplanet<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Gamesplanet.queries();

    let mut file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let mut raw = String::with_capacity(2 << 20);
    file.read_to_string(&mut raw)?;

    let tx = conn.transaction()?;
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "gamesplanet",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for line in raw.lines() {
            let items: Vec<gamesplanet::PriceInfo> = serde_json::from_str(&line)?;
            for row in items {
                let cname = normalize(&row.name);
                let params: [(&'static str, &dyn ToSql); 6] = [
                    (":meta_id", &0),
                    (":ts", &modified),
                    (":name", &cname),
                    (":price", &row.original_price),
                    (":discount_price", &row.price),
                    (":discount_percent", &row.discount),
                ];
                ins.insert_checked(&cname, &[&cname], &[&cname], &[&row.name, &cname], &params)?;
            }
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "gamesplanet")?)
}

fn insert_gmg<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Gmg.queries();

    let mut file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;
    let mut raw = String::with_capacity(2 << 20);
    file.read_to_string(&mut raw)?;

    let tx = conn.transaction()?;
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "gmg",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for line in raw.lines() {
            let row: algolia::AlgoliaResponse<gmg::AlgoliaHit> = serde_json::from_str(&line)?;
            for subrow in row.results {
                for hit in subrow.hits {
                    let cname = normalize(&hit.name);
                    let params: [(&'static str, &dyn ToSql); 7] = [
                        (":meta_id", &0),
                        (":ts", &modified),
                        (":appid", &hit.steam_app_id),
                        (":name", &cname),
                        (":price", &hit.regions.us.original_price),
                        (":discount_price", &hit.regions.us.price),
                        (":discount_percent", &hit.regions.us.discount_percent),
                    ];
                    ins.insert_checked(
                        &cname,
                        &[&cname],
                        &[&cname],
                        &[&hit.name, &cname],
                        &params,
                    )?;
                }
            }
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "gmg")?)
}

fn insert_gog<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Gog.queries();

    let mut file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;
    let mut raw = Vec::with_capacity(2 << 20);
    file.read_to_end(&mut raw)?;

    let rows: Vec<gog::PriceInfo> = serde_json::from_slice(&raw)?;

    let tx = conn.transaction()?;
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "gog",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for row in rows {
            let cname = normalize(&row.name);
            let params: [(&'static str, &dyn ToSql); 6] = [
                (":meta_id", &0),
                (":ts", &modified),
                (":name", &cname),
                (":price", &row.original_price),
                (":discount_price", &row.price),
                (":discount_percent", &row.percent_discount),
            ];
            ins.insert_checked(&cname, &[&cname], &[&cname], &[&row.name, &cname], &params)?;
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "gog")?)
}

fn insert_indiegala<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Indiegala.queries();

    let mut file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;
    let mut raw = Vec::with_capacity(2 << 20);
    file.read_to_end(&mut raw)?;

    let rows: Vec<indiegala::PriceInfo> = serde_json::from_slice(&raw)?;

    let tx = conn.transaction()?;
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "indiegala",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for row in rows {
            if row.drm_info != DrmInfo::SteamKey {
                continue;
            }
            let cname = normalize(&row.title);
            let params: [(&'static str, &dyn ToSql); 5] = [
                (":meta_id", &0),
                (":ts", &modified),
                (":name", &cname),
                (":price", &row.price),
                (":discount_price", &row.discount_price),
            ];
            ins.insert_checked(&cname, &[&cname], &[&cname], &[&row.title, &cname], &params)?;
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "indiegala")?)
}

fn insert_steam<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Steam.queries();

    let file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;
    let reader = BufReader::new(file);

    let tx = conn.transaction()?;

    // bundles and packages may have the same id even though the items are different
    let mut bundleids = BTreeSet::new();
    let mut packageids = BTreeSet::new();
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "steam",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for line in reader.lines() {
            let row: CStoreQueryQueryResponse = serde_json::from_str(&line?)?;
            'items: for item in row.store_items {
                // insert price info for purchase options (with different editions, bundles)
                for opt in item.purchase_options {
                    let (name, cname) = if let Some(name) = opt.purchase_option_name {
                        (name.clone(), normalize(&name))
                    } else {
                        continue;
                    };

                    // NOTE: skip purchase options for a game that are not discounted
                    if opt.active_discounts.is_empty() {
                        continue;
                    }

                    let packageid = if let Some(packageid) = opt.packageid {
                        if packageids.insert(packageid) == false {
                            continue 'items;
                        } else {
                            Some(packageid)
                        }
                    } else {
                        None
                    };

                    let bundleid = if let Some(bundleid) = opt.bundleid {
                        if bundleids.insert(bundleid) == false {
                            continue 'items;
                        } else {
                            Some(bundleid)
                        }
                    } else {
                        None
                    };

                    let original_price = opt
                        .original_price_in_cents
                        .expect("missing original_price_in_cents")
                        as f64
                        / 100f64;

                    let price =
                        opt.final_price_in_cents
                            .expect("missing final_price_in_cents") as f64
                            / 100f64;

                    let available_until = opt.active_discounts[0]
                        .discount_end_date
                        .expect("active_discount present but no discount_end_date");

                    let params: [(&'static str, &dyn ToSql); 9] = [
                        (":meta_id", &0),
                        (":ts", &modified),
                        (":packageid", &packageid),
                        (":bundleid", &bundleid),
                        (":name", &name),
                        (":price", &original_price),
                        (":discount_price", &price),
                        (":discount_percent", &opt.discount_pct),
                        (":available_until", &available_until),
                    ];
                    ins.insert_checked(
                        &cname,
                        &[&cname, &packageid, &bundleid],
                        &[&cname],
                        &[&name, &cname],
                        &params,
                    )?;
                }
            }
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "steam")?)
}

fn insert_wgs<P: AsRef<Path>>(conn: &mut Connection, path: P) -> Result<i64, Error> {
    let queries = Store::Wgs.queries();

    let mut file = File::open(path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;
    let mut raw = Vec::with_capacity(2 << 20);
    file.read_to_end(&mut raw)?;

    let rows: Vec<wgs::PriceInfo> = serde_json::from_slice(&raw)?;

    let tx = conn.transaction()?;

    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let mut ins = Inserter {
            table: "wgs",
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };

        for row in rows {
            let cname = normalize(&row.title);
            let params: [(&'static str, &dyn ToSql); 6] = [
                (":meta_id", &0),
                (":ts", &modified),
                (":name", &cname),
                (":discount_price", &row.price),
                (":discount_percent", &row.percent_discount),
                (":is_dlc", &(row.is_dlc as i32)),
            ];
            ins.insert_checked(&cname, &[&cname], &[&cname], &[&row.title, &cname], &params)?;
        }
    }

    tx.commit()?;
    Ok(rows_inserted(conn, "wgs")?)
}

fn import_bundles(conn: &mut Connection) -> Result<(), Error> {
    info!(
        "insert bundles fanatical: {}",
        import_fanatical_bundles(conn)?
    );
    info!("insert bundles steam: {}", import_steam_bundles(conn)?);
    info!(
        "import bundles indiegala: {}",
        import_indiegala_bundles(conn)?
    );

    Ok(())
}

fn import_fanatical_bundles(conn: &mut Connection) -> Result<i64, Error> {
    Ok(rows_inserted(conn, "bundles")?)
}

fn import_steam_bundles(conn: &mut Connection) -> Result<i64, Error> {
    Ok(rows_inserted(conn, "bundles")?)
}

fn import_indiegala_bundles(conn: &mut Connection) -> Result<i64, Error> {
    Ok(rows_inserted(conn, "bundles")?)
}

fn update_search_table(conn: &mut Connection) -> Result<(), Error> {
    let mut cmd = Command::new("sqlite3");
    cmd.args(&["games.db", "<src/importer/sql/insert_prices.sql"]);
    cmd.output()?;
    info!("inserted into prices: {}", rows_inserted(conn, "prices")?);

    Ok(())
}

struct Inserter<'a> {
    table: &'static str,
    exists_price: Statement<'a>,
    exists_meta: Statement<'a>,
    insert_meta: Statement<'a>,
    insert_price: Statement<'a>,
}

impl<'a> Inserter<'a> {
    fn insert_checked(
        &mut self,
        name: &str,
        ep_params: &[&dyn ToSql],
        em_params: &[&dyn ToSql],
        im_params: &[&dyn ToSql],
        ip_params: &[(&'static str, &dyn ToSql)],
    ) -> Result<(), Error> {
        let mut params = ip_params.to_owned();

        // if there is an existing row in the price table reuse the meta_id
        let meta_id = self
            .exists_price
            .query_row(ep_params, |r| r.get::<usize, i64>(0))
            .unwrap_or(0);
        params[0].1 = &meta_id;

        if meta_id > 0 {
            self.insert_price.execute(params.as_slice())?;
        } else {
            // check metadata table (row from a different store)
            let num_matches = self
                .exists_meta
                .query_map(em_params, |r| r.get::<usize, i64>(0))?
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            let collision = check_collision(&num_matches);

            match collision {
                NameCollision::DoesNotExist => {
                    let rowid = self
                        .insert_meta
                        .query_row(im_params, |r| r.get::<usize, i64>(0))?;
                    params[0].1 = &rowid;
                    self.insert_price.execute(params.as_slice())?;
                }
                NameCollision::Exists => {
                    params[0].1 = &num_matches[0];
                    self.insert_price.execute(params.as_slice())?;
                }
                NameCollision::Collision => {
                    error!("name collision in {}: {}", &self.table, &name);
                }
            }
        }
        Ok(())
    }
}

enum NameCollision {
    DoesNotExist,
    Exists,
    Collision,
}

fn check_collision(num_matches: &[i64]) -> NameCollision {
    match num_matches.len() {
        0 => NameCollision::DoesNotExist,
        1 => NameCollision::Exists,
        _ => NameCollision::Collision,
    }
}

enum Store {
    Fanatical,
    Gamebillet,
    Gamesplanet,
    Gmg,
    Gog,
    Indiegala,
    Steam,
    Wgs,
}

struct Queries {
    exists_price: &'static str,
    exists_meta: &'static str,
    insert_meta: &'static str,
    insert_price: &'static str,
}

impl Store {
    const EXISTS_META: &str = "SELECT rowid FROM metadata WHERE cname = ?1";
    const INSERT_META: &str =
        "INSERT INTO metadata(name, cname) VALUES(:name, :cname) RETURNING rowid";

    const fn queries(&self) -> Queries {
        match *self {
            Self::Fanatical => Queries {
                exists_price: "SELECT meta_id FROM fanatical WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO fanatical(meta_id, ts, name, price, discount_price, discount_percent, available_from, available_until) VALUES(:meta_id, :ts, :name, :price, :discount_price, :discount_percent, :available_from, :available_until)",
            },
            Self::Gamebillet => Queries {
                exists_price: "SELECT meta_id FROM gamebillet WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO gamebillet(meta_id, ts, name, discount_price, discount_percent) VALUES(:meta_id, :ts, :name, :discount_price, :discount_percent)",
            },
            Self::Gamesplanet => Queries {
                exists_price: "SELECT meta_id FROM gamesplanet WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO gamesplanet(meta_id, ts, name, price, discount_price, discount_percent) VALUES(:meta_id, :ts, :name, :price, :discount_price, :discount_percent)",
            },
            Self::Gmg => Queries {
                exists_price: "SELECT meta_id FROM gmg WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO gmg(meta_id, ts, appid, name, price, discount_price, discount_percent) VALUES(:meta_id, :ts, :appid, :name, :price, :discount_price, :discount_percent)",
            },
            Self::Gog => Queries {
                exists_price: "SELECT meta_id FROM gog WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO gog(meta_id, ts, name, price, discount_price, discount_percent) VALUES(:meta_id, :ts, :name, :price, :discount_price, :discount_percent)",
            },
            Self::Indiegala => Queries {
                exists_price: "SELECT meta_id FROM indiegala WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO indiegala(meta_id, ts, name, price, discount_price) VALUES(:meta_id, :ts, :name, :price, :discount_price)",
            },
            Self::Steam => Queries {
                exists_price: "SELECT meta_id FROM steam WHERE name = ?1 AND packageid = ?2 AND bundleid = ?3 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO steam(meta_id, ts, packageid, bundleid, name, price, discount_price, discount_percent, available_until) VALUES(:meta_id, :ts, :packageid, :bundleid, :name, :price, :discount_price, :discount_percent, :available_until)",
            },
            Self::Wgs => Queries {
                exists_price: "SELECT meta_id FROM wgs WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: "INSERT INTO wgs(meta_id, ts, name, discount_price, discount_percent, is_dlc) VALUES(:meta_id, :ts, :name, :discount_price, :discount_percent, :is_dlc)",
            },
        }
    }
}

fn latest_file(input_dir: &'static str, prefix: &'static str) -> Result<PathBuf, Error> {
    let mut dir = fs::read_dir(input_dir)?;

    let mut latest = 0;
    let mut path = PathBuf::new();

    while let Some(Ok(entry)) = dir.next() {
        if entry.file_type()?.is_dir() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        if file_name.starts_with(prefix) {
            let touched = entry
                .metadata()?
                .modified()?
                .duration_since(UNIX_EPOCH)?
                .as_secs();
            if touched > latest {
                latest = touched;
                path = entry.path();
            }
        }
    }

    info!("latest file in {}: {:?}", &input_dir, &path);
    Ok(path)
}

fn normalize(name: &str) -> String {
    static NON_ALPHABETIC: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^0-9A-Za-z\s\p{L}]").unwrap());
    static WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

    let tmp1 = NON_ALPHABETIC.replace_all(&name, "");
    let tmp1 = WHITESPACE.replace_all(&tmp1, " ");

    tmp1.trim().to_ascii_lowercase()
}

fn rows_inserted(conn: &Connection, table: &'static str) -> Result<i64, Error> {
    let query = format!("SELECT COUNT(1) FROM {}", table);
    let rows = conn.query_row(&query, [], |r| r.get::<usize, i64>(0))?;
    Ok(rows)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Item {
    name: String,
    appid: u32,
    developers: String,
    publishers: String,
    release_date: Option<u32>,
    purchase_options: Vec<PurchaseOption>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PurchaseOption {
    purchase_option_name: Option<String>,
    packageid: Option<i32>,
    bundleid: Option<i32>,
}

type SteamMap = HashMap<String, BTreeSet<u32>>;

#[derive(Default, Serialize, Deserialize)]
struct SteamMapItem {
    name: String,
    appid: u32,
    developers: String,
    publishers: String,
    release_date: Option<u32>,
    purchase_options: Vec<PurchaseOption>,
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
    category_type: u32,
    display_name: String,
}

#[derive(Deserialize)]
struct MetadataResponse {
    store_items: Vec<StoreItem>,
}

#[derive(Default, Clone)]
struct Metadata {
    appid: u32,
    is_dlc: bool,
    tags: Vec<u8>,
    categories_player: Vec<u8>,
    categories_controller: Vec<u8>,
    categories_features: Vec<u8>,
    review_count: Option<u32>,
    review_pct_positive: Option<i32>,
    short_desc: String,
    developers: String,
    publishers: String,
    franchises: String,
    release_date: Option<u32>,
    windows: bool,
    mac: bool,
    linux: bool,
    steam_deck_compat: Option<i32>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_normalize() {
        let mut title = "Stellaris: Species Pack Bundle".into();
        let mut normalized = normalize(title);
        assert_eq!(normalized, "Stellaris Species Pack Bundle");

        title = "Palworld - Game + Soundtrack Bundle".into();
        normalized = normalize(title);
        assert_eq!(normalized, "Palworld Game Soundtrack Bundle");
    }
}
