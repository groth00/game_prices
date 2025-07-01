use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::{self, Display, Formatter},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::Path,
    time::UNIX_EPOCH,
};

use anyhow::Error;
use bincode::config;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use log::{error, info};
use rusqlite::{Connection, Result, Statement, ToSql, named_params, params};
use serde::{Deserialize, Serialize};

use crate::utils::{all_files, execute_sql, latest_file, move_file, normalize, rows_inserted};
use games_core::{
    algolia, fanatical, gmg,
    gog::{self, GogResponse},
    indiegala::{self, DrmInfo},
    wgs,
};
use games_proto::generated::{CStoreBrowseGetItemsResponse, CStoreQueryQueryResponse, StoreItem};

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

/// create mapping of app names and nested purchase option names to appid
pub fn init_steam_map() -> Result<(), Error> {
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

    let output_path = "output/steam/map.json";

    let mut map: SteamMap = HashMap::new();
    for item in steam_items.into_iter() {
        let name = normalize(&item.name);
        map.entry(name).or_default().insert(item.appid);

        for subitem in item.purchase_options {
            if let Some(n) = subitem.purchase_option_name {
                map.entry(normalize(&n)).or_default().insert(item.appid);
            };
        }
    }

    let serialized = serde_json::to_string_pretty(&map)?;
    fs::write(output_path, serialized)?;

    fs::rename(
        "output/steam/appinfo.jsonl",
        "output/backup/steam/appinfo.jsonl",
    )?;

    Ok(())
}

/// fetch_appids -> fetch_appinfo -> update_steam_map
/// update mapping with info from IStoreService/GetItems
pub fn update_steam_map() -> Result<(), Error> {
    let map_path = "output/steam/map.json";
    let info_path = latest_file("output/steam", "appinfo")?.unwrap();
    info!("updating from {:?}", &info_path);

    let map_raw = fs::read(map_path)?;
    let mut map: SteamMap = serde_json::from_slice(&map_raw)?;
    info!("loaded map.json");

    let info = fs::read_to_string(&info_path)?;

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

struct NameInserter<'a> {
    conn: &'a mut Connection,
    seen_names: HashSet<String>,
}

impl<'a> NameInserter<'a> {
    const INSERT: &'static str = "INSERT INTO metadata(name, cname) VALUES(:name, :cname) ON CONFLICT(name, cname) DO NOTHING";
    const INSERT_WITH_APPID: &'static str = "INSERT INTO metadata(name, cname, appid) VALUES(:name, :cname, :appid) ON CONFLICT(name, cname) DO NOTHING";

    fn new(conn: &'a mut Connection) -> Self {
        Self {
            conn,
            seen_names: HashSet::new(),
        }
    }

    fn insert_steam(&mut self) -> Result<(), Error> {
        let mut raw_str = String::with_capacity((1 << 20) * 20);
        let mut steam =
            File::open("output/steam/map.json").expect("map.json not found, did you generate it?");
        steam.read_to_string(&mut raw_str)?;
        let steam_items: SteamMap = serde_json::from_str(&raw_str)?;

        let tx = self.conn.transaction()?;
        {
            let mut insert_with_appid_stmt = tx.prepare(Self::INSERT_WITH_APPID)?;
            for (name, appid_set) in steam_items.iter() {
                let cname = normalize(name);
                self.seen_names.insert(cname.clone());
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
        info!(
            "insert steam names: {}",
            rows_inserted(self.conn, "metadata")?
        );

        Ok(())
    }

    fn insert_fanatical(&mut self) -> Result<(), Error> {
        for file in all_files("output/fanatical", "on_sale")? {
            let fanatical = fs::read_to_string(file.path())?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;
                for line in fanatical.lines() {
                    let row: algolia::AlgoliaMultiResponse<fanatical::AlgoliaHit> =
                        serde_json::from_str(line)?;
                    for subrow in row.results {
                        for hit in subrow.hits {
                            let cname = normalize(&hit.name);
                            if self.seen_names.insert(cname.clone()) {
                                insert_stmt.execute(
                                    named_params! { ":name": &hit.name, ":cname": cname },
                                )?;
                            }
                        }
                    }
                }
            }
            tx.commit()?;
            info!(
                "insert fanatical names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }
        Ok(())
    }

    fn insert_gamebillet(&mut self) -> Result<(), Error> {
        for file in all_files("output/gamebillet", "on_sale")? {
            let gamebillet = fs::read_to_string(file.path())?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;

                for line in gamebillet.lines() {
                    let rows: Vec<GamebilletPriceInfo> = serde_json::from_str(&line)?;
                    for row in rows {
                        let cname = normalize(&row.name);
                        if self.seen_names.insert(cname.clone()) {
                            insert_stmt
                                .execute(named_params! { ":name": &row.name, ":cname": cname })?;
                        }
                    }
                }
            }

            tx.commit()?;
            info!(
                "insert gamebillet names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }

        Ok(())
    }

    fn insert_gamesplanet(&mut self) -> Result<(), Error> {
        for file in all_files("output/gamesplanet", "on_sale")? {
            let gamesplanet = fs::read_to_string(file.path())?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;

                for line in gamesplanet.lines() {
                    let rows: Vec<GamesplanetPriceInfo> = serde_json::from_str(&line)?;
                    for row in rows {
                        let cname = normalize(&row.name);
                        if self.seen_names.insert(cname.clone()) {
                            insert_stmt
                                .execute(named_params! { ":name": &row.name, ":cname": cname })?;
                        }
                    }
                }
            }

            tx.commit()?;
            info!(
                "insert gamesplanet names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }

        Ok(())
    }

    fn insert_gmg(&mut self) -> Result<(), Error> {
        for file in all_files("output/gmg", "on_sale")? {
            let gmg = fs::read_to_string(file.path())?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;
                for line in gmg.lines() {
                    let row: algolia::AlgoliaMultiResponse<gmg::AlgoliaHit> =
                        serde_json::from_str(line)?;
                    for subrow in row.results {
                        for hit in subrow.hits {
                            let cname = normalize(&hit.display_name);
                            if self.seen_names.insert(cname.clone()) {
                                insert_stmt.execute(
                                    named_params! { ":name": &hit.display_name, ":cname": cname },
                                )?;
                            }
                        }
                    }
                }
            }
            tx.commit()?;
            info!(
                "insert gmg names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }

        Ok(())
    }

    fn insert_gog(&mut self) -> Result<(), Error> {
        for file in all_files("output/gog", "on_sale")? {
            let gog = fs::read_to_string(file.path())?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;
                for line in gog.lines() {
                    let resp: GogResponse = serde_json::from_str(&line)?;
                    for row in resp.products {
                        let cname = normalize(&row.title);
                        if self.seen_names.insert(cname.clone()) {
                            insert_stmt
                                .execute(named_params! { ":name": &row.title, ":cname": cname })?;
                        }
                    }
                }
            }
            tx.commit()?;
            info!(
                "insert gog names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }

        Ok(())
    }

    fn insert_indiegala(&mut self) -> Result<(), Error> {
        for file in all_files("output/indiegala", "on_sale")? {
            let indiegala = fs::read(file.path())?;
            let rows: Vec<indiegala::PriceInfo> = serde_json::from_slice(&indiegala)?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;
                for row in rows {
                    if row.drm_info != DrmInfo::SteamKey {
                        continue;
                    }
                    let cname = normalize(&row.title);
                    if self.seen_names.insert(cname.clone()) {
                        insert_stmt
                            .execute(named_params! { ":name": &row.title, ":cname": cname })?;
                    }
                }
            }
            tx.commit()?;
            info!(
                "insert indiegala names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }

        Ok(())
    }

    fn insert_wgs(&mut self) -> Result<(), Error> {
        for file in all_files("output/wingamestore", "on_sale")? {
            let wgs = fs::read(file.path())?;
            let rows: Vec<wgs::PriceInfo> = serde_json::from_slice(&wgs)?;

            let tx = self.conn.transaction()?;
            {
                let mut insert_stmt = tx.prepare(Self::INSERT)?;
                for row in rows {
                    let cname = normalize(&row.name);
                    if self.seen_names.insert(cname.clone()) {
                        insert_stmt
                            .execute(named_params! { ":name": &row.name, ":cname": cname })?;
                    }
                }
            }
            tx.commit()?;
            info!(
                "insert wgs names: {}",
                rows_inserted(self.conn, "metadata")?
            );
        }

        Ok(())
    }
}

/// insert unique (name, cname) pairs into metadata table from steam map
#[inline]
pub fn upsert_names(conn: &mut Connection) -> Result<(), Error> {
    let mut ins = NameInserter::new(conn);
    ins.insert_steam()?;
    ins.insert_fanatical()?;
    ins.insert_gamebillet()?;
    ins.insert_gamesplanet()?;
    ins.insert_gmg()?;
    ins.insert_gog()?;
    ins.insert_indiegala()?;
    ins.insert_wgs()
}

pub fn update_steam_metadata(conn: &mut Connection) -> Result<(), Error> {
    for entry in all_files("output/steam", "appinfo")? {
        let insert_metadata = include_str!("../sql/update_metadata.sql");

        let file = File::open(entry.path())?;
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

                    let tagids = bincode::encode_to_vec(&item.tagids, config::standard())?;
                    metadata.tags = tagids;

                    if let Some(cats) = item.categories {
                        let categories_player = bincode::encode_to_vec(
                            &cats.supported_player_categoryids,
                            config::standard(),
                        )?;
                        metadata.categories_player = categories_player;

                        let categories_controller = bincode::encode_to_vec(
                            &cats.controller_categoryids,
                            config::standard(),
                        )?;
                        metadata.categories_controller = categories_controller;

                        let categories_features =
                            bincode::encode_to_vec(&cats.feature_categoryids, config::standard())?;
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

        move_file(&entry.path(), "steam")?;
    }
    Ok(())
}

pub fn import_prices(mut conn: &mut Connection) -> Result<(), Error> {
    let stores: [(
        &'static str,
        Store,
        &dyn Fn(Vec<u8>) -> Result<PriceData, Error>,
        &dyn Fn(Inserter, PriceData, u32) -> Result<(), Error>,
    ); 8] = [
        (
            "output/fanatical",
            Store::Fanatical,
            &fanatical_process,
            &fanatical_insert,
        ),
        (
            "output/gamebillet",
            Store::Gamebillet,
            &gamebillet_process,
            &gamebillet_insert,
        ),
        (
            "output/gamesplanet",
            Store::Gamesplanet,
            &gamesplanet_process,
            &gamesplanet_insert,
        ),
        ("output/gmg", Store::Gmg, &gmg_process, &gmg_insert),
        ("output/gog", Store::Gog, &gog_process, &gog_insert),
        (
            "output/indiegala",
            Store::Indiegala,
            &indiegala_process,
            &indiegala_insert,
        ),
        ("output/steam", Store::Steam, &steam_process, &steam_insert),
        ("output/wingamestore", Store::Wgs, &wgs_process, &wgs_insert),
    ];

    for (input_dir, store, process_fn, insert_fn) in stores {
        let store_name: &str = store.into();
        let entries = all_files(input_dir, "on_sale")?;
        for entry in entries {
            let filename = entry.file_name();
            info!("importing {:?}", filename);

            let rows_inserted =
                insert_store(&mut conn, entry.path(), store, process_fn, insert_fn)?;
            info!("insert prices {}: {}", store_name, rows_inserted);

            move_file(&entry.path(), store_name)?;
        }
    }

    Ok(())
}

fn insert_store<P: AsRef<Path>>(
    conn: &mut Connection,
    path: P,
    store: Store,
    process_fn: impl Fn(Vec<u8>) -> Result<PriceData, Error>,
    insert_fn: impl Fn(Inserter, PriceData, u32) -> Result<(), Error>,
) -> Result<i64, Error> {
    let queries = store.queries();
    let table: &'static str = store.into();
    let mut file = File::open(&path)?;
    let modified = file
        .metadata()?
        .modified()?
        .duration_since(UNIX_EPOCH)?
        .as_secs() as u32;

    let mut raw = Vec::with_capacity(2 * 2 << 20);
    file.read_to_end(&mut raw)?;

    let data = process_fn(raw)?;

    let tx = conn.transaction()?;
    {
        let exists_price = tx.prepare(queries.exists_price)?;
        let exists_meta = tx.prepare(queries.exists_meta)?;
        let insert_meta = tx.prepare(queries.insert_meta)?;
        let insert_price = tx.prepare(queries.insert_price)?;

        let ins = Inserter {
            table,
            exists_price,
            exists_meta,
            insert_meta,
            insert_price,
        };
        insert_fn(ins, data, modified)?;
    }

    tx.commit()?;
    Ok(rows_inserted(conn, table)?)
}

fn fanatical_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();
    for line in raw.lines() {
        let row: algolia::AlgoliaMultiResponse<fanatical::AlgoliaHit> =
            serde_json::from_str(&line?)?;
        for subrow in row.results {
            for hit in subrow.hits {
                let cname = normalize(&hit.name);
                if let Some(info) = data.get_mut(&cname) {
                    let price = if let Some(price) = hit.price.usd {
                        price
                    } else {
                        continue;
                    };
                    if price < info.discount_price {
                        info.discount_price = price;
                    }
                } else {
                    data.insert(
                        cname,
                        PriceFields {
                            original_name: hit.name,
                            original_price: hit.full_price.usd.expect("missing original_price"),
                            discount_price: hit.price.usd.expect("missing discount_price"),
                            discount_percent: hit.discount_percent,
                            best_ever: hit.best_ever,
                            flash_sale: hit.flash_sale,
                            os: hit.operating_systems.join(","),
                            release_date: hit.release_date,
                            valid_from: hit.available_valid_from,
                            valid_until: hit.available_valid_until,
                            ..Default::default()
                        },
                    );
                }
            }
        }
    }
    Ok(data)
}

fn fanatical_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 12] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":name", &cname),
            (":price", &fields.original_price),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
            (":best_ever", &fields.best_ever),
            (":flash_sale", &fields.flash_sale),
            (":os", &fields.os),
            (":release_date", &fields.release_date),
            (":available_from", &fields.valid_from),
            (":available_until", &fields.valid_until),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }
    Ok(())
}

fn gamebillet_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();

    for line in raw.lines() {
        let rows: Vec<GamebilletPriceInfo> = serde_json::from_str(&line?)?;

        for row in rows.iter() {
            let cname = normalize(&row.name);
            if let Some(info) = data.get_mut(&cname) {
                if row.price < info.discount_price {
                    info.discount_price = row.price;
                }
            } else {
                data.insert(
                    cname,
                    PriceFields {
                        original_name: row.name.clone(),
                        discount_price: row.price,
                        discount_percent: row.percent_discount,
                        ..Default::default()
                    },
                );
            }
        }
    }
    Ok(data)
}

fn gamebillet_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 5] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":name", &cname),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }

    Ok(())
}

fn gamesplanet_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();
    for line in raw.lines() {
        let items: Vec<GamesplanetPriceInfo> = serde_json::from_str(&line?)?;
        for row in items {
            let cname = normalize(&row.name);
            if let Some(info) = data.get_mut(&cname) {
                if row.price < info.discount_price {
                    info.discount_price = row.price;
                }
            } else {
                data.insert(
                    cname,
                    PriceFields {
                        original_name: row.name,
                        original_price: row.original_price,
                        discount_price: row.price,
                        discount_percent: row.discount,
                        ..Default::default()
                    },
                );
            }
        }
    }
    Ok(data)
}

fn gamesplanet_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 6] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":name", &cname),
            (":price", &fields.original_price),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }
    Ok(())
}

fn gmg_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();
    for line in raw.lines() {
        let row: algolia::AlgoliaMultiResponse<gmg::AlgoliaHit> = serde_json::from_str(&line?)?;
        for subrow in row.results {
            for hit in subrow.hits {
                let cname = normalize(&hit.display_name);
                if let Some(info) = data.get_mut(&cname) {
                    if hit.regions.us.price < info.discount_price {
                        info.discount_price = hit.regions.us.price;
                    }
                } else {
                    data.insert(
                        cname,
                        PriceFields {
                            steam_app_id: hit.steam_app_id,
                            original_name: hit.display_name,
                            original_price: hit.regions.us.original_price,
                            discount_price: hit.regions.us.price,
                            discount_percent: hit.regions.us.discount_percent,
                            is_dlc: hit.is_dlc,
                            franchise: hit.franchise,
                            publisher: hit.publisher_name,
                            ..Default::default()
                        },
                    );
                }
            }
        }
    }
    Ok(data)
}

fn gmg_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 10] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":appid", &fields.steam_app_id),
            (":name", &cname),
            (":price", &fields.original_price),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
            (":is_dlc", &fields.is_dlc),
            (":franchise", &fields.franchise),
            (":publisher", &fields.publisher),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }
    Ok(())
}

fn gog_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();
    for line in raw.lines() {
        let row: gog::GogResponse = serde_json::from_str(&line?)?;
        for r in row.products {
            let cname = normalize(&r.title);
            let original_price = r
                .price
                .base
                .trim_start_matches("$")
                .replace(",", "")
                .parse::<f64>()?;

            let discount_price = r
                .price
                .r#final
                .trim_start_matches("$")
                .replace(",", "")
                .parse::<f64>()?;

            let discount_percent = r
                .price
                .discount
                .trim_start_matches("-")
                .trim_end_matches("%")
                .parse::<u64>()?;

            let release_date = if let Some(date) = r.release_date {
                let d = NaiveDate::parse_from_str(&date, "%Y.%m.%d").expect("invalid %Y.%m.%d");
                <NaiveDate as Into<NaiveDateTime>>::into(d)
                    .and_utc()
                    .timestamp() as u64
            } else {
                0
            };

            if let Some(info) = data.get_mut(&cname) {
                if discount_price < info.discount_price {
                    info.discount_price = discount_price;
                }
            } else {
                data.insert(
                    cname,
                    PriceFields {
                        original_name: r.title,
                        original_price,
                        discount_price,
                        discount_percent,
                        release_date,
                        developer: r.developers.join(","),
                        publisher: r.publishers.join(","),
                        product_type: r.product_type.to_string(),
                        ..Default::default()
                    },
                );
            }
        }
    }
    Ok(data)
}

fn gog_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 10] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":name", &cname),
            (":price", &fields.original_price),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
            (":release_date", &fields.release_date),
            (":developer", &fields.developer),
            (":publisher", &fields.publisher),
            (":product_type", &fields.product_type),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }
    Ok(())
}

fn indiegala_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();
    let rows: Vec<indiegala::PriceInfo> = serde_json::from_slice(&raw)?;

    for row in rows {
        if row.drm_info != DrmInfo::SteamKey {
            continue;
        }
        let cname = normalize(&row.title);
        let release_date = DateTime::parse_from_rfc3339(&row.release_date)
            .unwrap_or_default()
            .timestamp() as u64;
        let valid_from = DateTime::parse_from_rfc3339(&row.discount_start)
            .unwrap_or_default()
            .timestamp() as u64;
        let valid_until = DateTime::parse_from_rfc3339(&row.discount_end)
            .unwrap_or_default()
            .timestamp() as u64;

        if let Some(info) = data.get_mut(&cname) {
            if row.discount_price < info.discount_price {
                info.discount_price = row.discount_price;
            }
        } else {
            data.insert(
                cname,
                PriceFields {
                    original_name: row.title,
                    original_price: row.price,
                    discount_price: row.discount_price,
                    valid_from,
                    valid_until,
                    os: row.platforms.join(","),
                    release_date,
                    publisher: row.publisher,
                    ..Default::default()
                },
            );
        }
    }
    Ok(data)
}

fn indiegala_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 10] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":name", &cname),
            (":price", &fields.original_price),
            (":discount_price", &fields.discount_price),
            (":valid_from", &fields.valid_from),
            (":valid_until", &fields.valid_until),
            (":os", &fields.os),
            (":release_date", &fields.release_date),
            (":publisher", &fields.publisher),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }

    Ok(())
}

fn steam_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();

    let mut bundleids = BTreeSet::new();
    let mut packageids = BTreeSet::new();

    for line in raw.lines() {
        let row: CStoreQueryQueryResponse = serde_json::from_str(&line?)?;
        'items: for item in row.store_items {
            // insert price info for purchase options (with different editions, bundles)
            for opt in item.purchase_options {
                let (cname, name) = if let Some(n) = opt.purchase_option_name {
                    (normalize(&n), n)
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

                let original_price =
                    opt.original_price_in_cents
                        .expect("missing original_price_in_cents") as f64
                        / 100f64;

                let price = opt
                    .final_price_in_cents
                    .expect("missing final_price_in_cents") as f64
                    / 100f64;

                let available_until = opt.active_discounts[0]
                    .discount_end_date
                    .expect("active_discount present but no discount_end_date");

                data.insert(
                    cname,
                    PriceFields {
                        packageid,
                        bundleid,
                        original_name: name,
                        original_price,
                        discount_price: price,
                        discount_percent: opt.discount_pct.unwrap_or_default() as u64,
                        valid_until: available_until as u64,
                        ..Default::default()
                    },
                );
            }
        }
    }

    Ok(data)
}

fn steam_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 9] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":packageid", &fields.packageid),
            (":bundleid", &fields.bundleid),
            (":name", &cname),
            (":price", &fields.original_price),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
            (":available_until", &fields.valid_until),
        ];
        ins.insert_checked(
            &cname,
            &[&cname, &fields.packageid, &fields.bundleid],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }

    Ok(())
}

fn wgs_process(raw: Vec<u8>) -> Result<PriceData, Error> {
    let mut data: PriceData = HashMap::new();
    let rows: Vec<wgs::PriceInfo> = serde_json::from_slice(&raw)?;

    for row in rows {
        if !row.is_steam_drm {
            continue;
        }
        let cname = normalize(&row.name);
        if let Some(info) = data.get_mut(&cname) {
            if row.discount_price < info.discount_price {
                info.discount_price = row.discount_price;
            }
        } else {
            data.insert(
                cname,
                PriceFields {
                    original_name: row.name,
                    discount_price: row.discount_price,
                    discount_percent: row.discount_percent,
                    is_dlc: row.is_dlc,
                    publisher: row.publisher,
                    ..Default::default()
                },
            );
        }
    }
    Ok(data)
}

fn wgs_insert(mut ins: Inserter, data: PriceData, modified: u32) -> Result<(), Error> {
    for (cname, fields) in data.iter() {
        let params: [(&'static str, &dyn ToSql); 7] = [
            (":meta_id", &0),
            (":ts", &modified),
            (":name", &cname),
            (":discount_price", &fields.discount_price),
            (":discount_percent", &fields.discount_percent),
            (":is_dlc", &(fields.is_dlc as i32)),
            (":publisher", &fields.publisher),
        ];
        ins.insert_checked(
            &cname,
            &[&cname],
            &[&cname],
            &[&fields.original_name, &cname],
            &params,
        )?;
    }

    Ok(())
}

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

#[derive(Deserialize)]
struct GamebilletPriceInfo {
    name: String,
    price: f64,
    percent_discount: u64,
}

#[derive(Deserialize)]
struct GamesplanetPriceInfo {
    name: String,
    original_price: f64,
    discount: u64,
    price: f64,
}

type PriceData = HashMap<String, PriceFields>;

#[derive(Default)]
struct PriceFields {
    steam_app_id: String,
    original_name: String,
    original_price: f64,
    discount_price: f64,
    discount_percent: u64,
    best_ever: bool,
    flash_sale: bool,
    os: String,
    release_date: u64,
    valid_from: u64,
    valid_until: u64,
    is_dlc: bool,
    developer: String,
    franchise: String,
    publisher: String,
    product_type: String,
    packageid: Option<i32>,
    bundleid: Option<i32>,
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

#[inline]
fn check_collision(num_matches: &[i64]) -> NameCollision {
    match num_matches.len() {
        0 => NameCollision::DoesNotExist,
        1 => NameCollision::Exists,
        _ => NameCollision::Collision,
    }
}

#[derive(Default, Clone, Copy)]
enum Store {
    #[default]
    Fanatical,
    Gamebillet,
    Gamesplanet,
    Gmg,
    Gog,
    Indiegala,
    Steam,
    Wgs,
}

impl From<Store> for &'static str {
    fn from(value: Store) -> &'static str {
        match value {
            Store::Fanatical => "fanatical",
            Store::Gamebillet => "gamebillet",
            Store::Gamesplanet => "gamesplanet",
            Store::Gmg => "gmg",
            Store::Gog => "gog",
            Store::Indiegala => "indiegala",
            Store::Steam => "steam",
            Store::Wgs => "wgs",
        }
    }
}

impl Display for Store {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fanatical => f.write_str("fanatical"),
            Self::Gamebillet => f.write_str("gamebillet"),
            Self::Gamesplanet => f.write_str("gamesplanet"),
            Self::Gmg => f.write_str("gmg"),
            Self::Gog => f.write_str("gog"),
            Self::Indiegala => f.write_str("indiegala"),
            Self::Steam => f.write_str("steam"),
            Self::Wgs => f.write_str("wgs"),
        }
    }
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
                insert_price: include_str!("../sql/insert_fanatical.sql"),
            },
            Self::Gamebillet => Queries {
                exists_price: "SELECT meta_id FROM gamebillet WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_gamebillet.sql"),
            },
            Self::Gamesplanet => Queries {
                exists_price: "SELECT meta_id FROM gamesplanet WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_gamesplanet.sql"),
            },
            Self::Gmg => Queries {
                exists_price: "SELECT meta_id FROM gmg WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_gmg.sql"),
            },
            Self::Gog => Queries {
                exists_price: "SELECT meta_id FROM gog WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_gog.sql"),
            },
            Self::Indiegala => Queries {
                exists_price: "SELECT meta_id FROM indiegala WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_indiegala.sql"),
            },
            Self::Steam => Queries {
                exists_price: "SELECT meta_id FROM steam WHERE name = ?1 AND packageid = ?2 AND bundleid = ?3 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_steam.sql"),
            },
            Self::Wgs => Queries {
                exists_price: "SELECT meta_id FROM wgs WHERE name = ?1 ORDER by ts DESC LIMIT 1",
                exists_meta: Self::EXISTS_META,
                insert_meta: Self::INSERT_META,
                insert_price: include_str!("../sql/insert_wgs.sql"),
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PurchaseOption {
    purchase_option_name: Option<String>,
    packageid: Option<i32>,
    bundleid: Option<i32>,
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

#[derive(Deserialize)]
pub struct MetadataResponse {
    pub store_items: Vec<StoreItem>,
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
