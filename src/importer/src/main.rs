use std::{
    fs::{self},
    path::PathBuf,
};

use anyhow::Error;
use clap::{Parser, Subcommand};
use env_logger::{Builder, Env};
use log::info;
use rusqlite::{Connection, Result, config::DbConfig};

use crate::{
    bundles::import_bundles,
    insert::{
        import_prices, init_steam_map, update_steam_map, update_steam_metadata, upsert_names,
    },
    search::{insert_wishlist, read_wishlist, update_search_table},
    utils::{execute_sql, move_file, rows_inserted},
};

mod bundles;
mod insert;
mod search;
mod utils;

const STORE_NAMES: [&'static str; 8] = [
    "fanatical",
    "gamebillet",
    "gamesplanet",
    "gmg",
    "gog",
    "indiegala",
    "steam",
    "wingamestore",
];

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// create metadata and price tables and indexes
    CreateTables,
    /// init map of name to steam appids from appinfo.jsonl
    InitSteamMap,
    /// insert pairs of unique names and normalized names for all stores
    UpdateNames,

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

    /// get prices for items in steam wishlist (need output/steam/wishlist.json)
    Wishlist,

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

    for store_name in STORE_NAMES.iter() {
        fs::create_dir_all(PathBuf::from("output/backup").join(store_name))?;
    }

    match &args.command {
        Commands::CreateTables => create_tables(),
        Commands::InitSteamMap => init_steam_map(),
        Commands::UpdateSteamMap => update_steam_map(),
        Commands::UpdateNames => upsert_names(&mut conn),
        Commands::UpdateMetadata => update_steam_metadata(&mut conn),
        Commands::ImportPrices => import_prices(&mut conn),
        Commands::ImportBundles => import_bundles(&mut conn),
        Commands::UpdateSearchTable => update_search_table(&mut conn),
        Commands::DropTables => drop_tables(),
        Commands::Wishlist => {
            insert_wishlist(&mut conn)?;
            let wishlist = read_wishlist(&mut conn)?;

            for item in wishlist {
                println!("{:?}", item);
            }
            Ok(())
        }
        Commands::Reset => {
            move_from_backup()?;
            drop_tables()?;
            create_tables()?;
            init_steam_map()?;
            update_steam_map()?;
            upsert_names(&mut conn)?;
            update_steam_metadata(&mut conn)?;
            import_prices(&mut conn)?;
            import_bundles(&mut conn)?;
            update_search_table(&mut conn)?;
            Ok(())
        }
    }?;

    Ok(())
}

fn move_from_backup() -> Result<(), Error> {
    for store in STORE_NAMES {
        let dir = PathBuf::from("output/backup").join(store);

        let mut entries = fs::read_dir(dir)?;
        while let Some(Ok(entry)) = entries.next() {
            if entry.file_type()?.is_file() {
                move_file(&entry.path(), store)?;
            }
        }
    }
    Ok(())
}

#[inline]
fn create_tables() -> Result<(), Error> {
    execute_sql("src/importer/sql/create_tables.sql")?;
    info!("created tables");
    Ok(())
}

#[inline]
fn drop_tables() -> Result<(), Error> {
    execute_sql("src/importer/sql/drop_tables.sql")?;
    info!("dropped tables");
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::utils::normalize;
    use bincode::config;
    use rusqlite::{Connection, params};

    #[test]
    fn test_normalize() {
        let mut title = "Stellaris: Species Pack Bundle".into();
        let mut normalized = normalize(title);
        assert_eq!(normalized, "stellaris species pack bundle");

        title = "Palworld - Game + Soundtrack Bundle".into();
        normalized = normalize(title);
        assert_eq!(normalized, "palworld game soundtrack bundle");
    }

    #[test]
    fn serialize_blob() {
        let conn = Connection::open_in_memory().expect("failed to open db");
        conn.execute("CREATE TABLE mytable(id INTEGER PRIMARY KEY, bin BLOB)", [])
            .expect("failed to create table");

        let data: Vec<u32> = vec![1, 20, 16, 8, 17, 9, 11, 4, 3, 27, 13, 10, 2, 5, 7, 6];
        let serialized =
            bincode::encode_to_vec(&data, config::standard()).expect("failed to encode data");
        conn.execute("INSERT INTO mytable(bin) VALUES(?1)", params![serialized])
            .expect("failed to insert data");

        let data_row = conn
            .query_row("SELECT bin FROM mytable LIMIT 1", [], |r| {
                r.get::<usize, Vec<u8>>(0)
            })
            .expect("failed to select row");
        let (decoded, _size): (Vec<u32>, usize) =
            bincode::decode_from_slice(&data_row, config::standard())
                .expect("failed to decode data from db");
        assert_eq!(data, decoded);
    }
}
