use std::{
    fs::{self, DirEntry},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::UNIX_EPOCH,
};

use anyhow::Error;
use log::{error, info};
use rusqlite::{Connection, Result};

use once_cell::sync::Lazy;
use regex::Regex;

pub fn execute_sql<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    let file = fs::read_to_string(path)?;
    let mut child = Command::new("sqlite3")
        .arg("games.db")
        .stdin(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(file.as_bytes())?;
    }
    let output = child.wait_with_output()?;
    if !output.stderr.is_empty() {
        error!("{:?}", String::from_utf8(output.stderr)?);
    }
    Ok(())
}

pub fn all_files(input_dir: &'static str, prefix: &'static str) -> Result<Vec<DirEntry>, Error> {
    let mut dir = fs::read_dir(input_dir)?;

    let mut entries: Vec<DirEntry> = Vec::new();

    while let Some(Ok(entry)) = dir.next() {
        if !entry.file_type()?.is_file() {
            continue;
        }

        if entry.file_name().to_string_lossy().starts_with(prefix) {
            entries.push(entry);
        }
    }
    entries.sort_by(|e1, e2| e1.file_name().cmp(&e2.file_name()));
    Ok(entries)
}

pub fn normalize(name: &str) -> String {
    static NON_ALPHABETIC: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^0-9A-Za-z\s\p{L}]").unwrap());
    static WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

    let tmp1 = NON_ALPHABETIC.replace_all(&name, "");
    let tmp1 = WHITESPACE.replace_all(&tmp1, " ");

    tmp1.trim().to_ascii_lowercase()
}

pub fn rows_inserted(conn: &Connection, table: &'static str) -> Result<i64, Error> {
    let query = format!("SELECT COUNT(1) FROM {}", table);
    let rows = conn.query_row(&query, [], |r| r.get::<usize, i64>(0))?;
    Ok(rows)
}

pub fn move_file(source: &PathBuf, store: &'static str) -> Result<(), Error> {
    let mut dest = source.clone();

    dest.pop();
    dest.pop();
    dest.push("backup");
    dest.push(store);
    dest.push(source.file_name().expect("invalid filename"));

    info!("mv {:?} -> {:?}", source.file_name(), &dest);
    fs::rename(source, dest)?;
    Ok(())
}

pub fn latest_file(
    input_dir: &'static str,
    prefix: &'static str,
) -> Result<Option<PathBuf>, Error> {
    let mut dir = fs::read_dir(input_dir)?;

    let mut latest = 0;
    let mut path: Option<PathBuf> = None;

    while let Some(Ok(entry)) = dir.next() {
        if !entry.file_type()?.is_file() {
            continue;
        }

        if entry.file_name().to_string_lossy().starts_with(prefix) {
            let touched = entry
                .metadata()?
                .modified()?
                .duration_since(UNIX_EPOCH)?
                .as_secs();
            if touched > latest {
                latest = touched;
                path = Some(entry.path());
            }
        }
    }

    info!("latest file in {}: {:?}", &input_dir, &path);
    Ok(path)
}

#[cfg(test)]
mod test {
    use std::env::set_current_dir;
    use std::fs::OpenOptions;

    use super::*;

    #[test]
    fn mv() {
        // NOTE: test dir is src/importer; want output/
        set_current_dir("../../").expect("chdir");
        let path = PathBuf::from("output/steam/foo");
        {
            let _file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .expect("open");
        }

        move_file(&path, "steam").expect("move");
        let mut entries = fs::read_dir("output/backup/steam").expect("read_dir");

        assert!(
            entries.any(|f| f.is_ok_and(|f| f.path().as_os_str() == "output/backup/steam/foo"))
        );

        fs::remove_file("output/backup/steam/foo").expect("remove_file")
    }
}
