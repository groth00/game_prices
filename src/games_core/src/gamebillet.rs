use std::{
    collections::HashSet,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use chrono::{Days, Local, NaiveDate};
use log::{error, info};
use quick_xml::{events::Event, reader::Reader};
use tokio::fs;

use crate::utils::{latest_file, move_file};

const OUTPUT_DIR: &str = "output/gamebillet";

#[derive(Default)]
pub struct Gamebillet {}

impl Gamebillet {
    pub async fn parse_sitemap() -> Result<(), Error> {
        let ignorelist = fs::read_to_string(PathBuf::from(OUTPUT_DIR).join("ignorelist.txt"))
            .await
            .expect("failed to open ignorelist");
        let ignore_set = ignorelist.split_whitespace().collect::<HashSet<_>>();

        let input_path =
            latest_file("output/gamebillet", "sitemap")?.expect("no sitemap.xml found");
        let mut reader = Reader::from_file(&input_path).expect("failed to open sitemap");

        let mut buf = Vec::new();
        let mut state = XmlState::default();

        let mut output = Vec::with_capacity(10000);
        let mut data = XmlRecord::default();

        // if url tag is empty, skip the following changefreq and lastmod tags
        // reset upon exiting a lastmod tag
        let mut should_skip = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(bytes)) => match bytes.as_ref() {
                    b"loc" => state = XmlState::Loc,
                    b"changefreq" => state = XmlState::Changefreq,
                    b"lastmod" => state = XmlState::Lastmod,
                    _ => (),
                },
                Ok(Event::Empty(_)) => should_skip = true,
                Ok(Event::End(_)) => state = XmlState::None,
                Ok(Event::Text(bytes)) => match state {
                    XmlState::Loc => data.loc = bytes.unescape()?.trim().to_string(),
                    XmlState::Changefreq => {
                        if should_skip {
                            continue;
                        }
                        data.changefreq = bytes.unescape()?.trim().to_string()
                    }
                    XmlState::Lastmod => {
                        if should_skip {
                            should_skip = false;
                            continue;
                        }
                        data.lastmod = bytes.unescape()?.trim().to_string();
                        output.push(data.clone());
                    }
                    XmlState::None => (),
                },
                Ok(Event::Eof) => break,
                Ok(_) => (),
                Err(e) => error!("error: {}", e),
            }
            buf.clear();
        }

        info!("{} records", output.len());

        let filtered = output
            .iter()
            .filter(|&r| !ignore_set.contains(&r.loc.as_str()))
            .collect::<Vec<_>>();
        info!("{} records after filtering", filtered.len());

        let now = Local::now();
        let last_week = now
            .checked_sub_days(Days::new(7))
            .expect("failed to subtract 7 days")
            .date_naive();

        let recent = filtered
            .iter()
            .filter(|&r| {
                let lastmod = NaiveDate::parse_from_str(&r.lastmod, "%F")
                    .expect("date not in %Y-%m-%d format");
                lastmod >= last_week
            })
            .map(|&r| &r.loc)
            .collect::<Vec<_>>();

        info!("{} modified records (last 7 days)", recent.len());

        let serialized = serde_json::to_string_pretty(&recent)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let output_path = format!("{}/changed_{}.json", OUTPUT_DIR, now);
        fs::write(output_path, serialized).await?;

        move_file(&input_path, "gamebillet")?;

        Ok(())
    }
}

#[derive(Debug, Default)]
enum XmlState {
    #[default]
    None,
    Loc,
    Changefreq,
    Lastmod,
}

#[derive(Debug, Default, Clone)]
struct XmlRecord {
    loc: String,
    changefreq: String,
    lastmod: String,
}
