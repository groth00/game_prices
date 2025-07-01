use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use headless_chrome::Browser;
use log::{error, info};
use quick_xml::{events::Event, reader::Reader};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use tokio::{fs, time::sleep};

const OUTPUT_DIR: &str = "output/indiegala";

pub struct Indiegala {
    links: Links,
}

impl Default for Indiegala {
    fn default() -> Self {
        Self {
            links: Links {
                // query parameters: page (int), sale (bool)
                rss: "https://indiegala.com/store_games_rss",
                rss_on_sale: "https://indiegala.com/store_games_rss?sale=true",
            },
        }
    }
}

impl Indiegala {
    pub async fn on_sale(&self) -> Result<(), Error> {
        self.fetch(self.links.rss_on_sale).await
    }

    pub async fn all_products(&self) -> Result<(), Error> {
        self.fetch(self.links.rss).await
    }

    async fn fetch(&self, url: &str) -> Result<(), Error> {
        let browser = Browser::default()?;

        let tab = browser.new_tab()?;
        tab.enable_stealth_mode()?;

        let mut output: Vec<PriceInfo> = Vec::with_capacity(2000);
        let mut page = 1;

        let rss_selector = Selector::parse("rss").unwrap();

        loop {
            info!("fetching page {}", page);
            let next_url = format!("{}&page={}", url, page);
            tab.navigate_to(&next_url)?;

            let source = tab
                .evaluate("document.documentElement.outerHTML", false)?
                .value
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .expect("page should not be empty");

            let html = Html::parse_document(&source);

            let tmp_path = PathBuf::from(OUTPUT_DIR).join(format!("{}.html", page));
            fs::write(tmp_path, html.html()).await?;

            let rss = html.select(&rss_selector).next().unwrap();
            let xml = rss.inner_html();

            let pagination = parse_xml(&xml, &mut output)?;
            if pagination.current_page == pagination.total_pages {
                break;
            }

            page += 1;
            sleep(Duration::from_millis(1000)).await;
        }

        let serialized = serde_json::to_string(&output)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join("on_sale.json");
        fs::write(output_path, serialized).await?;

        Ok(())
    }
}

pub enum ProductKind {
    All,
    OnSale,
}

impl From<ProductKind> for &str {
    fn from(value: ProductKind) -> Self {
        match value {
            ProductKind::All => "all",
            ProductKind::OnSale => "sale",
        }
    }
}

/// when using the python script to fetch the xml, run this afterward
pub async fn parse_files() -> Result<(), Error> {
    let mut output: Vec<PriceInfo> = Vec::with_capacity(2000);

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    for dir in ["all", "on_sale"] {
        let subdir = PathBuf::from(OUTPUT_DIR).join(dir);

        let mut entries = fs::read_dir(subdir).await?;
        let mut files = vec![];

        while let Ok(Some(entry)) = entries.next_entry().await {
            let filename = entry.file_name();
            let path = Path::new(&filename);

            if let Some(extension) = path.extension() {
                if extension == "xml" {
                    files.push(entry.path());
                } else {
                    info!("skip {:?}", entry.file_name());
                }
            }
        }

        if files.is_empty() {
            continue;
        } else {
            for path in &files {
                info!("processing {:?}", path.file_name());
                let xml = fs::read_to_string(path).await?;
                parse_xml(&xml, &mut output)?;
            }
        }

        let serialized = serde_json::to_string_pretty(&output)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join(format!("{}_{}.json", dir, now));
        fs::write(output_path, &serialized).await?;

        for path in files {
            fs::remove_file(path).await?;
        }
    }

    Ok(())
}

fn parse_xml(xml: &str, out: &mut Vec<PriceInfo>) -> Result<Pagination, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut buf = vec![];

    let mut state = State::default();
    let mut info = PriceInfo::default();

    let mut count = 0;

    let mut pagination = Pagination::default();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(bs)) => match bs.as_ref() {
                b"currentPage" => state = State::CurrentPage,
                b"totalPages" => state = State::TotalPages,
                b"totalGames" => state = State::TotalGames,
                b"title" => state = State::Title,
                b"product" => state = State::Product,
                b"sku" => state = State::Sku,
                b"platform" => state = State::Platform,
                b"link" => state = State::Link,
                b"publisher" => state = State::Publisher,
                b"priceUSD" => state = State::Price,
                b"discountPercentUSD" => state = State::DiscountPercentage,
                b"discountStart" => state = State::DiscountStart,
                b"discountEnd" => state = State::DiscountEnd,
                b"discountPriceUSD" => state = State::DiscountPrice,
                b"date" => state = State::ReleaseDate,
                b"state" => state = State::Available,
                b"notAvailableRegions" => state = State::UnavailableRegions,
                b"isDLC" => state = State::IsDlc,
                b"drminfo" => state = State::DrmInfo,
                _ => (),
            },
            Ok(Event::End(_)) => state = State::None,
            Ok(Event::Text(bt)) => {
                let txt = bt.unescape()?.into_owned();

                match state {
                    State::CurrentPage => pagination.current_page = txt.parse::<u32>()?,
                    State::TotalPages => pagination.total_pages = txt.parse::<u32>()?,
                    State::TotalGames => pagination.total_games = txt.parse::<u32>()?,
                    State::Title => info.title = txt,
                    State::Product => info.product = txt,
                    State::Sku => info.sku = txt,
                    State::Platform => info.platforms.push(txt),
                    State::Link => info.link = txt,
                    State::Publisher => info.publisher = txt,
                    State::Price => info.price = txt.parse::<f64>()?,
                    State::DiscountPercentage => info.discount_percentage = txt.parse::<f64>()?,
                    State::DiscountStart => info.discount_start = txt,
                    State::DiscountEnd => info.discount_end = txt,
                    State::DiscountPrice => info.discount_price = txt.parse::<f64>()?,
                    State::ReleaseDate => info.release_date = txt,
                    State::Available => info.available = txt,
                    State::UnavailableRegions => {
                        info.unavailable_regions = txt
                            .split(",")
                            .map(|s| s.to_string())
                            .collect::<Vec<String>>()
                    }
                    State::IsDlc => info.is_dlc = txt.to_lowercase(),
                    State::DrmInfo => {
                        info.drm_info = DrmInfo::from(txt.as_str());
                        out.push(info);
                        info = PriceInfo::default();
                        count += 1;
                    }
                    _ => (),
                }
            }
            Err(e) => error!("error: {}", e),
            _ => (),
        }
        buf.clear();
    }

    info!("processed {} items", count);

    Ok(pagination)
}

#[derive(Debug, Default)]
enum State {
    #[default]
    None,
    CurrentPage,
    TotalPages,
    TotalGames,
    Title,
    Product,
    Sku,
    Platform,
    Link,
    Publisher,
    Price,
    DiscountPercentage,
    DiscountStart,
    DiscountEnd,
    DiscountPrice,
    ReleaseDate,
    Available,
    UnavailableRegions,
    IsDlc,
    DrmInfo,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PriceInfo {
    pub title: String,
    pub product: String,
    pub sku: String,
    pub platforms: Vec<String>,
    pub link: String,
    pub publisher: String,
    pub price: f64,
    pub discount_percentage: f64,
    pub discount_start: String,
    pub discount_end: String,
    pub discount_price: f64,
    pub release_date: String,
    pub available: String,
    pub unavailable_regions: Vec<String>,
    pub is_dlc: String, // NOTE: this is not a binary value, skip insertion
    pub drm_info: DrmInfo,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum DrmInfo {
    #[default]
    Unknown,
    SteamKey,
    DRMFree,
}

impl From<&str> for DrmInfo {
    fn from(value: &str) -> Self {
        match value {
            "DRM free" => Self::DRMFree,
            "Steam key" => Self::SteamKey,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Pagination {
    current_page: u32,
    total_pages: u32,
    total_games: u32,
}

struct Links {
    rss: &'static str,
    rss_on_sale: &'static str,
}
