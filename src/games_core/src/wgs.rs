use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use futures::{
    join,
    stream::{FuturesUnordered, StreamExt},
};
use log::{error, info};
use serde::{Deserialize, Serialize};
use thirtyfour::{
    By, DesiredCapabilities, IntoArcStr, WebDriver, WebElement, prelude::ElementQueryable,
};
use tokio::{sync::Semaphore, time::sleep};

const OUTPUT_DIR: &str = "output/wingamestore";
const CHROMEDRIVER_SERVER_URL: &str = "http://localhost:9515";

pub struct Wgs {
    links: Links<&'static str>,
    selectors: Selectors<&'static str>,
}

impl Default for Wgs {
    fn default() -> Self {
        Self {
            links: Links {
                on_sale: "https://www.wingamestore.com/listing/Specials/",
                all: "https://www.wingamestore.com/listing/all",
                new: "https://www.wingamestore.com/listing/New-Releases/",
            },
            selectors: Selectors {
                max_items_btn: "maxitems-menu-btn",
                max_items_100: "a[title='100']",
                total_items: "list-results-total",
                // content: "list-results",
                items: "prodband",
                next_page_btn: "thumbimg-main-next",

                publisher: "em.small",
                name: "title",
                is_dlc: ".tags .isdlc",
                drm_steam: ".tags .steam",
                discount_percent: "percentoff",
                discount_price: ".price em",
            },
        }
    }
}

impl Wgs {
    pub async fn download(&self, download_kind: DownloadKind) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let output_path = match download_kind {
            DownloadKind::OnSale => PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now)),
            DownloadKind::All => PathBuf::from(OUTPUT_DIR).join(format!("all_{}.json", now)),
            DownloadKind::New => PathBuf::from(OUTPUT_DIR).join(format!("new_{}.json", now)),
        };

        let caps = DesiredCapabilities::chrome();
        let driver = Arc::new(WebDriver::new(CHROMEDRIVER_SERVER_URL, caps).await?);

        match download_kind {
            DownloadKind::OnSale => driver.goto(self.links.on_sale).await?,
            DownloadKind::All => driver.goto(self.links.all).await?,
            DownloadKind::New => driver.goto(self.links.new).await?,
        };
        sleep(Duration::from_millis(2000)).await;

        let max_items_btn = driver
            .query(By::Id(self.selectors.max_items_btn))
            .first()
            .await?;
        max_items_btn.click().await?; // expand dropdown
        sleep(Duration::from_millis(1500)).await;

        let dropdown_selector = driver
            .query(By::Css(self.selectors.max_items_100))
            .first()
            .await?;
        dropdown_selector.click().await?; // select 100 items per page
        sleep(Duration::from_millis(2000)).await;

        if let Ok(total_items) = driver
            .query(By::Id(self.selectors.total_items))
            .first()
            .await
        {
            info!("total_items: {:?}", total_items.inner_html().await);
        }

        let mut output: Vec<PriceInfo> = Vec::with_capacity(2000);
        let mut page = 0;
        let semaphore = Arc::new(Semaphore::new(10));

        loop {
            sleep(Duration::from_millis(3000)).await;
            page += 1;
            info!("scraping page {}", page);

            let items = driver
                .query(By::ClassName(self.selectors.items))
                .all_from_selector()
                .await?;

            let mut tasks = FuturesUnordered::new();

            let start = Instant::now();
            for elem in items {
                let semaphore = semaphore.clone();
                let selectors = self.selectors.clone();

                tasks.push(async move {
                    let permit = semaphore.acquire_owned().await.unwrap();
                    let res = extract(elem, &selectors).await;
                    drop(permit);

                    res
                });
            }

            while let Some(res) = tasks.next().await {
                match res {
                    Ok(info) => output.push(info),
                    Err(e) => error!("error: {}", e),
                }
            }
            info!("elapsed: {:?}", start.elapsed());

            if let Ok(button) = driver
                .find(By::ClassName(self.selectors.next_page_btn))
                .await
            {
                button.scroll_into_view().await?;
                button.click().await?;
            } else {
                break;
            }
        }

        <WebDriver as Clone>::clone(&driver).quit().await?;

        let serialized = serde_json::to_string(&output)?;
        fs::write(output_path, serialized)?;

        Ok(())
    }
}

pub enum DownloadKind {
    OnSale,
    All,
    New,
}

async fn extract(
    elem: WebElement,
    selectors: &Selectors<&'static str>,
) -> Result<PriceInfo, Error> {
    let name_fut = async { elem.find(By::ClassName(selectors.name)).await?.text().await };
    let publisher_fut = async { elem.find(By::Css(selectors.publisher)).await?.text().await };
    let tags_fut = async {
        let is_dlc = match elem.find(By::Css(selectors.is_dlc)).await {
            Ok(_) => true,
            Err(_) => false,
        };
        let is_steam_drm = match elem.find(By::Css(selectors.drm_steam)).await {
            Ok(_) => true,
            Err(_) => false,
        };
        Ok::<(bool, bool), ()>((is_dlc, is_steam_drm))
    };
    // <span class="percentoff">-30<i>%</i></span>
    let pct_fut = async {
        match elem.find(By::ClassName(selectors.discount_percent)).await {
            Ok(elem) => elem
                .text()
                .await
                .expect("failed to get discount_percent")
                .trim_start_matches("-")
                .trim_end_matches("%")
                .parse::<i64>()
                .expect("invalid discount_percent; not i64")
                .abs() as u64,
            Err(_e) => 0,
        }
    };
    // <em><i>$</i>49.99</em>
    let price_fut = async {
        elem.find(By::Css(selectors.discount_price))
            .await?
            .text()
            .await
    };

    let (name, publisher, tags, discount_percent, discount_price) =
        join!(name_fut, publisher_fut, tags_fut, pct_fut, price_fut);

    let name = name?;
    let publisher = publisher?;
    let mut iter = publisher.split(" – ");
    let (genre, publisher) = (
        iter.next().unwrap_or("Unknown"),
        iter.next().unwrap_or("Unknown"),
    );
    let (is_dlc, is_steam_drm) = tags.unwrap();
    let discount_price = discount_price?.trim_start_matches("$").parse::<f64>()?;

    Ok(PriceInfo {
        genre: genre.to_string(),
        publisher: publisher.to_string(),
        name,
        is_dlc,
        is_steam_drm,
        discount_percent,
        discount_price,
    })
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PriceInfo {
    pub genre: String,
    pub publisher: String,
    pub name: String,
    pub is_dlc: bool,
    pub is_steam_drm: bool,
    pub discount_percent: u64,
    pub discount_price: f64,
}

struct Links<T: IntoArcStr> {
    on_sale: T,
    all: T,
    new: T,
}

#[derive(Clone)]
struct Selectors<T: IntoArcStr> {
    max_items_btn: T,
    max_items_100: T,
    total_items: T,
    // content: T,
    items: T,
    next_page_btn: T,

    publisher: T,
    name: T,
    is_dlc: T,
    drm_steam: T,
    discount_percent: T,
    discount_price: T,
}
