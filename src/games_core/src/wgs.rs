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
            },
            selectors: Selectors {
                max_items_btn: "maxitems-menu-btn",
                max_items_100: "a[title='100']",
                total_items: "list-results-total",
                // content: "list-results",
                items: "prodband",
                next_page_btn: "thumbimg-main-next",

                title: "title",
                is_dlc: ".tags .isdlc",
                drm_steam: ".tags .steam",
                pct_discount: "percentoff",
                price: ".price em",
            },
        }
    }
}

impl Wgs {
    pub async fn on_sale(&self) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let caps = DesiredCapabilities::chrome();
        let driver = Arc::new(WebDriver::new(CHROMEDRIVER_SERVER_URL, caps).await?);

        driver.goto(self.links.on_sale).await?;
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

        let total_items = driver
            .query(By::Id(self.selectors.total_items))
            .first()
            .await?
            .inner_html()
            .await?;
        info!("total_items: {}", total_items);

        let mut output: Vec<PriceInfo> = Vec::with_capacity(2000);
        let mut page = 0;
        let semaphore = Arc::new(Semaphore::new(10));

        loop {
            sleep(Duration::from_millis(5000)).await;
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
            info!("processed page in {:?}", start.elapsed());

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
        let output_dir = PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now));
        fs::write(output_dir, serialized)?;

        Ok(())
    }
}

async fn extract(
    elem: WebElement,
    selectors: &Selectors<&'static str>,
) -> Result<PriceInfo, Error> {
    let title_fut = async {
        elem.find(By::ClassName(selectors.title))
            .await?
            .text()
            .await
    };
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
        elem.find(By::ClassName(selectors.pct_discount))
            .await?
            .text()
            .await
    };
    // <em><i>$</i>49.99</em>
    let price_fut = async { elem.find(By::Css(selectors.price)).await?.text().await };

    let (title, tags, percent_discount, price) = join!(title_fut, tags_fut, pct_fut, price_fut);

    let title = title?;
    let (is_dlc, is_steam_drm) = tags.unwrap();
    let percent_discount = percent_discount?
        .trim_start_matches("-")
        .trim_end_matches("%")
        .parse::<i64>()?;
    let price = price?.trim_start_matches("$").parse::<f64>()?;

    Ok(PriceInfo {
        title,
        is_dlc,
        is_steam_drm,
        percent_discount,
        price,
    })
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PriceInfo {
    pub title: String,
    pub is_dlc: bool,
    pub is_steam_drm: bool,
    pub percent_discount: i64,
    pub price: f64,
}

struct Links<T: IntoArcStr> {
    on_sale: T,
}

#[derive(Clone)]
struct Selectors<T: IntoArcStr> {
    max_items_btn: T,
    max_items_100: T,
    total_items: T,
    // content: T,
    items: T,
    next_page_btn: T,

    title: T,
    is_dlc: T,
    drm_steam: T,
    pct_discount: T,
    price: T,
}
