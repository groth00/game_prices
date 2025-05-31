use std::{
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
use thirtyfour::prelude::{ElementQueryable, ElementWaitable};
use thirtyfour::{By, DesiredCapabilities, IntoArcStr, WebDriver, WebElement};
use tokio::{fs, sync::Semaphore, time::sleep};

const CHROMEDRIVER_SERVER_URL: &str = "http://localhost:9515";
const OUTPUT_DIR: &str = "output/gog";

pub struct Gog {
    links: Links<&'static str>,
    selectors: Selectors<&'static str>,
}

impl Default for Gog {
    fn default() -> Self {
        Self {
            links: Links {
                on_sale: "https://www.gog.com/en/games?languages=en&discounted=true",
            },
            selectors: Selectors {
                cookies_btn: "CybotCookiebotDialogBodyButtonDecline",
                total_items: "h1[selenium-id='pageHeader']",
                total_pages: ".small-pagination__item[selenium-id='smallPaginationPage'] span",
                content: "[selenium-id='catalogContent']",
                items: ".paginated-products-grid .product-tile",
                button: "[selenium-id='smallPaginationNext']",

                name: "[selenium-id='productTileGameTitle']",
                is_dlc: "[selenium-id='productTitleLabel']",
                pct_discount: "[selenium-id='productPriceDiscount']",
                original_price: "base-value",
                price: "final-value",
            },
        }
    }
}

impl Gog {
    pub async fn on_sale(&self) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR).await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let caps = DesiredCapabilities::chrome();
        let driver = Arc::new(WebDriver::new(CHROMEDRIVER_SERVER_URL, caps).await?);

        driver.goto(self.links.on_sale).await?;
        sleep(Duration::from_millis(2000)).await;

        if let Ok(button) = driver
            .query(By::Id(self.selectors.cookies_btn))
            .first()
            .await
        {
            button.click().await?;
        } else {
            panic!("could not decline cookies");
        }

        let total_items = driver
            .query(By::Css(self.selectors.total_items))
            .first()
            .await?
            .text()
            .await?;
        info!("total_items: {}", total_items);

        let total_pages = driver
            .query(By::Css(self.selectors.total_pages))
            .first()
            .await?
            .text()
            .await?
            .parse::<u64>()?;

        let mut output: Vec<PriceInfo> = Vec::with_capacity(2000);

        let mut tasks = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(10));

        for i in 1..=total_pages {
            info!("scraping page {}", i);
            sleep(Duration::from_millis(3000)).await;
            driver
                .query(By::Css(self.selectors.content))
                .first()
                .await?
                .wait_until()
                .displayed()
                .await?;

            let items = driver
                .query(By::Css(self.selectors.items))
                .all_from_selector()
                .await?;

            let start = Instant::now();
            for item in items {
                let semaphore = semaphore.clone();
                item.wait_until().displayed().await?;
                item.scroll_into_view().await?;
                tasks.push(async move {
                    let permit = semaphore.acquire_owned().await.unwrap();
                    let result = self.extract(item).await;
                    drop(permit);

                    result
                });
            }

            while let Some(res) = tasks.next().await {
                match res {
                    Ok(info) => output.push(info),
                    Err(e) => error!("error: {}", e),
                }
            }
            let end = Instant::now();
            info!("processed page in {}", (end - start).as_secs_f64());

            if let Ok(button) = driver
                .query(By::Css(self.selectors.button))
                .and_enabled()
                .first()
                .await
            {
                button.scroll_into_view().await?;
                driver
                    .execute(
                        r#"document.querySelector('[selenium-id="paginationNext"]').click();"#,
                        Vec::new(),
                    )
                    .await?;
            } else {
                break;
            }
        }

        <WebDriver as Clone>::clone(&driver).quit().await?;

        let serialized = serde_json::to_string(&output)?;
        let output_dir = PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now));
        fs::write(output_dir, serialized).await?;

        Ok(())
    }

    async fn extract(&self, elem: WebElement) -> Result<PriceInfo, Error> {
        let name_fut = async {
            elem.find(By::Css(self.selectors.name))
                .await?
                .attr("title")
                .await
        };
        let dlc_fut = async {
            match elem.find(By::Css(self.selectors.is_dlc)).await {
                Ok(elem) => elem.text().await.expect("failed to get text of element") == "DLC",
                Err(_e) => false,
            }
        };
        let pct_fut = async {
            elem.find(By::Css(self.selectors.pct_discount))
                .await?
                .text()
                .await
        };
        let original_price_fut = async {
            elem.find(By::ClassName(self.selectors.original_price))
                .await?
                .text()
                .await
        };
        let price_fut = async {
            elem.find(By::ClassName(self.selectors.price))
                .await?
                .text()
                .await
        };

        let (name, is_dlc, percent_discount, original_price, price) =
            join!(name_fut, dlc_fut, pct_fut, original_price_fut, price_fut);

        let name = name?.unwrap();
        let percent_discount = percent_discount?;
        let original_price = original_price?;
        let price = price?;

        Ok(PriceInfo {
            name,
            is_dlc,
            percent_discount: percent_discount
                .trim_start_matches("-")
                .trim_end_matches("%")
                .parse::<u64>()?,
            original_price: original_price.trim_start_matches("$").parse::<f64>()?,
            price: price.trim_start_matches("$").parse::<f64>()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PriceInfo {
    pub name: String,
    pub is_dlc: bool,
    pub percent_discount: u64,
    pub original_price: f64,
    pub price: f64,
}

struct Links<T: IntoArcStr> {
    on_sale: T,
}

struct Selectors<T: IntoArcStr> {
    cookies_btn: T,
    total_items: T,
    total_pages: T,
    content: T,
    items: T,
    button: T,

    name: T,
    is_dlc: T,
    pct_discount: T,
    original_price: T,
    price: T,
}
