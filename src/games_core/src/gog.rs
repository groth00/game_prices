use std::{
    fmt::{self, Display, Formatter},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use log::info;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    time::sleep,
};

use crate::utils::retry;

const OUTPUT_DIR: &str = "output/gog";

pub struct Gog {
    client: Client,
}

impl Default for Gog {
    fn default() -> Self {
        let client = reqwest::ClientBuilder::new()
            .tcp_keepalive(Duration::from_secs(30))
            .https_only(true)
            .timeout(Duration::from_secs(60))
            .build()
            .expect("failed to build reqwest client");

        Self { client }
    }
}

impl Gog {
    pub async fn download(
        &self,
        product_type: ProductType,
        download_kind: DownloadKind,
    ) -> Result<(), Error> {
        let mut params = QueryParams::default();
        params
            .product_type(&product_type)
            .download_kind(&download_kind);

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;
        let output_path = match download_kind {
            DownloadKind::Discounted => {
                PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}_{}.json", &product_type, now))
            }
            DownloadKind::NotDiscounted => {
                PathBuf::from(OUTPUT_DIR).join(format!("{}_{}.json", &product_type, now))
            }
            DownloadKind::New => {
                PathBuf::from(OUTPUT_DIR).join(format!("new_{}_{}.json", &product_type, now))
            }
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)
            .await
            .expect("failed to open output file");
        let mut writer = BufWriter::new(file);

        loop {
            info!("on page {}", params.page);
            let url = params.build_url();
            sleep(Duration::from_millis(1000)).await;
            let req = self.client.request(Method::GET, &url).build()?;
            let resp = retry(&self.client, req).await?;
            let output = resp.bytes().await?;

            writer.write_all(&output).await?;
            writer.write_u8(b'\n').await?;

            let pagination: GogPagination = serde_json::from_slice(&output)?;
            if pagination.pages == params.page {
                break;
            }
            if params.page == 1 {
                info!(
                    "total: {}, available: {}",
                    pagination.product_count, pagination.currently_shown_product_count
                );
            }
            params.page += 1;
        }
        writer.flush().await?;

        Ok(())
    }
}

pub enum DownloadKind {
    Discounted,
    NotDiscounted,
    New,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryParams {
    limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_statuses: Option<&'static str>,
    languages: &'static str,
    order: &'static str,
    discounted: &'static str,
    product_type: &'static str,
    page: u64,
    country_code: &'static str,
    locale: &'static str,
    currency_code: &'static str,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            limit: 48,
            release_statuses: None,
            languages: "in:en",
            order: "desc:trending",
            discounted: "eq:false",
            product_type: "in:game,pack,dlc,extras",
            page: 1,
            country_code: "US",
            locale: "en-US",
            currency_code: "USD",
        }
    }
}

impl QueryParams {
    const BASE_URL: &'static str = "https://catalog.gog.com/v1/catalog";

    fn product_type(&mut self, product_type: &ProductType) -> &mut Self {
        match product_type {
            ProductType::Game => self.product_type = "in:game",
            ProductType::Pack => self.product_type = "in:pack",
            ProductType::Dlc => self.product_type = "in:dlc",
            ProductType::Extras => self.product_type = "in:extras",
            ProductType::GamePack => self.product_type = "in:game,pack",
            ProductType::DlcExtras => self.product_type = "in:dlc,extras",
            ProductType::All => self.product_type = "in:game,pack,dlc,extras",
        }
        self
    }

    fn download_kind(&mut self, kind: &DownloadKind) -> &mut Self {
        match kind {
            DownloadKind::Discounted => self.discounted = "eq:true",
            DownloadKind::NotDiscounted => self.discounted = "eq:false",
            DownloadKind::New => self.release_statuses = Some("in:new-arrival"),
        };
        self
    }

    fn build(&self) -> String {
        serde_urlencoded::to_string(self).expect("failed to serialize query params")
    }

    fn build_url(&self) -> String {
        let mut s = String::new();
        s.push_str(Self::BASE_URL);
        s.push_str("?");
        s.push_str(&self.build());
        s
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProductType {
    Game,
    Pack,
    Dlc,
    Extras,
    GamePack,
    DlcExtras,
    All,
}

impl Display for ProductType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Game => f.write_str("game"),
            Self::Pack => f.write_str("pack"),
            Self::Dlc => f.write_str("dlc"),
            Self::Extras => f.write_str("extras"),
            Self::GamePack => f.write_str("game_pack"),
            Self::DlcExtras => f.write_str("dlc_extras"),
            Self::All => f.write_str("all"),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GogPagination {
    pages: u64,
    currently_shown_product_count: u64,
    product_count: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GogResponse {
    pages: u64,
    currently_shown_product_count: u64,
    product_count: u64,
    pub products: Vec<GogProduct>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GogProduct {
    id: String,
    slug: String,
    #[serde(skip_deserializing)]
    features: Vec<ProductFeature>,
    #[serde(skip_deserializing)]
    screenshots: Vec<String>,
    #[serde(skip_deserializing)]
    user_preferred_language: UserPreferredLanguage,
    pub release_date: Option<String>,
    store_release_date: String,
    pub product_type: ProductType,
    pub title: String,
    #[serde(skip_deserializing)]
    cover_horizontal: String,
    #[serde(skip_deserializing)]
    cover_vertical: String,
    pub developers: Vec<String>,
    pub publishers: Vec<String>,
    operating_systems: Vec<String>,
    pub price: ProductPrice,
    product_state: String,
    genres: Vec<ProductFeature>,
    tags: Vec<ProductFeature>,
    reviews_rating: u64,
    #[serde(skip_deserializing)]
    editions: Vec<ProductEdition>,
    #[serde(skip_deserializing)]
    ratings: Vec<ProductAgeRating>,
    #[serde(skip_deserializing)]
    store_link: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProductFeature {
    name: String,
    slug: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename = "camelCase")]
struct UserPreferredLanguage {
    code: String,
    in_audio: bool,
    in_text: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProductPrice {
    pub r#final: String,
    pub base: String,
    pub discount: String,
    pub final_money: FinalMoney,
    pub base_money: BaseMoney,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FinalMoney {
    pub amount: String,
    pub currency: String,
    pub discount: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BaseMoney {
    pub amount: String,
    pub currency: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProductEdition {
    id: u64,
    name: String,
    is_root_edition: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProductAgeRating {
    name: String,
    age_rating: String,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn build_query_str() {
        let mut params = QueryParams::default();
        let s = params
            .product_type(&ProductType::Extras)
            .download_kind(&DownloadKind::NotDiscounted)
            .build();
        assert_eq!(
            s,
            "limit=48&languages=in%3Aen&order=desc%3Atrending&discounted=eq%3Afalse&productType=in%3Aextras&page=1&countryCode=US&locale=en-US&currencyCode=USD"
        );
    }

    #[test]
    fn build_url() {
        let mut params = QueryParams::default();
        let s = params
            .product_type(&ProductType::Extras)
            .download_kind(&DownloadKind::NotDiscounted)
            .build_url();
        assert_eq!(
            s,
            "https://catalog.gog.com/v1/catalog?limit=48&languages=in%3Aen&order=desc%3Atrending&discounted=eq%3Afalse&productType=in%3Aextras&page=1&countryCode=US&locale=en-US&currencyCode=USD"
        );
    }

    #[test]
    fn build_url_many() {
        let mut params = QueryParams::default();
        params
            .product_type(&ProductType::Extras)
            .download_kind(&DownloadKind::NotDiscounted);

        for i in 1..=10 {
            let url = params.build_url();

            let expected = format!(
                "https://catalog.gog.com/v1/catalog?limit=48&languages=in%3Aen&order=desc%3Atrending&discounted=eq%3Afalse&productType=in%3Aextras&page={}&countryCode=US&locale=en-US&currencyCode=USD",
                i
            );
            assert_eq!(expected, url);
            params.page += 1;
        }
    }

    #[test]
    fn deser() {
        let s = std::fs::read_to_string("../../temp/gog_response.json")
            .expect("failed to read temp/gog_response.json");
        let deser = serde_json::from_str::<GogResponse>(&s);
        if deser.is_err() {
            println!("{:?}", deser);
        }
        assert_eq!(true, deser.is_ok());
    }
}
