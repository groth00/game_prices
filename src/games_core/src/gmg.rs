use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::algolia::{OnSaleState, ParamsBuilder};
use anyhow::Error;
use reqwest::{Client, header::HeaderMap};
use serde::{Deserialize, Serialize};
use tokio::fs::{self};

const OUTPUT_DIR: &str = "output/gmg";
const USER_AGENT: &str = "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0";

pub struct Gmg {
    client: Client,
}

impl Default for Gmg {
    fn default() -> Self {
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .timeout(Duration::from_secs(60))
            .user_agent(USER_AGENT)
            .build()
            .expect("failed to build reqwest client");

        Self { client }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AlgoliaHit {
    #[serde(alias = "DisplayName")]
    pub name: String,
    #[serde(alias = "IsDlc")]
    pub is_dlc: bool,
    #[serde(alias = "Regions")]
    pub regions: RegionInfo,
    #[serde(alias = "SteamAppId")]
    pub steam_app_id: String, // NOTE: should be u32, be careful!
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RegionInfo {
    #[serde(alias = "US")]
    pub us: USInfo,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct USInfo {
    #[serde(alias = "Drp")]
    pub price: f64,
    #[serde(alias = "DrpDiscountPercentage")]
    pub discount_percent: u64,
    #[serde(alias = "Rrp")]
    pub original_price: f64,
}

impl Gmg {
    pub async fn on_sale(&self) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR).await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let output_path = PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now));

        let mut headers = HeaderMap::new();

        headers.insert("x-algolia-agent", "Algolia for JavaScript (4.5.1); Browser (lite); instantsearch.js (4.8.3); JS Helper (3.2.2)".parse().unwrap());
        headers.insert(
            "x-algolia-api-key",
            "3bc4cebab2aa8cddab9e9a3cfad5aef3".parse().unwrap(),
        );
        headers.insert("x-algolia-application-id", "SCZIZSP09Z".parse().unwrap());

        let url = "https://sczizsp09z-dsn.algolia.net/1/indexes/*/queries";

        // NOTE: algolia returns a maximum of 1000 results; use price ranges to partition
        let price_filters = [
            "[\"Regions.US.Drp>=30\"]",
            "[\"Regions.US.Drp>=20\",\"Regions.US.Drp<=30\"]",
            "[\"Regions.US.Drp>=10\",\"Regions.US.Drp<=20\"]",
            "[\"Regions.US.Drp>=5\",\"Regions.US.Drp<=10\"]",
            "[\"Regions.US.Drp>=4\",\"Regions.US.Drp<=5\"]",
            "[\"Regions.US.Drp>=3\",\"Regions.US.Drp<=4\"]",
            "[\"Regions.US.Drp>=2\",\"Regions.US.Drp<=3\"]",
            "[\"Regions.US.Drp>=1\",\"Regions.US.Drp<=2\"]",
            "[\"Regions.US.Drp<=1\"]",
        ];

        let mut params_games = ParamsBuilder::default();
        params_games
            .rule_contexts("[\"USD\",\"USD_US\",\"US\"]".into())
            .filters("IsSellable:true AND AvailableRegions:US AND NOT ExcludeCountryCodes:US AND IsDlc:false".into())
            .hits_per_page(100)
            .distinct(true)
            .max_values_per_facet(10)
            .facets("[\"Franchise\",\"IsEarlyAccess\",\"Genre\",\"PlatformName\",\"PublisherName\",\"SupportedVrs\",\"Regions.US.ReleaseDateStatus\",\"Regions.US.Drp\",\"Regions.US.IsOnSale\",\"DrmName\"]".into())
            .facet_filters("[[\"Regions.US.IsOnSale:true\"],[\"PlatformName:PC\"],[\"Regions.US.ReleaseDateStatus:InStock\"],[\"DrmName:Steam\"]]".into());

        let mut params_dlc = ParamsBuilder::default();
        params_dlc
            .rule_contexts("[\"USD\",\"USD_US\",\"US\"]".into())
            .filters("IsSellable:true AND AvailableRegions:US AND NOT ExcludeCountryCodes:US AND IsDlc:true".into())
            .hits_per_page(100)
            .distinct(true)
            .max_values_per_facet(10)
            .facets("[\"Franchise\",\"IsEarlyAccess\",\"Genre\",\"PlatformName\",\"PublisherName\",\"SupportedVrs\",\"Regions.US.ReleaseDateStatus\",\"Regions.US.Drp\",\"Regions.US.IsOnSale\",\"DrmName\"]".into())
            .facet_filters("[[\"Regions.US.IsOnSale:true\"],[\"PlatformName:PC\"],[\"Regions.US.ReleaseDateStatus:InStock\"],[\"DrmName:Steam\"]]".into());

        let algolia_index_name = "prod_ProductSearch_US";

        let mut state = OnSaleState {
            output_path,
            headers,
            url,
            client: &self.client,
            price_filters: price_filters.to_vec(),
            params_games,
            params_dlc,
            algolia_index_name,
        };

        state.algolia_on_sale::<AlgoliaHit>().await?;

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct PriceInfo {
    pub name: String,
    pub drm: Drm,
    pub percent_discount: u64,
    pub original_price: f64,
    pub price: f64,
}

#[derive(Debug, Serialize)]
pub enum Drm {
    Steam,
    Uplay,
    EpicGames,
    Microsoft,
    EveOnline,
    TESOnline,
    Xbox,
}

impl From<&str> for Drm {
    fn from(value: &str) -> Self {
        match value {
            "Steam" => Self::Steam,
            "Uplay" => Self::Uplay,
            "Epic Games" => Self::EpicGames,
            "Microsoft" => Self::Microsoft,
            "Eve Online" => Self::EveOnline,
            "TESO" => Self::TESOnline,
            "Xbox" => Self::Xbox,
            _ => unreachable!(),
        }
    }
}
