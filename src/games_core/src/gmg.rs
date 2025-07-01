use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::algolia::{Downloader, ParamsBuilder};
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

impl Gmg {
    const ALGOLIA_URL: &str = "https://sczizsp09z-dsn.algolia.net/1/indexes/*/queries";
    const ALGOLIA_AGENT: &str = "Algolia for JavaScript (4.5.1); Browser (lite); instantsearch.js (4.8.3); JS Helper (3.2.2)";
    const ALGOLIA_API_KEY: &str = "3bc4cebab2aa8cddab9e9a3cfad5aef3";
    const ALGOLIA_APP_ID: &str = "SCZIZSP09Z";
    const ALGOLIA_INDEX_NAME: &str = "prod_ProductSearch_US";

    const FILTERS_GAMES: &str =
        "IsSellable:true AND AvailableRegions:US AND NOT ExcludeCountryCodes:US AND IsDlc:false";
    const FILTERS_DLC: &str =
        "IsSellable:true AND AvailableRegions:US AND NOT ExcludeCountryCodes:US AND IsDlc:true";
    const FACETS: &str = "[\"Franchise\",\"IsEarlyAccess\",\"Genre\",\"PlatformName\",\"PublisherName\",\"SupportedVrs\",\"Regions.US.ReleaseDateStatus\",\"Regions.US.Drp\",\"Regions.US.IsOnSale\",\"DrmName\"]";
    const FACET_FILTERS_ON_SALE: &str = "[[\"Regions.US.IsOnSale:true\"],[\"PlatformName:PC\"],[\"Regions.US.ReleaseDateStatus:InStock\"],[\"DrmName:Steam\"]]";
    const FACET_FILTERS: &str =
        "[[\"PlatformName:PC\"],[\"Regions.US.ReleaseDateStatus:InStock\"],[\"DrmName:Steam\"]]";

    pub async fn download(&self, kind: DownloadKind) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR).await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let output_path = match kind {
            DownloadKind::OnSale => PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now)),
            DownloadKind::All => PathBuf::from(OUTPUT_DIR).join(format!("all_{}.json", now)),
            DownloadKind::New => PathBuf::from(OUTPUT_DIR).join(format!("new_{}.json", now)),
        };

        let mut headers = HeaderMap::new();

        headers.insert("x-algolia-agent", Self::ALGOLIA_AGENT.parse().unwrap());
        headers.insert("x-algolia-api-key", Self::ALGOLIA_API_KEY.parse().unwrap());
        headers.insert(
            "x-algolia-application-id",
            Self::ALGOLIA_APP_ID.parse().unwrap(),
        );

        let price_filters = [
            "[\"Regions.US.Drp>=45\"]",
            "[\"Regions.US.Drp>=30\",\"Regions.US.Drp<45\"]",
            "[\"Regions.US.Drp>=25\",\"Regions.US.Drp<30\"]",
            "[\"Regions.US.Drp>=20\",\"Regions.US.Drp<25\"]",
            "[\"Regions.US.Drp>=19\",\"Regions.US.Drp<20\"]",
            "[\"Regions.US.Drp>=18\",\"Regions.US.Drp<19\"]",
            "[\"Regions.US.Drp>=16\",\"Regions.US.Drp<18\"]",
            "[\"Regions.US.Drp>=14\",\"Regions.US.Drp<16\"]",
            "[\"Regions.US.Drp>=12\",\"Regions.US.Drp<14\"]",
            "[\"Regions.US.Drp>=10\",\"Regions.US.Drp<12\"]",
            "[\"Regions.US.Drp>=9\",\"Regions.US.Drp<10\"]",
            "[\"Regions.US.Drp>=8\",\"Regions.US.Drp<9\"]",
            "[\"Regions.US.Drp>=7\",\"Regions.US.Drp<8\"]",
            "[\"Regions.US.Drp>=6\",\"Regions.US.Drp<7\"]",
            "[\"Regions.US.Drp>=5\",\"Regions.US.Drp<6\"]",
            "[\"Regions.US.Drp>=4\",\"Regions.US.Drp<5\"]",
            "[\"Regions.US.Drp>=3\",\"Regions.US.Drp<4\"]",
            "[\"Regions.US.Drp>=2\",\"Regions.US.Drp<3\"]",
            "[\"Regions.US.Drp>=1\",\"Regions.US.Drp<2\"]",
            "[\"Regions.US.Drp<1\"]",
        ];

        let mut params_games = ParamsBuilder::default();
        params_games
            .rule_contexts("[\"USD\",\"USD_US\",\"US\"]")
            .filters(Self::FILTERS_GAMES)
            .hits_per_page(100)
            .distinct(true)
            .max_values_per_facet(10)
            .facets(Self::FACETS);

        let mut params_dlc = ParamsBuilder::default();
        params_dlc
            .rule_contexts("[\"USD\",\"USD_US\",\"US\"]")
            .filters(Self::FILTERS_DLC)
            .hits_per_page(100)
            .distinct(true)
            .max_values_per_facet(10)
            .facets(Self::FACETS);

        match kind {
            DownloadKind::OnSale => {
                params_games.facet_filters(Self::FACET_FILTERS_ON_SALE);
                params_dlc.facet_filters(Self::FACET_FILTERS_ON_SALE);
            }
            DownloadKind::All => {
                params_games.facet_filters(Self::FACET_FILTERS);
                params_dlc.facet_filters(Self::FACET_FILTERS);
            }
            DownloadKind::New => {
                params_games.query("new-games");
                params_dlc.query("new-games");
            }
        };

        let mut dl = Downloader {
            output_path,
            headers,
            url: Self::ALGOLIA_URL,
            client: &self.client,
            price_filters: &price_filters,
            params_games,
            params_dlc,
            algolia_index_name: Self::ALGOLIA_INDEX_NAME,
        };

        dl.download::<AlgoliaHit>().await?;

        Ok(())
    }
}

pub enum DownloadKind {
    OnSale,
    All,
    New,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct AlgoliaHit {
    pub display_name: String,
    pub is_dlc: bool,
    pub genre: Vec<String>,
    pub franchise: String,
    pub publisher_name: String,
    pub regions: RegionInfo,
    pub steam_app_id: String, // NOTE: convert to u32 later
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
