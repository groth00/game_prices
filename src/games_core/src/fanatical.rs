use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::algolia::{OnSaleState, ParamsBuilder};
use anyhow::Error;
use chrono::{DateTime, Utc};
use headless_chrome::{
    Browser,
    browser::{
        tab::{RequestInterceptor, RequestPausedDecision},
        transport::{SessionId, Transport},
    },
    protocol::cdp::{
        self,
        Fetch::{self, ContinueRequest, HeaderEntry, events::RequestPausedEvent},
        Network::ResourceType,
    },
};
use log::info;
use quick_xml::{events::Event, reader::Reader};
use reqwest::{Client, ClientBuilder, header::HeaderMap};
use serde::{Deserialize, Deserializer, Serialize, de};
use tokio::{
    fs::{self},
    time::{self},
};

const USER_AGENT: &str = "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0";
const OUTPUT_DIR: &str = "output/fanatical";

// NOTE: more pages of interest
// /new-releases
// /upcoming-games
// /top-sellers
// /latest-deals
// /trending-deals
// /ending-soon
pub struct Fanatical {
    links: Links,
    client: Client,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AlgoliaHit {
    pub name: String,
    pub r#type: ItemType,
    pub discount_percent: u64,
    pub best_ever: bool,
    pub flash_sale: bool,
    pub price: Prices,
    #[serde(rename = "fullPrice")]
    pub full_price: Prices,
    pub available_valid_from: u32,
    pub available_valid_until: u32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemType {
    Dlc,
    Game,
}

impl From<&str> for ItemType {
    fn from(value: &str) -> Self {
        match value {
            "dlc" => Self::Dlc,
            "game" => Self::Game,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Prices {
    #[serde(rename = "USD")]
    pub usd: f64,
}

#[derive(Debug, Deserialize)]
struct AlgoliaHeaders {
    #[serde(alias = "x-algolia-agent")]
    agent: String,
    #[serde(alias = "x-algolia-api-key")]
    api_key: String,
    #[serde(alias = "x-algolia-application-id")]
    application_id: String,
}

#[derive(Debug, Default)]
struct XhrInterceptor {
    captured_url: Arc<Mutex<String>>,
}

impl RequestInterceptor for XhrInterceptor {
    fn intercept(
        &self,
        _tr: Arc<Transport>,
        _id: SessionId,
        ev: RequestPausedEvent,
    ) -> RequestPausedDecision {
        // info!("{:?}", ev.params);
        if ev.params.resource_Type == ResourceType::Xhr {
            let url = &ev.params.request.url;
            // let headers = &ev.params.request.headers;

            // info!("XHR request sent to: {}", url);
            // info!("request headers: {:?}", headers);

            if let Ok(mut lock) = self.captured_url.lock() {
                *lock = url.clone();
            }
        }

        let headers_convert = ev.params.request.headers.0.unwrap().as_object().map(|map| {
            map.iter()
                .filter_map(|(key, val)| {
                    val.as_str().map(|v| HeaderEntry {
                        name: key.clone(),
                        value: v.to_string(),
                    })
                })
                .collect()
        });

        RequestPausedDecision::Continue(Some(ContinueRequest {
            request_id: ev.params.request_id,
            url: Some(ev.params.request.url),
            method: Some(ev.params.request.method),
            post_data: ev.params.request.post_data,
            headers: headers_convert,
            intercept_response: Some(true),
        }))
    }
}

/// Use headless_chrome to extract headers from XHR request made to Algolia
async fn get_algolia_headers() -> Result<AlgoliaHeaders, Error> {
    let browser = Browser::default()?;
    let tab = browser.new_tab()?;

    let patterns = vec![Fetch::RequestPattern {
        request_stage: Some(cdp::Fetch::RequestStage::Request),
        resource_Type: Some(cdp::Network::ResourceType::Xhr),
        url_pattern: Some("*w2m9492ddv-dsn.algolia.net*".into()),
    }];

    tab.call_method(Fetch::Enable {
        patterns: Some(patterns),
        handle_auth_requests: Some(false),
    })?;

    let captured_url = Arc::new(Mutex::new(String::new()));
    let interceptor = Arc::new(XhrInterceptor {
        captured_url: captured_url.clone(),
    });
    tab.enable_request_interception(interceptor.clone())?;

    const URL: &str = "https://www.fanatical.com/en/search";
    tab.navigate_to(URL)?.wait_until_navigated()?;

    std::thread::sleep(Duration::from_secs(8));

    let url = captured_url.lock().unwrap().clone();
    let raw = url.split("?").nth(1).unwrap(); // query parameters
    let headers: AlgoliaHeaders = serde_urlencoded::from_str(raw)?;
    Ok(headers)
}

impl Default for Fanatical {
    fn default() -> Self {
        let client = ClientBuilder::new()
            .https_only(true)
            .timeout(Duration::from_secs(60))
            .user_agent(USER_AGENT)
            .build()
            .expect("failed to build reqwest client");

        Self {
            links: Links {
                sitemaps: [
                    "https://www.fanatical.com/sitemaps-fanatical.xml", // leads to other paths of interest
                    "https://www.fanatical.com/sitemaps-franchises.xml",
                    "https://www.fanatical.com/sitemaps-categories.xml",
                    "https://www.fanatical.com/sitemaps-collections.xml",
                    "https://www.fanatical.com/sitemaps-publishers.xml",
                    "https://www.fanatical.com/sitemaps-games-like.xml",
                    "https://www.fanatical.com/sitemaps-products-en.xml",
                ],

                // includes non-game bundles
                pickandmix: "https://fanatical.com/api/all/en",
            },
            client,
        }
    }
}

impl Fanatical {
    pub async fn sitemaps(&self) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR).await?;
        let output_dir = PathBuf::from(OUTPUT_DIR);

        for url in self.links.sitemaps {
            info!("downloading {}", url);
            let body = reqwest::get(url).await?.text().await?;

            let mut output_path = PathBuf::from(url.rsplit("/").next().unwrap());
            output_path = output_dir.join(output_path);
            fs::write(output_path, body).await?;
            time::sleep(Duration::from_secs(1)).await;
        }

        let mut input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-categories.xml");
        let mut output_name = "categories.json";
        parse_entities(input_path, output_name).await?;

        input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-collections.xml");
        output_name = "collections.json";
        parse_entities(input_path, output_name).await?;

        input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-fanatical.xml");
        output_name = "main-sitemap.json";
        parse_entities(input_path, output_name).await?;

        input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-franchises.xml");
        output_name = "franchises.json";
        parse_entities(input_path, output_name).await?;

        input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-games-like.xml");
        output_name = "games-like.json";
        parse_entities(input_path, output_name).await?;

        input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-products-en.xml");
        output_name = "products.json";
        parse_products(input_path, output_name).await?;

        input_path = PathBuf::from(OUTPUT_DIR).join("sitemaps-publishers.xml");
        output_name = "publishers.json";
        parse_entities(input_path, output_name).await?;

        Ok(())
    }

    pub async fn bundles(&self) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR).await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let output_path = PathBuf::from(OUTPUT_DIR).join(format!("bundles_{}.json", now));

        let client = reqwest::Client::new();
        let data = client
            .get(self.links.pickandmix)
            .send()
            .await?
            .json::<Bundles>()
            .await?;

        let serialized = serde_json::to_string_pretty(&data)?;
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    pub async fn on_sale(&self) -> Result<(), Error> {
        fs::create_dir_all(OUTPUT_DIR).await?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let output_path = PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now));

        let algolia_headers = get_algolia_headers().await?;

        let mut headers = HeaderMap::new();

        headers.insert("x-algolia-agent", algolia_headers.agent.parse().unwrap());
        headers.insert(
            "x-algolia-api-key",
            algolia_headers.api_key.parse().unwrap(),
        );
        headers.insert(
            "x-algolia-application-id",
            algolia_headers.application_id.parse().unwrap(),
        );

        let url = "https://w2m9492ddv-dsn.algolia.net/1/indexes/*/queries";

        let price_filters = [
            "[\"price.USD>=30\"]",
            "[\"price.USD>=20\",\"price.USD<30\"]",
            "[\"price.USD>=15\",\"price.USD<20\"]",
            "[\"price.USD>=10\",\"price.USD<15\"]",
            "[\"price.USD>=5\",\"price.USD<10\"]",
            "[\"price.USD>=4\",\"price.USD<5\"]",
            "[\"price.USD>=3\",\"price.USD<4\"]",
            "[\"price.USD>=2\",\"price.USD<3\"]",
            "[\"price.USD>=1\",\"price.USD<2\"]",
            "[\"price.USD<1\"]",
        ];

        // NOTE: hits_per_page is capped at 100 for this index specifically
        let mut params_games = ParamsBuilder::default();
        params_games
        .facet_filters("[[\"display_type:game\"],[\"drm:steam\"],[\"languages:English\"],[\"on_sale:true\"]]")
        .facets("[\"collections\",\"display_type\",\"drm\",\"features\",\"genresSlug\",\"languages\",\"on_sale\",\"operating_systems\",\"playstylesSlug\",\"price.USD\",\"publishers\",\"steam_deck_support\",\"themesSlug\",\"vr_support\"]")
        .hits_per_page(100)
        .max_values_per_facet(50)
        .faceting_after_distinct(true);

        let mut params_dlc = ParamsBuilder::default();
        params_dlc
        .facet_filters("[[\"display_type:dlc\"],[\"drm:steam\"],[\"languages:English\"],[\"on_sale:true\"]]")
        .facets("[\"collections\",\"display_type\",\"drm\",\"features\",\"genresSlug\",\"languages\",\"on_sale\",\"operating_systems\",\"playstylesSlug\",\"price.USD\",\"publishers\",\"steam_deck_support\",\"themesSlug\",\"vr_support\"]")
        .hits_per_page(100)
        .max_values_per_facet(50)
        .faceting_after_distinct(true);

        let algolia_index_name = "fan_unlimited";

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

async fn parse_entities(input_path: PathBuf, output_name: &str) -> Result<(), Error> {
    let mut reader = Reader::from_file(input_path)?;
    reader.config_mut().trim_text(true);

    let mut should_keep = false;

    let mut last_url_str: String = String::new();
    let mut entities: HashSet<String> = HashSet::new();
    let mut output: Vec<(String, String)> = Vec::new();

    let mut buf = vec![];

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => panic!("error at position {}: {:?}", reader.error_position(), e),
            Ok(Event::Eof) => break,
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap();
                if e.starts_with(b"https") {
                    if entities.contains(text.as_ref()) {
                        should_keep = false;
                    } else {
                        should_keep = true;
                        entities.insert(text.clone().into_owned());
                        last_url_str = text.clone().into_owned();
                    }
                } else if should_keep {
                    output.push((last_url_str.clone(), text.into_owned()));
                    should_keep = false;
                }
            }
            _ => (),
        }
        buf.clear();
    }

    let mut filtered = output
        .iter()
        .filter(|p| {
            let mut parts = p.0.split("/");
            if let Some(lang) = parts.nth(3) {
                lang == "en"
            } else {
                false
            }
        })
        .collect::<Vec<_>>();
    filtered.sort();

    let serialized = serde_json::to_string(&filtered)?;
    let mut output_path = PathBuf::from(OUTPUT_DIR);
    output_path = output_path.join(output_name);
    fs::write(output_path, serialized).await?;

    Ok(())
}

async fn parse_products(input_path: PathBuf, output_name: &str) -> Result<(), Error> {
    let mut reader = Reader::from_file(input_path)?;
    reader.config_mut().trim_text(true);

    let mut urls = vec![];
    let mut last_mods = vec![];

    let mut buf = vec![];

    loop {
        match reader.read_event_into(&mut buf) {
            Err(e) => panic!("error at position {}: {:?}", reader.error_position(), e),
            Ok(Event::Eof) => break,
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap().to_string();

                if e.starts_with(b"https") {
                    urls.push(text);
                } else {
                    last_mods.push(text);
                }
            }
            _ => (),
        }
        buf.clear();
    }

    let mut links = vec![];
    for (url, last_mod) in urls.into_iter().zip(last_mods) {
        let mut parts = url.rsplitn(4, "/");
        if let (Some(maybe_dlc), Some(maybe_name), Some(maybe_kind)) =
            (parts.next(), parts.next(), parts.next())
        {
            let mut meta = GameMetadata {
                ..Default::default()
            };

            // two cases for the path segments reversed
            // "dlc", <game>, <kind>
            // <game>, <kind>
            if maybe_dlc == "dlc" {
                meta.name = maybe_dlc.to_string() + maybe_name;
                meta.kind = Product::from(maybe_kind);
            } else {
                meta.name = maybe_dlc.to_string();
                meta.kind = Product::from(maybe_name);
            };

            meta.url = url;
            meta.last_mod = last_mod;

            links.push(meta);
        }
    }

    info!("processed {} products", links.len());
    let serialized = serde_json::to_string(&links)?;

    let mut output_path = PathBuf::from(OUTPUT_DIR);
    output_path = output_path.join(output_name);
    fs::write(output_path, serialized).await?;

    Ok(())
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
enum Product {
    #[default]
    Unknown,
    Game,
    Dlc,
    PickAndMix,
    Bundle,
    Software,
    GiftCard,
}

impl From<&str> for Product {
    fn from(value: &str) -> Self {
        match value {
            "game" => Self::Game,
            "dlc" => Self::Dlc,
            "bundle" => Self::Bundle,
            "pick-and-mix" => Self::PickAndMix,
            "software" => Self::Software,
            "gift-card" => Self::GiftCard,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct GameMetadata {
    name: String,
    kind: Product,
    url: String,
    last_mod: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Bundles {
    pickandmix: Vec<BundleInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BundleInfo {
    #[serde(alias = "_id")]
    id: String,
    cover_image: String,
    name: String,
    products: Vec<BundleProduct>,
    sku: String,
    slug: String,
    tiers: Vec<Tier>,
    #[serde(alias = "type")]
    bundle_type: String,
    valid_from: DateTime<Utc>,
    #[serde(default = "current_time", deserialize_with = "replace_invalid_time")]
    valid_until: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Tier {
    quantity: u64,
    // originally a HashMap<String, f64> where the keys are country codes
    // and the values are in cents
    #[serde(deserialize_with = "extract_usd_price")]
    price: f64,
    #[serde(alias = "_id")]
    id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct BundleProduct {
    #[serde(alias = "_id")]
    id: String,
    name: String,
    slug: String,
    cover: String,
}

fn extract_usd_price<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let map: HashMap<String, f64> = HashMap::deserialize(deserializer)?;
    match map.get("USD") {
        Some(v) => Ok(*v / 100.0),
        None => Err(de::Error::custom("missing USD price")),
    }
}

fn replace_invalid_time<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<DateTime<Utc>>::deserialize(deserializer)?.unwrap_or_else(current_time))
}

fn current_time() -> DateTime<Utc> {
    chrono::Utc::now()
}

struct Links {
    sitemaps: [&'static str; 7],

    pickandmix: &'static str,
}
