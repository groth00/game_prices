use std::{
    borrow::{Borrow, Cow},
    collections::BTreeSet,
    env,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Error;
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{Datelike, Local, TimeZone};
use log::{error, info};
use prost::Message;
use reqwest::{Client, ClientBuilder, IntoUrl, Method, Response};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    time::sleep,
};

use games_proto::generated::{
    CSteamChartsGetGamesByConcurrentPlayersRequest,
    CSteamChartsGetGamesByConcurrentPlayersResponse, CSteamChartsGetMonthTopAppReleasesRequest,
    CSteamChartsGetMonthTopAppReleasesResponse, CSteamChartsGetMostPlayedGamesRequest,
    CSteamChartsGetMostPlayedGamesResponse, CSteamChartsGetMostPlayedSteamDeckGamesRequest,
    CSteamChartsGetMostPlayedSteamDeckGamesResponse, CSteamChartsGetTopReleasesPagesRequest,
    CSteamChartsGetTopReleasesPagesResponse, CStoreBrowseGetItemsRequest,
    CStoreBrowseGetItemsResponse, CStoreQueryFilters, CStoreQueryFiltersPriceFilters,
    CStoreQueryFiltersTypeFilters, CStoreQueryParams, CStoreQueryQueryRequest,
    CStoreQueryQueryResponse, CStoreTopSellersGetWeeklyTopSellersRequest,
    CStoreTopSellersGetWeeklyTopSellersResponse, CWishlistFilters,
    CWishlistGetWishlistSortedFilteredRequest, CWishlistGetWishlistSortedFilteredResponse,
    StoreBrowseContext, StoreBrowseItemDataRequest, StoreItemId,
};

const OUTPUT_DIR: &str = "output/steam";

pub struct Steam {
    links: Links<&'static str>,
    api_key: Cow<'static, str>,
    client: Client,
    path_appids: PathBuf,
    pub steam_id: u64,
}

impl Default for Steam {
    fn default() -> Self {
        let links = Links::default();
        let api_key = match env::var("STEAM_API_KEY") {
            Ok(value) => Cow::from(value),
            Err(e) => panic!("{}", e),
        };

        let builder = ClientBuilder::new();
        let client = builder
            .https_only(true)
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to create reqwest client");

        let path_appids = PathBuf::from(OUTPUT_DIR).join("apps.jsonl");
        info!("initialized steam struct");

        Self {
            links,
            api_key,
            client,
            path_appids,
            steam_id: 76561198064322679,
        }
    }
}

impl Steam {
    pub async fn most_concurrent(&self) -> Result<(), Error> {
        let params = CSteamChartsGetGamesByConcurrentPlayersRequest {
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                include_tag_count: Some(16),
                include_reviews: Some(true),
                include_basic_info: Some(true),
                include_links: Some(true),
                ..Default::default()
            }),
        };

        let params_vec = params.encode_to_vec();
        let encoded_params = BASE64_STANDARD.encode(&params_vec);
        let query_params = [
            ("key", self.api_key.to_string()),
            ("input_protobuf_encoded", encoded_params),
        ];

        let resp = retry(
            &self.client,
            Method::GET,
            self.links.most_concurrent,
            &query_params,
        )
        .await?;
        let bytes = resp.bytes().await?;

        let decoded = CSteamChartsGetGamesByConcurrentPlayersResponse::decode(bytes)?;
        let serialized = serde_json::to_string_pretty(&decoded)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join("most_concurrent.json");
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    pub async fn month_top(&self) -> Result<(), Error> {
        let now = Local::now();
        let original_month = now.month();
        // NOTE: no current month, lag for previous month
        let dt = Local
            .with_ymd_and_hms(now.year(), original_month - 2, 1, 0, 0, 0)
            .single()
            .expect("failed to create date");
        let start_time = dt.timestamp() as u32;

        let params = CSteamChartsGetMonthTopAppReleasesRequest {
            rtime_month: Some(start_time),
            include_dlc: Some(false),
            top_results_limit: None,
        };

        let params_vec = params.encode_to_vec();
        let encoded_params = BASE64_STANDARD.encode(&params_vec);
        let query_params = [
            ("key", self.api_key.to_string()),
            ("input_protobuf_encoded", encoded_params),
        ];

        let resp = retry(
            &self.client,
            Method::GET,
            self.links.month_top,
            &query_params,
        )
        .await?;
        let bytes = resp.bytes().await?;
        let decoded = CSteamChartsGetMonthTopAppReleasesResponse::decode(bytes)?;

        let serialized = serde_json::to_string_pretty(&decoded)?;
        let output_path = PathBuf::from("monthly_top.json");
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    pub async fn most_played(&self) -> Result<(), Error> {
        let params = CSteamChartsGetMostPlayedGamesRequest {
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                include_tag_count: Some(16),
                include_reviews: Some(true),
                include_basic_info: Some(true),
                ..Default::default()
            }),
        };

        let params_vec = params.encode_to_vec();
        let encoded_params = BASE64_STANDARD.encode(&params_vec);
        let query_params = [
            ("key", self.api_key.to_string()),
            ("input_protobuf_encoded", encoded_params),
        ];

        let resp = retry(
            &self.client,
            Method::GET,
            self.links.most_played,
            &query_params,
        )
        .await?;
        let bytes = resp.bytes().await?;
        let decoded = CSteamChartsGetMostPlayedGamesResponse::decode(bytes)?;

        let serialized = serde_json::to_string_pretty(&decoded)?;
        let output_path = PathBuf::from("most_played.json");
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    pub async fn most_played_deck(&self) -> Result<(), Error> {
        let params = CSteamChartsGetMostPlayedSteamDeckGamesRequest {
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                include_tag_count: Some(16),
                include_reviews: Some(true),
                include_basic_info: Some(true),
                ..Default::default()
            }),
            top_played_period: None,
            count: Some(100),
        };

        let params_vec = params.encode_to_vec();
        let encoded_params = BASE64_STANDARD.encode(&params_vec);
        let query_params = [
            ("key", self.api_key.to_string()),
            ("input_protobuf_encoded", encoded_params),
        ];

        let resp = retry(
            &self.client,
            Method::GET,
            self.links.most_played_deck,
            &query_params,
        )
        .await?;
        let bytes = resp.bytes().await?;
        let decoded = CSteamChartsGetMostPlayedSteamDeckGamesResponse::decode(bytes)?;

        let serialized = serde_json::to_string_pretty(&decoded)?;
        let output_path = PathBuf::from("most_played_deck.json");
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    pub async fn weekly_top(&self, start_date: Option<u32>) -> Result<(), Error> {
        // NOTE: no pagination, just top 100
        let params = CStoreTopSellersGetWeeklyTopSellersRequest {
            country_code: None,
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                include_tag_count: Some(16),
                include_reviews: Some(true),
                include_basic_info: Some(true),
                ..Default::default()
            }),
            start_date,
            page_start: None, // no pagination
            page_count: Some(100),
        };

        let params_vec = params.encode_to_vec();
        let encoded_params = BASE64_STANDARD.encode(&params_vec);
        let query_params = [
            ("key", self.api_key.to_string()),
            ("input_protobuf_encoded", encoded_params),
        ];

        let resp = retry(
            &self.client,
            Method::GET,
            self.links.weekly_top,
            &query_params,
        )
        .await?;
        let bytes = resp.bytes().await?;
        let decoded = CStoreTopSellersGetWeeklyTopSellersResponse::decode(bytes)?;

        let serialized = serde_json::to_string_pretty(&decoded)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join("weekly_top.json");
        fs::write(output_path, serialized).await?;

        Ok(())
    }

    pub async fn top_releases(&self) -> Result<(), Error> {
        let params = CSteamChartsGetTopReleasesPagesRequest {};
        let params_vec = params.encode_to_vec();
        let encoded_params = BASE64_STANDARD.encode(&params_vec);
        let query_params = [
            ("key", self.api_key.to_string()),
            ("input_protobuf_encoded", encoded_params),
        ];

        let resp = retry(
            &self.client,
            Method::GET,
            self.links.top_releases,
            &query_params,
        )
        .await?;

        let bytes = resp.bytes().await?;
        let decoded = CSteamChartsGetTopReleasesPagesResponse::decode(bytes)?;

        let serialized = serde_json::to_string_pretty(&decoded)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join("top_releases.json");
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    async fn store_query(
        &self,
        output_path: PathBuf,
        params: &mut CStoreQueryQueryRequest,
    ) -> Result<(), Error> {
        let outfile = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&output_path)
            .await?;
        let mut writer = BufWriter::new(outfile);

        const OFFSET: i32 = 500;

        let mut count = 0;
        let mut total_records = 0;

        loop {
            let params_vec = params.encode_to_vec();
            let encoded_params = BASE64_STANDARD.encode(&params_vec);

            let query_params = [
                ("key", self.api_key.to_string()),
                ("input_protobuf_encoded", encoded_params),
            ];

            let resp = retry(&self.client, Method::GET, self.links.query, &query_params).await?;
            let bytes = resp.bytes().await?;

            let deser = CStoreQueryQueryResponse::decode(bytes)?;
            let serialized = serde_json::to_string(&deser)?;
            writer.write_all(serialized.as_bytes()).await?;
            writer.write_u8(b'\n').await?;

            if let Some(meta) = deser.metadata {
                info!(
                    "{} of {}",
                    meta.start() + meta.count(),
                    meta.total_matching_records()
                );
                total_records = meta.total_matching_records();
            }

            count += OFFSET;
            if count >= total_records {
                break;
            }

            if let Some(query_params) = params.query.as_mut() {
                if let Some(start) = query_params.start.as_mut() {
                    *start += OFFSET;
                }
            }
            info!("new_start: {:?}", params.query.as_ref().map(|p| p.start()));
            sleep(Duration::from_millis(1000)).await;
        }

        writer.flush().await?;

        Ok(())
    }

    pub async fn on_sale(&self) -> Result<(), Error> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let path = PathBuf::from(OUTPUT_DIR).join(format!("on_sale_{}.json", now));
        let mut params = CStoreQueryQueryRequest {
            query_name: None,
            query: Some(CStoreQueryParams {
                start: Some(0),
                count: Some(500),
                sort: Some(12), // trending items
                filters: Some(CStoreQueryFilters {
                    type_filters: Some(CStoreQueryFiltersTypeFilters {
                        include_games: Some(true),
                        include_dlc: Some(true),
                        ..Default::default()
                    }),
                    price_filters: Some(CStoreQueryFiltersPriceFilters {
                        min_discount_percent: Some(1),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }),
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_basic_info: Some(true),
                include_reviews: Some(true),
                include_tag_count: Some(16),
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                ..Default::default()
            }),
            override_country_code: None,
        };

        self.store_query(path, &mut params).await?;
        Ok(())
    }

    pub async fn bundles(&self) -> Result<(), Error> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let path = PathBuf::from(OUTPUT_DIR).join(format!("bundles_{}.json", now));
        let mut params = CStoreQueryQueryRequest {
            query_name: None,
            query: Some(CStoreQueryParams {
                start: Some(0),
                count: Some(500),
                sort: Some(12), // trending items
                filters: Some(CStoreQueryFilters {
                    type_filters: Some(CStoreQueryFiltersTypeFilters {
                        include_bundles: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }),
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_included_items: Some(true),
                include_all_purchase_options: Some(true),
                ..Default::default()
            }),
            override_country_code: None,
        };

        self.store_query(path, &mut params).await?;
        Ok(())
    }

    pub async fn most_wishlisted(&self) -> Result<(), Error> {
        let path = "most_wishlisted.json";
        let params = QueryParams {
            start: 0,
            count: 50,
            sort_by: "released_asc".into(),
            category1: "998,21".into(),
            supportedlang: "english".into(),
            hidef2p: 1,
            filter: "popularwishlist".into(),
        };

        self.query_ui(path, params).await?;

        Ok(())
    }

    pub async fn coming_soon(&self) -> Result<(), Error> {
        let path = "coming_soon.jsonl";
        let params = QueryParams {
            start: 0,
            count: 50,
            sort_by: "released_asc".into(),
            category1: "998,21".into(),
            supportedlang: "english".into(),
            hidef2p: 1,
            filter: "comingsoon".into(),
        };

        self.query_ui(path, params).await?;

        Ok(())
    }

    // TODO: need a versioned approach; appending won't be accurate
    async fn query_ui<P: AsRef<Path>>(
        &self,
        path: P,
        mut params: QueryParams,
    ) -> Result<(), Error> {
        let mut seen_ids = self.build_seen_set(&path).await?;

        let file_path = PathBuf::from(OUTPUT_DIR).join(path);
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)
            .await?;

        let mut writer = BufWriter::new(file);

        let total_results_selector =
            Selector::parse("#search_results_filtered_warning div:first-child").unwrap();
        let container_selector = Selector::parse("#search_resultsRows").unwrap();
        let rows_selector = Selector::parse("#search_resultsRows a").unwrap();
        let released_selector = Selector::parse(".search_released").unwrap();
        let discount_pct_selector = Selector::parse(".discount_pct").unwrap();
        let original_price_selector = Selector::parse(".discount_original_price").unwrap();
        let price_selector = Selector::parse(".discount_final_price").unwrap();

        let mut total_results_flag = true;

        loop {
            info!(
                "fetching {} to {}",
                params.start,
                params.start + params.count
            );

            let resp = retry(&self.client, Method::GET, self.links.query_ui, &params).await?;
            let text = resp.text().await?;

            let html = Html::parse_document(&text);

            if total_results_flag {
                if let Some(total_results) = html.select(&total_results_selector).next() {
                    info!("{}", total_results.text().next().unwrap().trim())
                }
                total_results_flag = false;
            }

            if html.select(&container_selector).next().is_none() {
                break;
            }

            let rows = html.select(&rows_selector);
            for row in rows {
                let appid = row
                    .attr("data-ds-appid")
                    .expect("missing data-ds-appid")
                    .parse::<u32>()?;

                if seen_ids.contains(&appid) {
                    continue;
                } else {
                    // shouldn't be necessary, but better safe than sorry
                    seen_ids.insert(appid);
                }

                let href = row.attr("href").unwrap();
                let name = href.split("/").nth(5).expect("invalid href split");
                let tags = row
                    .attr("data-ds-tagids")
                    .expect("missing data-ds-tagids")
                    .trim_start_matches("[")
                    .trim_end_matches("]")
                    .split(",")
                    .map(|s| s.parse::<u32>().expect("parse tag failed"))
                    .collect::<Vec<_>>();
                let release_date = match row.select(&released_selector).next() {
                    Some(elem) => elem.text().next().expect("missing release date").trim(),
                    None => "Unknown",
                };
                let discount = match row.select(&discount_pct_selector).next() {
                    Some(elem) => elem
                        .text()
                        .next()
                        .expect("missing discount")
                        .trim_start_matches("-")
                        .trim_end_matches("%")
                        .parse::<i64>()?,
                    None => -1,
                };
                let original_price = match row.select(&original_price_selector).next() {
                    Some(elem) => elem
                        .text()
                        .next()
                        .expect("missing original price")
                        .trim_start_matches("$")
                        .parse::<f64>()?,
                    None => -1f64,
                };
                let price = match row.select(&price_selector).next() {
                    Some(elem) => elem
                        .text()
                        .next()
                        .expect("missing price")
                        .trim_start_matches("$")
                        .parse::<f64>()?,
                    None => -1f64,
                };

                let meta = GameMetadata {
                    href: href.into(),
                    name: name.into(),
                    appid,
                    tags,
                    release_date: release_date.into(),
                    discount,
                    original_price,
                    price,
                };

                let serialized = serde_json::to_string(&meta)?;
                writer.write_all(&serialized.into_bytes()).await?;
                writer.write_u8(b'\n').await?;
            }

            params.start += 50;
            sleep(Duration::from_millis(1000)).await;
        }

        writer.flush().await?;

        Ok(())
    }

    async fn build_seen_set<P: AsRef<Path>>(&self, path: P) -> Result<BTreeSet<u32>, Error> {
        let file_path = PathBuf::from(OUTPUT_DIR).join(path);
        let file = match OpenOptions::new().read(true).open(file_path).await {
            Ok(file) => file,
            Err(e) => {
                error!("error: {}", e);
                return Ok(BTreeSet::new());
            }
        };
        let mut reader = BufReader::new(file);

        let mut set = BTreeSet::new();
        let mut str = String::new();
        while let Ok(size) = reader.read_line(&mut str).await {
            match size {
                0 => break,
                _ => {
                    let deser: GameMetadata = serde_json::from_str(&str)?;
                    set.insert(deser.appid);
                }
            }
            str.clear();
        }
        Ok(set)
    }

    pub async fn achievements_percentages(&self, gameid: u64) -> Result<(), Error> {
        let resp = self
            .client
            .request(Method::GET, self.links.achievement_pct)
            .query(&[("gameid", gameid)])
            .send()
            .await?
            .text()
            .await?;

        fs::write("temp/achievements_percentages", resp).await?;

        Ok(())
    }

    pub async fn achievements_schema(&self, appid: u64) -> Result<(), Error> {
        let appid = appid.to_string();

        let resp = self
            .client
            .request(Method::GET, self.links.stats_schema)
            .query(&[("key", self.api_key.to_string()), ("appid", appid)])
            .send()
            .await?
            .text()
            .await?;

        fs::write("temp/achievements_schema", resp).await?;

        Ok(())
    }

    pub async fn news(&self, appid: u64) -> Result<(), Error> {
        let appid = appid.to_string();

        let resp = self
            .client
            .request(Method::GET, self.links.news)
            .query(&[("key", self.api_key.to_string()), ("appid", appid)])
            .send()
            .await?
            .text()
            .await?;

        fs::write("temp/news", resp).await?;

        Ok(())
    }

    pub async fn categories(&self) -> Result<(), Error> {
        let resp = self
            .client
            .request(Method::GET, self.links.categories)
            .query(&[("key", self.api_key.borrow()), ("language", "english")])
            .send()
            .await?
            .json::<Categories>()
            .await?;

        let serialized = serde_json::to_string_pretty(&resp.response.categories)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join("categories.json");
        fs::write(output_path, &serialized).await?;

        Ok(())
    }

    pub async fn tags(&self) -> Result<(), Error> {
        let resp = self
            .client
            .request(Method::GET, self.links.tags)
            .query(&[("key", self.api_key.borrow()), ("language", "english")])
            .send()
            .await?
            .json::<TagList>()
            .await?;

        let serialized = serde_json::to_string_pretty(&resp)?;
        let output_path = PathBuf::from(OUTPUT_DIR).join("tags.json");
        fs::write(output_path, serialized).await?;

        Ok(())
    }

    pub async fn wishlist(&self, on_sale: bool) -> Result<(), Error> {
        let mut params = CWishlistGetWishlistSortedFilteredRequest {
            steamid: Some(self.steam_id),
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                include_tag_count: Some(10),
                include_reviews: Some(true),
                include_basic_info: Some(true),
                include_supported_languages: Some(true),
                include_included_items: Some(true),
                ..Default::default()
            }),
            sort_order: Some(3), // highest to lowest discount
            filters: None,
            start_index: Some(0),
            page_size: Some(500),
        };

        if on_sale {
            params.filters = Some(CWishlistFilters {
                min_discount_percent: Some(1),
                ..Default::default()
            });
        }

        let mut wishlist = Vec::with_capacity(1000);

        loop {
            let encoded_params = params.encode_to_vec();
            let encoded = BASE64_STANDARD.encode(&encoded_params);

            let bytes = self
                .client
                .request(Method::GET, self.links.wishlist_sorted_filtered)
                .query(&[
                    ("key", self.api_key.to_string()),
                    ("input_protobuf_encoded", encoded),
                ])
                .send()
                .await?
                .bytes()
                .await?;

            let resp = CWishlistGetWishlistSortedFilteredResponse::decode(bytes)?;
            let mut items = resp.items;
            if items.is_empty() {
                break;
            } else {
                wishlist.append(&mut items);
                params.start_index.replace(params.start_index() + 500);
            }
        }

        let serialized = serde_json::to_string_pretty(&wishlist)?;
        let output_path = if on_sale {
            PathBuf::from(OUTPUT_DIR).join("wishlist_on_sale.json")
        } else {
            PathBuf::from(OUTPUT_DIR).join("wishlist.json")
        };
        fs::write(output_path, serialized).await?;

        Ok(())
    }

    /// fetch_appids gives the list of appids, now we fetch game metadata from appids
    /// if we find an appinfo_*.jsonl file, this means we've already processed the initial set of
    /// appids
    /// so we use the last line of the appids file, which contains the most recently changed appids
    pub async fn fetch_appinfo(&self) -> Result<(), Error> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut seen = BTreeSet::new();
        let mut appids = Vec::with_capacity(8192);

        let appids_str = fs::read_to_string(&self.path_appids).await?;

        if let Ok(Some(_appinfo)) = crate::utils::latest_file("output/backup/steam", "appinfo") {
            info!("using last line of appids for update");
            let last_line = appids_str.lines().nth_back(0).expect("empty apps.jsonl");
            let info = serde_json::from_str::<Apps>(last_line)?;
            info.response.apps.iter().for_each(|app: &App| {
                if seen.insert(app.appid) {
                    appids.push(StoreItemId {
                        appid: Some(app.appid),
                        ..Default::default()
                    })
                }
            });
        } else {
            info!("using all appids");
            for line in appids_str.lines() {
                let info = serde_json::from_str::<Apps>(line)?;
                info.response.apps.iter().for_each(|app: &App| {
                    if seen.insert(app.appid) {
                        appids.push(StoreItemId {
                            appid: Some(app.appid),
                            ..Default::default()
                        })
                    }
                });
            }
        }

        let output_path = format!("output/steam/appinfo_{}.jsonl", now);
        let outfile = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(output_path)
            .await?;

        let mut writer = BufWriter::new(outfile);

        let mut request_template = CStoreBrowseGetItemsRequest {
            ids: vec![],
            context: Some(StoreBrowseContext {
                language: Some("english".into()),
                country_code: Some("US".into()),
                steam_realm: Some(1),
                ..Default::default()
            }),
            data_request: Some(StoreBrowseItemDataRequest {
                include_release: Some(true),
                include_platforms: Some(true),
                include_all_purchase_options: Some(true),
                include_tag_count: Some(16),
                include_reviews: Some(true),
                include_basic_info: Some(true),
                include_supported_languages: Some(true),
                include_included_items: Some(true),
                ..Default::default()
            }),
        };

        let mut i = 0;
        for chunk in appids.chunks(500) {
            info!("fetching chunk {}", i);
            i += 1;
            request_template.ids = chunk.to_owned();

            let request_vec = request_template.encode_to_vec();
            let encoded = BASE64_STANDARD.encode(&request_vec);

            let resp = retry(
                &self.client,
                Method::GET,
                self.links.browse,
                &[
                    ("key", self.api_key.to_string()),
                    ("input_protobuf_encoded", encoded),
                ],
            )
            .await?;
            let bytes = resp.bytes().await?;

            let decoded = CStoreBrowseGetItemsResponse::decode(bytes)?;
            let serialized = serde_json::to_string(&decoded)?;
            writer.write_all(serialized.as_bytes()).await?;
            writer.write_u8(b'\n').await?;

            sleep(Duration::from_millis(750)).await;
        }

        writer.flush().await?;

        Ok(())
    }

    // NOTE: steam_deck_compat_category / steam_os_compat_category: 1 = unsupported
    pub async fn fetch_appids(&self) -> Result<(), Error> {
        let last_time = match fs::try_exists(&self.path_appids).await {
            Ok(_) => {
                let appids_str = fs::read_to_string(&self.path_appids).await?;
                let last_line = appids_str.lines().nth_back(0).expect("empty apps.jsonl");
                let info = serde_json::from_str::<Apps>(last_line)?;
                let mut times = info
                    .response
                    .apps
                    .iter()
                    .map(|app: &App| app.last_modified)
                    .collect::<Vec<_>>();
                times.sort_by(|a1, a2| a2.cmp(a1));
                let last_time = times[0];
                info!("last_time: {}", last_time);
                Some(last_time)
            }
            Err(_) => None,
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path_appids)
            .await
            .expect("failed to open output file");

        let mut writer = BufWriter::new(file);

        let mut page = 0;

        let mut params = GrpcQueryParams {
            key: self.api_key.as_ref(),
            if_modified_since: last_time,
            have_description_language: Some("english".into()),
            include_games: true,
            include_dlc: true,
            include_software: false,
            include_videos: false,
            include_hardware: false,
            last_appid: None,
            max_results: 50_000,
        };

        loop {
            info!("fetching page {}", page);

            let str = self
                .client
                .request(Method::GET, self.links.apps)
                .query(&params)
                .send()
                .await?
                .text()
                .await?;

            writer.write_all(str.as_bytes()).await?;
            writer.write_u8(b'\n').await?;

            let pagination = serde_json::from_str::<AppsResponsePartial>(&str)?.pagination;
            info!("{:?}", pagination);

            if pagination.have_more_results.unwrap_or_default() {
                params.last_appid = pagination.last_appid;
                page += 1;
                sleep(Duration::from_secs(2)).await;
            } else {
                break;
            }
        }

        writer.flush().await?;

        Ok(())
    }
}

async fn retry<U: IntoUrl + Copy, Q: Serialize + ?Sized>(
    client: &Client,
    method: Method,
    url: U,
    params: &Q,
) -> Result<Response, Error> {
    let mut attempts = 0;
    let mut backoff = 1;

    while attempts < 3 {
        let resp = client
            .request(method.clone(), url)
            .query(&params)
            .send()
            .await?;

        if resp.status() == 429 || resp.status().is_server_error() {
            error!("{:?}", resp.error_for_status());
            backoff *= 2;
            attempts += 1;
            sleep(Duration::from_secs(backoff)).await;
        } else if resp.status().is_client_error() {
            error!("{:?}", resp.error_for_status());
            return Err(RetryError::ClientError.into());
        } else {
            return Ok(resp);
        }
    }

    Err(RetryError::MaxAttemptsExceeded.into())
}

#[derive(Debug, Error)]
enum RetryError {
    #[error("max attempts exceeded")]
    MaxAttemptsExceeded,
    #[error("client error")]
    ClientError,
}

#[derive(Serialize, Deserialize)]
struct GameMetadata {
    href: String,
    name: String,
    appid: u32,
    tags: Vec<u32>,
    release_date: String,
    discount: i64,
    original_price: f64,
    price: f64,
}

#[derive(Serialize)]
struct QueryParams {
    start: u32,
    count: u32,        // 50
    sort_by: String,   // released_asc
    category1: String, // 998,21 = games + dlc
    supportedlang: String,
    hidef2p: u32,   // 0/1
    filter: String, // comingsoon
}

#[derive(Deserialize, Serialize)]
struct Categories {
    response: CategoriesResponse,
}

#[derive(Deserialize, Serialize)]
struct CategoriesResponse {
    categories: Vec<Category>,
}

#[derive(Deserialize, Serialize)]
struct Category {
    categoryid: u64,
    #[serde(alias = "type")]
    category_type: u64,
    internal_name: String,
    display_name: String,
    image_url: String,
    computed: bool,
    edit_url: String,
    edit_sort_order: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct TagList {
    response: TagListResponse,
}

#[derive(Debug, Deserialize, Serialize)]
struct TagListResponse {
    version_hash: Option<String>,
    tags: Vec<Tag>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Tag {
    tagid: Option<u32>,
    name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Wishlist {
    response: WishlistResponse,
}

#[derive(Debug, Deserialize, Serialize)]
struct WishlistResponse {
    items: Vec<WishlistItem>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WishlistItem {
    appid: u32,
    priority: u32,
    date_added: u32,
}

#[derive(Debug, Serialize)]
struct GrpcQueryParams<'a> {
    key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    if_modified_since: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    have_description_language: Option<String>,
    include_games: bool,
    include_dlc: bool,
    include_software: bool,
    include_videos: bool,
    include_hardware: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_appid: Option<u32>,
    max_results: u32,
}

#[derive(Debug, Deserialize)]
struct Apps {
    response: AppsResponse,
}

#[derive(Debug, Deserialize)]
struct AppsResponse {
    apps: Vec<App>,
}

#[derive(Debug, Deserialize)]
struct App {
    appid: u32,
    last_modified: u32,
}

#[derive(Debug, Deserialize)]
struct AppsResponsePartial {
    #[serde(alias = "response")]
    pagination: AppsPagination,
}

#[derive(Debug, Deserialize)]
struct AppsPagination {
    have_more_results: Option<bool>,
    last_appid: Option<u32>,
}

struct Links<T: IntoUrl> {
    // ISteamChartsService
    most_concurrent: T,
    month_top: T,
    most_played: T,
    most_played_deck: T,
    top_releases: T,

    // ISteamNews
    news: T,

    // ISteamUserStats
    achievement_pct: T,
    stats_schema: T,

    // IStoreBrowseService
    // @protobuf
    browse: T,
    categories: T,

    // IStoreQueryService
    // @protobuf
    query: T,

    // IStoreService
    apps: T,
    tags: T,

    // IStoreTopSellersService
    weekly_top: T,

    // IWishlistService
    wishlist_sorted_filtered: T,

    query_ui: T,
}

impl Default for Links<&str> {
    fn default() -> Self {
        let most_concurrent =
            "https://api.steampowered.com/ISteamChartsService/GetGamesByConcurrentPlayers/v1";
        let month_top =
            "https://api.steampowered.com/ISteamChartsService/GetMonthTopAppReleases/v1";
        let most_played = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1";
        let most_played_deck =
            "https://api.steampowered.com/ISteamChartsService/GetMostPlayedSteamDeckGames/v1";
        let top_releases =
            "https://api.steampowered.com/ISteamChartsService/GetTopReleasesPages/v1";

        let news = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2";

        let achievement_pct =
            "https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2";
        let stats_schema = "https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2";

        let query = "https://api.steampowered.com/IStoreQueryService/Query/v1";
        let browse = "https://api.steampowered.com/IStoreBrowseService/GetItems/v1";
        let categories = "https://api.steampowered.com/IStoreBrowseService/GetStoreCategories/v1";

        let apps = "https://api.steampowered.com/IStoreService/GetAppList/v1";
        let tags = "https://api.steampowered.com/IStoreService/GetTagList/v1";

        let weekly_top =
            "https://api.steampowered.com/IStoreTopSellersService/GetWeeklyTopSellers/v1";

        let wishlist_sorted_filtered =
            "https://api.steampowered.com/IWishlistService/GetWishlistSortedFiltered/v1";

        let query_ui = "https://store.steampowered.com/search/results";

        Self {
            most_concurrent,
            month_top,
            most_played,
            most_played_deck,
            top_releases,

            news,

            achievement_pct,
            stats_schema,

            browse,

            query,
            categories,

            apps,
            tags,

            weekly_top,

            wishlist_sorted_filtered,

            query_ui,
        }
    }
}
