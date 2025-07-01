use std::{path::PathBuf, time::Duration};

use crate::utils::retry;
use anyhow::Error;
use chrono::{Datelike, Local, TimeZone};
use log::info;
use reqwest::{Client, Method, header::HeaderMap};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    time::sleep,
};

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ParamsBuilder {
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rule_contexts: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<String>,
    hits_per_page: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    numeric_filters: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    distinct: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_values_per_facet: Option<u64>,
    pub page: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag_filters: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facets: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    facet_filters: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    faceting_after_distinct: Option<bool>,
}

impl ParamsBuilder {
    pub fn build(&mut self) -> String {
        serde_urlencoded::to_string(&self).expect("failed to serialize params")
    }

    pub fn query(&mut self, s: &'static str) -> &mut Self {
        self.query = Some(s);
        self
    }

    pub fn rule_contexts(&mut self, s: &'static str) -> &mut Self {
        self.rule_contexts = Some(s);
        self
    }

    pub fn filters(&mut self, s: &'static str) -> &mut Self {
        self.filters = Some(s.to_string());
        self
    }

    pub fn hits_per_page(&mut self, u: u64) -> &mut Self {
        self.hits_per_page = u;
        self
    }

    pub fn numeric_filters(&mut self, s: &'static str) -> &mut Self {
        self.numeric_filters = Some(s);
        self
    }

    pub fn distinct(&mut self, b: bool) -> &mut Self {
        self.distinct = Some(b);
        self
    }

    pub fn max_values_per_facet(&mut self, u: u64) -> &mut Self {
        self.max_values_per_facet = Some(u);
        self
    }

    pub fn page(&mut self, u: u64) -> &mut Self {
        self.page = u;
        self
    }

    pub fn _tag_filters(&mut self, s: &'static str) -> &mut Self {
        self.tag_filters = Some(s);
        self
    }

    pub fn facets(&mut self, s: &'static str) -> &mut Self {
        self.facets = Some(s);
        self
    }

    pub fn facet_filters(&mut self, s: &'static str) -> &mut Self {
        self.facet_filters = Some(s);
        self
    }

    pub fn faceting_after_distinct(&mut self, b: bool) -> &mut Self {
        self.faceting_after_distinct = Some(b);
        self
    }

    pub fn fanatical_date_filter(&mut self) -> &mut Self {
        let now = Local::now();
        let end = now.timestamp() as u32;
        let adjusted = Local.with_ymd_and_hms(now.year(), now.month() - 3, now.day(), 0, 0, 0);
        let start = adjusted
            .single()
            .expect("time zone mapping error")
            .timestamp() as u32;

        let formatted = format!(
            "release_date > {} AND release_date < {} AND (display_type:game OR display_type:dlc)",
            start, end
        );
        self.filters = Some(formatted);
        self
    }
}

#[derive(Debug, Serialize)]
pub struct AlgoliaMultiRequest {
    pub requests: Vec<AlgoliaQuery>,
}

#[derive(Serialize)]
pub struct AlgoliaRequest {
    pub params: String,
}

#[derive(Debug, Serialize)]
pub struct AlgoliaQuery {
    #[serde(rename = "indexName")]
    pub index_name: &'static str,
    pub params: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AlgoliaMultiResponse<T> {
    pub results: Vec<AlgoliaResult<T>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AlgoliaResult<T> {
    pub hits: Vec<T>,
    #[serde(alias = "nbHits")]
    pub nb_hits: u64,
    pub page: u64,
    #[serde(alias = "nbPages")]
    pub nb_pages: u64,
    #[serde(alias = "hitsPerPage")]
    pub hits_per_page: u64,
}

pub struct Downloader<'a> {
    pub output_path: PathBuf,
    pub headers: HeaderMap,
    pub url: &'static str,
    pub client: &'a Client,
    pub price_filters: &'a [&'static str],
    pub params_games: ParamsBuilder,
    pub params_dlc: ParamsBuilder,
    pub algolia_index_name: &'static str,
}

impl<'a> Downloader<'a> {
    pub async fn download<T: Serialize + for<'b> Deserialize<'b>>(&mut self) -> Result<(), Error> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.output_path)
            .await
            .expect("failed to open output file");
        let mut writer = BufWriter::new(file);

        for &filter_str in self.price_filters {
            info!("fetching for {}", filter_str);
            self.params_games.numeric_filters(filter_str);
            self.params_games.page(0);

            self.params_dlc.numeric_filters(filter_str);
            self.params_dlc.page(0);

            let mut games_done = false;
            let mut dlc_done = false;

            loop {
                let params_g = self.params_games.build();
                let params_d = self.params_dlc.build();

                let games_query = AlgoliaQuery {
                    index_name: self.algolia_index_name,
                    params: params_g,
                };

                let dlc_query = AlgoliaQuery {
                    index_name: self.algolia_index_name,
                    params: params_d,
                };

                let queries = if !games_done && !dlc_done {
                    vec![games_query, dlc_query]
                } else if !games_done && dlc_done {
                    vec![games_query]
                } else if games_done && !dlc_done {
                    vec![dlc_query]
                } else {
                    break;
                };

                info!("fetching page {}", self.params_games.page);

                let body = AlgoliaMultiRequest { requests: queries };

                let req = self
                    .client
                    .request(Method::POST, self.url)
                    .headers(self.headers.clone())
                    .json(&body)
                    .build()
                    .expect("failed to build request");

                let resp = retry(&self.client, req).await?;

                let algolia = resp.json::<AlgoliaMultiResponse<T>>().await?;

                // NOTE: nb_hits == 0 doesn't work because nb_hits is the total
                if !algolia.results.iter().all(|x| x.hits.is_empty()) {
                    let ser = serde_json::to_string(&algolia)?;
                    writer.write_all(ser.as_bytes()).await?;
                    writer.write_u8(b'\n').await?;
                }

                if algolia.results.len() == 2 {
                    // results are returned in the same order as the requests (game 0, dlc 1)
                    info!(
                        "{} games, {} dlc",
                        algolia.results[0].nb_hits, algolia.results[1].nb_hits
                    );

                    if algolia.results[0].page == algolia.results[0].nb_pages
                        || algolia.results[0].hits.is_empty()
                    {
                        info!("no more games");
                        games_done = true;
                    }
                    if algolia.results[1].page == algolia.results[0].nb_pages
                        || algolia.results[1].hits.is_empty()
                    {
                        info!("no more dlc");
                        dlc_done = true;
                    }
                } else if algolia.results.len() == 1 {
                    if algolia.results[0].page == algolia.results[0].nb_pages
                        || algolia.results[0].hits.is_empty()
                    {
                        info!("single result set is empty, done");
                        break;
                    }
                } else {
                    break;
                }

                if games_done && dlc_done {
                    break;
                }

                self.params_games.page += 1;
                self.params_dlc.page += 1;
                sleep(Duration::from_millis(1500)).await;
            }
        }

        writer.flush().await?;

        Ok(())
    }
}

pub struct DownloaderSingleIndex<'a> {
    pub output_path: PathBuf,
    pub headers: HeaderMap,
    pub url: &'static str,
    pub client: &'a Client,
    pub params: ParamsBuilder,
}

impl<'a> DownloaderSingleIndex<'a> {
    pub async fn download<T: Serialize + std::fmt::Debug + for<'b> Deserialize<'b>>(
        &mut self,
    ) -> Result<(), Error> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.output_path)
            .await
            .expect("failed to open output file");
        let mut writer = BufWriter::new(file);
        self.params.page = 0;

        loop {
            info!("fetching page {}", self.params.page);

            let req = self
                .client
                .request(Method::POST, self.url)
                .headers(self.headers.clone())
                .json(&self.params)
                .build()
                .expect("failed to build request");

            let resp = retry(&self.client, req).await?;
            let algolia = resp.json::<AlgoliaResult<T>>().await?;

            let ser = serde_json::to_string(&algolia)?;
            writer.write_all(ser.as_bytes()).await?;
            writer.write_u8(b'\n').await?;

            if algolia.page == algolia.nb_pages || algolia.hits.is_empty() {
                break;
            }

            self.params.page += 1;
            sleep(Duration::from_millis(1500)).await;
        }

        writer.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn date_range() {
        let mut params = ParamsBuilder::default();

        params.fanatical_date_filter();
        params.page(0);
        params.hits_per_page(36);

        println!("{:?}", params.filters);
        let serialized = serde_json::to_string(&params);
        assert!(serialized.is_ok());

        let serialized = serialized.unwrap();
        println!("{:?}", serialized);
    }
}
