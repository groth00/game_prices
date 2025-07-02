use std::{process::Stdio, time::Duration};

use anyhow::Error;
use clap::{Parser, Subcommand};
use env_logger::Builder;
use log::{LevelFilter, error, info};
use reqwest::Client;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    join, process,
    task::JoinError,
    time,
};

use games_core::{
    fanatical::Fanatical,
    gamebillet::Gamebillet,
    gmg::{self, Gmg},
    gog::{self, Gog},
    indiegala::{self},
    steam::Steam,
    wgs::{self, Wgs},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    Builder::new().filter_level(LevelFilter::Info).init();

    dotenvy::dotenv()?;

    let args = Args::parse();

    match &args.command {
        Commands::Fanatical { action } => {
            let fanatical = Fanatical::default();

            use FanaticalCmd::*;
            match action {
                Bundles => fanatical.bundles().await?,
                OnSale => fanatical.download(true).await?,
                All => fanatical.download(false).await?,
                New => fanatical.new_releases().await?,
                Sitemaps => fanatical.sitemaps().await?,
            }
        }
        Commands::Gamebillet { action } => {
            use GamebilletCmd::*;
            match action {
                OnSale => run_python(Retailer::GamebilletSale).await?,
                All => run_python(Retailer::GamebilletAll).await?,
                Sitemap => run_python(Retailer::GamebilletSitemap).await?,
                ParseSitemap => Gamebillet::parse_sitemap().await?,
            }
        }
        Commands::Gamesplanet { action } => {
            use GamesplanetCmd::*;

            match action {
                All => run_python(Retailer::Gamesplanet).await?,
            }
        }
        Commands::Gmg { action } => {
            let gmg = Gmg::default();

            use GmgCmd::*;
            match action {
                OnSale => gmg.download(gmg::DownloadKind::OnSale).await?,
                All => gmg.download(gmg::DownloadKind::All).await?,
                New => gmg.download(gmg::DownloadKind::New).await?,
            }
        }
        Commands::Gog { action } => {
            let gog = Gog::default();

            use GogCmd::*;
            match action {
                OnSale => {
                    gog.download(gog::ProductType::All, gog::DownloadKind::Discounted)
                        .await?
                }
                All => {
                    gog.download(gog::ProductType::GamePack, gog::DownloadKind::NotDiscounted)
                        .await?;
                    gog.download(
                        gog::ProductType::DlcExtras,
                        gog::DownloadKind::NotDiscounted,
                    )
                    .await?;
                }
                New => {
                    gog.download(gog::ProductType::All, gog::DownloadKind::New)
                        .await?
                }
            }
        }
        Commands::Indiegala { action } => {
            use IndiegalaCmd::*;

            match action {
                All => run_python(Retailer::IndiegalaAll).await?,
                OnSale => run_python(Retailer::IndiegalaSale).await?,
                Bundles => run_python(Retailer::IndiegalaBundles).await?,
                Parse => indiegala::parse_files().await?,
            }
        }
        Commands::Steam { action } => {
            let steam = Steam::default();

            use SteamCmd::*;
            match action {
                Apps => steam.fetch_appids().await?,
                AppInfo => steam.fetch_appinfo().await?,
                Bundles => steam.bundles().await?,
                Charts => {
                    info!("STEAM: fetching top_releases");
                    steam.top_releases().await?;
                    time::sleep(Duration::from_secs(1)).await;

                    info!("STEAM: fetching most_concurrent");
                    steam.most_concurrent().await?;
                    time::sleep(Duration::from_secs(1)).await;

                    info!("STEAM: fetching month_top");
                    steam.month_top().await?;
                    time::sleep(Duration::from_secs(1)).await;

                    info!("STEAM: fetching most_played");
                    steam.most_played().await?;
                    time::sleep(Duration::from_secs(1)).await;

                    info!("STEAM: fetching most_played_deck");
                    steam.most_played_deck().await?;
                    time::sleep(Duration::from_secs(1)).await;

                    info!("STEAM: fetching weekly_top");
                    steam.weekly_top(None).await?;
                }
                ComingSoon => steam.coming_soon().await?,
                Ids => {
                    steam.tags().await?;
                    steam.categories().await?;
                }
                MostWishlisted => steam.most_wishlisted().await?,
                News { appid } => steam.news(*appid).await?,
                OnSale => steam.on_sale().await?,
                Wishlist => steam.wishlist(false).await?,
                WishlistOnSale => steam.wishlist(true).await?,
            }
        }
        Commands::Wingamestore { action } => {
            let wgs = Wgs::default();

            use WingamestoreCmd::*;
            match action {
                OnSale => run_with_chromedriver(|| wgs.download(wgs::DownloadKind::OnSale)).await?,
                All => run_with_chromedriver(|| wgs.download(wgs::DownloadKind::All)).await?,
                New => run_with_chromedriver(|| wgs.download(wgs::DownloadKind::New)).await?,
            }
        }
        Commands::Api { action } => {
            let fanatical = Fanatical::default();
            let gmg = Gmg::default();
            let gog = Gog::default();
            let steam = Steam::default();

            use ApiCmd::*;

            match action {
                OnSale => {
                    let h1 = tokio::spawn(async move { fanatical.download(true).await });
                    let h2 =
                        tokio::spawn(async move { gmg.download(gmg::DownloadKind::OnSale).await });
                    let h3 = tokio::spawn(async move {
                        gog.download(gog::ProductType::All, gog::DownloadKind::Discounted)
                            .await
                    });
                    let h4 = tokio::spawn(async move { steam.on_sale().await });
                    let (res1, res2, res3, res4) = join!(h1, h2, h3, h4);
                    task_status(res1, res2, res3, res4).await;
                }
            }
        }
        Commands::Browser { action } => {
            let wgs = Wgs::default();

            use BrowserCmd::*;
            match action {
                OnSale => {
                    let h1 =
                        tokio::spawn(async move { run_python(Retailer::GamebilletSale).await });
                    let h2 = tokio::spawn(async move { run_python(Retailer::Gamesplanet).await });
                    let h3 = tokio::spawn(async move { run_python(Retailer::IndiegalaSale).await });
                    let h4 =
                        tokio::spawn(async move { wgs.download(wgs::DownloadKind::OnSale).await });
                    let (res1, res2, res3, res4) = join!(h1, h2, h3, h4);
                    task_status(res1, res2, res3, res4).await;
                }
            }
        }
    }

    Ok(())
}

async fn run_with_chromedriver<Fn, Fut>(f: Fn) -> Result<(), Error>
where
    Fn: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let mut cmd = process::Command::new("chromedriver");
    cmd.arg("--port=9515");
    let mut child = cmd.spawn()?;

    let client = Client::new();
    wait_for_chromedriver(&client).await?;

    f().await?;
    child.kill().await?;

    Ok(())
}

async fn wait_for_chromedriver(client: &Client) -> Result<(), Error> {
    let url = "http://localhost:9515/status";

    let max_attempts = 10;
    let mut attempts = 0;

    while attempts < max_attempts {
        match client.get(url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(());
                }
            }
            Err(_) => (),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        attempts += 1;
    }

    anyhow::bail!("chromedriver is not responding")
}

async fn run_python(retailer: Retailer) -> Result<(), Error> {
    let args = retailer.args();
    let mut cmd = process::Command::new("uv");
    cmd.args(&["run", "sbase/main.py", args[0], args[1]]);
    cmd.stdout(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn command");
    let stdout = child.stdout.take().expect("failed to take child stdout");
    let mut reader = BufReader::new(stdout).lines();

    tokio::spawn(async move {
        let status = child
            .wait()
            .await
            .expect("child process encountered an error");
        info!("{}", status);
    });

    while let Some(line) = reader.next_line().await? {
        info!("{}", line);
    }

    Ok(())
}

type TaskResult = Result<Result<(), Error>, JoinError>;
async fn task_status(res1: TaskResult, res2: TaskResult, res3: TaskResult, res4: TaskResult) {
    let mut errors = vec![];

    if let Err(e) = res1 {
        errors.push(e);
    }
    if let Err(e) = res2 {
        errors.push(e);
    }
    if let Err(e) = res3 {
        errors.push(e);
    }
    if let Err(e) = res4 {
        errors.push(e);
    }
    if !errors.is_empty() {
        error!("{:?}", errors);
    }
    info!("all good");
}

enum Retailer {
    GamebilletSale,
    GamebilletAll,
    GamebilletSitemap,
    Gamesplanet,
    IndiegalaSale,
    IndiegalaAll,
    IndiegalaBundles,
}

impl Retailer {
    const fn args(&self) -> [&'static str; 2] {
        match self {
            Self::GamebilletSale => ["gamebillet", "--sale"],
            Self::GamebilletAll => ["gamebillet", "--all"],
            Self::GamebilletSitemap => ["gamebillet", "--sitemap"],
            Self::Gamesplanet => ["gamesplanet", "--steam"],
            Self::IndiegalaSale => ["indiegala", "--sale"],
            Self::IndiegalaAll => ["indiegala", "--all"],
            Self::IndiegalaBundles => ["indiegala", "--bundles"],
        }
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Fanatical {
        #[command(subcommand)]
        action: FanaticalCmd,
    },
    Gamebillet {
        #[command(subcommand)]
        action: GamebilletCmd,
    },
    Gamesplanet {
        #[command(subcommand)]
        action: GamesplanetCmd,
    },
    Gmg {
        #[command(subcommand)]
        action: GmgCmd,
    },
    Gog {
        #[command(subcommand)]
        action: GogCmd,
    },
    /// fetch data via RSS feeds
    Indiegala {
        #[command(subcommand)]
        action: IndiegalaCmd,
    },
    Steam {
        #[command(subcommand)]
        action: SteamCmd,
    },
    Wingamestore {
        #[command(subcommand)]
        action: WingamestoreCmd,
    },
    /// fetch data from stores that use APIs
    Api {
        #[command(subcommand)]
        action: ApiCmd,
    },
    /// fetch data from stores that require a browser
    Browser {
        #[command(subcommand)]
        action: BrowserCmd,
    },
}

#[derive(Subcommand, Debug)]
#[group(multiple = false)]
enum FanaticalCmd {
    /// download and process all sitemaps
    Sitemaps,
    /// fetch prices for all games on sale
    OnSale,
    /// fetch prices for all games
    All,
    /// fetch prices and info for all bundles
    Bundles,
    /// get new titles since previous run of New/All
    New,
}

#[derive(Subcommand)]
enum GamebilletCmd {
    /// fetch from /hotdeals
    OnSale,
    /// fetch from /allproducts
    All,
    /// fetch sitemap.xml
    Sitemap,
    /// todo
    ParseSitemap,
}

#[derive(Subcommand)]
enum GamesplanetCmd {
    /// fetch prices for all steam drm games
    All,
}

#[derive(Subcommand)]
enum GmgCmd {
    /// fetch prices for all games on sale
    OnSale,
    /// fetch prices for all games
    All,
    /// fetch info on new releases
    New,
}

#[derive(Subcommand)]
enum GogCmd {
    /// fetch prices for all games on sale
    OnSale,
    /// fetches all games by iterating over game, pack, dlc, extras
    All,
    /// fetches info on new games
    New,
}

#[derive(Subcommand)]
enum IndiegalaCmd {
    /// fetch XML for all games on sale
    OnSale,
    /// fetch XML for all games
    All,
    /// fetch bundles
    Bundles,
    /// parse XML files
    Parse,
}

#[derive(Subcommand, Debug)]
#[group(multiple = false)]
enum SteamCmd {
    /// download list of steam appids or update since last run
    Apps,
    /// download store metadata for each app
    AppInfo,
    /// download bundle info
    Bundles,
    /// download info on popular games
    Charts,
    /// download info of upcoming games
    ComingSoon,
    /// download categories and tag mappings
    Ids,
    /// download info of most wishlisted upcoming games
    MostWishlisted,
    /// download news for an app
    News { appid: u64 },
    /// download info on all discounted games
    OnSale,
    /// download info of all games on wishlist
    Wishlist,
    /// download info of discounted games on wishlist
    WishlistOnSale,
}

#[derive(Subcommand)]
enum WingamestoreCmd {
    /// fetch prices for all games on sale
    OnSale,
    /// fetch prices for all games
    All,
    /// fetch info on new releases
    New,
}

#[derive(Subcommand)]
enum ApiCmd {
    OnSale,
}

#[derive(Subcommand)]
enum BrowserCmd {
    OnSale,
}
