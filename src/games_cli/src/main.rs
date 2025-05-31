use std::{process::Stdio, time::Duration};

use anyhow::Error;
use clap::{Parser, Subcommand};
use env_logger::Builder;
use log::{LevelFilter, info};
use reqwest::Client;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process, time,
};

use games_core::{
    fanatical::Fanatical,
    gamebillet::Gamebillet,
    gmg::Gmg,
    gog::Gog,
    indiegala::{self},
    steam::Steam,
    wgs::Wgs,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    Builder::new().filter_level(LevelFilter::Info).init();

    dotenvy::dotenv()?;

    let args = Args::parse();

    match &args.command {
        Commands::Steam { action } => {
            let steam = Steam::default();
            match action {
                SteamCmd::Apps => {
                    steam.fetch_appids().await?;
                }
                SteamCmd::AppInfo => {
                    steam.fetch_appinfo().await?;
                }
                SteamCmd::Bundles => {
                    steam.bundles().await?;
                }
                SteamCmd::Charts => {
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
                SteamCmd::ComingSoon => {
                    steam.coming_soon().await?;
                }
                SteamCmd::Ids => {
                    steam.tags().await?;
                    steam.categories().await?;
                }
                SteamCmd::MostWishlisted => {
                    steam.most_wishlisted().await?;
                }
                SteamCmd::News => {
                    steam.news(1604030).await?;
                }
                SteamCmd::OnSale => {
                    steam.on_sale().await?;
                }
                SteamCmd::Wishlist => {
                    steam.wishlist(false).await?;
                }
                SteamCmd::WishlistOnSale => {
                    steam.wishlist(true).await?;
                }
            }
        }
        Commands::Fanatical { action } => {
            let fanatical = Fanatical::default();
            match action {
                FanaticalCmd::Bundles => {
                    fanatical.bundles().await?;
                }
                FanaticalCmd::OnSale => {
                    fanatical.on_sale().await?;
                }
                FanaticalCmd::Sitemaps => {
                    fanatical.sitemaps().await?;
                }
            }
        }
        Commands::Indiegala { action } => match action {
            IndiegalaCmd::All => {
                run_python(Retailer::IndiegalaAll).await?;
            }
            IndiegalaCmd::OnSale => {
                run_python(Retailer::IndiegalaSale).await?;
            }
            IndiegalaCmd::Bundles => {
                run_python(Retailer::IndiegalaBundles).await?;
            }
            IndiegalaCmd::Parse => {
                indiegala::parse_files().await?;
            }
        },
        Commands::Wingamestore { action } => {
            let wgs = Wgs::default();
            match action {
                WingamestoreCmd::OnSale => {
                    run_with_chromedriver(|| wgs.on_sale()).await?;
                }
            }
        }
        Commands::Gog { action } => {
            let gog = Gog::default();
            match action {
                GogCmd::OnSale => {
                    run_with_chromedriver(|| gog.on_sale()).await?;
                }
            }
        }
        Commands::Gamebillet { action } => match action {
            GamebilletCmd::OnSale => {
                run_python(Retailer::Gamebillet).await?;
            }
            GamebilletCmd::ParseSitemap => {
                Gamebillet::parse_sitemap().await?;
            }
        },
        Commands::Gmg { action } => {
            let gmg = Gmg::default();

            match action {
                GmgCmd::OnSale => {
                    gmg.on_sale().await?;
                }
            }
        }
        Commands::Gamesplanet { action } => match action {
            GamesplanetCmd::All => {
                run_python(Retailer::Gamesplanet).await?;
            }
        },
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
        tokio::time::sleep(Duration::from_millis(200)).await;
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

enum Retailer {
    Gamebillet,
    Gamesplanet,
    IndiegalaSale,
    IndiegalaAll,
    IndiegalaBundles,
}

impl Retailer {
    const fn args(&self) -> [&'static str; 2] {
        match self {
            Self::Gamebillet => ["gamebillet", "--steam"],
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
    /// fetch data via undocumented APIs
    Steam {
        #[command(subcommand)]
        action: SteamCmd,
    },
    /// fetch data via scraping with thirtyfour
    Fanatical {
        #[command(subcommand)]
        action: FanaticalCmd,
    },
    /// fetch data via RSS feeds
    Indiegala {
        #[command(subcommand)]
        action: IndiegalaCmd,
    },
    /// fetch data via scraping with thirtyfour
    Wingamestore {
        #[command(subcommand)]
        action: WingamestoreCmd,
    },
    /// fetch data via scraping with thirtyfour
    Gog {
        #[command(subcommand)]
        action: GogCmd,
    },
    Gamebillet {
        #[command(subcommand)]
        action: GamebilletCmd,
    },
    Gmg {
        #[command(subcommand)]
        action: GmgCmd,
    },
    Gamesplanet {
        #[command(subcommand)]
        action: GamesplanetCmd,
    },
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
    News,
    /// download info on all discounted games
    OnSale,
    /// download info of all games on wishlist
    Wishlist,
    /// download info of discounted games on wishlist
    WishlistOnSale,
}

#[derive(Subcommand, Debug)]
#[group(multiple = false)]
enum FanaticalCmd {
    /// download and process all sitemaps
    Sitemaps,
    /// fetch prices for all games on sale
    OnSale,
    /// fetch prices and info for all bundles
    Bundles,
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

#[derive(Subcommand)]
enum WingamestoreCmd {
    /// fetch prices for all games on sale
    OnSale,
}

#[derive(Subcommand)]
enum GogCmd {
    /// fetch prices for all games on sale
    OnSale,
}

#[derive(Subcommand)]
enum GamebilletCmd {
    /// fetch prices for all games on sale
    OnSale,
    ParseSitemap,
}

#[derive(Subcommand)]
enum GmgCmd {
    /// fetch prices for all games on sale
    OnSale,
}

#[derive(Subcommand)]
enum GamesplanetCmd {
    /// fetch prices for all steam drm games
    All,
}
