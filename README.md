# Introduction

A game price scraper written in Rust and Python

- Fanatical
- Gamebillet
- Gamesplanet
- GOG
- Greenmangaming
- Indiegala
- Steam
- Wingamestore

# Requirements

Rust
- rustup

Python
- uv package manager
  - lxml
  - seleniumbase

chromedriver


# Notes

For steam, an API key is needed (set STEAM_API_KEY environment variable)  
Some API endpoints are public, some are undocumented (via SteamDB)  
The SteamDB protobuf files are available in [this repository](https://github.com/SteamDatabase/Protobufs/tree/master)  
