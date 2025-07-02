import json
import os
from typing import Sequence
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree
from seleniumbase import SB


class Game:
    def __init__(self, name: str, developer: str):
        self.name = name
        self.developer = developer


class Bundle:
    def __init__(
        self, price: float, name: str, games: Sequence[Game], active_until: str
    ):
        self.price = price
        self.name = name
        self.games = games
        self.active_until = active_until


class Indiegala:
    output_dir = "output/indiegala"

    url_on_sale = "https://indiegala.com/store_games_rss?sale=true"
    url_all = "https://indiegala.com/store_games_rss?sale=false"
    url_bundles = "https://indiegala.com/bundles"

    def on_sale(self):
        self.fetch(self.url_on_sale, "on_sale")

    def all(self):
        self.fetch(self.url_all, "all")

    def bundles(self):
        output_dir = "output/indiegala"
        os.makedirs(output_dir, exist_ok=True)

        bundles = []

        with SB(
            uc=True, undetectable=True, ad_block_on=True, block_images=True, locale="en"
        ) as sb:
            sb.activate_cdp_mode(self.url_bundles)
            sb.sleep(2)

            anchors = [
                urljoin("https://indiegala.com/bundles", e.get_attribute("href"))
                for e in sb.cdp.find_elements(".fit-click")
            ]
            names = [
                e.text
                for e in sb.cdp.find_all("div.container-item-figcaption-inner h3")
            ]
            expirations = [e.text for e in sb.cdp.find_all(".container-item-ends span")]
            print(anchors, names, expirations)

            for i, a in enumerate(anchors):
                sb.cdp.open(a)
                sb.cdp.sleep(2)

                bundle_price = float(
                    sb.cdp.select(".plt-color-main:first-child")
                    .text.replace("$", "")
                    .replace(",", "")
                )
                seen = set()
                games = []

                sb.cdp.scroll_into_view(".carousel-inner")
                sb.cdp.sleep(0.5)

                while True:
                    # site uses js to transition active element into view
                    name = sb.cdp.select(
                        "div.active h3.bundle-slider-game-info-title"
                    ).text
                    if name in seen:
                        break

                    developer = sb.cdp.select(
                        "div.active div.bundle-slider-game-info-pub-dev a"
                    ).text
                    games.append(Game(name, developer).__dict__)
                    seen.add(name)
                    print(games)

                    sb.cdp.click("#bundle-slider-carousel .carousel-control-next")
                    sb.cdp.sleep(1)

                bundles.append(
                    Bundle(bundle_price, names[i], games, expirations[i]).__dict__
                )
                print(bundles)

        filename = f"{output_dir}/bundles.json"
        with open(filename, "w") as f:
            json.dump(bundles, f, indent=4)

    def fetch(self, url: str, subdir: str):
        subdir_path = f"{self.output_dir}/{subdir}"
        os.makedirs(subdir_path, exist_ok=True)

        with SB(uc=True, undetectable=True, ad_block_on=True, locale="en") as sb:
            sb.activate_cdp_mode(url)
            sb.uc_gui_click_captcha()
            sb.sleep(2)

            page = 1
            current_page = 1
            total_pages = 0
            total_games = 0
            parser = etree.XMLParser(remove_blank_text=True)

            while True:
                print(f"fetching page {page}")

                page_source = sb.cdp.get_page_source()
                soup = BeautifulSoup(page_source, "lxml-xml")
                rss_tag = soup.find("rss")

                rss_xml = str(rss_tag)

                root = etree.fromstring(rss_xml.encode("utf-8"), parser)

                parent = root.find("channel")
                current_page = int(parent.find("currentPage").text)
                total_pages = int(parent.find("totalPages").text)
                total_games = int(parent.find("totalGames").text)
                print(f"{current_page}/{total_pages} ({total_games})")

                xml_bytes = etree.tostring(
                    root,
                    pretty_print=True,
                    encoding="utf-8",
                    xml_declaration=True,
                )
                output_path = f"{subdir_path}/{page}.xml"
                with open(output_path, "w") as f:
                    f.write(xml_bytes.decode("utf-8"))

                if current_page == total_pages:
                    break
                else:
                    sb.cdp.sleep(2)
                    page += 1
                    next_url = f"{url}&page={page}"
                    print("going to next page: {url}")
                    sb.cdp.open(next_url)
