from seleniumbase import SB

import json
import os
import random
import time


class Gamebillet:
    output_dir = "output/gamebillet"

    url = "https://www.gamebillet.com/allproducts?drm=74"  # steam
    url_on_sale = "https://www.gamebillet.com/hotdeals?drm=74"
    url_sitemap = "https://www.gamebillet.com/sitemap.xml"

    css_item = ".grid-item--card"

    css_name = "h3 a"
    css_price = ".buy-wrapper span"
    css_discount = ".buy-wrapper a"
    css_next_page = ".next-page"

    def all(self):
        self.download("all", self.url)

    def on_sale(self):
        self.download("on_sale", self.url_on_sale)

    def sitemap(self):
        now = time.clock_gettime(time.CLOCK_REALTIME)
        output_path = f"{self.output_dir}/sitemap_{now}.xml"

        with SB(uc=True, undetectable=True, locale="en") as sb:
            sb.activate_cdp_mode(self.url_sitemap)
            sb.sleep(2)
            page_source = sb.cdp.get_page_source()
            with open(output_path, "w") as f:
                f.write(page_source)

    def download(self, output_prefix: str, url: str):
        os.makedirs(self.output_dir, exist_ok=True)
        now = time.clock_gettime(time.CLOCK_REALTIME)

        output_path = f"{self.output_dir}/{output_prefix}_{now}.json"
        f = open(output_path, "w")

        with SB(
            uc=True,
            undetectable=True,
            ad_block_on=True,
            locale="en",
            disable_js=True,
            block_images=True,
        ) as sb:
            sb.activate_cdp_mode(url)
            sb.uc_gui_click_captcha()
            sb.sleep(2)

            count = 0
            while True:
                start = time.perf_counter()

                output = []

                count += 1
                print(f"on page {count}")
                sb.cdp.sleep(1.2 + random.random())

                items = sb.cdp.select_all(self.css_item)
                for item in items:
                    name = item.query_selector(self.css_name).text
                    discount_str = item.query_selector(self.css_discount).text
                    price_str = item.query_selector(self.css_price).text

                    # unreleased items have no discount/price
                    try:
                        discount = int(
                            discount_str.removeprefix("Sale")
                            .replace("-", "")
                            .replace("%", "")
                        )
                    except ValueError:
                        discount = 0

                    try:
                        price = float(price_str.replace("$", "").replace(",", ""))
                    except ValueError:
                        price = 0

                    output.append(GamebilletPrice(name, discount, price).__dict__)

                end = time.perf_counter()
                duration = end - start
                print(f"{duration:.6f} seconds")

                json.dump(output, f)
                f.write("\n")

                button = sb.cdp.find(self.css_next_page)
                if button.get_attribute("href") == "javascript:void(0);":
                    break
                else:
                    sb.cdp.scroll_into_view(self.css_next_page)
                    sb.cdp.click(self.css_next_page)

        f.flush()
        f.close()


class GamebilletPrice:
    def __init__(self, name: str, percent_discount: int, price: float):
        self.name = name
        self.percent_discount = percent_discount
        self.price = price
