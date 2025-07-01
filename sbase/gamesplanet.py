import json
import os
import random
import time

from seleniumbase import SB


class GamesplanetPrice:
    def __init__(self, name: str, original_price: float, discount: int, price: float):
        self.name = name
        self.original_price = original_price
        self.discount = discount
        self.price = price


class Gamesplanet:
    output_dir = "output/gamesplanet"
    url_old = "https://us.gamesplanet.com/games/offers/all"
    url = "https://us.gamesplanet.com/search?dt=steam&av=rel"

    total_pages = ".page-item:nth-last-child(2) a"

    css_item = "div[class~='no-gutters']"
    css_name = "h4 a"
    css_base_price = ".price_base strike"
    css_discount = ".price_saving"
    css_price = ".price_current"

    css_next_page = "a[class='page-link next-page']"

    def steam(self):
        os.makedirs(self.output_dir, exist_ok=True)
        now = time.clock_gettime(time.CLOCK_REALTIME)

        output_path = f"{self.output_dir}/on_sale_{now}.json"
        f = open(output_path, "w")

        with SB(
            uc=True,
            undetectable=True,
            ad_block_on=True,
            locale="en",
            disable_js=True,
            block_images=True,
        ) as sb:
            sb.activate_cdp_mode(self.url)
            sb.uc_gui_click_captcha()
            sb.cdp.sleep(2)

            total_pages = int(sb.cdp.select(self.total_pages).text)
            print(f"{total_pages = }")

            for i in range(1, total_pages + 1):
                start = time.perf_counter()
                print(f"on page {i}")

                output = []

                sb.cdp.sleep(1.1 + random.random())

                items = sb.cdp.select_all(self.css_item)
                for item in items:
                    # always has name and .price_current
                    name = item.query_selector(self.css_name).text

                    price_str = item.query_selector(self.css_price).text
                    price = float(price_str.replace("$", "").replace(",", ""))

                    info = GamesplanetPrice(
                        name=name, original_price=0.0, discount=0, price=0.0
                    )

                    # if an item is discounted, it will have .price_base strike (original), .price_saving, and .price_current (discounted)
                    # if not, it will only have .price_current, which is the ORIGINAL PRICE, NOT DISCOUNTED
                    if base_price_elem := item.query_selector(self.css_base_price):
                        info.original_price = float(
                            base_price_elem.text.replace("$", "")
                            .replace("%", "")
                            .replace(",", "")
                        )
                        discount_str = item.query_selector(self.css_discount).text
                        info.discount = int(
                            discount_str.replace("-", "")
                            .replace("%", "")
                            .replace(",", "")
                        )
                        info.price = price
                    else:
                        info.original_price = price
                        info.discount = 0
                        info.price = price

                    output.append(info.__dict__)

                end = time.perf_counter()
                duration = end - start
                print(f"{duration:.6f} seconds")

                json.dump(output, f)
                f.write("\n")

                # find method raises instead of returning None
                try:
                    if sb.cdp.find(self.css_next_page):
                        sb.cdp.click(self.css_next_page)
                except Exception:
                    break

            f.flush()
            f.close()
