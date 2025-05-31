import json
import os
import time
from seleniumbase import SB


class Gamebillet:
    output_dir = "output/gamebillet"

    url = "https://www.gamebillet.com/allproducts?drm=74"  # steam

    css_item = ".grid-item--card"

    css_name = "h3 a"
    css_price = ".buy-wrapper span"
    css_discount = ".buy-wrapper a"
    css_next_page = ".next-page"

    def steam(self):
        os.makedirs(self.output_dir, exist_ok=True)
        now = time.clock_gettime(time.CLOCK_REALTIME)

        output_path = f"{self.output_dir}/on_sale_{now}.json"
        f = open(output_path, "w")

        with SB(uc=True, undetectable=True, ad_block_on=True, locale="en") as sb:
            sb.activate_cdp_mode(self.url)
            sb.uc_gui_click_captcha()
            sb.sleep(2)

            count = 0

            while True:
                start = time.perf_counter()

                output = []

                count += 1
                print(f"on page {count}")
                sb.cdp.sleep(3)

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

                # this site doesn't disable the button if there's no next page
                button = sb.cdp.find(self.css_next_page)
                if button.get_attribute("href") == "javascript:void(0);":
                    break

                end = time.perf_counter()
                duration = end - start
                print(f"{duration:.6f} seconds")

                json.dump(output, f, indent=4)
                f.write("\n")

                sb.cdp.scroll_into_view(self.css_next_page)
                sb.cdp.click(self.css_next_page)

        f.flush()
        f.close()


class GamebilletPrice:
    def __init__(self, name: str, percent_discount: int, price: float):
        self.name = name
        self.percent_discount = percent_discount
        self.price = price
