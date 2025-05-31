import json
import re
from collections import defaultdict
from typing import Mapping, Optional, Sequence


class Item:
    def __init__(
        self,
        name: str,
        appid: int,
        developers: str,
        publishers: str,
        release_date: int,
        purchase_options: Sequence[Mapping],
    ) -> None:
        self.name = name
        self.appid = appid
        self.developers = developers
        self.publishers = publishers
        self.release_date = release_date
        self.purchase_options = purchase_options

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.appid,
                self.developers,
                self.publishers,
                self.release_date,
            )
        )

    def __repr__(self) -> str:
        fields = [f"{k}: {v}" for k, v in self.__dict__.items()]
        return "\n".join(fields)

    def __format__(self, format_spec: str) -> str:
        fields = [f"{k}: {v}" for k, v in self.__dict__.items()]
        return "\n".join(fields)


# jaq -rs '[.[].store_items[] | {name: .name?, appid: .appid?, developers: .basic_info?.developers[]?.name?, publishers: .basic_info?.publishers[]?.name?, release_date: .release?.steam_release_date?, purchase_options: (.purchase_options | map({purchase_option_name: .purchase_option_name, packageid: .packageid, bundleid: .bundleid}))}]' < output/steam/appinfo.jsonl > temp/steam_appinfo.json


def main():
    with open("output/steam/appinfo.jsonl", "r") as f:
        steam = json.load(f)

    for item in steam:
        item["name"] = re.sub(r"\s+", " ", item["name"]).strip()
        item["developers"] = re.sub(r"\s+", " ", item["developers"]).strip()
        item["publishers"] = re.sub(r"\s+", " ", item["publishers"]).strip()

    steam_items = [
        Item(
            x["name"],
            x["appid"],
            x["developers"],
            x["publishers"],
            x["release_date"],
            x["purchase_options"],
        )
        for x in steam
    ]
    print(len(steam_items))

    counts = defaultdict(set)
    for item in steam_items:
        counts[item.name].add(item.appid)
        for subitem in item.purchase_options:
            counts[subitem["purchase_option_name"]].add(item.appid)

    print(len(counts))

    reused = [k for k in counts.keys() if len(counts[k]) > 1]

    with open("output/fanatical/on_sale.json") as f:
        fanatical = json.load(f)


if __name__ == "__main__":
    main()
