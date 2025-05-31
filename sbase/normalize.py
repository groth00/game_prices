import json
import re
from functools import partial


# skip GOG because of separate DRM
def main():
    load_single_field_name = partial(load_single_field, name="name")
    load_single_field_title = partial(load_single_field, name="title")

    files = [
        # {"results": [{"hits": [{"name"}]}]}
        (
            "output/fanatical/on_sale.json",
            load_algolia,
        ),
        # name
        ("output/gamebillet/on_sale.json", load_single_field_name),
        # each line is [{}], name
        (
            "output/gamesplanet/on_sale.json",
            load_gamesplanet,
        ),
        # {"results": [{"hits": [{"name"}]}]}
        (
            "output/gmg/on_sale.json",
            load_algolia,
        ),
        ("output/gog/on_sale.json", load_single_field_name),
        # title
        ("output/indiegala/on_sale.json", load_single_field_title),
        # title
        ("output/wingamestore/on_sale.json", load_single_field_title),
    ]

    with open("temp/steam_names.txt") as f:
        steam = f.read()
    steam = json.loads(steam)
    steam = [normalize(x) for x in steam]
    steam_set = set(steam)
    print(f"{len(steam_set)}")

    for file, loader in files:
        print(file)
        names = loader(file)

        names_set = set(names)
        print(f"{len(names_set)}")
        diff = names_set - steam_set
        print(len(diff))

        if "fanatical" in file:
            for name in names_set:
                print(name)


def load_algolia(path: str):
    with open(path, "r") as f:
        lines = f.readlines()
    output = []
    for line in lines:
        temp = json.loads(line)
        for arr in temp["results"]:
            if arr["hits"]:
                for subarr in arr["hits"]:
                    output.append(normalize(subarr["name"]))
    return output


def load_gamesplanet(path: str):
    with open(path, "r") as f:
        lines = f.readlines()

    output = []
    for line in lines:
        line = json.loads(line)
        for obj in line:
            output.append(normalize(obj["name"]))
    return output


def load_single_field(path: str, name: str):
    with open(path, "r") as f:
        items = f.read()
    items = json.loads(items)
    return [normalize(x[name]) for x in items]


def normalize1(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", s)).strip().lower()


def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()


if __name__ == "__main__":
    main()
