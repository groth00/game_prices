import argparse

from gamebillet import Gamebillet
from gamesplanet import Gamesplanet
from indiegala import Indiegala


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    indiegala = subparsers.add_parser("indiegala")
    indiegala.add_argument(
        "--sale", action="store_true", help="get xml file(s) for items on sale"
    )
    indiegala.add_argument(
        "--all", action="store_true", help="get xml files for all items on sale"
    )
    indiegala.add_argument("--bundles", action="store_true", help="get bundles")
    indiegala.set_defaults(func=do_indiegala)

    gamebillet = subparsers.add_parser("gamebillet")
    gamebillet.add_argument(
        "--all", action="store_true", help="get all steam items from /allproducts"
    )
    gamebillet.add_argument(
        "--sale", action="store_true", help="get all steam items from /hotdeals"
    )
    gamebillet.set_defaults(func=do_gamebillet)

    gamesplanet = subparsers.add_parser("gamesplanet")
    gamesplanet.add_argument(
        "--steam", action="store_true", help="get all items with steam drm"
    )
    gamesplanet.set_defaults(func=do_gamesplanet)

    return parser.parse_args()


def do_indiegala(args: argparse.Namespace):
    print("indiegala")

    indiegala = Indiegala()
    if args.sale:
        print("--sale")
        indiegala.on_sale()
    elif args.all:
        print("--all")
        indiegala.all()
    elif args.bundles:
        print("--bundles")
        indiegala.bundles()


def do_gamebillet(args: argparse.Namespace):
    print("gamebillet")

    gamebillet = Gamebillet()
    if args.sale:
        print("--sale")
        gamebillet.on_sale()
    if args.all:
        print("--all")
        gamebillet.all()


def do_gamesplanet(args: argparse.Namespace):
    print("gamesplanet")

    gamesplanet = Gamesplanet()
    if args.steam:
        print("--steam")
        gamesplanet.steam()


if __name__ == "__main__":
    args = parse_args()
    args.func(args)
