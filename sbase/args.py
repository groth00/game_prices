import argparse


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
    gamebillet.add_argument("--sale", action="store_true", help="get items on sale")
    gamebillet.set_defaults(func=do_gamebillet)

    gamesplanet = subparsers.add_parser("gamesplanet")
    gamesplanet.add_argument(
        "--steam", action="store_true", help="get all items with steam drm"
    )
    gamesplanet.set_defaults(func=do_gamesplanet)

    return parser.parse_args()


def do_indiegala(args: argparse.Namespace):
    print("did indiegala")
    if args.sale:
        print("--sale")
    elif args.all:
        print("--all")
    elif args.bundles:
        print("--bundles")
    else:
        print("nothing active")


def do_gamebillet(args: argparse.Namespace):
    print("did gamebillet")
    if args.sale:
        print("--sale")
    else:
        print("nothing")


def do_gamesplanet(args: argparse.Namespace):
    print("did gamesplanet")
    if args.steam:
        print("--steam")
    else:
        print("nothing")


if __name__ == "__main__":
    args = parse_args()
    args.func(args)
