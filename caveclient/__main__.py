import argparse

from caveclient.frameworkclient import CAVEclient


def main():
    """Main entry point for the CAVEclient CLI."""
    parser = argparse.ArgumentParser(description="CAVEclient command-line interface")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Setup token command
    setup_parser = subparsers.add_parser(
        "setup-token", help="Set up authentication token"
    )
    setup_parser.add_argument(
        "server_address", help="Server address for which to set up your token."
    )
    setup_parser.add_argument(
        "--overwrite",
        action="store_true",
        default=True,
        help="Overwrite existing token",
    )
    setup_parser.add_argument(
        "--no-open",
        action="store_false",
        dest="open",
        default=True,
        help="Do not open browser for token setup",
    )

    args = parser.parse_args()

    if args.command == "setup-token":
        CAVEclient.setup_token(
            server_address=args.server_address, overwrite=args.overwrite, open=args.open
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
