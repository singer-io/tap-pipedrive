#!/usr/bin/env python3

import singer
from tap_pipedrive.tap import PipedriveTap


logger = singer.get_logger()


def main_impl():
    args = singer.utils.parse_args(['api_token', 'start_date'])

    pipedrive_tap = PipedriveTap(args.config, args.state, args.catalog)

    if args.discover:
        pipedrive_tap.do_discover()
    elif args.catalog:
        pipedrive_tap.do_sync()


def main():
    try:
        main_impl()
    except Exception as e:
        logger.critical(e)
        raise e


if __name__ == '__main__':
    main()
