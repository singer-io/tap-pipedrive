#!/usr/bin/env python3

import singer
from tap_pipedrive.tap import PipedriveTap


logger = singer.get_logger()


def main_impl():
    args = singer.utils.parse_args(['api_token', 'start_date'])

    if args.properties:
        logger.error('Tap does not support --properties')
        exit(1)

    if args.discover:
        logger.error('Tap does not support -d/--discover')
        exit(1)

    pipedrive_tap = PipedriveTap(args.config, args.state)
    pipedrive_tap.do_sync()


def main():
    try:
        main_impl()
    except Exception as e:
        logger.critical(e)
        raise e


if __name__ == '__main__':
    main()
