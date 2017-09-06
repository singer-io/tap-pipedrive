#!/usr/bin/env python3

import singer
from tap_pipedrive.tap import PipedriveTap


def main():
    args = singer.utils.parse_args(['api_token'])
    pipedrive_tap = PipedriveTap(args.config, args.state)
    pipedrive_tap.do_sync()


if __name__ == '__main__':
    main()
