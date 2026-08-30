import runpy
import unittest
from unittest.mock import MagicMock, patch

import tap_pipedrive.cli as cli


class TestCli(unittest.TestCase):
    @patch("tap_pipedrive.cli.logger.info")
    @patch("tap_pipedrive.cli.json.dump")
    @patch("tap_pipedrive.cli.PipedriveTap")
    @patch("tap_pipedrive.cli.singer.utils.parse_args")
    def test_main_discover_mode(self, mock_parse_args, mock_tap_cls, mock_json_dump, mock_log_info):
        args = MagicMock()
        args.config = {"api_token": "token", "start_date": "2024-01-01T00:00:00Z"}
        args.state = {"bookmarks": {}}
        args.discover = True
        args.catalog = None
        mock_parse_args.return_value = args

        tap_instance = MagicMock()
        catalog_obj = MagicMock()
        catalog_obj.to_dict.return_value = {"streams": []}
        tap_instance.do_discover.return_value = catalog_obj
        mock_tap_cls.return_value = tap_instance

        cli.main.__wrapped__()

        mock_parse_args.assert_called_once_with(["api_token", "start_date"])
        mock_tap_cls.assert_called_once_with(args.config, args.state)
        tap_instance.do_discover.assert_called_once_with()
        mock_json_dump.assert_called_once()
        mock_log_info.assert_called_once_with("Finished discover")

    @patch("tap_pipedrive.cli.PipedriveTap")
    @patch("tap_pipedrive.cli.singer.utils.parse_args")
    def test_main_sync_mode_with_catalog(self, mock_parse_args, mock_tap_cls):
        args = MagicMock()
        args.config = {"api_token": "token", "start_date": "2024-01-01T00:00:00Z"}
        args.state = {"bookmarks": {}}
        args.discover = False
        args.catalog = {"streams": []}
        mock_parse_args.return_value = args

        tap_instance = MagicMock()
        mock_tap_cls.return_value = tap_instance

        cli.main.__wrapped__()

        tap_instance.do_discover.assert_not_called()
        tap_instance.do_sync.assert_called_once_with(args.catalog)

    @patch("tap_pipedrive.cli.PipedriveTap")
    @patch("tap_pipedrive.cli.singer.utils.parse_args")
    def test_main_sync_mode_without_catalog_discovers_first(self, mock_parse_args, mock_tap_cls):
        args = MagicMock()
        args.config = {"api_token": "token", "start_date": "2024-01-01T00:00:00Z"}
        args.state = {"bookmarks": {}}
        args.discover = False
        args.catalog = None
        mock_parse_args.return_value = args

        tap_instance = MagicMock()
        discovered_catalog = MagicMock()
        tap_instance.do_discover.return_value = discovered_catalog
        mock_tap_cls.return_value = tap_instance

        cli.main.__wrapped__()

        tap_instance.do_discover.assert_called_once_with()
        tap_instance.do_sync.assert_called_once_with(discovered_catalog)

    @patch("tap_pipedrive.tap.PipedriveTap")
    @patch("singer.utils.parse_args")
    def test_module_main_guard(self, mock_parse_args, mock_tap_cls):
        args = MagicMock()
        args.config = {"api_token": "token", "start_date": "2024-01-01T00:00:00Z"}
        args.state = {"bookmarks": {}}
        args.discover = False
        args.catalog = {"streams": []}
        mock_parse_args.return_value = args

        tap_instance = MagicMock()
        mock_tap_cls.return_value = tap_instance

        runpy.run_module("tap_pipedrive.cli", run_name="__main__")

        tap_instance.do_sync.assert_called_once_with(args.catalog)
