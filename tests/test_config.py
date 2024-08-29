# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
import pathlib
import base64

from parasnake.ps_config import PSConfiguration


class TestConfiguration(unittest.TestCase):
    def test_init(self):
        """
        Test normal initialisazion.
        """

        c: PSConfiguration = PSConfiguration("11111111222222223333333344444444")
        del c

    def test_wrong_key_len(self):
        """
        Test various wrong key length.
        """

        with self.assertRaises(AssertionError):
            c1: PSConfiguration = PSConfiguration("")
            del c1

        with self.assertRaises(AssertionError):
            c2: PSConfiguration = PSConfiguration("1111111122222222333333334444444")
            del c2

        with self.assertRaises(AssertionError):
            c3: PSConfiguration = PSConfiguration("111111112222222233333333444444444")
            del c3

    def create_and_load_config(self, data: str) -> PSConfiguration:
        """
        Helper function to create a json file that is loaded by the configuration class.
        """

        config_file_name: str = "test_config1.json"

        with open(config_file_name, "w") as f:
            f.write(data)

        try:
            cfg: PSConfiguration = PSConfiguration.from_json(config_file_name)
        except Exception as e:
            p: pathlib.Path = pathlib.Path(config_file_name)
            p.unlink()
            raise e

        return cfg

    def test_load_json1(self):
        """
        Test loading valid json configuration values.
        """

        data = """
        {
            "server_address": "33.44.55.66",
            "server_port": 9999,
            "heartbeat_timeout": 123,
            "secret_key": "<key>",
            "quit_counter": 3
        }
        """

        secret_key = "aaaaaaaabbbbbbbbccccccccdddddddd"
        encoded_key = base64.urlsafe_b64encode(secret_key.encode())

        data = data.replace("<key>", secret_key)

        cfg: PSConfiguration = self.create_and_load_config(data)

        self.assertEqual(cfg.server_address, "33.44.55.66")
        self.assertEqual(cfg.server_port, 9999)
        self.assertEqual(cfg.heartbeat_timeout, 123)
        self.assertEqual(cfg.secret_key, encoded_key)
        self.assertEqual(cfg.quit_counter, 3)

    def test_load_json2(self):
        """
        Test wrong key length.
        """

        data = """
        {
            "server_address": "33.44.55.66",
            "server_port": 9999,
            "heartbeat_timeout": 10,
            "secret_key": "<key>",
            "quit_counter": 3
        }
        """

        with self.assertRaises(AssertionError):
            cfg: PSConfiguration = self.create_and_load_config(data)
            del cfg

    def test_load_json3(self):
        """
        Test wrong heartbeat timeout.
        """

        data = """
        {
            "server_address": "33.44.55.66",
            "server_port": 9999,
            "heartbeat_timeout": 1,
            "secret_key": "<key>",
            "quit_counter": 3
        }
        """

        secret_key = "aaaaaaaabbbbbbbbccccccccdddddddd"
        data = data.replace("<key>", secret_key)

        with self.assertRaises(AssertionError):
            cfg: PSConfiguration = self.create_and_load_config(data)
            del cfg

    def test_load_json4(self):
        """
        Test wrong quit counter.
        """

        data = """
        {
            "server_address": "33.44.55.66",
            "server_port": 9999,
            "heartbeat_timeout": 10,
            "secret_key": "<key>",
            "quit_counter": 0
        }
        """

        secret_key = "aaaaaaaabbbbbbbbccccccccdddddddd"
        data = data.replace("<key>", secret_key)

        with self.assertRaises(AssertionError):
            cfg: PSConfiguration = self.create_and_load_config(data)
            del cfg


if __name__ == "__main__":
    unittest.main()

