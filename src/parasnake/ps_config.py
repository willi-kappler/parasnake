# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines the configuration for the server and the node.
"""


import json
import base64

import logging
logger = logging.getLogger(__name__)



class PSConfiguration:
    def __init__(self, secret_key: str):
        self.server_address: str = "127.0.0.1"
        self.server_port: int = 3100
        self.heartbeat_timeout: int = 60 * 5

        assert len(secret_key) == 32, f"Key must be exactly 32 bytes long: {secret_key}"
        self.secret_key: bytes = base64.urlsafe_b64encode(secret_key.encode())

    @staticmethod
    def from_json(file_name):
        """
        Load the configuration (JSON format) from the given file name.

        :param file_name: File name of the configuration.
        """

        logger.debug(f"Load configuration from file: {file_name}.")

        with open(file_name, "r") as f:
            data = json.load(f)

        secret_key: str = data["secret_key"]

        config = PSConfiguration(secret_key)

        if "server_address" in data:
            config.server_address = data["server_address"]

        if "server_port" in data:
            config.server_port = data["server_port"]

        if "heartbeat_timeout" in data:
            config.heartbeat_timeout = data["heartbeat_timeout"]
            assert config.heartbeat_timeout > 9, f"Heartbeat timeout must be greater then 9 seconds: {config.heartbeat_timeout}"

        return config

