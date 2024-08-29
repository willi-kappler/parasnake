# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
# import copy
# import uuid

from parasnake.ps_nodeid import PSNodeId
from parasnake.ps_node import PSNode
from parasnake.ps_server import PSServer
from parasnake.ps_config import PSConfiguration


class TestNode(PSNode):
    def __init__(self, configuration: PSConfiguration):
        super().__init__(configuration)
        self.init_data: int = 0

    def ps_init(self, data: int):
        self.init_data = data

    def ps_process_data(self, data: int) -> int:
        return data + 1


class TestServer(PSServer):
    def __init__(self, configuration: PSConfiguration):
        super().__init__(configuration)
        self.job_data: dict[PSNodeId, int] = {}

    def ps_get_init_data(self, node_id: PSNodeId) -> int:
        return 10

    def ps_is_job_done(self) -> bool:
        return False

    def ps_save_data(self) -> None:
        pass

    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        pass

    def ps_get_new_data(self, node_id: PSNodeId) -> int:
        return 0

    def ps_process_result(self, node_id: PSNodeId, result: int):
        pass


class TestNodeId(unittest.TestCase):
    def gen_config(self) -> PSConfiguration:
        key = "test_key44444444ddddddddxxxxxxxx"
        config = PSConfiguration(key)

        config.heartbeat_timeout = 10
        config.quit_counter = 1

        return config

    def test_init(self):
        config = self.gen_config()

        node = TestNode(config)
        server = TestServer(config)


if __name__ == "__main__":
    unittest.main()

