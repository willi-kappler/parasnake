# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
from typing import Optional
from time import sleep
# import copy
# import uuid

from parasnake.ps_nodeid import PSNodeId
from parasnake.ps_node import PSNode
from parasnake.ps_server import PSServer
from parasnake.ps_config import PSConfiguration


class WorkData:
    def __init__(self):
        self.value: int = 0
        self.assigned: bool = False

class TestNode(PSNode):
    def __init__(self, configuration: PSConfiguration):
        super().__init__(configuration)
        self.init_data: int = 0

    def ps_init(self, data: int):
        self.init_data = data

    def ps_process_data(self, data: int) -> int:
        sleep(1.5)
        return data + 10


class TestServer(PSServer):
    def __init__(self, configuration: PSConfiguration):
        super().__init__(configuration)
        self.job_data: list[WorkData] = []
        self.active_nodes: dict[PSNodeId, int] = {}
        self.timeout_nodes: list[PSNodeId] = []

        for _ in range(100):
            self.job_data.append(WorkData())

    def ps_get_init_data(self, node_id: PSNodeId) -> int:
        return 10

    def ps_is_job_done(self) -> bool:
        for jd in self.job_data:
            if jd.value < 100:
                return False

        return True

    def ps_save_data(self) -> None:
        pass

    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        index = self.active_nodes[node_id]
        # Give other nodes a chance to finish this job:
        self.job_data[index].assigned = False

        self.timeout_nodes.append(node_id)

        # Node is no longer active.
        del self.active_nodes[node_id]

    def ps_get_new_data(self, node_id: PSNodeId) -> Optional[int]:
        if node_id in self.active_nodes:
            index = self.active_nodes[node_id]
            value = self.job_data[index].value
            if value < 100:
                return self.job_data[index].value

        for i, jd in enumerate(self.job_data):
            if not jd.assigned:
                self.active_nodes[node_id] = i
                jd.assigned = True
                return jd.value

        # No more data to distribute.
        return None

    def ps_process_result(self, node_id: PSNodeId, result: int):
        if node_id in self.active_nodes:
            index = self.active_nodes[node_id]
            self.job_data[index].value = result


class TestCommunication(unittest.TestCase):
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

