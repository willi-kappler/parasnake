# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
import asyncio
import logging
import pathlib
import time
from typing import Optional, override
#from time import sleep
# import copy
# import uuid

from parasnake.ps_nodeid import PSNodeId
from parasnake.ps_node import PSNode
from parasnake.ps_server import PSServer
from parasnake.ps_config import PSConfiguration

logger = logging.getLogger(__name__)


class WorkData:
    def __init__(self):
        self.value: int = 0
        self.assigned: bool = False

class TestNode(PSNode):
    def __init__(self, configuration: PSConfiguration):
        super().__init__(configuration)
        self.init_data: int = 0

    @override
    def ps_init(self, data: int):
        self.init_data = data

    @override
    def ps_process_data(self, data: int) -> int:
        logger.debug(f"Process data: {data}")
        time.sleep(1.5)
        return data + 10


class TestServer(PSServer):
    def __init__(self, configuration: PSConfiguration):
        super().__init__(configuration)
        self.job_data: list[WorkData] = []
        self.active_nodes: dict[PSNodeId, int] = {}
        self.timeout_nodes: list[PSNodeId] = []
        self.max_value = 50

        for _ in range(10):
            self.job_data.append(WorkData())

    @override
    def ps_get_init_data(self, node_id: PSNodeId) -> int:
        return 10

    @override
    def ps_is_job_done(self) -> bool:
        for jd in self.job_data:
            if jd.value < self.max_value:
                return False

        return True

    @override
    def ps_save_data(self) -> None:
        pass

    @override
    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        logger.debug(f"Timeout for node: {node_id}")

        if node_id in self.active_nodes:
            index = self.active_nodes[node_id]
            # Give other nodes a chance to finish this job:
            self.job_data[index].assigned = False

            self.timeout_nodes.append(node_id)

            # Node is no longer active.
            del self.active_nodes[node_id]

    @override
    def ps_get_new_data(self, node_id: PSNodeId) -> Optional[int]:
        if node_id in self.active_nodes:
            index = self.active_nodes[node_id]
            value = self.job_data[index].value

            logger.debug(f"Active node {node_id} found with index: {index} and value: {value}")

            if value < self.max_value:
                return value

        for i, jd in enumerate(self.job_data):
            if not jd.assigned:
                logger.debug(f"New index assigned for node {node_id}: {i}, value: {jd.value}")

                self.active_nodes[node_id] = i
                jd.assigned = True
                return jd.value

        # No more data to distribute.
        return None

    @override
    def ps_process_result(self, node_id: PSNodeId, result: int):
        if node_id in self.active_nodes:
            logger.debug(f"Got result from active node: {node_id}, value: {result}")

            index = self.active_nodes[node_id]
            self.job_data[index].value = result


class TestCommunication(unittest.IsolatedAsyncioTestCase):
    def gen_config(self) -> PSConfiguration:
        key = "test_key44444444ddddddddxxxxxxxx"
        config = PSConfiguration(key)

        config.heartbeat_timeout = 10
        config.quit_counter = 4

        return config

    async def test_one_node(self):
        config = self.gen_config()
        logger.info("Start test case test_one_node")

        server = TestServer(config)
        node = TestNode(config)

        async with asyncio.TaskGroup() as tg:
            server_task = tg.create_task(server.ps_main_loop())
            server_task.set_name("ServerTask")

            # Give the server some time to start up.
            await asyncio.sleep(2.0)

            node_task = tg.create_task(node.ps_start_tasks())
            node_task.set_name("NodeTask")

        self.assertEqual(node.init_data, 10)

        self.assertEqual(len(server.timeout_nodes), 0)
        self.assertEqual(server.quit_counter, 0)

        for jd in server.job_data:
            self.assertTrue(jd.assigned)
            self.assertEqual(jd.value, server.max_value)

    async def test_two_nodes(self):
        config = self.gen_config()

        logger.info("Start test case test_two_nodes")

        server = TestServer(config)
        node1 = TestNode(config)
        node2 = TestNode(config)

        async with asyncio.TaskGroup() as tg:
            server_task = tg.create_task(server.ps_main_loop())
            server_task.set_name("ServerTask")

            # Give the server some time to start up.
            await asyncio.sleep(2.0)

            node_task1 = tg.create_task(node1.ps_start_tasks())
            node_task1.set_name("NodeTask1")

            await asyncio.sleep(1.0)

            node_task2 = tg.create_task(node2.ps_start_tasks())
            node_task2.set_name("NodeTask2")


        self.assertEqual(node1.init_data, 10)
        self.assertEqual(node2.init_data, 10)

        self.assertEqual(len(server.timeout_nodes), 0)
        self.assertEqual(server.quit_counter, 0)

        for jd in server.job_data:
            self.assertTrue(jd.assigned)
            self.assertEqual(jd.value, server.max_value)

    async def test_heartbeat_error(self):
        config = self.gen_config()

        logger.info("Start test case test_heartbeat_error")

        server = TestServer(config)
        node1 = TestNode(config)
        node2 = TestNode(config)

        async with asyncio.TaskGroup() as tg:
            server_task = tg.create_task(server.ps_main_loop())
            server_task.set_name("ServerTask")

            # Give the server some time to start up.
            await asyncio.sleep(2.0)

            node_task1 = tg.create_task(node1.ps_start_tasks())
            node_task1.set_name("NodeTask1")

            await asyncio.sleep(1.0)

            node_task2 = tg.create_task(node2.ps_start_tasks())
            node_task2.set_name("NodeTask2")

            await asyncio.sleep(2.0)

            # Now simulate a broken node:
            node_task2.cancel()

        self.assertEqual(node1.init_data, 10)
        self.assertEqual(node2.init_data, 10)

        self.assertEqual(len(server.timeout_nodes), 1)
        self.assertTrue(node2.node_id in server.timeout_nodes)
        self.assertEqual(server.quit_counter, 0)

        for jd in server.job_data:
            self.assertTrue(jd.assigned)
            self.assertEqual(jd.value, server.max_value)


if __name__ == "__main__":
    log_file_name: str = "communication.log"
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG, format=log_format)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    try:
        unittest.main()
    finally:
        p: pathlib.Path = pathlib.Path(log_file_name)
        p.unlink()

