# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the server class that distributes the work load to each node.
"""

import time
import asyncio
from typing import Any
import logging
logger = logging.getLogger(__name__)


from parasnake.ps_config import PSConfiguration
from parasnake.ps_nodeid import PSNodeId
import parasnake.ps_message as ps_msg


class PDServer:
    def __init__(self, configuration: PSConfiguration):
        self.server_port: int = configuration.server_port
        self.secret_key: bytes = configuration.secret_key
        self.heartbeat_timeout: int = configuration.heartbeat_timeout
        self.all_nodes = {}
        self.quit: bool = False
        self.quit_counter: int = configuration.quit_counter

    def ps_run(self):
        logger.info("Starting server with port: {self.server_port}.")
        logger.debug(f"Heartbeat timeout: {self.heartbeat_timeout}.")

        asyncio.run(self.ps_main_loop())

        logger.info("Saving data...")
        self.ps_save_data()
        logger.info("Will exit server now.")

    def ps_register_new_node(self, node_id: PSNodeId):
        logger.debug("Register new node.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    def ps_update_node_time(self, node_id: PSNodeId):
        logger.debug("Update node time.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    async def ps_handle_node(self, reader, writer):
        logger.debug("Connection from node.")

        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor

        data = await reader.read()
        msg = ps_msg.decode_message(data, self.secret_key)

        if self.quit:
            writer.write(ps_msg.ps_gen_quit_message(self.secret_key))
            await writer.drain()
            return

        match msg:
            case (ps_msg.PS_INIT_MESSAGE, node_id):
                logger.debug("Init message.")
                if node_id in self.all_nodes:
                    logger.error("Node id already registered: {node_id}")
                    writer.write(ps_msg.ps_gen_init_message_error(self.secret_key))
                    await writer.drain()
                else:
                    self.ps_register_new_node(node_id)
                    init_data = self.ps_get_init_data()
                    writer.write(ps_msg.ps_gen_init_message_ok(init_data, self.secret_key))
                    await writer.drain()
            case (ps_msg.PS_HEARTBEAT_MESSAGE, node_id):
                logger.debug("Heartbeat message.")
                if node_id in self.all_nodes:
                    self.ps_update_node_time(node_id)
                    writer.write(ps_msg.ps_gen_heartbeat_message_ok(self.secret_key))
                    await writer.drain()
                else:
                    logger.error("Node id not registered yet: {node_id}")
                    writer.write(ps_msg.ps_gen_heartbeat_message_error(self.secret_key))
                    await writer.drain()
            case (ps_msg.PS_NODE_NEEDS_MORE_DATA, node_id):
                logger.debug("Node needs more data.")
                if node_id in self.all_nodes:
                    self.ps_update_node_time(node_id)
                    new_data = self.ps_get_new_data(node_id)
                    writer.write(ps_msg.ps_gen_new_data_message(new_data, self.secret_key))
                    await writer.drain()
                else:
                    logger.error("Node id note registered yet: {node_id}")
                    writer.write(ps_msg.ps_gen_init_message_error(self.secret_key))
                    await writer.drain()
            case (ps_msg.PS_NEW_RESULT_FROM_NODE, node_id, result):
                logger.debug("New result from node.")
                if node_id in self.all_nodes:
                    self.ps_update_node_time(node_id)
                    self.ps_process_result(node_id, result)
                    writer.write(ps_msg.ps_gen_result_ok_message(self.secret_key))
                    await writer.drain()
                else:
                    logger.error("Node id note registered yet: {node_id}")
                    writer.write(ps_msg.ps_gen_init_message_error(self.secret_key))
                    await writer.drain()

    async def ps_main_loop(self):
        logger.debug("Start main task.")

        server = await asyncio.start_server(self.ps_handle_node, "0.0.0.0", self.server_port)

        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.debug(f"Serving on {addrs}")

        while True:
            await asyncio.sleep(10)

            if self.quit:
                self.quit_counter = self.quit_counter - 1
                if self.quit_counter == 0:
                    break
            else:
                if self.ps_is_job_done():
                    self.quit = True
                else:
                    self.ps_check_heartbeat()

        logger.debug("Closing server connections...")
        server.close()
        await server.wait_closed()

    def ps_check_heartbeat(self):
        logger.debug("Check heartbeat of all nodes.")

        now: float = time.time()

        for k, v in self.all_nodes.items():
            if int(now - v + 1.0) > self.heartbeat_timeout:
                self.ps_node_timeout(k)

    def ps_get_init_data(self) -> Any:
        # Must be implemented by the user.
        return None

    def ps_is_job_done(self) -> bool:
        # Must be implemented by the user.
        return False

    def ps_save_data(self):
        # Must be implemented by the user.
        pass

    def ps_node_timeout(self, node_id: PSNodeId):
        # Must be implemented by the user.
        pass

    def ps_get_new_data(self, node_id: PSNodeId) -> Any:
        # Must be implemented by the user.
        return None

    def ps_process_result(self, node_id: PSNodeId, result: Any):
        # Must be implemented by the user.
        pass


