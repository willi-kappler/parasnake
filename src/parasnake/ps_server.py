# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the server class that distributes the work load to each node.
"""

# Python std modules:
import time
import datetime
import asyncio
from typing import Any, Optional
import logging
import threading

# Local modules:
from parasnake.ps_config import PSConfiguration
from parasnake.ps_nodeid import PSNodeId
import parasnake.ps_message as psm

logger = logging.getLogger(__name__)


class PSServer:
    def __init__(self, configuration: PSConfiguration):
        self.server_port: int = configuration.server_port
        self.secret_key: bytes = configuration.secret_key
        self.heartbeat_timeout: int = configuration.heartbeat_timeout
        self.all_nodes: dict[PSNodeId, float] = {}
        self.quit: bool = False
        self.quit_counter: int = configuration.quit_counter
        self.lock: threading.Lock = threading.Lock()

    def ps_run(self) -> None:
        logger.info(f"Starting server with port: {self.server_port}.")
        logger.debug(f"Heartbeat timeout: {self.heartbeat_timeout}.")

        asyncio.run(self.ps_main_loop())

        logger.info("Saving data...")
        self.ps_save_data()
        logger.info("Will exit server now.")

    def ps_register_new_node(self, node_id: PSNodeId) -> None:
        logger.debug("Register new node.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    def ps_update_node_time(self, node_id: PSNodeId) -> None:
        logger.debug("Update node time.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    async def ps_write_msg(self, writer, msg) -> None:
        writer.write(msg)
        writer.write_eof()
        await writer.drain()

    async def ps_handle_node(self, reader, writer) -> None:
        logger.debug("Connection from node.")

        data = await reader.read()
        msg = psm.decode_message(data, self.secret_key)

        if self.quit:
            logger.debug("Send quit message, job is done.")
            await self.ps_write_msg(writer, psm.ps_gen_quit_message(self.secret_key))
        else:
            match msg:
                case (psm.PS_INIT_MESSAGE, node_id):
                    logger.debug("Init message.")
                    if node_id in self.all_nodes:
                        logger.error("Node id already registered: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))
                    else:
                        self.ps_register_new_node(node_id)
                        init_data = await self.ps_get_init_data_thread(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_ok(init_data, self.secret_key))
                case (psm.PS_HEARTBEAT_MESSAGE, node_id):
                    logger.debug("Heartbeat message.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_heartbeat_message_ok(self.secret_key))
                    else:
                        logger.error("Node id not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_heartbeat_message_error(self.secret_key))
                case (psm.PS_NODE_NEEDS_MORE_DATA, node_id):
                    logger.debug("Node needs more data.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        new_data = await self.ps_get_new_data_thread(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_new_data_message(new_data, self.secret_key))
                    else:
                        logger.error("Node id not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))
                case (psm.PS_NEW_RESULT_FROM_NODE, node_id, result):
                    logger.debug("New result from node.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        await self.ps_process_result_thread(node_id, result)
                        await self.ps_write_msg(writer, psm.ps_gen_result_ok_message(self.secret_key))
                    else:
                        logger.error("Node id not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))

        writer.close()
        await writer.wait_closed()

    async def ps_main_loop(self) -> None:
        logger.debug("Start main server task.")
        start_time = datetime.datetime.now()
        logger.info(f"Starting now: {start_time}")

        server = await asyncio.start_server(self.ps_handle_node, "0.0.0.0", self.server_port)

        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        logger.debug(f"Serving on {addrs}")

        while True:
            await asyncio.sleep(10)

            if self.quit:
                logger.debug(f"Quit counter: {self.quit_counter}")
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

        end_time = datetime.datetime.now()
        logger.info(f"Finished on: {end_time}")

        time_taken = end_time - start_time
        in_seconds = time_taken.total_seconds()
        in_minutes = in_seconds / 60.0
        in_hours = in_minutes / 60.0

        logger.info(f"Time taken: in seconds: {in_seconds}")
        logger.info(f"Time taken: in minutes: {in_minutes}")
        logger.info(f"Time taken: in hours: {in_hours}")

    def ps_check_heartbeat(self) -> None:
        logger.debug("Check heartbeat of all nodes.")

        now: float = time.time()

        for k, v in self.all_nodes.items():
            if int(now - v + 1.0) > self.heartbeat_timeout:
                self.ps_node_timeout(k)

    async def ps_get_init_data_thread(self, node_id: PSNodeId) -> Any:
        return await asyncio.to_thread(self.ps_get_init_data_lock, node_id)

    def ps_get_init_data_lock(self, node_id: PSNodeId) -> Any:
        with self.lock:
            return self.ps_get_init_data(node_id)

    def ps_get_init_data(self, node_id: PSNodeId) -> Any:
        # Must be implemented by the user.
        return None

    async def ps_get_new_data_thread(self, node_id: PSNodeId) -> Optional[Any]:
        return await asyncio.to_thread(self.ps_get_new_data_lock, node_id)

    def ps_get_new_data_lock(self, node_id: PSNodeId) -> Optional[Any]:
        with self.lock:
            return self.ps_get_new_data(node_id)

    async def ps_process_result_thread(self, node_id: PSNodeId, result: Any):
        await asyncio.to_thread(self.ps_process_result_lock, node_id, result)

    def ps_process_result_lock(self, node_id: PSNodeId, result: Any):
        with self.lock:
            self.ps_process_result(node_id, result)

    def ps_is_job_done(self) -> bool:
        # Must be implemented by the user.
        return False

    def ps_save_data(self) -> None:
        # Must be implemented by the user.
        pass

    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        # Must be implemented by the user.
        pass

    def ps_get_new_data(self, node_id: PSNodeId) -> Optional[Any]:
        # Must be implemented by the user.
        return None

    def ps_process_result(self, node_id: PSNodeId, result: Any):
        # Must be implemented by the user.
        pass


