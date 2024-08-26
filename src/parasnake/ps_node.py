# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines the node class that does the computation (number crunching).
"""



import asyncio
from typing import Any
import logging
logger = logging.getLogger(__name__)

from parasnake.ps_config import PSConfiguration
from parasnake.ps_nodeid import PSNodeId
import parasnake.ps_message

class PSNode:
    def __init__(self, configuration: PSConfiguration):
        self.server_address = configuration.server_address
        self.server_port = configuration.server_port
        self.secret_key = configuration.secret_key
        self.heartbeat_timeout = configuration.heartbeat_timeout
        self.node_id = PSNodeId()

    def ps_run(self):
        asyncio.run(self.ps_start_tasks())

    async def ps_start_tasks(self):
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(self.ps_main_loop())
            task2 = tg.create_task(self.ps_send_heartbeat())

    async def ps_main_loop(self):
        reader, writer = await asyncio.open_connection(self.server_address, self.server_port)

        # Send init message to server...
        writer.write(ps_message.ps_gen_init_message(self.node_id, self.secret_key))
        await writer.drain()

        while True:
            data = await reader.read()
            msg = ps_message.decode_message(data, self.secret_key)

            match msg:
                case (ps_message.INIT_OK, data):
                    logger.debug("Init node OK.")
                    self.ps_init(data)
                case ps_message.INIT_ERROR:
                    logger.error("Init node failed.")
                    break
                case (ps_message.NEW_DATA_FROM_SERVER, new_data):
                    logger.debug("Received new data from server.")
                    new_result = self.ps_process_data(new_data)
                    writer.write(ps_message.ps_gen_result_message(self.node_id, self.secret_key, new_result))
                    await writer.drain()
                case ps_message.QUIT:
                    logger.debug("Job finished, quit.")
                    break
                case _:
                    logger.error("Received unknown message from server...")
                    break

            writer.write(ps_message.ps_gen_need_more_data_message(self.node_id, self.secret_key))
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def ps_send_heartbeat(self):
        reader, writer = await asyncio.open_connection(self.server_address, self.server_port)
        heartbeat_message = ps_message.ps_gen_heartbeat_message(self.node_id, self.secret_key)

        while True:
            await asyncio.sleep(self.heartbeat_timeout)

            writer.write(heartbeat_message)
            await writer.drain()

            data = await reader.read()
            msg = ps_message.decode_message(data, self.secret_key)

            match msg:
                case ps_message.HEARTBEAT_OK:
                    logger.debug("Heartbeat OK.")
                case ps_message.HEARTBEAT_ERROR:
                    logger.debug("Heartbeat Error.")
                    break
                case ps_message.QUIT:
                    logger.debug("Job finished, quit.")
                    break
                case _:
                    logger.error("Received unknown message from server...")
                    break

        writer.close()
        await writer.wait_closed()

    def ps_init(self, data: Any):
        pass

    def ps_process_data(self, data: Any) -> Any:
        pass


