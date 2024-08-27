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
import parasnake.ps_message as ps_msg

class PSNode:
    def __init__(self, configuration: PSConfiguration):
        self.server_address: str = configuration.server_address
        self.server_port: int = configuration.server_port
        self.secret_key: bytes = configuration.secret_key
        self.heartbeat_timeout: int = configuration.heartbeat_timeout
        self.node_id: PSNodeId = PSNodeId()

    def ps_run(self):
        logger.info("Starting node with node id: {self.node_id}.")
        logger.debug(f"Server address: {self.server_address}, port: {self.server_port}.")
        logger.debug(f"Heartbeat timeout: {self.heartbeat_timeout}")

        asyncio.run(self.ps_start_tasks())

        logger.info("Will exit node now.")

    async def ps_start_tasks(self):
        async with asyncio.TaskGroup() as tg:
            main_task = tg.create_task(self.ps_main_loop())
            heartbeat_task = tg.create_task(self.ps_send_heartbeat())

    async def ps_send_msg_return_answer(self, msg: bytes) -> Any:
        logger.debug("Send message to server.")
        reader, writer = await asyncio.open_connection(self.server_address, self.server_port)

        writer.write(msg)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        data = await reader.read()
        msg = ps_msg.decode_message(data, self.secret_key)
        logger.debug("Received message from server.")

        return msg

    async def ps_main_loop(self):
        logger.debug("Start main task.")
        need_more_data_message = ps_msg.ps_gen_need_more_data_message(self.node_id, self.secret_key)
        init_message = ps_msg.ps_gen_init_message(self.node_id, self.secret_key)

        msg = self.ps_send_msg_return_answer(init_message)

        match msg:
            case (ps_msg.PS_INIT_OK, data):
                logger.debug("Init node OK.")
                self.ps_init(data)
            case ps_msg.PS_INIT_ERROR:
                logger.error("Init node failed!")
                return
            case ps_msg.PS_QUIT:
                logger.debug("Job finished.")
                return
            case _:
                logger.error("Received unknown message from server!")
                return

        while True:
            msg = self.ps_send_msg_return_answer(need_more_data_message)

            match msg:
                case (ps_msg.PS_NEW_DATA_FROM_SERVER, new_data):
                    logger.debug("Received new data from server.")
                    new_result = self.ps_process_data(new_data)
                    logger.debug("New data has been processed.")
                    result_msg = ps_msg.ps_gen_result_message(self.node_id, self.secret_key, new_result)
                    msg = self.ps_send_msg_return_answer(result_msg)

                    match msg:
                        case ps_msg.PS_RESULT_OK:
                            logger.debug("New processed data has been sent to server.")
                        case ps_msg.PS_QUIT:
                            logger.debug("Job finished.")
                            break
                        case _:
                            logger.error("Received unknown message from server!")
                            break
                case ps_msg.PS_QUIT:
                    logger.debug("Job finished.")
                    break
                case _:
                    logger.error("Received unknown message from server!")
                    break

    async def ps_send_heartbeat(self):
        logger.debug("Start heartbeat task.")
        heartbeat_message = ps_msg.ps_gen_heartbeat_message(self.node_id, self.secret_key)

        while True:
            await asyncio.sleep(self.heartbeat_timeout)

            logger.debug("Send heartbeat message to server.")
            msg = self.ps_send_msg_return_answer(heartbeat_message)

            match msg:
                case ps_msg.PS_HEARTBEAT_OK:
                    logger.debug("Heartbeat OK.")
                case ps_msg.PS_HEARTBEAT_ERROR:
                    logger.debug("Heartbeat Error!")
                    break
                case ps_msg.PS_QUIT:
                    logger.debug("Job finished, quit.")
                    break
                case _:
                    logger.error("Received unknown message from server!")
                    break

    def ps_init(self, data: Any):
        # Must be implemented by the user.
        pass

    def ps_process_data(self, data: Any) -> Any:
        # Must be implemented by the user.
        pass


