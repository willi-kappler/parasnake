# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines the node class that does the computation (number crunching).
"""

# Python std modules:
import asyncio
from typing import Any
import logging

# Local modules:
from parasnake.ps_config import PSConfiguration
from parasnake.ps_nodeid import PSNodeId
import parasnake.ps_message as psm

logger = logging.getLogger(__name__)


class PSNode:
    def __init__(self, configuration: PSConfiguration):
        self.server_address: str = configuration.server_address
        self.server_port: int = configuration.server_port
        self.secret_key: bytes = configuration.secret_key
        self.heartbeat_timeout: int = configuration.heartbeat_timeout
        self.node_id: PSNodeId = PSNodeId()

    def ps_run(self) -> None:
        logger.info("Starting node with node id: {self.node_id}.")
        logger.debug(f"Server address: {self.server_address}, port: {self.server_port}.")
        logger.debug(f"Heartbeat timeout: {self.heartbeat_timeout}")

        asyncio.run(self.ps_start_tasks())

        logger.info("Will exit node now.")

    async def ps_start_tasks(self) -> None:
        async with asyncio.TaskGroup() as tg:
            main_task = tg.create_task(self.ps_main_loop())
            main_task.set_name("MainTask")
            heartbeat_task = tg.create_task(self.ps_send_heartbeat())
            heartbeat_task.set_name("HeartbeatTask")

    async def ps_send_msg_return_answer(self, msg: bytes) -> Any:
        try:
            logger.debug("Send message to server.")
            reader, writer = await asyncio.open_connection(self.server_address, self.server_port)

            writer.write(msg)
            writer.write_eof()
            await writer.drain()

            data = await reader.read()
            msg = psm.decode_message(data, self.secret_key)

            writer.close()
            await writer.wait_closed()

            return msg
        except ConnectionRefusedError:
            logger.error("Could not connect to server. Will exit now.")
            return psm.PSMessageType.ConnectionError

    async def ps_main_loop(self) -> None:
        logger.debug("Start main task.")
        need_more_data_message = psm.ps_gen_need_more_data_message(self.node_id, self.secret_key)
        init_message = psm.ps_gen_init_message(self.node_id, self.secret_key)

        msg = None
        new_result = None
        mode: str = "init"

        while True:
            match mode:
                case "init":
                    msg = await self.ps_send_msg_return_answer(init_message)
                case "need_data":
                    msg = await self.ps_send_msg_return_answer(need_more_data_message)
                case "has_data":
                    result_msg = psm.ps_gen_result_message(self.node_id, self.secret_key, new_result)
                    msg = await self.ps_send_msg_return_answer(result_msg)

            match msg:
                case (psm.PSMessageType.InitOK, data):
                    if mode == "init":
                        logger.debug("Init node OK.")
                        self.ps_init(data)
                        mode = "need_data"
                    else:
                        logger.error("Mode should be init: {mode}.")
                        break
                case psm.PSMessageType.InitError:
                    logger.error("Init node failed!")
                    break
                case (psm.PSMessageType.NewDataFromServer, new_data):
                    if mode == "need_data":
                        logger.debug("Received new data from server.")
                        match new_data:
                            case None:
                                logger.debug("No more data to process! Waiting for other nodes to finish the job.")
                                await asyncio.sleep(10.0)
                                mode = "need_data"
                            case _:
                                new_result = await self.ps_process_data_thread(new_data)
                                logger.debug("New data has been processed.")
                                mode = "has_data"
                    else:
                        logger.error("Mode should be need_data: {mode}.")
                        break
                case psm.PSMessageType.ResultOK:
                    if mode == "has_data":
                        logger.debug("New processed data has been sent to server.")
                        mode = "need_data"
                    else:
                        logger.error("Mode should be has_data: {mode}")
                        break
                case psm.PSMessageType.Quit:
                    logger.debug("Job finished.")
                    break
                case psm.PSMessageType.ConnectionError:
                    logger.error("Connection error, will exit now.")
                    break
                case _:
                    logger.error("Received unknown message from server!")
                    break

    async def ps_send_heartbeat(self) -> None:
        logger.debug("Start heartbeat task.")
        heartbeat_message = psm.ps_gen_heartbeat_message(self.node_id, self.secret_key)

        while True:
            await asyncio.sleep(self.heartbeat_timeout)

            logger.debug("Send heartbeat message to server.")
            msg = await self.ps_send_msg_return_answer(heartbeat_message)

            match msg:
                case psm.PSMessageType.HeartbeatOK:
                    logger.debug("Heartbeat OK.")
                case psm.PSMessageType.HeartbeatError:
                    logger.debug("Heartbeat Error!")
                    break
                case psm.PSMessageType.Quit:
                    logger.debug("Job finished, quit.")
                    break
                case _:
                    logger.error("Received unknown message from server!")
                    break

    async def ps_process_data_thread(self, data: Any) -> Any:
        return await asyncio.to_thread(self.ps_process_data, data)

    def ps_init(self, data: Any) -> None:
        # Must be implemented by the user.
        pass

    def ps_process_data(self, data: Any) -> Any:
        # Must be implemented by the user.
        pass

