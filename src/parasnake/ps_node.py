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
    """
    This is the main class that does all coomputation and communication to the server.
    The user of this library has to derive from this class in order to add new custom
    code.
    """

    def __init__(self, configuration: PSConfiguration):
        """
        Initializes the node using the given configuration.

        Args:
            configuration: The configuration for the node.
        """

        self.server_address: str = configuration.server_address
        self.server_port: int = configuration.server_port
        self.secret_key: bytes = configuration.secret_key
        self.heartbeat_timeout: int = configuration.heartbeat_timeout
        self.node_id: PSNodeId = PSNodeId()

    def ps_run(self) -> None:
        """
        This method starts an async task to run the node code.
        It is the main entry point that the user has to call.
        """

        logger.info("Starting node with node id: {self.node_id}.")
        logger.debug(f"Server address: {self.server_address}, port: {self.server_port}.")
        logger.debug(f"Heartbeat timeout: {self.heartbeat_timeout}")

        asyncio.run(self.ps_start_tasks())

        logger.info("Will exit node now.")

    async def ps_start_tasks(self) -> None:
        """
        This async methods start the main loop and the heartbeat task.
        It is called by the ps_run() method.
        """

        async with asyncio.TaskGroup() as tg:
            main_task = tg.create_task(self.ps_main_loop())
            main_task.set_name("MainTask")
            heartbeat_task = tg.create_task(self.ps_send_heartbeat())
            heartbeat_task.set_name("HeartbeatTask")

    async def ps_send_msg_return_answer(self, msg: bytes) -> Any:
        """
        This async method sends an encoded message to the server and awaits the server's response.

        The message from the server is decoded and returned.
        This async method is called from ps_main_loop().

        Args:
            msg: The already encoded message that is sent to the server.

        Returns:
            The decoded message from the server.
        """

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
        """
        This async method is the main loop for the node.
        It manages all the communication between the node and the server.
        It is called from ps_start_tasks().

        The main loop is driven by the mode state:

            - init: This is the first state and the node has to register itself to the server.
            - need_data: The node is in this state when it needs data from the server.
            - has_data: After processing all the data, the node has to send the result back to
              the server.

        """

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
        """
        This async method sends the heartbeat message to the server.
        The rate can be configured in the configuration file:
        heartbeat_timeout. The default value is 5 minutes.
        If the node misses a heartbeat the server knows that ther is sth. wrong.
        The data is marked as dirty and will be sent to another node to be processed.
        The heartbeat task is run asynchronouesly in the background and is called
        by ps_start_tasks().
        """

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
        """
        This async method is called from the main loop to process new data sent by the server to the node.

        Since data processing can take a lot of time, a new OS thread is started to
        do the processing in the background so that no other async task is blocked.
        This calls the user-defined ps_process_data() as a new thread.

        Args:
            data: The new data that has been sent from the server and needs to be processed.

        Returns:
            The result after processing, which will be sent back to the server.
        """

        return await asyncio.to_thread(self.ps_process_data, data)

    def ps_init(self, data: Any) -> None:
        """
        This method is called from the main loop after the node has registered for the first time to the server.

        The server may send back some initialization data for the node.
        This data can be processed here. The user does not have to implement this method
        if the server has no init data.

        Args:
            data: The initialization data sent by the server (if any).
        """

        pass

    def ps_process_data(self, data: Any) -> Any:
        """
        This method is called from the main loop in a background thread to avoid blocking other async tasks.

        The user has to implement it to process the new data from the server, and the result will be
        sent back to the server.

        Args:
            data: The new data from the server that will be processed by this node.

        Returns:
            The processed data from this node, which will be sent back to the server.
        """

        # Must be implemented by the user!
        pass

