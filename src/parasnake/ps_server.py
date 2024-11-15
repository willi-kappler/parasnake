# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the server class that distributes the work load to each node.
TODO: Describe the caller and callee.
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
        """
        This methods uses asyncio.run to call the main loop (ps_main_loop).
        It is the main entry point for the user code.
        """

        logger.info(f"Starting server with port: {self.server_port}.")
        logger.debug(f"Heartbeat timeout: {self.heartbeat_timeout}.")

        asyncio.run(self.ps_main_loop())

        logger.info("Saving data...")
        self.ps_save_data()
        logger.info("Will exit server now.")

    def ps_register_new_node(self, node_id: PSNodeId) -> None:
        """
        This method registers a new node with the given node id.
        It also sets the heartbeat time to the current time.
        It's called from the ps_handle_node method.

        :param node_id: The node id of the new node.
        """

        logger.debug("Register new node.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    def ps_update_node_time(self, node_id: PSNodeId) -> None:
        """
        This methods updates the heartbeat time for the given node.
        It's called from the ps_handle_node method.

        :param node_id: The node id of the node whose heartbeat time should be updated.
        """

        logger.debug("Update node time.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    async def ps_write_msg(self, writer, msg) -> None:
        """
        This is a helper method to send a message over the network and await for it to finish.
        It's called from the ps_handle_node method.

        :param writer: The network socket to write (send) the message to.
        :param msg: The message to write (send).
        """

        writer.write(msg)
        writer.write_eof()
        await writer.drain()

    async def ps_handle_node(self, reader, writer) -> None:
        """
        This method handles all the node communication.
        It is called from ps_main_loop() when a node connects to the server.
        TODO: Describe the message types.

        The nodes can send the following messages:

        PSMessageType.Init: The node registeres with it's unique node id.
            If the node id is already taken the server logs an error and sends
            the InitError message to the node.
            The method ps_register_new_node is called.
            The method ps_get_init_data is called and the result is sent back to
            the node with the InitOK message.

        PSMessageType.Heartbeat: 


        :param reader: The network socket to read (receive) the node message from.
        :param writer: The network socket to write (send) the answer to.
        """

        logger.debug("Connection from node.")

        data = await reader.read()
        msg = psm.decode_message(data, self.secret_key)

        if self.quit:
            logger.debug("Send quit message, job is done.")
            await self.ps_write_msg(writer, psm.ps_gen_quit_message(self.secret_key))
        else:
            match msg:
                case (psm.PSMessageType.Init, node_id):
                    logger.debug("Init message.")
                    if node_id in self.all_nodes:
                        logger.error("Node id already registered: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))
                    else:
                        self.ps_register_new_node(node_id)
                        init_data = await self.ps_get_init_data_thread(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_ok(init_data, self.secret_key))
                case (psm.PSMessageType.Heartbeat, node_id):
                    logger.debug("Heartbeat message.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_heartbeat_message_ok(self.secret_key))
                    else:
                        logger.error("Node id not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_heartbeat_message_error(self.secret_key))
                case (psm.PSMessageType.NodeNeedsMoreData, node_id):
                    logger.debug("Node needs more data.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        new_data = await self.ps_get_new_data_thread(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_new_data_message(new_data, self.secret_key))
                    else:
                        logger.error("Node id not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))
                case (psm.PSMessageType.NewResultFromNode, node_id, result):
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
        """
        This method is the main loop and starts the server.
        It is called from ps_run().
        If a node connects the message is processed in the ps_handle_node() method.
        """

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
        """
        This method checks the heartbeat values for all the active nodes.
        If one node failed to send the heartbeat message, the ps_node_timeout()
        method is called with the corresponding nodeid.
        """

        logger.debug("Check heartbeat of all nodes.")

        now: float = time.time()

        for k, v in self.all_nodes.items():
            if int(now - v + 1.0) > self.heartbeat_timeout:
                self.ps_node_timeout(k)

    async def ps_get_init_data_thread(self, node_id: PSNodeId) -> Any:
        """
        This method starts a new background thread to retrieve the initial data
        for the given node. Since this call may block it is run in a separate thread.

        :param node_id: The id of the node that receives the initialisation data.
        :return: The init data for the given node.
        :rtype: Any
        """

        return await asyncio.to_thread(self.ps_get_init_data_lock, node_id)

    def ps_get_init_data_lock(self, node_id: PSNodeId) -> Any:
        """
        This method locks the server data before retrieving the initial data.
        It may block and is called in a separate thread.

        :param node_id: The id of the node that receives the initialisation data.
        :return: The init data for the given node.
        :rtype: Any
        """

        with self.lock:
            return self.ps_get_init_data(node_id)

    def ps_get_init_data(self, node_id: PSNodeId) -> Any:
        """
        This method must be implemented by the user.
        It retrieves (calculates) the initial data for the given node.
        Since it may block it is called in a separate thread.

        :param node_id: The id of the node that receives the initialisation data.
        :return: The init data for the given node.
        :rtype: Any
        """

        # Must be implemented by the user.
        return None

    async def ps_get_new_data_thread(self, node_id: PSNodeId) -> Optional[Any]:
        """
        This method is called to create new data for the given node.
        This data is sent over the network to the node to be processed by that node.
        Since this method may block it is run in a separate thread.
        If there is no more data to process (=job is done) this method returns None.

        :param node_id: The id of the node that receives the new data.
        :return: The new data for the given node.
        :rtype: Optional[Any]
        """

        return await asyncio.to_thread(self.ps_get_new_data_lock, node_id)

    def ps_get_new_data_lock(self, node_id: PSNodeId) -> Optional[Any]:
        """
        This method locks the server data before creating new date for the node.
        It may block and is run in a separate thread.
        If there is no more data to process (=job is done) this method returns None.

        :param node_id: The id of the node that receives the new data.
        :return: The new data for the given node.
        :rtype: Optional[Any]
        """

        with self.lock:
            return self.ps_get_new_data(node_id)

    async def ps_process_result_thread(self, node_id: PSNodeId, result: Any):
        """
        This method processes the data (result) from the given node.
        Usually the server will merge the data with its internal data.
        Since this method may block it is run in a separate thread.

        :param node_id: The id of the node that has processed the data and sent
                        the results back to the server.
        :param result: The processed data from the node.
        """

        await asyncio.to_thread(self.ps_process_result_lock, node_id, result)

    def ps_process_result_lock(self, node_id: PSNodeId, result: Any):
        """
        This method locks the server data before processing the result from the node.
        It may block and is run in a separate thread.

        :param node_id: The id of the node that has processed the data and sent
                        the results back to the server.
        :param result: The processed data from the node.
        """

        with self.lock:
            self.ps_process_result(node_id, result)

    def ps_is_job_done(self) -> bool:
        """
        This method must be implemented by the user.
        It determines if the overall job is done, that is if everything has been
        processed.
        If the job is done it returns True and all the nodes are notyfied at their next
        connection to the server.

        :return: True if the job is finished, False otherwise.
        :rtype: Bool
        """

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


