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
    """
    This class describes the server part. The user of this library has to derive
    from it and implement some or all of the stub methods.
    In here the connections to each node is managed and the job is split up and
    distributed to all the nodes.
    """

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
        This methods uses asyncio.run to call the main loop
        ps_main_loop().
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
        This method registers a new node with the given node ID
        and sets its heartbeat time to the current time.

        It's called from the ps_handle_node() method.

        :param node_id: The node ID of the new node.
        """

        logger.debug("Register new node.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    def ps_update_node_time(self, node_id: PSNodeId) -> None:
        """
        This method updates the heartbeat time for the given node.

        It's called from the ps_handle_node() method.

        :param node_id: The node ID of the node whose heartbeat time should be updated.
        """

        logger.debug("Update node time.")
        now: float = time.time()
        self.all_nodes[node_id] = now

    async def ps_write_msg(self, writer, msg) -> None:
        """
        This method sends a message over the network and awaits its completion.

        It's called from the ps_handle_node() method.

        :param writer: The network socket to write (send) the message to.
        :param msg: The message to write (send).
        """

        writer.write(msg)
        writer.write_eof()
        await writer.drain()

    async def ps_handle_node(self, reader, writer) -> None:
        """
        This method handles all node communication.

        It's called from ps_main_loop() when a node connects to the server.

        The nodes can send the following messages:

        **PSMessageType.Init**: The node registers with its unique node ID.
            If the node ID is already taken, the server logs an error and sends
            the InitError message to the node.
            The method ps_register_new_node is called.
            The method ps_get_init_data is called, and the result is sent back to
            the node with the InitOK message.

        **PSMessageType.Heartbeat**: Each node must send a heartbeat message to the server
            after registering itself via the Init message. The interval is set in the
            configuration file with the "heartbeat_timeout" option. This is checked
            in the ps_check_heartbeat() method. If a node misses the heartbeat, then
            the method ps_node_timeout() is called. Here, the user can decide what to
            do with the unprocessed data. Usually, it is marked as "unprocessed" and
            given to another node to process.

        **PSMessageType.NodeNeedsMoreData**: After sending the processed data to the server,
            the node sends this message to receive new data to process. If the job is
            done and no more data is left, then the server returns None.
            The method ps_get_new_data() is called to get the new data and has to be
            implemented by the user.

        **PSMessageType.NewResultFromNode**: This message is sent when the node is done
            processing the data it got from the server. The method ps_process_result()
            is called with the newly processed data. This method has to be implemented
            by the user. Normally, the data is merged back into some data structure.

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
                        logger.error("Node ID already registered: {node_id}")
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
                        logger.error("Node ID not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_heartbeat_message_error(self.secret_key))
                case (psm.PSMessageType.NodeNeedsMoreData, node_id):
                    logger.debug("Node needs more data.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        new_data = await self.ps_get_new_data_thread(node_id)
                        await self.ps_write_msg(writer, psm.ps_gen_new_data_message(new_data, self.secret_key))
                    else:
                        logger.error("Node ID not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))
                case (psm.PSMessageType.NewResultFromNode, node_id, result):
                    logger.debug("New result from node.")
                    if node_id in self.all_nodes:
                        self.ps_update_node_time(node_id)
                        await self.ps_process_result_thread(node_id, result)
                        await self.ps_write_msg(writer, psm.ps_gen_result_ok_message(self.secret_key))
                    else:
                        logger.error("Node ID not registered yet: {node_id}")
                        await self.ps_write_msg(writer, psm.ps_gen_init_message_error(self.secret_key))

        writer.close()
        await writer.wait_closed()

    async def ps_main_loop(self) -> None:
        """
        This method is the main loop and starts the server.
        It's called from ps_run().
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
        This method is called from ps_main_loop().
        """

        logger.debug("Check heartbeat of all nodes.")

        now: float = time.time()

        for k, v in self.all_nodes.items():
            if int(now - v + 1.0) > self.heartbeat_timeout:
                self.ps_node_timeout(k)

    async def ps_get_init_data_thread(self, node_id: PSNodeId) -> Any:
        """
        This method starts a new background thread to retrieve the initial data
        for the given node.

        Since this call may block, it is run in a separate thread.
        This method is called from ps_handle_node() (Init message).

        :param node_id: The ID of the node that receives the initialisation data.
        :return: The init data for the given node.
        """

        return await asyncio.to_thread(self.ps_get_init_data_lock, node_id)

    def ps_get_init_data_lock(self, node_id: PSNodeId) -> Any:
        """
        This method locks the server data before retrieving the initial data.

        It may block and is called in a separate thread.
        It's called from ps_get_init_data_thread() (Init message).

        :param node_id: The ID of the node that receives the initialisation data.
        :return: The init data for the given node.
        """

        with self.lock:
            return self.ps_get_init_data(node_id)

    def ps_get_init_data(self, node_id: PSNodeId) -> Any:
        """
        This method retrieves (calculates) the initial data for the given node.

        This method must be implemented by the user.
        Since it may block, it is called in a separate thread.
        It's called from ps_get_init_data_lock() (Init message).

        :param node_id: The ID of the node that receives the initialisation data.
        :return: The init data for the given node.
        """

        # Must be implemented by the user.
        return None

    async def ps_get_new_data_thread(self, node_id: PSNodeId) -> Optional[Any]:
        """
        This method creates new data for the given node.

        This data is sent over the network to the node to be processed by that node.
        Since this method may block, it is run in a separate thread.
        If there is no more data to process (=job is done), this method returns None.
        Otherwise, it returns the new data to be processed by the node.
        It's called from ps_handle_node() (NodeNeedsMoreData message).

        :param node_id: The ID of the node that receives the new data.
        :return: The new data for the given node.
        """

        return await asyncio.to_thread(self.ps_get_new_data_lock, node_id)

    def ps_get_new_data_lock(self, node_id: PSNodeId) -> Optional[Any]:
        """
        This method locks the server data before creating new data for the node.

        It may block and is run in a separate thread.
        If there is no more data to process (=job is done), this method returns None.
        It's called from ps_get_new_data_thread() (NodeNeedsMoreData message).

        :param node_id: The ID of the node that receives the new data.
        :return: The new data for the given node.
        """

        with self.lock:
            return self.ps_get_new_data(node_id)

    async def ps_process_result_thread(self, node_id: PSNodeId, result: Any):
        """
        This method processes the data (result) from the given node.
        Usually the server will merge the data with its internal data.
        Since this method may block it is run in a separate thread.
        It's called from ps_handle_node() (NewResultFromNode message).

        :param node_id: The ID of the node that has processed the data and sent
                        the results back to the server.
        """

        await asyncio.to_thread(self.ps_process_result_lock, node_id, result)

    def ps_process_result_lock(self, node_id: PSNodeId, result: Any):
        """
        This method locks the server data before processing the result from the node.
        It may block and is run in a separate thread.
        It's called from ps_process_result_thread() (NewResultFromNode message).

        :param node_id: The ID of the node that has processed the data and sent
                        the results back to the server.
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
        Otherwise it returns False.
        It's called from ps_main_loop().

        :return: True if the job is finished, False otherwise.
        :rtype: Bool
        """

        # Must be implemented by the user.
        return False

    def ps_save_data(self) -> None:
        """
        This method must be implemented by the user.
        It's called from ps_run() and only when the job is done.
        Here the use can decide what to save in which format and where.
        """

        # Must be implemented by the user.
        pass

    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        """
        This method can be implemented by the user. It is called from ps_check_heartbeat() and when a
        node hasn't send the heartbeat message in time or at all.
        The user can keep track of the nodes and mark the data as "not taken" so that other
        nodes can process the data of this node. See the mandelbrot example on how this can be done.

        :param node_id: The ID of the node that has missed the heartbeat message.
        """

        # Can be implemented by the user.
        pass

    def ps_get_new_data(self, node_id: PSNodeId) -> Optional[Any]:
        """
        This method must be implemented by the user. It is called from
        ps_get_new_data_lock() (NodeNeedsMoreData message).
        It returns the new data that has to be processed by the given node (node_id).
        If the job is done and no more data has to be processed then this method must
        return None.

        :param node_id: The ID of the node that receives the new data.
        :return: The new data for the given node.
        :rtype: Optional[Any]
        """

        # Must be implemented by the user.
        return None

    def ps_process_result(self, node_id: PSNodeId, result: Any):
        """
        This method must be implemented by the user. It is called from
        ps_process_result_lock() (NewResultFromNode message).
        The node sends the processed data to the server and the user has to handle
        this processed result in this method. Usually it is merged in some data structure.

        :param node_id: The ID of the node that has processed the data and sent the results back to
                        the server.
        :param result: This is the processed data from the node.
        """

        # Must be implemented by the user.
        pass


