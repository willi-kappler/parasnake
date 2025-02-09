# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the messages and how to decode and encode them.
"""


# Python std modules:
from typing import Any
from enum import Enum
import pickle
import lzma

# External modules:
from cryptography.fernet import Fernet

# Local modules:
from parasnake.ps_nodeid import PSNodeId


class PSMessageType(Enum):
    """
    This enum class defines the various message types.
    From the server to the node:

        - HeartbeatOK
        - HeartbeatError
        - InitOK
        - InitError
        - NewDataFromServer
        - ResultOK
        - ConnectionError
        - Quit

    From the node to the server:

        - Heartbeat
        - Init
        - NewResultFromNode
        - NodeNeedsMoreData

    """

    Heartbeat = 0
    HeartbeatOK = 1
    HeartbeatError = 2
    Init = 3
    InitOK = 4
    InitError = 5
    NewDataFromServer = 6
    NewResultFromNode = 7
    NodeNeedsMoreData = 8
    ResultOK = 9
    ConnectionError = 10
    Quit = 11


def encode_message(message: Any, secret_key: bytes) -> bytes:
    """
    Encodes a message with the given key.

    :param message: The message to encode.
    :param secret_key: A secret key that is known by the server and client.
    :return: The encoded message.
    :rtype: bytes
    """

    f = Fernet(secret_key)

    msg_ser = pickle.dumps(message)
    msg_cmp = lzma.compress(msg_ser)
    msg_enc = f.encrypt(msg_cmp)

    return msg_enc


def decode_message(message: bytes, secret_key: bytes) -> Any:
    """
    Decodes a message with the given key.

    :param message: The message to dencode.
    :param secret_key: A secret key that is known by the server and client.
    :return: The dencoded message.
    :rtype: Any
    """

    f = Fernet(secret_key)

    msg_cmp = f.decrypt(message)
    msg_ser = lzma.decompress(msg_cmp)
    obj = pickle.loads(msg_ser)

    return obj


def ps_gen_heartbeat_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    """
    Generate a heartbeat message to be sent from the node to the server.

    The node sends its node_id that the server will check.
    The secret key is used to encode the message.

    :param node_id: The ID of the node that sends the heartbeat message.
    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded heartbeat message.
    :rtype: bytes
    """

    msg = (PSMessageType.Heartbeat, node_id)
    return encode_message(msg, secret_key)


def ps_gen_heartbeat_message_ok(secret_key: bytes) -> bytes:
    """
    Generate a "heartbeat OK" message to be sent from the server to the node.

    This message is only sent if the heartbeat message from the node was
    sent in the time limit and contained a valid node id.
    The secret key is used to encode the message.

    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "heartbeat OK" message.
    :rtype: bytes
    """

    msg = PSMessageType.HeartbeatOK
    return encode_message(msg, secret_key)


def ps_gen_heartbeat_message_error(secret_key: bytes) -> bytes:
    """
    Generate a "heartbeat error" message to be sent from the server to the node.

    This message is only sent if the heartbeat message from the node was
    sent too late or did not contain a valid node id.
    The secret key is used to encode the message.

    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "heartbeat error" message.
    :rtype: bytes
    """

    msg = PSMessageType.HeartbeatError
    return encode_message(msg, secret_key)


def ps_gen_init_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    """
    Generate an initialisation message to be sent from the node to the server.

    This message is only sent once when the node connects for the first time to the server.
    The node registers itself to the server given its own node id.
    The secret key is used to encode the message.

    :param node_id: The node ID of the new node.
    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded init message.
    :rtype: bytes
    """

    msg = (PSMessageType.Init, node_id)
    return encode_message(msg, secret_key)


def ps_gen_init_message_ok(init_data: Any, secret_key: bytes) -> bytes:
    """
    Generate an "init ok" message to be sent from the server to the node.

    This message is only sent once when the node has registered itself correctly to the server.
    The server then can send some initial data to the node, if needed.
    The secret key is used to encode the message.

    :param init_data: Some data to initialize the node (optional).
    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "init ok" message.
    :rtype: bytes
    """

    msg = (PSMessageType.InitOK, init_data)
    return encode_message(msg, secret_key)


def ps_gen_init_message_error(secret_key: bytes) -> bytes:
    """
    Generate an "init error" message to be sent from the server to the node.

    This message is only sent when the registration of the new node has failed.
    The secret key is used to encode the message.

    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "init error" message.
    :rtype: bytes
    """

    msg = PSMessageType.InitError
    return encode_message(msg, secret_key)


def ps_gen_result_message(node_id: PSNodeId, secret_key: bytes, new_data: Any) -> bytes:
    """
    Generate a result message to be sent from the node to the server.

    This message is only sent when the node has finished processing the data and sends
    the result back to the server.
    The secret key is used to encode the message.

    :param node_id: The ID of the node.
    :param secret_key: A secret key that is used to encode the message.
    :param new_data: The processed data (result).
    :return: The encoded result message.
    :rtype: bytes
    """

    msg = (PSMessageType.NewResultFromNode, node_id, new_data)
    return encode_message(msg, secret_key)


def ps_gen_need_more_data_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    """
    Generate a "need more data" message to be sent from the node to the server.

    This message is only sent when the node has finished processing the data and needs
    more data to be processed from the server.
    The secret key is used to encode the message.

    :param node_id: The ID of the node.
    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "need mode data" message.
    :rtype: bytes
    """

    msg = (PSMessageType.NodeNeedsMoreData, node_id)
    return encode_message(msg, secret_key)


def ps_gen_new_data_message(new_data: Any, secret_key: bytes) -> bytes:
    """
    Generate a "new data" message to be sent from the server to the node.

    This message is only sent when the node has asked for more data from the server.
    The secret key is used to encode the message.

    :param new_data: New data to be processed by the node.
    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "new data" message.
    :rtype: bytes
    """

    msg = (PSMessageType.NewDataFromServer, new_data)
    return encode_message(msg, secret_key)


def ps_gen_result_ok_message(secret_key: bytes) -> bytes:
    """
    Generate a "result ok" message to be sent from the server to the node.

    This message is only sent when the node has sent processed data to the server
    and the server has accepted it.
    The secret key is used to encode the message.

    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded "result ok" message.
    :rtype: bytes
    """

    msg = PSMessageType.ResultOK
    return encode_message(msg, secret_key)


def ps_gen_quit_message(secret_key: bytes) -> bytes:
    """
    Generate a quit message to be sent from the server to the node.

    This message is only sent when the job is done and no more data has to be
    processed by the nodes.
    When receiving this message, the nodes will quit immediately.
    The server will wait some more time since not all nodes may have received
    the quit message yet.

    :param secret_key: A secret key that is used to encode the message.
    :return: The encoded quit message.
    :rtype: bytes
    """

    msg = PSMessageType.Quit
    return encode_message(msg, secret_key)


