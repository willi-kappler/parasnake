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
    f = Fernet(secret_key)

    msg_ser = pickle.dumps(message)
    msg_cmp = lzma.compress(msg_ser)
    msg_enc = f.encrypt(msg_cmp)

    return msg_enc


def decode_message(message: bytes, secret_key: bytes) -> Any:
    f = Fernet(secret_key)

    msg_cmp = f.decrypt(message)
    msg_ser = lzma.decompress(msg_cmp)
    obj = pickle.loads(msg_ser)

    return obj


def ps_gen_heartbeat_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PSMessageType.Heartbeat, node_id)
    return encode_message(msg, secret_key)


def ps_gen_heartbeat_message_ok(secret_key: bytes) -> bytes:
    msg = PSMessageType.HeartbeatOK
    return encode_message(msg, secret_key)


def ps_gen_heartbeat_message_error(secret_key: bytes) -> bytes:
    msg = PSMessageType.HeartbeatError
    return encode_message(msg, secret_key)


def ps_gen_init_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PSMessageType.Init, node_id)
    return encode_message(msg, secret_key)


def ps_gen_init_message_ok(init_data: Any, secret_key: bytes) -> bytes:
    msg = (PSMessageType.InitOK, init_data)
    return encode_message(msg, secret_key)


def ps_gen_init_message_error(secret_key: bytes) -> bytes:
    msg = PSMessageType.InitError
    return encode_message(msg, secret_key)


def ps_gen_result_message(node_id: PSNodeId, secret_key: bytes, new_data: Any) -> bytes:
    msg = (PSMessageType.NewResultFromNode, node_id, new_data)
    return encode_message(msg, secret_key)


def ps_gen_need_more_data_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PSMessageType.NodeNeedsMoreData, node_id)
    return encode_message(msg, secret_key)


def ps_gen_new_data_message(new_data: Any, secret_key: bytes) -> bytes:
    msg = (PSMessageType.NewDataFromServer, new_data)
    return encode_message(msg, secret_key)


def ps_gen_result_ok_message(secret_key: bytes) -> bytes:
    msg = PSMessageType.ResultOK
    return encode_message(msg, secret_key)


def ps_gen_quit_message(secret_key: bytes) -> bytes:
    msg = PSMessageType.Quit
    return encode_message(msg, secret_key)


