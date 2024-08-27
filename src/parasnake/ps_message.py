# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the messages and how to decode and encode them.
"""


# Python std modules:
from typing import Any
import pickle
import lzma

# External modules:
from cryptography.fernet import Fernet

# Locel modules:
from parasnake.ps_nodeid import PSNodeId


PS_HEARTBEAT_MESSAGE = "heartbeat_from_node"
PS_HEARTBEAT_OK = "heartbeat_ok"
PS_HEARTBEAT_ERROR = "heartbeat_error"
PS_QUIT = "quit"
PS_INIT_MESSAGE = "init_from_node"
PS_INIT_OK = "init_ok"
PS_INIT_ERROR = "init_error"
PS_NEW_DATA_FROM_SERVER = "new_data_from_server"
PS_NEW_RESULT_FROM_NODE = "new_result_from_node"
PS_RESULT_OK = "result_ok"
PS_NODE_NEEDS_MORE_DATA = "node_needs_more_data"


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
    msg = (PS_HEARTBEAT_MESSAGE, node_id)
    return encode_message(msg, secret_key)

def ps_gen_heartbeat_message_ok(secret_key: bytes) -> bytes:
    msg = PS_HEARTBEAT_OK
    return encode_message(msg, secret_key)

def ps_gen_init_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PS_INIT_MESSAGE, node_id)
    return encode_message(msg, secret_key)

def ps_gen_init_message_ok(init_data: Any, secret_key: bytes) -> bytes:
    msg = (PS_INIT_OK, init_data)
    return encode_message(msg, secret_key)

def ps_gen_result_message(node_id: PSNodeId, secret_key: bytes, new_data: Any) -> bytes:
    msg = (PS_NEW_RESULT_FROM_NODE, node_id, new_data)
    return encode_message(msg, secret_key)

def ps_gen_need_more_data_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PS_NODE_NEEDS_MORE_DATA, node_id)
    return encode_message(msg, secret_key)

def ps_gen_new_data_message(new_data: Any, secret_key: bytes) -> bytes:
    msg = (PS_NEW_DATA_FROM_SERVER, new_data)
    return encode_message(msg, secret_key)

def ps_gen_result_ok_message(secret_key: bytes) -> bytes:
    msg = PS_RESULT_OK
    return encode_message(msg, secret_key)

def ps_gen_quit_message(secret_key: bytes) -> bytes:
    msg = PS_QUIT
    return encode_message(msg, secret_key)


