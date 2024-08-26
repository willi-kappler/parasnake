# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the messages and how to decode and encode them.
"""


from typing import Any
import pickle
import lzma


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
PS_NODE_NEEDS_MORE_DATA = "node_needs_more_data"


def encode_message(message: Any, secret_key: bytes) -> bytes:
    msg_ser = pickle.dumps(message)
    msg_cmp = lzma.compress(msg_ser)
    msg_enc = msg_cmp
    return "Message!".encode()

def decode_message(message: bytes, secret_key: bytes) -> Any:
    # pickle.loads
    # lzma.decompress
    #
    return "Message!".encode()

def ps_gen_heartbeat_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PS_HEARTBEAT_MESSAGE, node_id)
    return encode_message(msg, secret_key)

def ps_gen_init_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PS_INIT_MESSAGE, node_id)
    return encode_message(msg, secret_key)

def ps_gen_result_message(node_id: PSNodeId, secret_key: bytes, new_data: Any) -> bytes:
    msg = (PS_NEW_RESULT_FROM_NODE, node_id, new_data)
    return encode_message(msg, secret_key)

def ps_gen_need_more_data_message(node_id: PSNodeId, secret_key: bytes) -> bytes:
    msg = (PS_NODE_NEEDS_MORE_DATA, node_id)
    return encode_message(msg, secret_key)




