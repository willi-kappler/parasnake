# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines all the messages and how to decode and encode them.
"""


from typing import Any

from parasnake.ps_nodeid import PSNodeId


HEARTBEAT_OK = "heartbeat_ok"
HEARTBEAT_ERROR = "heartbeat_error"
QUIT = "quit"
INIT_OK = "init_ok"
INIT_ERROR = "init_error"
NEW_DATA_FROM_SERVER = "new_data_from_server"




def encode_message(message: str, secret_key: str) -> str:
    return "Message!"

def decode_message(message: str, secret_key: str) -> str:
    return "Message!"

def ps_gen_heartbeat_message(noed_id: PSNodeId, secret_key: str) -> str:
    return "Hello!"

def ps_gen_init_message(node_id: PSNodeId, secret_key: str) -> str:
    return "Init!"

def ps_gen_result_message(node_id: PSNodeId, secret_key: str, new_data: Any) -> str:
    return "Result!"

def ps_gen_need_more_data_message(node_id: PSNodeId, secret_key: str) -> str:
    return "Need Data!"




