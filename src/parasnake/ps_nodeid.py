# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines the unique node id that is assigned to each node.
When a new node registers for the first time to the server the node's own
node id is send to the server. Each message from the node to the server
contains this unique node id. If the node id is unknown to the server,
it sends an error to the node.
"""


import uuid


class PSNodeId:
    def __init__(self):
        self.id = uuid.uuid4()

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __repr__(self) -> str:
        return f"PSNodeId({self.id})"


