# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

"""
This module defines the unique nodeid that is assigned to each node.
"""


import uuid


class PSNodeId:
    def __init__(self):
        self.id = uuid.uuid4()

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __repr__(self) -> str:
        return f"PSNodeId({self.id})"


