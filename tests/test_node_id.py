# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
import copy

from parasnake.ps_nodeid import PSNodeId

class TestNodeId(unittest.TestCase):
    def test_equality(self):
        id1 = PSNodeId()
        id2 = PSNodeId()

        id1c = copy.deepcopy(id1)

        self.assertEqual(id1, id1c)
        self.assertNotEqual(id1, id2)
        self.assertNotEqual(id1c, id2)

    def test_hash(self):
        id1 = PSNodeId()
        id2 = PSNodeId()
        id3 = PSNodeId()

        d = {}
        d[id1] = "node1"
        d[id2] = "node2"
        d[id3] = "node3"

        self.assertEqual(d[id1], "node1")
        self.assertEqual(d[id2], "node2")
        self.assertEqual(d[id3], "node3")

if __name__ == "__main__":
    unittest.main()

