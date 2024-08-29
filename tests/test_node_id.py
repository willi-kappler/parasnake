# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
import copy
import uuid

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

    def test_repr1(self):
        id1 = PSNodeId()

        s = f"{id1}"

        self.assertTrue(len(s) > 0)
        self.assertTrue(s.startswith("PSNodeId("))

    def test_repr2(self):
        id1 = PSNodeId()
        init_value = "11111111CCCCCCCC55555555AAAAAAAA"
        id1.id = uuid.UUID(init_value)

        s = f"{id1}"

        self.assertEqual(s, "PSNodeId(11111111-cccc-cccc-5555-5555aaaaaaaa)")


if __name__ == "__main__":
    unittest.main()

