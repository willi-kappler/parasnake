# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest

from parasnake.ps_config import PSConfiguration


class TestNodeId(unittest.TestCase):
    def test_init(self):
        c = PSConfiguration("11111111222222223333333344444444")

    def test_wrong_key_len(self):
        with self.assertRaises(AssertionError):
            c = PSConfiguration("")

        with self.assertRaises(AssertionError):
            c = PSConfiguration("1111111122222222333333334444444")

        with self.assertRaises(AssertionError):
            c = PSConfiguration("111111112222222233333333444444444")




if __name__ == "__main__":
    unittest.main()

