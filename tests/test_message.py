# This file is part of Parasnake, a distributed number crunching library for Python
# written by Willi Kappler, MIT license.
#
# See: https://github.com/willi-kappler/parasnake

import unittest
import base64

import parasnake.ps_message as psm
from parasnake.ps_nodeid import PSNodeId


class TestMessage(unittest.TestCase):
    def gen_key(self) -> bytes:
        key = "1111111155555555ccccccccllllllll"
        encoded_key = base64.urlsafe_b64encode(key.encode())

        return encoded_key

    def test_encode_decode1(self):
        key = self.gen_key()

        msg1 = "This is test 1"
        msg2 = psm.encode_message(msg1, key)
        msg3 = psm.decode_message(msg2, key)

        self.assertEqual(msg1, msg3)

    def test_encode_decode2(self):
        key = self.gen_key()

        msg1 = ("This is test 2", [33.5, True, -789])
        msg2 = psm.encode_message(msg1, key)
        msg3 = psm.decode_message(msg2, key)

        self.assertEqual(msg1, msg3)

    def test_heartbeat_message(self):
        id = PSNodeId()
        key = self.gen_key()

        msg1 = psm.ps_gen_heartbeat_message(id, key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, (psm.PS_HEARTBEAT_MESSAGE, id))

    def test_heartbeat_message_ok(self):
        key = self.gen_key()

        msg1 = psm.ps_gen_heartbeat_message_ok(key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, psm.PS_HEARTBEAT_OK)

    def test_heartbeat_message_error(self):
        key = self.gen_key()

        msg1 = psm.ps_gen_heartbeat_message_error(key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, psm.PS_HEARTBEAT_ERROR)

    def test_init_message(self):
        id = PSNodeId()
        key = self.gen_key()

        msg1 = psm.ps_gen_init_message(id, key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, (psm.PS_INIT_MESSAGE, id))

    def test_init_message_ok(self):
        key = self.gen_key()

        data = [33, False, "Some init data"]
        msg1 = psm.ps_gen_init_message_ok(data, key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, (psm.PS_INIT_OK, data))

    def test_init_message_error(self):
        key = self.gen_key()

        msg1 = psm.ps_gen_init_message_error(key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, psm.PS_INIT_ERROR)

    def test_result_message(self):
        id = PSNodeId()
        key = self.gen_key()

        data = [33, False, "Some init data"]
        msg1 = psm.ps_gen_result_message(id, key, data)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, (psm.PS_NEW_RESULT_FROM_NODE, id, data))

    def test_need_data_message(self):
        id = PSNodeId()
        key = self.gen_key()

        msg1 = psm.ps_gen_need_more_data_message(id, key)
        msg2 = psm.decode_message(msg1, key)

        self.assertEqual(msg2, (psm.PS_NODE_NEEDS_MORE_DATA, id))


if __name__ == "__main__":
    unittest.main()

