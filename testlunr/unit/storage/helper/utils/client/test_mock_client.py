#!/usr/bin/env python
# Copyright (c) 2011-2015 Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest
from StringIO import StringIO

from lunr.storage.helper.utils.client import memory


class TestMockClient(unittest.TestCase):

    def test_write_str_object(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        body = 'message'
        conn.put_object('ocean', 'bottle', body)
        self.assertEquals('message', conn.get_object('ocean', 'bottle')[1])

    def test_write_readable_object(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        body = StringIO('message')
        conn.put_object('ocean', 'bottle', body)
        self.assertEquals('message', conn.get_object('ocean', 'bottle')[1])

    def test_read_str_object(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        conn.put_object('ocean', 'bottle', 'message')
        _headers, body = conn.get_object('ocean', 'bottle')
        self.assertEquals(body, 'message')

    def test_read_iter_object(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        orig_body = 'message'
        conn.put_object('ocean', 'bottle', 'message')
        resp_chunk_size = 2
        _headers, body = conn.get_object('ocean', 'bottle',
                                         resp_chunk_size=resp_chunk_size)
        read_body = ''
        for chunk in body:
            remaining_body_length = len(orig_body) - len(read_body)
            if remaining_body_length > resp_chunk_size:
                self.assertEquals(len(chunk), resp_chunk_size)
            else:
                self.assertEquals(len(chunk), remaining_body_length)
            read_body += chunk
        self.assertEquals(read_body, 'message')

    def test_object_not_found(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        self.assertRaises(memory.ClientException, conn.get_object,
                          'ocean', 'bottle')

    def test_get_container(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        _headers, listing = conn.get_container('ocean')

    def test_delete_container(self):
        conn = memory.Connection()
        conn.put_container('ocean')
        body = '1234567890'
        conn.put_object('ocean', '1', body)
        self.assertRaises(memory.ClientException,
                          conn.delete_container, 'ocean')
        conn.delete_object('ocean', '1')
        conn.delete_container('ocean')
        self.assertRaises(memory.ClientException,
                          conn.get_container, 'ocean')


if __name__ == "__main__":
    unittest.main()
