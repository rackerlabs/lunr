#!/usr/bin/env python
# Copyright (c) 2011-2016 Rackspace US, Inc.
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


from contextlib import contextmanager
import errno
import os
from shutil import rmtree
from StringIO import StringIO
from tempfile import mkdtemp
import unittest

from lunr.storage.helper.utils.client import disk

from testlunr.unit import patch, Struct


@contextmanager
def temp_client():
    d = mkdtemp()
    temp_client.path = d
    try:
        yield disk.Connection(d)
    finally:
        try:
            rmtree(d)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise


class TestMockClient(unittest.TestCase):

    def test_write_str_object(self):
        with temp_client() as conn:
            conn.put_container('ocean')
            body = 'message'
            conn.put_object('ocean', 'bottle', body)
            self.assertEquals('message', conn.get_object('ocean', 'bottle')[1])

    def test_write_readable_object(self):
        with temp_client() as conn:
            conn.put_container('ocean')
            body = StringIO('message')
            conn.put_object('ocean', 'bottle', body)
            self.assertEquals('message', conn.get_object('ocean', 'bottle')[1])

    def test_read_str_object(self):
        with temp_client() as conn:
            conn.put_container('ocean')
            conn.put_object('ocean', 'bottle', 'message')
            _headers, body = conn.get_object('ocean', 'bottle')
            self.assertEquals(body, 'message')

    def test_read_iter_object(self):
        with temp_client() as conn:
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
        with temp_client() as conn:
            conn.put_container('ocean')
            self.assertRaises(disk.ClientException, conn.get_object,
                              'ocean', 'bottle')

    def test_create_container(self):
        with temp_client() as conn:
            try:
                # coverage grab for container does not exist
                conn.put_object('ocean', 'bottle', 'message')
            except disk.ClientException:
                conn.put_container('ocean')
                # coverage grab for container already exists
                conn.put_container('ocean')
                conn.put_object('ocean', 'bottle', 'message')
            self.assertEquals('message', conn.get_object('ocean', 'bottle')[1])

    def test_head_account(self):
        with temp_client() as conn:
            status = conn.head_account()
            expected = {'path': '/tmp/tmpnFYF9C', 'free': 802638946304}
            self.assertEquals(status['path'], temp_client.path)
            self.assert_(status['free'] > 0)

    def test_head_account_path_missing(self):
        with temp_client() as conn:
            rmtree(temp_client.path)
            self.assertRaises(conn.ClientException, conn.head_account)
            try:
                conn.head_account()
            except disk.ClientException, e:
                self.assert_('does not exist' in str(e))
                self.assert_(temp_client.path in str(e))

    def test_no_free_space(self):

        def mock_statvfs(path):
            stat = os.statvfs(path)
            kwargs = {}
            for key in [k for k in dir(stat) if k.startswith('f_')]:
                if key == 'f_bfree':
                    kwargs[key] = 0
                else:
                    kwargs[key] = getattr(stat, key)
            return Struct(**kwargs)

        class MockOS(object):

            def __getattribute__(self, attr):
                if attr == 'statvfs':
                    return mock_statvfs
                else:
                    return getattr(os, attr)

        with patch(disk, 'os', MockOS()):
            with temp_client() as conn:
                self.assertRaises(disk.ClientException, conn.head_account)
                try:
                    conn.head_account()
                except disk.ClientException, e:
                    self.assert_('no free space' in str(e).lower())

    def test_slashes_in_objects(self):
        with temp_client() as conn:
            conn.put_container('ocean')
            body = 'message'
            conn.put_object('ocean', 'foo/1', body)
            self.assertEquals('message', conn.get_object('ocean', 'foo/1')[1])

    def test_head_container(self):
        with temp_client() as conn:
            conn.put_container('ocean')
            body = '1234567890'
            conn.put_object('ocean', '1', body)
            conn.put_object('ocean', 'foo/2', body)
            conn.put_object('ocean', 'foo/bar/3', body)
            head = conn.head_container('ocean')
            self.assertEquals(head['x-container-object-count'], 3)
            self.assertEquals(head['x-container-bytes-used'], 30)

    def test_delete_container(self):
        with temp_client() as conn:
            conn.put_container('ocean')
            body = '1234567890'
            conn.put_object('ocean', '1', body)
            self.assertRaises(disk.ClientException,
                              conn.delete_container, 'ocean')
            conn.delete_object('ocean', '1')
            conn.delete_container('ocean')
            self.assertRaises(disk.ClientException,
                              conn.get_container, 'ocean')


if __name__ == "__main__":
    unittest.main()
