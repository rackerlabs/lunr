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


from lunr.common.lock import LockFile, JsonLockFile, ResourceFile
from tempfile import NamedTemporaryFile
import unittest
import os


class TestLockFile(unittest.TestCase):

    def test_lock(self):
        with NamedTemporaryFile('rw') as file:
            with LockFile(file.name) as lock:
                lock.write('test')
                self.assertEquals(lock.read(), 'test')

            with LockFile(file.name) as lock:
                self.assertEquals(lock.read(), 'test')

    def test_json_lock(self):
        with NamedTemporaryFile('rw') as file:
            with JsonLockFile(file.name) as lock:
                lock.write({'test': 1, 'foo': 'bar'})
                self.assertEquals(lock.read(), {'test': 1, 'foo': 'bar'})

    def test_resource_lock(self):
        with NamedTemporaryFile('rw') as file:
            with ResourceFile(file.name) as lock:
                self.assertEquals(lock.used(), False)
                lock.acquire({'foo': 'bar', 'pid': os.getpid()})

            # Used should return a dict
            with ResourceFile(file.name) as lock:
                info = lock.used()
                self.assertEquals(info['foo'], 'bar')
                self.assertEquals(info['pid'], os.getpid())
