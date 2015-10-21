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
from tempfile import mkdtemp
from shutil import rmtree
import os

from lunr.storage.helper.utils.worker import Block


class TestBlock(unittest.TestCase):

    def setUp(self):
        self.scratch = mkdtemp()

    def tearDown(self):
        rmtree(self.scratch)

    def test_stats(self):
        size = 4 * 1024 * 1024
        path = os.path.join(self.scratch, 'block_dev1')
        with open(path, 'w') as f:
            f.write('\x00' * size)

        b = Block(path, 0, 'salt')
        self.assertEqual({}, b.stats)
        b.skipped()
        self.assertEqual(b.stats['skipped'], 1)
        b.ignored()
        self.assertEqual(b.stats['ignored'], 1)
        b.hash
        self.assert_(b.stats['read'] > 0)
        self.assert_(b.stats['hash'] > 0)


if __name__ == "__main__":
    unittest.main()
