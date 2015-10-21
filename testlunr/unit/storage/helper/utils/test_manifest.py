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
from collections import defaultdict
from StringIO import StringIO
from time import time as _orig_time
from random import randint
import json

from testlunr.unit import temp_disk_file
from lunr.storage.helper.utils import manifest


def mock_time(start=_orig_time()):
    """
    Create a callable that will return a float that resembals a unix timestamp.
    The timestamps returned will start at the given timestamp and subsequent
    calls to the returned callable object will increase exponentially.

    :params start: a timestamp as a float

    :returns: a callable which cause be used as a replacement for time.time()
    """
    def count(start):
        growth = 1
        while True:
            yield start
            start += growth
            growth *= 2
    # start counter
    counter = count(start)

    def mock():
        # each call yields one value off the generator
        return counter.next()
    return mock


class ManifestTestCase(unittest.TestCase):

    VERSION = '1.0'

    def setUp(self):
        start = _orig_time()
        manifest.time = mock_time(start)
        self.time = mock_time(start)

    def tearDown(self):
        manifest.time = _orig_time


class TestManifest(ManifestTestCase):

    def test_create_manifest(self):
        size = 10
        m = manifest.Manifest()
        self.assert_(isinstance(m, dict))
        self.assert_(isinstance(m, manifest.Manifest))
        self.assertEquals(m.version, self.VERSION)
        self.assertEquals(m['version'], self.VERSION)
        self.assert_(isinstance(m.salt, basestring))
        self.assert_(len(m.salt) > 0)
        # can't calculate m.block_count until you set base
        self.assertFalse(hasattr(m, 'block_count'))
        expected = [manifest.EMPTY_BLOCK for b in xrange(size)]
        m.base = list(expected)
        self.assertEquals(m.block_count, size)
        self.assertEquals(m.replay(), expected)

    def test_create_populate_empty_base(self):
        size = 10
        m = manifest.Manifest()
        # can't populate m.base until you set block_count
        self.assertFalse(hasattr(m, 'base'))
        m.block_count = size
        expected = [manifest.EMPTY_BLOCK for b in xrange(size)]
        self.assertEquals(m.base, expected)
        self.assertEquals(m.replay(), expected)

    def test_create_blank_manifest(self):
        size = 100
        m = manifest.Manifest.blank(size)
        self.assertEquals(m.backups, {})
        self.assertEquals(m.block_count, size)

    def test_create_backup(self):
        size = 10
        m = manifest.Manifest.blank(size)
        backup = m.create_backup('id0')
        volume = ['0000'] * size
        for blockno, block in enumerate(volume):
            backup[blockno] = block
        self.assertEquals(m.base, volume)
        self.assertEquals(m.get_backup('id0'), volume)
        # mix it up a bit
        volume[randint(0, size-1)] = '1111'
        backup = m.create_backup('id1')
        for blockno, block in enumerate(volume):
            backup[blockno] = block
        self.assertEquals(m.get_backup('id1'), volume)
        self.assertEquals(m.get_backup('id0'), ['0000'] * size)

    def test_replay(self):
        size = 100
        m = manifest.Manifest.blank(size)
        # create initial backup
        base = ['0000'] * size
        backup = m.create_backup('base')
        for blockno, block in enumerate(base):
            backup[blockno] = block
        base_ts = self.time()

        # create backup1
        backup1 = {1: '1111', 2: '1111'}
        backup = m.create_backup('backup1')
        for blockno, block in backup1.items():
            backup[blockno] = block
        backup1_ts = self.time()

        backup2 = {1: '2222'}
        backup = m.create_backup('backup2')
        for blockno, block in backup2.items():
            backup[blockno] = block
        backup2_ts = self.time()

        backup3 = {3: '3333'}
        backup = m.create_backup('backup3')
        for blockno, block in backup3.items():
            backup[blockno] = block
        backup3_ts = self.time()

        expected = list(base)
        self.assertEquals(m.replay(0), expected)
        self.assertEquals(m.replay(base_ts), expected)
        # update expected with first backup
        for blockno, block in backup1.items():
            expected[blockno] = block
        # replay to first timestamp
        self.assertEquals(m.replay(backup1_ts), expected)
        # update expected with second backup
        for blockno, block in backup2.items():
            expected[blockno] = block
        # replay to second timestamp
        self.assertEquals(m.replay(backup2_ts), expected)
        # test middle timestamps land on closest diff without going over
        self.assertEquals(m.replay(backup3_ts - 1), expected)
        # update expected with third backup
        for blockno, block in backup3.items():
            expected[blockno] = block
        # replay to third timestamp
        self.assertEquals(m.replay(backup3_ts), expected)
        # test MOST_RECENT same as last timestamp
        self.assertEquals(m.replay(), expected)

    def test_delete_backup(self):
        size = 100
        m = manifest.Manifest.blank(size)
        # create initial backup
        base = ['0000'] * size
        backup = m.create_backup('base')
        for blockno, block in enumerate(base):
            backup[blockno] = block
        base_ts = self.time()

        # create backup1
        backup1 = {1: '1111', 2: '1111'}
        backup = m.create_backup('backup1')
        for blockno, block in backup1.items():
            backup[blockno] = block
        backup1_ts = self.time()

        backup2 = {1: '2222'}
        backup = m.create_backup('backup2')
        for blockno, block in backup2.items():
            backup[blockno] = block
        backup2_ts = self.time()

        backup3 = {3: '3333'}
        backup = m.create_backup('backup3')
        for blockno, block in backup3.items():
            backup[blockno] = block
        backup3_ts = self.time()

        # replay all backups
        expected = list(base)
        for backup in (backup1, backup2, backup3):
            for blockno, block in backup.items():
                expected[blockno] = block
        # delete base
        m.delete_backup('base')
        self.assert_('base' not in m.backups)
        self.assertEquals(m.replay(), expected)

        # replay all backups 1 and 2
        expected = list(base)
        for backup in (backup1, backup2):
            for blockno, block in backup.items():
                expected[blockno] = block
        # delete backup3
        m.delete_backup('backup3')
        self.assert_('backup3' not in m.backups)
        self.assertEquals(m.replay(), expected)


class MockConnection(object):

    def __init__(self):
        self.data = defaultdict(dict)

    def get_object(self, container, object_id, **kwargs):
        try:
            return {}, self.data[container][object_id]
        except KeyError:
            raise Exception('404')

    def put_object(self, container, object_id, body):
        self.data[container][object_id] = str(body)


class TestJSON(ManifestTestCase):

    def test_basic_store(self):
        size = 2
        m = manifest.Manifest.blank(size)
        backup = m.create_backup('id0')
        for blockno in range(size):
            backup[blockno] = '0000'
        backup = m.create_backup('id1')
        for blockno in range(0, size, 10):
            backup[blockno] = '1111'

        c = MockConnection()
        with temp_disk_file() as lock_file:
            manifest.save_manifest(m, c, 'vol1', lock_file)

            expected = dict(m)
            vol1 = m.get_backup('id1')

            m = manifest.load_manifest(c, 'vol1', lock_file)
            self.assertEquals(m, expected)
            self.assertEquals(m.get_backup('id1'), vol1)
            self.assertEquals(m.version, self.VERSION)

    def test_version_mismatch(self):
        m = manifest.Manifest.blank(1)
        c = MockConnection()
        stuff = {
            0: [],
            'backups': {},
            'version': '2.0'
        }
        c.put_object('vol1', 'manifest', json.dumps(stuff))
        with temp_disk_file() as lock_file:
            self.assertRaises(manifest.ManifestVersionError,
                              manifest.load_manifest, c, 'vol1', lock_file)

    def test_load_salt(self):
        m = manifest.Manifest.blank(1)
        c = MockConnection()
        salt = 'salty!'
        stuff = {
            0: [],
            'backups': {},
            'version': '1.0',
            'salt': salt
        }
        c.put_object('vol1', 'manifest', json.dumps(stuff))
        with temp_disk_file() as lock_file:
            m = manifest.load_manifest(c, 'vol1', lock_file)
        self.assertEquals(m.salt, salt)

    def test_load_default_salt(self):
        m = manifest.Manifest.blank(1)
        c = MockConnection()
        salt = 'salty!'
        stuff = {
            0: [],
            'backups': {},
            'version': '1.0'
        }
        c.put_object('vol1', 'manifest', json.dumps(stuff))
        with temp_disk_file() as lock_file:
            m = manifest.load_manifest(c, 'vol1', lock_file)
        self.assertEquals(m.salt, '')

    def test_integer_backup_id(self):
        size = 2
        m = manifest.Manifest.blank(size)
        backup = m.create_backup(0)
        for blockno in range(size):
            backup[blockno] = '0000'
        backup = m.create_backup(1)
        for blockno in range(0, size, 10):
            backup[blockno] = '1111'
        backup = m.create_backup('id2')
        for blockno in range(0, size, 10):
            backup[blockno] = '2222'

        c = MockConnection()
        with temp_disk_file() as lock_file:
            manifest.save_manifest(m, c, 'vol1', lock_file)

            expected = dict(m)
            vol1 = m.get_backup(1)
            vol1_str = m.get_backup('1')
            vol2 = m.get_backup('id2')

            m = manifest.load_manifest(c, 'vol1', lock_file)
            self.assertEquals(m, expected)

            self.assertEquals(m.get_backup(1), vol1)
            self.assertEquals(m.get_backup('1'), vol1_str)
            self.assertEquals(vol1, vol1_str)
            self.assertEquals(m.get_backup('id2'), vol2)


if __name__ == "__main__":
    unittest.main()
