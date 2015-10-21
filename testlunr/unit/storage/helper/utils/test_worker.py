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
import multiprocessing
import os
from tempfile import mkdtemp
from shutil import rmtree
from time import sleep
import json

from lunr.common.config import LunrConfig
from lunr.common.lock import JsonLockFile
from lunr.storage.helper.utils import get_conn
from lunr.storage.helper.utils.client.memory import ClientException, reset
from lunr.storage.helper.utils.manifest import Manifest, save_manifest
from lunr.storage.helper.utils.worker import Worker, SaveProcess,\
    StatsSaveProcess, RestoreProcess, StatsRestoreProcess, Block


class MockCinder(object):
    def __init__(self):
        self.snapshot_progress_called = 0
        self.update_volume_metadata_called = 0

    def snapshot_progress(self, *args, **kwargs):
        self.snapshot_progress_called += 1

    def update_volume_metadata(self, *args, **kwargs):
        self.update_volume_metadata_called += 1


class TestStatsRestoreProcess(unittest.TestCase):
    def setUp(self):
        self.cinder = MockCinder()
        self.scratch = mkdtemp()
        self.stats_path = os.path.join(self.scratch, 'stats')
        self.stat_queue = multiprocessing.Queue()
        with JsonLockFile(self.stats_path) as lock:
            self.stats_lock = lock
        self.volume_id = 'volume_id'
        self.block_count = 10
        self.process = StatsRestoreProcess(
            self.cinder, self.volume_id, self.stat_queue,
            self.block_count, self.stats_lock, update_interval=1)
        self.process.start()

    def tearDown(self):
        rmtree(self.scratch)
        self.assertFalse(self.process.is_alive())

    def test_restored(self):
        blocks_restored = 3
        for i in xrange(blocks_restored):
            task = ('restored', 1)
            self.stat_queue.put(task)
        self.stat_queue.put(None)
        while self.process.is_alive():
            sleep(0.1)
        with open(self.stats_path) as f:
            stats = json.loads(f.read())
            self.assertEqual(stats['block_count'], self.block_count)
            self.assertEqual(stats['blocks_restored'], blocks_restored)
            percent = 3 * 100.0 / 10
            self.assertEqual(stats['progress'], percent)


class TestStatsSaveProcess(unittest.TestCase):
    def setUp(self):
        self.cinder = MockCinder()
        self.scratch = mkdtemp()
        self.stats_path = os.path.join(self.scratch, 'stats')
        self.stat_queue = multiprocessing.Queue()
        with JsonLockFile(self.stats_path) as lock:
            self.stats_lock = lock
        self.backup_id = 'backup_id'
        self.block_count = 10
        self.process = StatsSaveProcess(
            self.cinder, self.backup_id, self.stat_queue,
            self.block_count, self.stats_lock, update_interval=1)
        self.process.start()

    def tearDown(self):
        rmtree(self.scratch)
        self.assertFalse(self.process.is_alive())

    def test_read(self):
        blocks_read = 8
        for i in xrange(blocks_read):
            task = ('read', 1)
            self.stat_queue.put(task)
        self.stat_queue.put(None)
        while self.process.is_alive():
            sleep(0.1)
        with open(self.stats_path) as f:
            stats = json.loads(f.read())
            self.assertEqual(stats['blocks_read'], blocks_read)
            self.assertEqual(stats['block_count'], self.block_count)
            self.assertEqual(stats['upload_count'], self.block_count)
            self.assertEqual(stats['blocks_uploaded'], 0)
            percent = (8 + 0) * 100.0 / (10 + 10)
            self.assertEqual(stats['progress'], percent)

    def test_uploaded(self):
        blocks_uploaded = 3
        for i in xrange(blocks_uploaded):
            task = ('uploaded', 1)
            self.stat_queue.put(task)
        self.stat_queue.put(None)
        while self.process.is_alive():
            sleep(0.1)
        with open(self.stats_path) as f:
            stats = json.loads(f.read())
            self.assertEqual(stats['blocks_read'], 0)
            self.assertEqual(stats['block_count'], self.block_count)
            self.assertEqual(stats['upload_count'], self.block_count)
            self.assertEqual(stats['blocks_uploaded'], blocks_uploaded)
            percent = (0 + 3) * 100.0 / (10 + 10)
            self.assertEqual(stats['progress'], percent)

    def test_upload_count(self):
        upload_count = 7
        task = ('upload_count', upload_count)
        self.stat_queue.put(task)
        blocks_uploaded = 3
        for i in xrange(blocks_uploaded):
            task = ('uploaded', 1)
            self.stat_queue.put(task)
        self.stat_queue.put(None)
        while self.process.is_alive():
            sleep(0.1)
        with open(self.stats_path) as f:
            stats = json.loads(f.read())
            self.assertEqual(stats['blocks_read'], 0)
            self.assertEqual(stats['block_count'], self.block_count)
            self.assertEqual(stats['upload_count'], upload_count)
            self.assertEqual(stats['blocks_uploaded'], 3)
            percent = (0 + 3) * 100.0 / (10 + 7)
            self.assertEqual(stats['progress'], percent)


class TestSaveProcess(unittest.TestCase):
    def setUp(self):
        self.block_queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.stat_queue = multiprocessing.Queue()
        self.volume_id = 'volume_id'
        self.scratch = mkdtemp()
        backup_path = os.path.join(self.scratch, 'backups')
        self.conf = LunrConfig({
                            'backup': {'client': 'disk'},
                            'disk': {'path': backup_path},
                    })
        self.conn = get_conn(self.conf)
        self.conn.put_container(self.volume_id)
        self.process = SaveProcess(self.conf, self.volume_id,
                                   self.block_queue, self.result_queue,
                                   self.stat_queue)
        self.process.start()

    def tearDown(self):
        rmtree(self.scratch)
        self.assertFalse(self.process.is_alive())

    def test_upload(self):
        dev = '/dev/zero'
        salt = 'salt'

        block_count = 3
        for i in xrange(block_count):
            block = Block(dev, i, salt)
            # Lie about the hash.
            block._hydrate()
            hash_ = "hash_%s" % i
            block._hash = hash_
            self.block_queue.put(block)

        self.block_queue.put(None)
        while self.process.is_alive():
            sleep(0.1)

        stats, errors = self.result_queue.get()
        self.assertEquals(stats['uploaded'], block_count)
        self.assertEquals(len(errors.keys()), 0)
        headers, listing = self.conn.get_container(self.volume_id)
        self.assertEquals(len(listing), block_count)


class TestWorker(unittest.TestCase):

    def setUp(self):
        reset()
        self.scratch = mkdtemp()

    def tearDown(self):
        rmtree(self.scratch)

    def test_salt_empty_blocks(self):
        vol1 = 'vol1'
        vol2 = 'vol2'
        manifest1 = Manifest()
        manifest2 = Manifest()
        conf = LunrConfig({'backup': {'client': 'memory'}})
        worker1 = Worker(vol1, conf, manifest1)
        worker2 = Worker(vol1, conf, manifest2)
        self.assert_(worker1.manifest.salt != worker2.manifest.salt)
        self.assert_(worker1.empty_block_hash != worker2.empty_block_hash)
        self.assertEquals(worker1.empty_block, worker2.empty_block)

    def test_delete_with_missing_blocks(self):
        stats_path = os.path.join(self.scratch, 'stats')
        manifest = Manifest.blank(2)
        worker = Worker('foo',
                        LunrConfig({
                            'backup': {'client': 'memory'},
                            'storage': {'run_dir': self.scratch}
                        }),
                        manifest=manifest)
        conn = worker.conn
        conn.put_container('foo')
        backup = manifest.create_backup('bak1')
        backup[0] = worker.empty_block_hash
        backup[1] = 'some_random_block_that_isnt_uploaded'
        save_manifest(manifest, conn, worker.id, worker._lock_path())
        obj = conn.get_object('foo', 'manifest', newest=True)
        self.assertRaises(ClientException, conn.get_object,
                          'foo', backup[0], newest=True)
        self.assertRaises(ClientException, conn.get_object,
                          'foo', backup[1], newest=True)
        # Shouldn't blow up on 404.
        worker.delete('bak1')
        # Manifest should still be nicely deleted.
        self.assertRaises(ClientException, conn.get_object,
                          'foo', 'manifest', newest=True)

    def test_audit(self):
        manifest = Manifest.blank(2)
        worker = Worker('foo',
                        LunrConfig({
                            'backup': {'client': 'memory'},
                            'storage': {'run_dir': self.scratch}
                        }),
                        manifest=manifest)
        conn = worker.conn
        conn.put_container('foo')
        backup = manifest.create_backup('bak1')
        backup[0] = worker.empty_block_hash
        conn.put_object('foo', backup[0], 'zeroes')
        backup[1] = 'some_block_hash'
        conn.put_object('foo', backup[1], ' more stuff')
        save_manifest(manifest, conn, worker.id, worker._lock_path())
        # Add some non referenced blocks.
        conn.put_object('foo', 'stuff1', 'unreferenced stuff1')
        conn.put_object('foo', 'stuff2', 'unreferenced stuff2')
        conn.put_object('foo', 'stuff3', 'unreferenced stuff3')

        _headers, original_list = conn.get_container('foo')
        # Manifest, 2 blocks, 3 stuffs.
        self.assertEquals(len(original_list), 6)

        worker.audit()
        _headers, new_list = conn.get_container('foo')
        # Manifest, 2 blocks.
        self.assertEquals(len(new_list), 3)

    def test_save_stats(self):
        manifest = Manifest.blank(2)
        stats_path = os.path.join(self.scratch, 'statsfile')
        worker = Worker('foo',
                        LunrConfig({
                            'backup': {'client': 'memory'},
                            'storage': {'run_dir': self.scratch}
                        }),
                        manifest=manifest,
                        stats_path=stats_path)
        conn = worker.conn
        conn.put_container('foo')

        worker.save('/dev/zero', 'backup_id', timestamp=1)

        try:
            with open(stats_path) as f:
                json.loads(f.read())
        except ValueError:
            self.fail("stats path does not contain valid json")


if __name__ == "__main__":
    unittest.main()
