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

import unittest
from contextlib import contextmanager
from tempfile import mkdtemp
from shutil import rmtree
import os
import sys
import json
import fcntl
from time import sleep

import logging

from lunr.storage.helper.utils import jobs, ServiceUnavailable
from lunr.storage.helper.utils.manifest import Manifest
from lunr.storage.helper.utils.worker import Block
from lunr.storage.helper import backup
from lunr.storage.helper.utils.client import get_conn
from lunr.storage.helper.utils.client import memory
from lunr.common.config import LunrConfig
from lunr.storage.helper.utils import NotFound
from testlunr.unit import MockResourceLock, patch
from lunr.storage.helper.utils.worker import BlockReadFailed


logging.basicConfig()


# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=False)


LOCKS = {}


def _spawn(lock_file, job, *args, **kwargs):
    def run_job():
        LOCKS[lock_file] = True
        try:
            callback = kwargs.pop('callback', lambda: None)
            error_callback = kwargs.pop('error_callback', lambda: None)
            skip_fork = kwargs.pop('skip_fork', None)
            try:
                job(*args, **kwargs)
            except:
                error_callback()
                return 1
            callback()
        finally:
            try:
                del LOCKS[lock_file]
            except KeyError:
                pass
    if lock_file in LOCKS:
        raise jobs.JobAlreadyRunningError()
    _spawn.run_job = run_job
    return


@contextmanager
def mock_spawn():
    _orig_spawn = backup.spawn
    try:
        backup.spawn = _spawn
        yield _spawn
    finally:
        backup.spawn = _orig_spawn


class TestBackupHelper(unittest.TestCase):

    def setUp(self):
        memory.reset()
        self.scratch = mkdtemp()
        self.run_dir = os.path.join(self.scratch, 'run')
        self.backup_dir = os.path.join(self.scratch, 'backups')
        os.mkdir(self.backup_dir)
        self.conf = LunrConfig({
            'storage': {'run_dir': self.run_dir, 'skip_fork': True},
            'backup': {'client': 'disk'},
            'disk': {'path': self.backup_dir}
        })

    def tearDown(self):
        rmtree(self.scratch)

    def test_get(self):
        snapshot = {
            'id': 'test_snapshot',
            'origin': 'test_volume'
        }
        backup_id = 'test_backup'
        info = {
            'asdf': 42,
            'id': backup_id,
            'pid': os.getpid(),
        }
        h = backup.BackupHelper(self.conf)
        lock_file = h._resource_file(snapshot['id'])
        spawning_dir = os.path.dirname(lock_file)
        os.makedirs(spawning_dir)
        with open(lock_file, "w") as f:
            f.write(json.dumps(info))
        # self.assertRaises(NotFound, h.get, snapshot, backup_id)
        backup_info = h.get(snapshot, backup_id)
        self.assertEquals(backup_info['lock'], lock_file)
        self.assertEquals(backup_info['status'], 'RUNNING')

    def test_get_with_junk_info(self):
        snapshot = {
            'id': 'test_snapshot',
            'origin': 'test_volume'
        }
        backup_id = 'test_backup'
        h = backup.BackupHelper(self.conf)
        lock_file = h._resource_file(snapshot['id'])
        spawning_dir = os.path.dirname(lock_file)
        os.makedirs(spawning_dir)
        with open(lock_file, "w") as f:
            f.write('THIS IS NOT JSON')
        self.assertRaises(NotFound, h.get, snapshot, backup_id)

    def test_create_first_backup_for_new_volume(self):
        h = backup.BackupHelper(self.conf)

        def callback():
            callback.ran = True

        snapshot = {
            'id': 'bak1',
            'timestamp': 1.0,
        }
        snapshot['path'] = os.path.join(self.scratch, 'bak1')
        snapshot['origin'] = 'vol1'
        snapshot['size'] = 4 * 1024 * 1024
        with open(snapshot['path'], 'w') as f:
            f.write('\x00' * snapshot['size'])

        backup_id = 'backup1'

        with mock_spawn() as j:
            h.create(snapshot, backup_id, callback)
            j.run_job()
            self.assert_(callback.ran)

        conn = get_conn(self.conf)
        _headers, raw_json_string = conn.get_object('vol1',
                                                    'manifest', newest=True)
        m = Manifest.loads(raw_json_string)
        self.assertEquals(m.block_count, 1)
        self.assertEquals(m.backups['backup1'], 1.0)
        self.assertEquals(m.history, [1.0])
        self.assert_(isinstance(m[m.history[0]], list))
        stats_path = h._stats_file('vol1')
        self.assertFalse(os.path.exists(stats_path))

    def test_create_fail_ioerror(self):
        h = backup.BackupHelper(self.conf)

        def callback():
            callback.ran = True
        callback.ran = False

        def error_callback():
            error_callback.ran = True
        error_callback.ran = False

        snapshot = {
            'id': 'bak1',
            'timestamp': 1.0,
        }
        snapshot['path'] = os.path.join(self.scratch, 'bak1')
        snapshot['origin'] = 'vol1'
        snapshot['size'] = 4 * 1024 * 1024
        with open(snapshot['path'], 'w') as f:
            f.write('\x00' * snapshot['size'])

        backup_id = 'backup1'

        def fake_hydrate(junk):
            raise BlockReadFailed("cant read!")

        with patch(Block, "_hydrate", fake_hydrate):
            h.create(snapshot, backup_id,
                     callback=callback, error_callback=error_callback,
                     lock=MockResourceLock())
            self.assertFalse(callback.ran)
            self.assertTrue(error_callback.ran)
        stats_path = h._stats_file('vol1')
        self.assertFalse(os.path.exists(stats_path))

    def test_create_first_backup_create_container(self):
        h = backup.BackupHelper(self.conf)
        conn = get_conn(self.conf)
        _orig_put_container = conn.put_container

        def mock_put_container(*args, **kwargs):
            # force the race
            sleep(0.2)
            mock_put_container.called.append(*args, **kwargs)
            _orig_put_container(*args, **kwargs)
        conn.put_container = mock_put_container
        mock_put_container.called = []
        with patch(backup, 'get_conn', lambda *args: conn):
            snapshot = {
                'id': 'bak1',
                'timestamp': 1.0,
            }
            snapshot['path'] = os.path.join(self.scratch, 'bak1')
            snapshot['origin'] = 'vol1'
            snapshot['size'] = 16 * 1024 ** 2
            with open(snapshot['path'], 'w') as f:
                f.write('\x00')
                f.seek(4 * 1024 ** 2, 1)
                f.write('\x01')
                f.seek(4 * 1024 ** 2, 1)
                f.write('\x02')

            backup_id = 'backup1'

            with mock_spawn() as j:
                h.create(snapshot, backup_id, lambda *args, **kwargs: None)
                j.run_job()
                # return doesn't matter, check it doesn't raise ClientException
                _headers, listing = conn.get_container(snapshot['origin'])
                # wrote 3 blocks + manifest.
                self.assertEquals(len(listing), 4)
                self.assertEquals(len(mock_put_container.called), 1)
        stats_path = h._stats_file('vol1')
        self.assertFalse(os.path.exists(stats_path))

    def test_status(self):
        self.conf = LunrConfig({
            'storage': {'run_dir': self.run_dir, 'skip_fork': True},
            'backup': {'client': 'memory'},
        })
        h = backup.BackupHelper(self.conf)
        expected = {'client': 'memory', 'containers': 0, 'objects': 0}
        self.assertEquals(h.status(), expected)

    def test_status_client_exception(self):
        h = backup.BackupHelper(self.conf)
        conn = get_conn(self.conf)

        def mock_head_account(*args, **kwargs):
            raise conn.ClientException('unable to connect')
        conn.head_account = mock_head_account
        with patch(backup, 'get_conn', lambda *args: conn):
            self.assertRaises(ServiceUnavailable, h.status)

    def test_prune_no_manifest(self):
        h = backup.BackupHelper(self.conf)
        volume = {'id': 'vol1', 'size': 1}
        backup_id = 'unused'
        h.prune(volume, backup_id)
        # Shouldn't blow up on missing mainfest.
        self.assert_(True)

    def test_prune_missing_backup_id(self):
        h = backup.BackupHelper(self.conf)
        volume = {'id': 'vol1', 'size': 1}
        existing_backup_id = 'backup1'
        missing_backup_id = 'something_else'
        m = Manifest.blank(volume['size'])
        b = m.create_backup(existing_backup_id, timestamp=1.0)
        for i in range(volume['size']):
            b[i] = '00'
        conn = get_conn(self.conf)
        conn.put_container('vol1')
        conn.put_object('vol1', '00', 'asdf')
        conn.put_object('vol1', 'manifest', m.dumps())
        h.prune(volume, missing_backup_id)
        # Shouldn't blow up on missing backup_id.
        self.assert_(True)

    def test_audit_no_manifest(self):
        h = backup.BackupHelper(self.conf)
        volume = {'id': 'vol1', 'size': 1}
        h.audit(volume)


if __name__ == "__main__":
    unittest.main()
