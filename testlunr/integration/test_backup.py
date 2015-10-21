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

from lunr.storage.helper.volume import VolumeHelper
from lunr.storage.helper.backup import BackupHelper
from lunr.storage.helper.utils import ServiceUnavailable, directio, execute
from lunr.storage.helper.utils.scrub import Scrub
from lunr.common.config import LunrConfig
from testlunr.integration import IetTest
from lunr.common import logger
from tempfile import mkdtemp
from uuid import uuid4
from os import path
import unittest
import shutil
import time
import sys
import re


# configure logging to log to console if nose was called with -s
logger.configure(log_to_console=('-s' in sys.argv), capture_stdio=False)


class MockResourceLock(object):
    def remove(self):
        pass


class TestBackupHelper(IetTest):

    def setUp(self):
        IetTest.setUp(self)
        self.tempdir = mkdtemp()
        self.conf = self.config(self.tempdir)
        self.volume = VolumeHelper(self.conf)
        self.backup = BackupHelper(self.conf)

    def tearDown(self):
        backup_dir = self.conf.string('disk', 'path', None)
        # Remove the temp dir where backups are created
        shutil.rmtree(self.tempdir)
        IetTest.tearDown(self)

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_create_snapshot(self):
        # Create a Volume
        volume_id = str(uuid4())
        self.volume.create(volume_id)
        # Create a snap-shot with a timestamp of 123456
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')

        # Assert snapshot values exist
        self.assertEquals(int(snapshot['timestamp']), 123456)
        self.assertEquals(snapshot['backup_id'], backup_id)
        self.assertEquals(snapshot['id'], backup_id)
        self.assertIn('size', snapshot)
        self.assertIn('path', snapshot)
        self.assertIn('origin', snapshot)
        self.assertTrue(path.exists(snapshot['path']))

        # Deleting the origin also removes the snapshot
        self.volume.remove(self.volume.get(volume_id)['path'])

    def test_delete_active_backup_origin_fails(self):
        # Create a Volume
        volume_id = str(uuid4())
        self.volume.create(volume_id)
        volume_id2 = str(uuid4())
        self.volume.create(volume_id2)
        # Create a snap-shot with a timestamp of 123456
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')
        # Ensure requests to delete the origin fail
        self.assertRaises(ServiceUnavailable, self.volume.delete, volume_id)
        # Should delete ok, no backup running
        self.volume.delete(volume_id2, lock=MockResourceLock())
        # Deleting the origin also removes the snapshot
        self.volume.remove(self.volume.get(volume_id)['path'])

    def test_delete_active_backup_origin_fails_is_isolated(self):
        first_vol_id = 'vol1'
        self.volume.create(first_vol_id)
        second_vol_id = 'vol11'  # contains 'vol1'
        self.volume.create(second_vol_id)
        backup_id = 'backup1'
        second_vol_snapshot = self.volume.create_snapshot(
            second_vol_id, backup_id)
        self.backup.create(second_vol_snapshot, 'backup1',
                           lock=MockResourceLock())
        # delete 'vol1' should not fail because of snapshot on 'vol11'
        self.volume.delete(first_vol_id, lock=MockResourceLock())
        # cleanup
        self.volume.delete(backup_id, lock=MockResourceLock())
        self.volume.delete(second_vol_id, lock=MockResourceLock())

    def test_snapshot_scrub(self):
        block_size = 32768
        # Create a Volume
        volume_id = str(uuid4())
        self.volume.create(volume_id)
        # Get the volume information
        volume = self.volume.get(volume_id)
        # Fill the volume with 'ZERG's
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            for i in xrange(0, size / block_size):
                # 32768 / 4 = 8192
                file.write('ZERG' * (block_size / 4))

        # Create a snap-shot with a timestamp of 123456
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')

        # Now that the snapshot is made, simulate users making writes
        # to the origin during a normal backup. This should generate
        # exceptions in the cow
        with directio.open(volume['path']) as file:
            # Overwrite all the zergs.
            for i in xrange(0, size / block_size):
                file.write('A' * block_size)

        # Tell scrub we don't want it to remove the cow after scrubbing
        scrub = Scrub(LunrConfig())

        # Build the cow-zero
        (cow_name, cow_path) = scrub.get_writable_cow(snapshot, volume)

        with directio.open(cow_path) as file:
            size = directio.size(cow_path)
            for i in xrange(0, size / block_size):
                block = file.read(block_size)
                if 'ZERG' in block:
                    self.assert_(True)
                    break

        with directio.open(self._ramdisk) as file:
            size = directio.size(self._ramdisk)
            for i in xrange(0, size / block_size):
                block = file.read(block_size)
                if 'ZERG' in block:
                    self.assert_(True)
                    break

        # Scrub the cow of all exceptions
        scrub.scrub_cow(cow_path)
        scrub.remove_cow(cow_name)

        # Remove & scrub the volume. LVM removes snapshot itself.
        self.volume.remove_lvm_volume(volume)

        # Read full disk for hidden zergs.
        with directio.open(self._ramdisk) as file:
            size = directio.size(self._ramdisk)
            for i in xrange(0, size / block_size):
                block = file.read(block_size)
                if 'ZERG' in block:
                    self.fail("Found zergs on disk: %s" % self._ramdisk)

    def test_writable_cow_multiline_table(self):
        # Let's do some silly math
        size = directio.size(self._ramdisk)
        megs = size / 1024 / 1024
        megs = megs - megs % 4
        # 12 megs for a volume, 4 for lvm itself
        alloc = megs - 12 - 4
        vg = self.conf.string('volume', 'volume_group', None)
        # Reserve a 4m hole at the front, and 8m at the end
        execute('lvcreate', vg, size='4m', name='tmpvol')
        execute('lvcreate', vg, size='%sm' % alloc, name='wasted')
        execute('lvremove', '%s/tmpvol' % vg, force=None)
        foo = execute('pvs', self._ramdisk)
        foo = execute('vgs', vg)
        foo = execute('lvs', vg)
        volume_id = str(uuid4())
        self.volume.create(volume_id)
        volume = self.volume.get(volume_id)
        execute('lvremove', '%s/wasted' % vg, force=None)
        dmname = '%s-%s' % (re.sub('-', '--', vg),
                            re.sub('-', '--', volume_id))
        foo = execute('dmsetup', 'table', dmname)
        self.assert_('\n' in foo)
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')
        scrub = Scrub(LunrConfig())
        (cow_name, cow_path) = scrub.get_writable_cow(snapshot, volume)
        execute('dmsetup', 'remove', cow_name)
        self.assertTrue(True)

    def test_create_backup(self):
        # Create a Volume
        volume_id = str(uuid4())
        self.volume.create(volume_id)
        # Create a snap-shot with a timestamp of 123456
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')

        def callback():
            # Delete the snapshot after completion
            self.volume.delete(snapshot['id'])

        # Create the backup
        self.backup.create(snapshot, backup_id,
                           callback=callback,
                           lock=MockResourceLock())

        # Assert the backup exists in the dir and has the
        # same name as the volume
        backup_dir = self.conf.string('disk', 'path', None)
        self.assertTrue(path.exists(path.join(backup_dir, volume_id)))

        # Deleting the origin also removes the snapshot
        self.volume.remove(self.volume.get(volume_id)['path'])

    def test_restore_backup(self):
        # Create a Volume
        volume_id = str(uuid4())
        self.volume.create(volume_id)

        # Write ZERG to the volume
        volume = self.volume.get(volume_id)
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            block_size = 32768
            for i in xrange(0, size / block_size):
                file.write('ZERG' * (block_size / 4))

        # Create a snap-shot with a timestamp of 123456
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')

        def callback():
            # Delete the snapshot after completion
            self.volume.delete(snapshot['id'])

        # Create the backup
        self.backup.create(snapshot, backup_id,
                           callback=callback,
                           lock=MockResourceLock())

        # Deleting the origin also removes the snapshot
        self.volume.remove(self.volume.get(volume_id)['path'])

        # Create a Restore Volume
        restore_volume_id = str(uuid4())
        self.volume.create(
            restore_volume_id, backup_source_volume_id=volume_id,
            backup_id=backup_id, lock=MockResourceLock())
        volume = self.volume.get(restore_volume_id)

        # Read the restored volume, it should contain ZERGS
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            for i in xrange(0, size / block_size):
                block = file.read(block_size)
                if 'ZERG' not in block:
                    self.fail("zergs missing on disk: %s" % volume['path'])

    def test_overflow_snapshot(self):
        # Better to use conf, but helper is already created.
        self.volume.max_snapshot_bytes = 4 * 1024 * 1024
        volume_id = str(uuid4())
        self.volume.create(volume_id)
        volume = self.volume.get(volume_id)
        backup_id = str(uuid4())
        snapshot = self.volume.create_snapshot(volume_id, backup_id, '123456')

        def callback():
            self.volume.delete(snapshot['id'])
            self.fail("didnt get the proper error callback")

        def error_callback():
            self.volume.delete(snapshot['id'])
            error_callback.ran = True
        error_callback.ran = False
        # Overflow the snapshot! Only reserved 4m
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            block_size = 32768
            for i in xrange(0, size / block_size):
                file.write('ZERG' * (block_size / 4))
        with open(snapshot['path']) as file:
            self.assertRaises(IOError, file.read, block_size)
        self.backup.create(
            snapshot, backup_id, callback=callback,
            error_callback=error_callback, lock=MockResourceLock())
        self.assertTrue(error_callback.ran)

        # Make sure scrubbing still happened correctly.
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            block_size = 32768
            for i in xrange(0, size / block_size):
                file.write('\0' * block_size)

        # Read full disk for hidden zergs.
        with directio.open(self._ramdisk) as file:
            size = directio.size(self._ramdisk)
            for i in xrange(0, size / block_size):
                block = file.read(block_size)
                if 'ZERG' in block:
                    self.fail("Found zergs on disk: %s" % self._ramdisk)


if __name__ == "__main__":
    unittest.main()
