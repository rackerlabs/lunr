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


from lunr.storage.helper.utils import directio
from lunr.storage.helper.export import ExportHelper, NotFound
from lunr.storage.helper.volume import VolumeHelper
from testlunr.integration import IetTest
from tempfile import mkdtemp
from uuid import uuid4

import shutil
import pprint


class MockResourceLock(object):
    def remove(self):
        pass


class MockCinder(object):
    def __init__(self):
        self.called = 0

    def update_volume_metadata(self, volume_id, metadata):
        print "metadata: ", metadata
        self.called += 1


class TestClone(IetTest):

    def setUp(self):
        IetTest.setUp(self)
        self.tempdir = mkdtemp()
        self.conf = self.config(self.tempdir)
        self.volume = VolumeHelper(self.conf)
        self.export = ExportHelper(self.conf)

    def tearDown(self):
        try:
            # Remove any exports
            self.export.delete(self.volume2)
        except NotFound:
            pass

        shutil.rmtree(self.tempdir)
        IetTest.tearDown(self)

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_create_clone(self):
        block_size = 32768

        # Create 2 Volumes
        self.volume1 = str(uuid4())
        self.volume2 = str(uuid4())
        self.volume.create(self.volume1)
        self.volume.create(self.volume2)

        # Write some stuff to volume1
        volume = self.volume.get(self.volume1)
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            for i in xrange(0, size / block_size):
                # 32768 / 4 = 8192
                file.write('ZERG' * (block_size / 4))

        # Create an export for volume 2
        info = self.export.create(self.volume2)

        # Now clone Volume 1 to Volume 2
        clone = self.volume.create_clone(self.volume1, self.volume2,
                                         info['name'], '127.0.0.1',
                                         3260, lock=MockResourceLock())

        compare_block = 'ZERG' * (block_size / 4)
        # Ensure the stuff we wrote to Volume 1 is in Volume 2
        volume = self.volume.get(self.volume2)
        with directio.open(volume['path']) as file:
            size = directio.size(volume['path'])
            for i in xrange(0, size / block_size):
                block = file.read(block_size)
                self.assertTrue(block == compare_block)

        # Remove the export
        self.export.delete(self.volume2)

    def test_cinder_callback(self):
        block_size = 32768

        # Create 2 Volumes
        self.volume1 = str(uuid4())
        self.volume2 = str(uuid4())
        self.volume.create(self.volume1)
        self.volume.create(self.volume2)

        # Create an export for volume 2
        info = self.export.create(self.volume2)

        cinder = MockCinder()
        # Now clone Volume 1 to Volume 2
        clone = self.volume.create_clone(self.volume1, self.volume2,
                                         info['name'], '127.0.0.1',
                                         3260, lock=MockResourceLock(),
                                         cinder=cinder)

        # Assert cinder was called atleast once
        self.assertTrue(cinder.called != 0)
        # Remove the export
        self.export.delete(self.volume2)
