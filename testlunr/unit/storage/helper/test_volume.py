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
import os
from collections import namedtuple
from tempfile import mkdtemp
from shutil import rmtree
import socket
from uuid import uuid4

from testlunr.unit.storage.helper.test_helper import BaseHelper
from testlunr.unit import patch, MockResourceLock

from lunr.common.config import LunrConfig
from lunr.storage.helper.utils import ProcessError, ServiceUnavailable, \
        AlreadyExists, NotFound, InvalidImage
from lunr.storage.helper.volume import encode_tag, decode_tag
from lunr.storage.helper import volume
from lunr.storage.helper.utils.glance import GlanceError

# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=False)


class TestVolumeHelper(unittest.TestCase):

    def setUp(self):
        self.orig_execute = volume.execute

    def tearDown(self):
        volume.execute = self.orig_execute

    def test_status(self):
        def mock_vgs(cmd, vg, **kwargs):
            mock_values = {
                'vg_size': '20000B',
                'vg_free': '10000B',
                'lv_count': '20',
            }
            values = []
            options = kwargs['options'].split(',')
            for key in options:
                values.append(mock_values[key])
            sep = kwargs.get('separator', ' ')
            out = '  ' + sep.join(values)
            if 'noheadings' not in kwargs:
                heading = '  ' + sep.join(options)
                out = '\n'.join([heading, out])
            return out

        volume.execute = mock_vgs
        h = volume.VolumeHelper(LunrConfig())
        status = h.status()
        expected = {
            'volume_group': 'lunr-volume',
            'vg_size': 20000,
            'vg_free': 10000,
            'lv_count': 20,
        }
        self.assertEquals(status, expected)

    def test_status_no_vg(self):
        def mock_vgs(cmd, vg, **kwargs):
            raise ProcessError('%s %s' % (cmd, vg), '',
                               '%s not found' % vg, 5)
        volume.execute = mock_vgs
        h = volume.VolumeHelper(LunrConfig())
        self.assertRaises(ServiceUnavailable, h.status)
        try:
            h.status()
        except ServiceUnavailable, e:
            self.assert_('not found' in str(e))
            self.assert_('lunr-volume' in str(e))

    def test_status_unknown_error(self):
        def mock_vgs(cmd, vg, **kwargs):
            raise ProcessError('%s %s' % (cmd, vg), '',
                               '%s is broken' % vg, -1)
        volume.execute = mock_vgs
        h = volume.VolumeHelper(LunrConfig())
        self.assertRaises(ServiceUnavailable, h.status)
        try:
            h.status()
        except ServiceUnavailable, e:
            self.assert_('-1' in str(e))
            self.assert_('is broken' in str(e))
            self.assert_('lunr-volume' in str(e))

    def test_volume_group_id_to_long(self):
        conf = LunrConfig({
                'volume': {
                    'volume_group': 'A' * 31
                }
            })
        helper = volume.VolumeHelper(conf)
        self.assertRaises(RuntimeError, helper.check_config)

    def test_max_snapshot_size(self):
        h = volume.VolumeHelper(LunrConfig())
        # One hundred gigs!
        vol_size = 100 * 1024 * 1024 * 1024
        max_snap = h._max_snapshot_size(vol_size)
        self.assertEquals(max_snap, 107793620992)

    def test_max_snapshot_bytes_flag(self):
        max_size = 4 * 1024 * 1024
        vol_size = 100 * 1024 * 1024 * 1024
        conf = LunrConfig({'volume': {'max_snapshot_bytes': max_size}})
        h = volume.VolumeHelper(conf)
        max_snap = h._max_snapshot_size(vol_size)
        self.assertEquals(max_snap, max_size)

    def test_max_snapshot_bytes_aligned(self):
        # Has to be a multiple of sector size or lvm chokes.
        sector_size = 423
        max_size = sector_size * 100 + 1  # 1 too many!
        vol_size = 100 * 1024 * 1024 * 1024
        conf = LunrConfig({'volume': {'max_snapshot_bytes': max_size,
                                      'sector_size': sector_size}})
        h = volume.VolumeHelper(conf)
        max_snap = h._max_snapshot_size(vol_size)
        self.assertEquals(max_snap, max_size - 1)  # We round down ourselves

    def test_old_mkfs(self):
        h = volume.VolumeHelper(LunrConfig())

        def mock_old_mkfs():
            version = """mke2fs 1.42 (29-Nov-2011)
    Using EXT2FS Library version 1.42
"""
            return version

        h._mkfs_version = mock_old_mkfs
        self.assertTrue(h.old_mkfs())

        def mock_new_mkfs():
            version = """mke2fs 1.42.9 (4-Feb-2014)
    Using EXT2FS Library version 1.42.9
"""
            return version

        h._mkfs_version = mock_new_mkfs
        self.assertFalse(h.old_mkfs())

        def mock_broken_mkfs():
            version = """notmke2fs 1.42.9 (4-Feb-2014)
    Using EXT2FS Library version 1.42.9"""
            return version

        h._mkfs_version = mock_broken_mkfs
        self.assertRaises(ValueError, h.old_mkfs)

        def mock_broken_mkfs():
            version = """mke2fs notaversion (4-Feb-2014)
    Using EXT2FS Library version 1.42.9"""
            return version

        h._mkfs_version = mock_broken_mkfs
        self.assertRaises(ValueError, h.old_mkfs)


class MockRequest(object):
    def get_method(self):
        return 'POST'

    def get_full_url(self):
        return 'https://localhost/'

    def getcode(self):
        return '500'


class MockGlance(object):
    def __init__(self, image_id=1, data=None, attempts=1):
        self.image_id = image_id
        self.data = data or []
        self.attempt = 0
        self.attempts = attempts

    def head(self, image_id):
        return {'id': self.image_id}

    def get(self, image_id):
        if self.attempt + 1 > self.attempts:
            raise GlanceError("No more attempts!")
        self.attempt += 1
        return self.data


class MockHeadBlowupGlance(object):
    def head(self, image_id):
        raise GlanceError('head kaboom')

    def get(self, image_id):
        return []


class MockGetBlowupGlance(object):
    def head(self, image_id):
        return MockImageHead(image_id, 42, 'vhd', 'ova', 10, {}, 'ACTIVE')

    def get(self, image_id):
        raise GlanceError('get kaboom')


class TestCreateConvertScratch(BaseHelper):
    def test_create(self):
        image = MockImageHead('someimage', 42, 'vhd', 'ova', 10, {}, 'ACTIVE')
        h = volume.VolumeHelper(self.conf)
        vol = h.create_convert_scratch(image, 0)
        self.assertIn('id', vol)
        self.assertEquals(vol['image_id'], 'someimage')
        # 12 MB
        self.assertEquals(vol['size'], 12582912)


class TestGetScratchMultiplier(BaseHelper):

    def test_custom(self):
        conf = LunrConfig()
        h = volume.VolumeHelper(conf)
        min_disk = 40
        image = MockImageHead('imgid', 'size', 'format', 'container',
                              'min_disk', {'image_type': 'notbase'}, 'ACTIVE')
        result = h._get_scratch_multiplier(image)
        self.assertEquals(result, 4)

    def test_custom_conf(self):
        multiplier = 42
        conf = LunrConfig(
            {'glance': {'custom_convert_multiplier': multiplier}})
        h = volume.VolumeHelper(conf)
        min_disk = 40
        image = MockImageHead('imgid', 'size', 'format', 'container',
                              'min_disk', {'image_type': 'notbase'}, 'ACTIVE')
        result = h._get_scratch_multiplier(image)
        self.assertEquals(result, multiplier)

    def test_base_default(self):
        conf = LunrConfig()
        h = volume.VolumeHelper(conf)
        min_disk = 40
        image = MockImageHead('imgid', 'size', 'format', 'container',
                              'min_disk', {'image_type': 'base'}, 'ACTIVE')

        result = h._get_scratch_multiplier(image)
        self.assertEquals(result, 2)

    def test_base_conf(self):
        multiplier = 1.5
        conf = LunrConfig({'glance': {'base_convert_multiplier': multiplier}})
        h = volume.VolumeHelper(conf)
        min_disk = 40
        image = MockImageHead('imgid', 'size', 'format', 'container',
                              'min_disk', {'image_type': 'base'}, 'ACTIVE')

        result = h._get_scratch_multiplier(image)

        self.assertEquals(result, multiplier)


class TestCreateFromImage(BaseHelper):
    def setUp(self):
        super(TestCreateFromImage, self).setUp()
        self.orig_get_glance_conn = volume.get_glance_conn

    def tearDown(self):
        super(TestCreateFromImage, self).tearDown()
        volume.get_glance_conn = self.orig_get_glance_conn

    def test_create_from_image(self):
        image_id = uuid4()
        data = 'A' * 4096
        glance_conns = []

        def glance_conn(conf, tenant_id, glance_urls=None):
            image = MockImage(image_id, len(data), data)
            glance = MockImageGlance(image, glance_urls)
            glance_conns.append(glance)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        sizes = []

        def mock_get_size(size):
            sizes.append(size)
            return '12M'
        h._get_size_str = mock_get_size
        volume_id = uuid4()

        def cinder_cb():
            cinder_cb.called = True
        cinder_cb.called = False

        def lunr_cb():
            lunr_cb.called = True
        lunr_cb.called = False
        h.create(volume_id, image_id=image_id, lock=self.lock,
                 callback=cinder_cb, scrub_callback=lunr_cb)
        v = h.get(volume_id)
        self.assert_(v)
        with open(v['path'], 'r') as f:
            # Raw
            stuff = f.read()
            self.assertEqual(data, stuff)
        # First one is none from the create.
        # Second one is from config convert_gbs
        self.assertEquals(sizes, [None, 100])
        self.assertEquals(len(glance_conns), 2)
        # We override the first one with the mgmt urls, but not the 2nd one.
        self.assertItemsEqual(glance_conns[0].urls, ['mgmt1', 'mgmt2'])
        self.assertEquals(glance_conns[1].urls, None)
        self.assertEquals(lunr_cb.called, True)
        self.assertEquals(cinder_cb.called, True)

    def test_create_from_image_too_big(self):
        image_id = uuid4()
        # >127 fails to convert
        min_disk = 128

        def glance_conn(conf, tenant_id, glance_urls=None):
            image = MockImage(image_id, len('data'), 'data', min_disk=min_disk)
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        volume_id = uuid4()
        self.assertRaises(InvalidImage, h.create, volume_id,
                          image_id=image_id, lock=self.lock)
        self.assertRaises(NotFound, h.get, volume_id)

    def test_create_from_nonactive_image(self):
        image_id = uuid4()

        def glance_conn(conf, tenant_id, glance_urls=None):
            image = MockImage(image_id, len('data'), 'data', status='UNKNOWN')
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        volume_id = uuid4()
        self.assertRaises(InvalidImage, h.create, volume_id,
                          image_id=image_id, lock=self.lock)
        self.assertRaises(NotFound, h.get, volume_id)

    def test_create_from_image_nonraw(self):
        image_id = uuid4()
        data = 'A' * 4096

        def glance_conn(conf, tenant_id, glance_urls=None):
            image = MockImage(image_id, len(data), data, disk_format='foo')
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        volume_id = uuid4()
        h.create(volume_id, image_id=image_id, lock=self.lock)
        v = h.get(volume_id)
        self.assert_(v)
        with open(v['path'], 'r') as f:
            # Raw
            stuff = f.read()
            self.assertIn('qemu-img', stuff)
            self.assertIn(v['path'], stuff)

    def test_create_from_image_head_error(self):

        def glance_conn(conf, tenant_id, glance_urls=None):
            return MockHeadBlowupGlance()
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        volume_id = uuid4()
        image_id = uuid4()
        self.assertRaises(InvalidImage, h.create, volume_id,
                          image_id=image_id, lock=self.lock)
        self.assertRaises(NotFound, h.get, volume_id)

    # What do we do if we can head the image but not get it?
    # Have orbit keeps retrying? Currently orbit doesn't.
    def test_create_from_image_get_error(self):

        def glance_conn(conf, tenant_id, glance_urls=None):
            return MockGetBlowupGlance()
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        volume_id = uuid4()
        image_id = uuid4()
        h.create(volume_id, image_id=image_id, lock=self.lock)

    def test_create_from_image_no_scratch_space(self):

        def mock_lvcreate_v1(cmd, vg, **kwargs):
            raise ProcessError('%s %s' % (cmd, vg), '',
                               'Insufficient free extents', 5)

        def glance_conn(conf, tenant_id, glance_urls=None):
            data = 'A' * 4096
            image = MockImage(image_id, len(data), data)
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        volume_id = uuid4()
        image_id = uuid4()
        with patch(volume, 'execute', mock_lvcreate_v1):
            self.assertRaises(ServiceUnavailable, h.create, volume_id,
                              image_id=image_id, lock=self.lock)

        def mock_lvcreate_v2(cmd, vg, **kwargs):
            raise ProcessError('%s %s' % (cmd, vg), '',
                               'insufficient free space', 5)
        with patch(volume, 'execute', mock_lvcreate_v2):
            self.assertRaises(ServiceUnavailable, h.create, volume_id,
                              image_id=image_id, lock=self.lock)

    def test_create_from_image_no_volume_space(self):
        # If we run out of space after allocating the scratch space,
        # we need to clean that up.
        volume_id = uuid4()
        image_id = uuid4()

        def glance_conn(conf, tenant_id, glance_urls=None):
            data = 'A' * 4096
            image = MockImage(image_id, len(data), data)
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)

        def mock_do_create(*args, **kwargs):
            raise ServiceUnavailable("Kaboom, out of space!")
        h._do_create = mock_do_create

        def mock_create_convert_scratch(image, size):
            return {'id': volume_id}
        h.create_convert_scratch = mock_create_convert_scratch

        def mock_remove(volume):
            mock_remove.called = True
            self.assertEquals(volume['id'], volume_id)
        mock_remove.called = False
        h.remove_lvm_volume = mock_remove
        self.assertRaises(ServiceUnavailable, h.create, volume_id,
                          image_id=image_id, lock=self.lock)
        self.assertEquals(mock_remove.called, True)

    def test_create_from_image_min_disk(self):
        image_id = uuid4()
        data = 'A' * 4096
        min_disk = 42

        def glance_conn(conf, tenant_id, glance_urls=None):
            image = MockImage(image_id, len(data), data, min_disk=min_disk)
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        sizes = []

        def mock_get_size(size):
            sizes.append(size)
            return '12M'
        h._get_size_str = mock_get_size
        volume_id = uuid4()
        h.create(volume_id, image_id=image_id, lock=self.lock)
        v = h.get(volume_id)
        self.assert_(v)
        with open(v['path'], 'r') as f:
            # Raw
            stuff = f.read()
            self.assertEqual(data, stuff)
        # First one is none from the create.
        # Second one is from image.min_disk * 4 (custom image)
        self.assertEquals(sizes, [None, min_disk * 4])

    def test_create_from_base_image_min_disk(self):
        image_id = uuid4()
        data = 'A' * 4096
        min_disk = 42

        def glance_conn(conf, tenant_id, glance_urls=None):
            image = MockImage(image_id, len(data), data, min_disk=min_disk,
                              properties={'image_type': 'base'})
            glance = MockImageGlance(image)
            return glance
        volume.get_glance_conn = glance_conn
        h = volume.VolumeHelper(self.conf)
        sizes = []

        def mock_get_size(size):
            sizes.append(size)
            return '12M'
        h._get_size_str = mock_get_size
        volume_id = uuid4()
        h.create(volume_id, image_id=image_id, lock=self.lock)
        v = h.get(volume_id)
        self.assert_(v)
        with open(v['path'], 'r') as f:
            # Raw
            stuff = f.read()
            self.assertEqual(data, stuff)
        # First one is none from the create.
        # Second one is from image.min_disk * 2 (base image)
        self.assertEquals(sizes, [None, min_disk * 2])


MockImageHead = namedtuple('MockImageHead', ['id',
                                             'size',
                                             'disk_format',
                                             'container_format',
                                             'min_disk',
                                             'properties',
                                             'status'])


class MockImage(object):

    def __init__(self, image_id, size, data, disk_format='raw',
                 container_format='raw', min_disk=0, properties=None,
                 status='ACTIVE'):
        if properties is None:
            properties = {}
        self.image_id = image_id
        self.head = MockImageHead(image_id, size, disk_format,
                                  container_format, min_disk, properties,
                                  status)
        self.data = data


class MockImageGlance(object):
    def __init__(self, image, urls=None):
        self.image = image
        self.urls = urls

    def head(self, image_id, glance_urls=None):
        if image_id == self.image.image_id:
            return self.image.head
        raise GlanceError('Image not found')

    def get(self, image_id, glance_urls=None):
        if image_id == self.image.image_id:
            return self.image.data
        raise GlanceError('Image not found')


class BlowupIterator(object):
    def __iter__(self):
        return self

    def next(self):
        raise socket.timeout("TIMEOUT!")


class TestWriteRawImage(BaseHelper):
    def setUp(self):
        super(TestWriteRawImage, self).setUp()
        self.helper = volume.VolumeHelper(self.conf)

    def tearDown(self):
        super(TestWriteRawImage, self).tearDown()

    def test_happy_path(self):
        image_id = 1
        image = MockImageHead(image_id, 3, 'raw', 'raw', 1, {}, 'ACTIVE')
        data = ['1', '2', '3']
        glance = MockGlance(image_id, data)
        dest = os.path.join(self.scratch, 'raw')
        self.helper.write_raw_image(glance, image, dest)
        with open(dest, 'r') as f:
            stuff = f.read(4096)
            self.assertEquals(stuff, ''.join(data))

    def test_glance_timeout(self):
        image_id = 1
        image = MockImageHead(image_id, 3, 'raw', 'raw', 1, {}, 'ACTIVE')
        data = BlowupIterator()
        attempts = 3
        glance = MockGlance(image_id, data, attempts=attempts)
        dest = os.path.join(self.scratch, 'raw')
        self.assertRaises(GlanceError, self.helper.write_raw_image, glance,
                          image, dest)
        self.assertEquals(glance.attempts, attempts)


class TestCopyImage(BaseHelper):
    def setUp(self):
        super(TestCopyImage, self).setUp()
        self.volume_id = uuid4()
        self.helper = volume.VolumeHelper(self.conf)
        self.helper.create(self.volume_id)
        self.volume = self.helper.get(self.volume_id)
        self.tmp_vol_id = 'tmp' + str(self.volume_id)
        self.helper.create(self.tmp_vol_id)
        self.tmp_vol = self.helper.get(self.tmp_vol_id)

    def tearDown(self):
        if self.volume_id:
            self.helper.delete(self.volume_id, lock=MockResourceLock())
        super(TestCopyImage, self).tearDown()

    def test_glance_error(self):
        glance = MockGetBlowupGlance()
        image = MockImage('unused_id', len(''), '', disk_format='')

        def scrub_cb():
            scrub_cb.called = True
        scrub_cb.called = False

        self.helper.copy_image(self.volume, image.head, glance, self.tmp_vol,
                               scrub_cb)
        self.assertRaises(NotFound, self.helper.get, self.volume_id)
        self.assertEquals(scrub_cb.called, True)
        self.volume_id = None

    def test_raw(self):
        image_id = uuid4()
        data = 'A' * 4096
        image = MockImage(image_id, len(data), data)
        glance = MockImageGlance(image)

        def scrub_cb():
            scrub_cb.called = True
        scrub_cb.called = False
        self.helper.copy_image(self.volume, image.head, glance, self.tmp_vol,
                               scrub_cb)
        with open(self.volume['path'], 'r') as f:
            stuff = f.read(4096)
            self.assertEquals(stuff, data)
        self.assertEquals(scrub_cb.called, True)

    def test_nonraw(self):
        image_id = uuid4()
        data = 'A' * 4096
        image = MockImage(image_id, len(data), data, disk_format='nonraw')
        glance = MockImageGlance(image)

        def scrub_cb():
            scrub_cb.called = True
        scrub_cb.called = False
        self.helper.copy_image(self.volume, image.head, glance, self.tmp_vol,
                               scrub_cb)
        with open(self.volume['path'], 'r') as f:
            # Fake qemu-img just writes to the volume
            stuff = f.read()
            self.assertIn('qemu-img', stuff)
            self.assertIn(self.volume['path'], stuff)
        self.assertEquals(scrub_cb.called, True)

    def test_oldstyle_vhd_ovf(self):
        image_id = uuid4()
        data = 'A' * 4096
        image = MockImage(image_id, len(data), data, disk_format='vhd',
                          container_format='ovf')
        glance = MockImageGlance(image)

        def mock_getsize(*args, **kwargs):
            return 1234567

        def mock_oldstyle_vhd(path):
            return 'oldstyle.vhd'

        def scrub_cb():
            scrub_cb.called = True
        scrub_cb.called = False
        with patch(self.helper, 'get_oldstyle_vhd', mock_oldstyle_vhd):
            with patch(os.path, 'getsize', mock_getsize):
                self.helper.copy_image(self.volume, image.head, glance,
                                       self.tmp_vol, scrub_cb)
        with open(self.volume['path'], 'r') as f:
            # Fake qemu-img just writes to the volume
            stuff = f.read()
            self.assertIn('qemu-img', stuff)
            self.assertIn(self.volume['path'], stuff)
        self.assertEquals(scrub_cb.called, True)

    def test_chain_vhd_ovf(self):
        image_id = uuid4()
        data = 'A' * 4096
        image = MockImage(image_id, len(data), data, disk_format='vhd',
                          container_format='ovf')
        glance = MockImageGlance(image)

        def mock_getsize(*args, **kwargs):
            return 1234567

        def mock_vhd_chain(path):
            return ['0.vhd', '1.vhd', '2.vhd', '3.vhd']

        def scrub_cb():
            scrub_cb.called = True
        scrub_cb.called = False
        with patch(self.helper, 'get_vhd_chain', mock_vhd_chain):
            with patch(os.path, 'getsize', mock_getsize):
                self.helper.copy_image(self.volume, image.head, glance,
                                       self.tmp_vol, scrub_cb)
        with open(self.volume['path'], 'r') as f:
            # Fake qemu-img just writes to the volume
            stuff = f.read()
            self.assertIn('qemu-img', stuff)
            self.assertIn(self.volume['path'], stuff)
        self.assertEquals(scrub_cb.called, True)


class TestVolumeNameEncoding(unittest.TestCase):

    def test_encode_tag(self):
        # normal volume
        self.assertEquals(encode_tag(), 'volume')
        # backup volume
        result = encode_tag(timestamp=1, backup_id='backup1')
        self.assertEquals(result, 'backup.1.backup1')
        # restore volume
        result = encode_tag(backup_source_volume_id='vol1',
                            backup_id='backup1')
        self.assertEquals(result, 'restore.vol1.backup1')
        # scrub volume
        result = encode_tag(zero=True)
        self.assertEquals(result, 'zero')
        # image conversion
        result = encode_tag(image_id='someimageid')
        self.assertEquals(result, 'convert.someimageid')

    def test_decode_tag(self):
        # normal volume
        self.assertEquals(decode_tag('volume'), {
                'volume': True
            })
        # backup volume
        self.assertEquals(decode_tag('backup.1.backup1'), {
                'backup_id': 'backup1',
                'timestamp': 1.0
            })
        # restore volume
        self.assertEquals(decode_tag('restore.vol1.backup1'), {
                'backup_id': 'backup1',
                'backup_source_volume_id': 'vol1',
            })
        # scrub volume
        self.assertEquals(decode_tag('zero'), {
                'zero': True,
            })
        # image conversion volume
        self.assertEquals(decode_tag('convert.someimageid'), {
                'image_id': 'someimageid',
            })

    def test_unknown_tag(self):
        self.assertEquals(decode_tag('blah'), {'volume': True})

    def test_encode_decode_are_inverse(self):
        tag = 'volume'
        volume = {'volume': True}
        # regular volume
        self.assertEquals(encode_tag(**decode_tag(tag)), tag)
        self.assertEquals(decode_tag(encode_tag(**volume)), volume)

        # backup volume
        tag = 'backup.1.backup1'
        volume = {
            'timestamp': 1.0,
            'backup_id': 'backup1'
        }
        self.assertEquals(encode_tag(**decode_tag(tag)), tag)
        self.assertEquals(decode_tag(encode_tag(**volume)), volume)
        # restore volume
        tag = 'restore.vol1.backup1'
        volume = {
            'backup_source_volume_id': 'vol1',
            'backup_id': 'backup1'
        }
        self.assertEquals(encode_tag(**decode_tag(tag)), tag)
        self.assertEquals(decode_tag(encode_tag(**volume)), volume)
        # zero volume
        tag = 'zero'
        volume = {'zero': True}
        self.assertEquals(encode_tag(**decode_tag(tag)), tag)
        self.assertEquals(decode_tag(encode_tag(**volume)), volume)


class TestVolumeSnapshot(BaseHelper):
    def setUp(self):
        BaseHelper.setUp(self)

    def tearDown(self):
        BaseHelper.tearDown(self)

    def test_snapshot_existing_snapshot(self):

        def mock_scrub(snap, vol):
            pass

        h = volume.VolumeHelper(self.conf)
        volume_id = 'v1'
        snap_id1 = 'b1'
        snap_id2 = 'b2'
        h.create(volume_id)
        h.create_snapshot(volume_id, snap_id1)
        self.assertRaises(AlreadyExists,
                          h.create_snapshot, volume_id, snap_id2)
        with patch(h.scrub, 'scrub_snapshot', mock_scrub):
            h.delete(snap_id1, lock=MockResourceLock())
            h.delete(volume_id, lock=MockResourceLock())

    def test_existing_snap_for_clone(self):

        def mock_scrub(snap, vol):
            pass

        h = volume.VolumeHelper(self.conf)
        volume_id = 'v1'
        target_id = 'somevolume'
        snap_id1 = 'b1'
        snap_id2 = 'b2'
        h.create(volume_id)
        snap1 = h.create_snapshot(volume_id, snap_id1, type_='clone',
                                  clone_id=target_id)
        snap2 = h.create_snapshot(volume_id, snap_id2, type_='clone',
                                  clone_id=target_id)

        self.assertEquals(snap1['id'], snap2['id'])

        with patch(h.scrub, 'scrub_snapshot', mock_scrub):
            h.delete(snap_id1, lock=MockResourceLock())
            h.delete(volume_id, lock=MockResourceLock())


if __name__ == "__main__":
    unittest.main()
