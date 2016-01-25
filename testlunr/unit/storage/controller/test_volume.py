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
from uuid import uuid4
from collections import defaultdict
from urllib2 import URLError
from webob.exc import HTTPNotFound

from testlunr.unit import WsgiTestBase, MockResourceLock, patch
from testlunr.unit.storage.helper.test_helper import BaseHelper

from lunr.storage.urlmap import urlmap
from lunr.storage.controller import volume
from lunr.storage.server import StorageWsgiApp
from lunr.storage.helper.utils import NotFound, ResourceBusy, \
    ServiceUnavailable
from lunr.storage.helper.volume import AlreadyExists, InvalidImage
from lunr.common.exc import NodeError

# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=False)


class MockCinder(object):

    def __init__(self):
        self.update_volume_metadata_called = False
        self.delete_volume_metadata_called = False
        self.update_called = False

    def update_volume_metadata(self, volume_id, metadata):
        self.update_volume_metadata_called = True

    def delete_volume_metadata(self, volume_id, key):
        self.delete_volume_metadata_called = True

    def update(self, slug, slug1, slug2):
        self.update_called = True

    def __call__(self, account):
        self.account = account
        return self


class MockRequest(object):

    def get_method(self):
        return 'PUT'

    def get_full_url(self):
        return 'https://localhost/'


class MockCgroupHelper(object):

    def __init__(self, *args, **kwargs):
        self.sets = defaultdict(list)

    def set_read_iops(self, volume, throttle):
        self.set(volume, throttle, 'blkio.throttle.read_iops_device')

    def set_write_iops(self, volume, throttle):
        self.set(volume, throttle, 'blkio.throttle.write_iops_device')

    def set(self, volume, throttle, param=None):
        value = "%s %s" % (volume['device_number'], throttle)
        self.sets[param].append(value)


class TestVolumeController(WsgiTestBase, BaseHelper):

    def setUp(self):
        super(WsgiTestBase, self).setUp()
        super(BaseHelper, self).setUp()
        self.app = StorageWsgiApp(self.conf, urlmap)
        self.app.helper.cgroups = MockCgroupHelper()

        def fake_node_request(*args, **kwargs):
            pass
        self.app.helper.node_request = fake_node_request

        def mock_api_request(*args, **kwargs):
            pass
        self.app.helper.make_api_request = mock_api_request

    def tearDown(self):
        self.app = None
        super(BaseHelper, self).tearDown()
        super(WsgiTestBase, self).tearDown()

    def test_show(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        url = "/volumes/%s" % volume1_id
        resp = self.request(url)
        self.assertEquals(resp.code // 100, 2)

    def test_create_id_too_long(self):
        volume_id = 'A' * 100
        url = '/volumes/%s' % volume_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)

    def test_create_invalid_id(self):
        volume_id = 'no.dots.please'
        url = '/volumes/%s' % volume_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)

    def test_create_invalid_size(self):
        size = 'foo'
        url = '/volumes/foo?size=%s' % size
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)
        size = -42
        url = '/volumes/foo?size=%s' % size
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)

    def test_create_invalid_backup(self):
        source_id = "foo"
        backup_id = "A" * 100
        url = '/volumes/foo?backup_source_volume_id=%s&backup_id=%s&size=0' % (
                source_id, backup_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)
        self.assertIn("length of 'backup_id'", resp.body['reason'])
        backup_id = "bar"
        source_id = "B" * 100
        url = '/volumes/foo?backup_source_volume_id=%s&backup_id=%s&size=0' % (
                source_id, backup_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)
        self.assertIn("length of 'backup_source_volume_id'",
                      resp.body['reason'])
        url = '/volumes/foo?backup_id=%s&size=0' % backup_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 400)
        self.assertIn("Must specify backup_source_volume_id",
                      resp.body['reason'])

    def test_create_from_source(self):
        size = 1
        source_id = 'foo'
        destination_id = 'bar'
        source_host = '127.0.0.1'
        source_port = '8080'
        url = ('/volumes/bar?size=%s&source_volume_id=%s'
               '&source_host=%s&source_port=%s' %
               (size, source_id, source_host, source_port))
        resp = self.request(url, method='PUT')
        self.assertEqual(resp.code // 100, 2)
        volume = self.app.helper.volumes.get(destination_id)
        self.assertNotEquals(volume, None)

    def test_create_from_source_node_fails(self):
        def node_request(*args, **kwargs):
            raise NodeError(MockRequest(), URLError('something bad'))
        clone_id = 'bar'
        url = '/volumes/%s' % clone_id
        params = {
            'size': 1,
            'source_volume_id': 'foo',
            'source_host': '127.0.0.1',
            'source_port': '8080',
        }
        with patch(self.app.helper, 'node_request', node_request):
            resp = self.request(url, 'PUT', params)
        self.assertEqual(resp.code // 100, 5)
        # Clean up after yourself!
        self.assertRaises(NotFound, self.app.helper.exports.get, clone_id)
        self.assertRaises(NotFound, self.app.helper.volumes.get, clone_id)

    def test_create_from_source_export_fails(self):
        def export_create(*args, **kwargs):
            raise ServiceUnavailable("no export for you")
        clone_id = 'bar'
        url = '/volumes/%s' % clone_id
        params = {
            'size': 1,
            'source_volume_id': 'foo',
            'source_host': '127.0.0.1',
            'source_port': '8080',
        }
        with patch(self.app.helper.exports, 'create', export_create):
            resp = self.request(url, 'PUT', params)
        self.assertEqual(resp.code // 100, 5)
        # Clean up after yourself!
        self.assertRaises(NotFound, self.app.helper.exports.get, clone_id)
        self.assertRaises(NotFound, self.app.helper.volumes.get, clone_id)

    def test_create_from_source_no_size(self):
        source_id = 'foo'
        destination_id = 'bar'
        source_host = '127.0.0.1'
        source_port = '8080'
        url = ('/volumes/bar?source_volume_id=%s'
               '&source_host=%s&source_port=%s' %
               (source_id, source_host, source_port))
        resp = self.request(url, method='PUT')
        self.assertEqual(resp.code, 400)

    def test_create_from_source_no_sourcehost(self):
        size = 1
        source_id = 'foo'
        destination_id = 'bar'
        source_port = '8080'
        url = '/volumes/bar?size=%s&source_volume_id=%s&source_port=%s' % (
             size, source_id, source_port)
        resp = self.request(url, method='PUT')
        self.assertEqual(resp.code, 400)

    def test_create_from_source_no_sourceport(self):
        size = 1
        source_id = 'foo'
        destination_id = 'bar'
        source_host = '127.0.0.1'
        url = '/volumes/bar?size=%s&source_volume_id=%s&source_host=%s' % (
             size, source_id, source_host)
        resp = self.request(url, method='PUT')
        self.assertEqual(resp.code, 400)

    def test_create_from_backup(self):
        def mock_create(volume_id, *args, **kwargs):
            self.assert_(kwargs['size'])
            self.assert_(kwargs['lock'])
            self.assert_(kwargs['callback'])
            self.assert_(kwargs['backup_id'])
            self.assert_(kwargs['backup_source_volume_id'])
            self.assert_(kwargs['cinder'])
            self.orig_create(volume_id, **kwargs)

        volume1_id = str(uuid4())
        backup1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        snap1 = self.app.helper.volumes.create_snapshot(volume1_id,
                                                        backup1_id, 1)
        backup1 = self.app.helper.backups.create(snap1, backup1_id,
                                                 lock=MockResourceLock())
        volume2_id = str(uuid4())

        self.orig_create = self.app.helper.volumes.create
        self.app.helper.volumes.create = mock_create
        self.app.helper.get_cinder = MockCinder()

        url = ('/volumes/%s?backup_source_volume_id=%s'
               '&backup_id=%s&size=1&account=dev' %
               (volume2_id, volume1_id, backup1_id))
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)
        self.assertTrue(self.app.helper.cgroups.sets)
        self.assertTrue(
            self.app.helper.get_cinder(None).update_volume_metadata_called)
        self.assertTrue(
            self.app.helper.get_cinder(None).delete_volume_metadata_called)

    def test_create_from_image(self):
        def mock_create(volume_id, *args, **kwargs):
            self.assert_(kwargs['size'])
            self.assert_(kwargs['lock'])
            self.assert_(kwargs['callback'])
            self.assert_(kwargs['image_id'])
            # Being a little lazy.
            del kwargs['image_id']
            self.orig_create(volume_id, **kwargs)

        self.orig_create = self.app.helper.volumes.create
        self.app.helper.volumes.create = mock_create

        volume_id = str(uuid4())
        image_id = str(uuid4())
        url = '/volumes/%s?size=1&image_id=%s' % (volume_id, image_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'IMAGING')

    def test_create_from_invalid_image(self):
        def mock_create(volume_id, *args, **kwargs):
            raise InvalidImage("invalid image!")
        self.app.helper.volumes.create = mock_create
        volume_id = str(uuid4())
        image_id = str(uuid4())
        url = '/volumes/%s?size=1&image_id=%s' % (volume_id, image_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 412)

    def test_create_already_exists_from_image(self):
        def mock_create(volume_id, *args, **kwargs):
            raise AlreadyExists("already exists!")
        self.app.helper.volumes.create = mock_create
        volume_id = str(uuid4())
        image_id = str(uuid4())
        url = '/volumes/%s?size=1&image_id=%s' % (volume_id, image_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 409)

    def test_create(self):
        volume1_id = str(uuid4())
        url = '/volumes/%s?size=1' % volume1_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code // 100, 2)
        self.assertTrue(self.app.helper.cgroups.sets)

    def test_create_nosize(self):
        volume1_id = str(uuid4())
        url = '/volumes/%s' % volume1_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 400)

    def test_create_cgroups(self):
        volume1_id = str(uuid4())
        read_iops = 200
        write_iops = 150
        url = '/volumes/%s?size=1&read_iops=%s&write_iops=%s' % (
            volume1_id, read_iops, write_iops)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code // 100, 2)
        cgroups = self.app.helper.cgroups.sets
        self.assertTrue(cgroups)
        self.assertEquals(
            cgroups['blkio.throttle.read_iops_device'],
            ['%s %s' % (resp.body['device_number'], read_iops)])
        self.assertEquals(
            cgroups['blkio.throttle.write_iops_device'],
            ['%s %s' % (resp.body['device_number'], write_iops)])

    def test_create_already_exists(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        url = '/volumes/%s?size=1' % volume1_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 409)

    def test_delete(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        export1 = self.app.helper.exports.create(volume1_id)
        url = '/volumes/%s' % volume1_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 200)

    def test_delete_no_export(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        url = '/volumes/%s' % volume1_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 200)

    def test_delete_lost_race(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)

        def mock_delete(*args, **kwargs):
            raise NotFound('you are too late')
        self.app.helper.volumes.delete = mock_delete
        url = '/volumes/%s' % volume1_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 404)

    def test_delete_busy(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        export1 = self.app.helper.exports.create(volume1_id)

        def mock_delete(*args, **kwargs):
            raise ResourceBusy('go away, I am busy')
        self.app.helper.exports.delete = mock_delete
        url = '/volumes/%s' % volume1_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 409)

    def test_audit(self):
        volume1_id = str(uuid4())
        volume1 = self.app.helper.volumes.create(volume1_id)
        url = '/volumes/%s/audit' % volume1_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)


if __name__ == "__main__":
    unittest.main()
