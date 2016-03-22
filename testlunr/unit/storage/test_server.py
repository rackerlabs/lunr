#! /usr/bin/env python
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


import os
import json
from StringIO import StringIO
from simplejson import dumps
import socket
import unittest
from urllib import urlencode
from urllib2 import HTTPError
from webob import Request
from tempfile import mkdtemp
from shutil import rmtree

from lunr.common import logger
from lunr.common.wsgi import LunrServeCommand
from lunr.storage.server import StorageWsgiApp
from lunr.storage.urlmap import urlmap
from lunr.storage.helper import base
from lunr.storage.helper.utils import NotFound
from lunr.common.config import LunrConfig

from testlunr.unit import patch
from testlunr.unit.common.test_wsgi import BaseServeApp

# logger.configure(log_to_console=True, capture_stdio=False)


class MockHelper(object):

    class VolumeHelper(object):

        def __init__(self, mock):
            self.volumes = mock.node.volumes
            self.run_dir = mock.conf.string('storage', 'run_dir',
                                            mock.conf.path('run'))

        def list(self):
            return [dict(v) for v in self.volumes.values()]

        def get(self, id):
            try:
                return dict(self.volumes[id])
            except KeyError:
                raise NotFound()

        def create(self, id, size=None, callback=None, lock=None):
            volume = {'id': id, 'size': size, 'device_number': 9000}
            self.volumes[id] = volume
            return 'created'

        def delete(self, id, callback=None, lock=None):
            try:
                del self.volumes[id]
            except KeyError:
                raise NotFound()
            return 'deleted'

    class ExportHelper(object):

        def __init__(self, mock):
            self.volumes = mock.volumes
            self.exports = mock.node.exports

        def list(self):
            return [dict(e) for id, e in self.exports.items()]

        def get(self, id):
            try:
                return dict(self.exports[id])
            except KeyError:
                raise NotFound()

        def create(self, id):
            volume = self.volumes.get(id)
            export = {'volume': volume['id'], 'name': 'iqn-%s' % id}
            self.exports[id] = export
            return 'created'

        def delete(self, id):
            try:
                del self.exports[id]
            except KeyError:
                raise NotFound()
            return 'deleted'

    class Node(object):

        def __init__(self):
            self.volumes = {}
            self.exports = {}

    def __init__(self, conf):
        self.conf = conf
        self.node = MockHelper.Node()
        self.volumes = MockHelper.VolumeHelper(self)
        self.exports = MockHelper.ExportHelper(self)
        self.cgroups = base.CgroupHelper(conf)

    def close(self):
        pass

    def rollback(self):
        pass

    def commit(self):
        pass


class MockVolumeHelper(object):

    def __init__(self, *args, **kwargs):
        pass

    def status(self):
        return {'vg_size': 2 ** 30}

    def check_config(self):
        pass

    def list(self):
        return []


class TestServeStorageApp(BaseServeApp):
    config_filename = 'storage-server.conf'
    use_app = "egg:lunr#storage_server"

    def setUp(self):
        BaseServeApp.setUp(self)
        self._orig_VolumeHelper = base.VolumeHelper
        base.VolumeHelper = MockVolumeHelper

    def tearDown(self):
        BaseServeApp.tearDown(self)
        base.VolumeHelper = self._orig_VolumeHelper

    def test_serve_storage(self):

        def serve(app):
            self.assert_(isinstance(app, StorageWsgiApp))
        self.serve = serve
        # force a quick exit on client registration

        def mock_request(*args, **kwargs):
            raise HTTPError('http://api:8080/v1.0/admin/nodes',
                            404, 'Not Found', {},
                            StringIO("{'reason': 'not found'}"))

        with patch(base, 'make_api_request', mock_request):
            cmd = LunrServeCommand('storage-server')
            cmd.run([self.config_file])
        self.assertTrue(serve.called)

    def test_simple_check_registration(self):
        responses = [
            # listing
            [{'id': 'node1'}],
            # show
            {
                'name': socket.gethostname(),
                'volume_type_name': 'vtype',
                'hostname': '127.0.0.1',
                'port': 8081,
                'storage_hostname': '127.0.0.1',
                'storage_port': 3260,
                'size': 1,
                'status': 'ACTIVE',
                'id': 'node1',
                'cinder_host': '127.0.0.1',
                'affinity_group': '',
                'maintenance_zone': '',
            },
        ]
        response_gen = iter(responses)

        def mock_request(*args, **kwargs):
            info = response_gen.next()
            return StringIO(dumps(info))

        def serve(app):
            local_info = app.helper._local_info()
            node_info = responses[1]
            exceptions = base.get_registration_exceptions(local_info,
                                                          node_info)
            self.assertFalse(exceptions)
        self.serve = serve

        with patch(base, 'make_api_request', mock_request):
            cmd = LunrServeCommand('storage-server')
            cmd.run([self.config_file])
        self.assertTrue(serve.called)

    def test_unhandeled_exception_in_check_registration(self):
        def mock_request(*args, **kwargs):
            mock_request.called = True
            raise Exception('Something unexpected happened')
        with patch(base, 'make_api_request', mock_request):
            cmd = LunrServeCommand('storage-server')
            cmd.run([self.config_file])
        # mock request was called
        self.assert_(mock_request.called)
        # app still started
        self.assert_(self.serve.called)


class TestStorageApp(unittest.TestCase):

    pass


class TestVolumeController(TestStorageApp):

    def setUp(self):
        self.scratch = mkdtemp()
        open(os.path.join(
            self.scratch, 'blkio.throttle.read_iops_device'), 'a').close()
        open(os.path.join(
            self.scratch, 'blkio.throttle.write_iops_device'), 'a').close()
        self.conf = LunrConfig({'cgroup': {'cgroup_path': self.scratch},
                                'storage': {'run_dir': self.scratch}})
        self.mock_helper = MockHelper(self.conf)

    def tearDown(self):
        rmtree(self.scratch)

    def test_list_empty_volumes(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes')
        resp = app(req)
        info = json.loads(resp.body)
        self.assertEquals(resp.status_int // 100, 2)
        self.assertEquals(info, [])

    def test_create_volume(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes/vol1?size=0', method='PUT')
        resp = app(req)
        expected = {
            'id': 'vol1',
            'size': 0,
            'device_number': 9000,
            'status': 'ACTIVE',
        }
        self.assertEquals(resp.status_int // 100, 2)
        info = json.loads(resp.body)
        self.assertEquals(info, expected)

    def test_show_one_volume(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes/vol1?size=0', method='PUT')
        app(req)
        req = Request.blank('/volumes/vol1')
        resp = app(req)
        info = json.loads(resp.body)
        self.assertEquals(resp.status_int // 100, 2)
        expected = {
            'id': 'vol1',
            'size': 0,
            'device_number': 9000,
        }
        self.assertEquals(info, expected)

    def test_list_many_volumes(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        for i in range(3):
            volume_id = 'vol%s' % (i + 1)
            req = Request.blank('/volumes/%s?size=0' % volume_id, method='PUT')
            app(req)

        req = Request.blank('/volumes')
        resp = app(req)
        self.assertEquals(resp.status_int // 100, 2)
        info = json.loads(resp.body)
        expected = [
            {'id': 'vol1', 'size': 0, 'device_number': 9000},
            {'id': 'vol2', 'size': 0, 'device_number': 9000},
            {'id': 'vol3', 'size': 0, 'device_number': 9000},
        ]
        for volume in info:
            self.assert_(volume in expected)
        self.assertEquals(len(info), len(expected))

    def test_delete_volume(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes/vol1?size=0', method='PUT')
        app(req)
        req = Request.blank('/volumes/vol1', method='DELETE')
        resp = app(req)
        self.assertEquals(resp.status_int // 100, 2)
        info = json.loads(resp.body)
        expected = {
            'id': 'vol1',
            'size': 0,
            'device_number': 9000,
            'status': 'DELETING',
        }
        self.assertEquals(info, expected)

    def test_show_volume_not_found(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes/vol1')
        resp = app(req)
        self.assertEquals(resp.status_int, 404)

    def test_delete_volume_not_found(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes/vol1', method='DELETE')
        resp = app(req)
        self.assertEquals(resp.status_int, 404)

    def test_invalid_path_not_found(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('//')
        resp = app(req)
        self.assertEquals(resp.status_int, 404)

    def test_create_volume_with_invalid_size(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes/vol1', method='PUT',
                            content_type='application/x-www-form-urlencoded',
                            body=urlencode({'size': 'X'}))
        resp = app(req)
        self.assertEquals(resp.status_int // 100, 4)

    def test_invalid_method_on_volumes_collection(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes', method='PUT')
        resp = app(req)
        self.assertEquals(resp.status_int, 405)
        req = Request.blank('/volumes', method='POST')
        resp = app(req)
        self.assertEquals(resp.status_int, 405)
        req = Request.blank('/volumes', method='DELETE')
        resp = app(req)
        self.assertEquals(resp.status_int, 405)

    def test_update_volume(self):
        app = StorageWsgiApp(self.conf, urlmap, helper=self.mock_helper)
        req = Request.blank('/volumes', method='POST')
        resp = app(req)
        self.assertEquals(resp.status_int, 405)


if __name__ == "__main__":
    unittest.main()
