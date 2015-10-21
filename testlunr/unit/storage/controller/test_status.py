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
from webob import Request, Response
from routes import Mapper

from lunr.common import logger
from lunr.common.config import LunrConfig
from lunr.common.jsonify import loads
from lunr.storage.server import StorageWsgiApp
from lunr.storage.urlmap import urlmap
from lunr.storage.helper.utils import ServiceUnavailable
from lunr.storage.controller.status import StatusController

from testlunr.unit import WsgiTestBase


class MockVolumeHelper(object):

    def status(self):
        return {'volumes_status': 'OK'}


class MockExportHelper(object):

    def status(self):
        return {'exports_status': 'OK'}


class MockBackupHelper(object):

    def status(self):
        return {'backups_status': 'OK'}


class MockHelper(object):

    def __init__(self):
        self.volumes = MockVolumeHelper()
        self.exports = MockExportHelper()
        self.backups = MockBackupHelper()

    def api_status(self):
        return {'api_status': 'OK'}


class ExplodingVolumeHelper(object):

    def status(self):
        raise ServiceUnavailable('volumes are not configured')


class ExplodingExportHelper(object):

    def status(self):
        raise ServiceUnavailable('exports are not configured')


class ExplodingBackupHelper(object):

    def status(self):
        raise ServiceUnavailable('volumes are not configured')


class ExplodingHelper(object):

    def __init__(self):
        self.volumes = ExplodingVolumeHelper()
        self.exports = ExplodingExportHelper()
        self.backups = ExplodingBackupHelper()

    def api_status(self):
        raise ServiceUnavailable('node is not configured')


class BadVolumeHelper(object):

    def status(self):
        raise Exception('something unexpected happened with volumes')


class BadExportHelper(object):

    def status(self):
        raise Exception('something unexpected happened with exports')


class BadBackupHelper(object):

    def status(self):
        raise Exception('something unexpected happened with backups')


class BadHelper(object):

    def __init__(self):
        self.volumes = BadVolumeHelper()
        self.exports = BadExportHelper()
        self.backups = BadBackupHelper()

    def api_status(self):
        raise Exception('Something unexpected happened')


class TestStatusController(WsgiTestBase):

    def setUp(self):
        self.test_conf = LunrConfig()

    def tearDown(self):
        self.app = None

    def test_api_status(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/api')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, {'api_status': 'OK'})

    def test_api_unavaiable(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, ExplodingHelper())
        resp = self.request('/status/api')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('not configured' in resp.body['reason'])

    def test_api_unhandled_error(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, BadHelper())
        resp = self.request('/status/api')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('Something unexpected happened' not in
                     resp.body['reason'])

    def test_volume_status(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/volumes')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, {'volumes_status': 'OK'})

    def test_volume_unavailable(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, ExplodingHelper())
        resp = self.request('/status/volumes')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('not configured' in resp.body['reason'])

    def test_volume_unhandled_error(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, BadHelper())
        resp = self.request('/status/volumes')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('Something unexpected happened' not in
                     resp.body['reason'])

    def test_export_status(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/exports')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, {'exports_status': 'OK'})

    def test_export_unavailable(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, ExplodingHelper())
        resp = self.request('/status/exports')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('not configured' in resp.body['reason'])

    def test_export_unhandled_error(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, BadHelper())
        resp = self.request('/status/exports')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('Something unexpected happened' not in
                     resp.body['reason'])

    def test_backup_status(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/backups')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, {'backups_status': 'OK'})

    def test_backup_unavailable(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, ExplodingHelper())
        resp = self.request('/status/backups')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('not configured' in resp.body['reason'])

    def test_backup_unhandled_error(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, BadHelper())
        resp = self.request('/status/backups')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('Something unexpected happened' not in
                     resp.body['reason'])

    def test_status(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status')
        self.assertEquals(resp.code // 100, 2)
        expected = {
            'api': {'api_status': 'OK'},
            'volumes': {'volumes_status': 'OK'},
            'exports': {'exports_status': 'OK'},
            'backups': {'backups_status': 'OK'},
        }
        self.assertEquals(resp.body, expected)

    def test_status_unavailable(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, ExplodingHelper())
        resp = self.request('/status')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('not configured' in resp.body['reason'])

    def test_status_unhandled_error(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, BadHelper())
        resp = self.request('/status')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('Something unexpected happened' not in
                     resp.body['reason'])

    def test_status_mixed_results(self):
        class MixedHelper(MockHelper):

            def api_status(self):
                raise ServiceUnavaiable('can not contact api server')

        self.app = StorageWsgiApp(self.test_conf, urlmap, MixedHelper())
        resp = self.request('/status')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('can not contact' not in resp.body['reason'])
        resp = self.request('/status/api')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('can not contact' not in resp.body['reason'])
        resp = self.request('/status/volumes')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, {'volumes_status': 'OK'})

    def test_status_mixed_helper_results(self):
        class MixedHelper(MockHelper):

            def __init__(self):
                super(MixedHelper, self).__init__()
                self.volumes = ExplodingVolumeHelper()

        self.app = StorageWsgiApp(self.test_conf, urlmap, MixedHelper())
        resp = self.request('/status')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('volumes are not configured' in resp.body['reason'])
        resp = self.request('/status/api')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, {'api_status': 'OK'})
        resp = self.request('/status/volumes')
        self.assertEquals(resp.code // 100, 5)
        self.assert_('volumes are not configured' in resp.body['reason'])

    def test_status_route_not_found(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/')  # trailing slash
        self.assertEquals(resp.code, 404)

    def test_status_helper_route_not_found(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/volume')  # no trailing s
        self.assertEquals(resp.code, 404)

    def test_status_helper_not_found(self):
        urlmap = Mapper()
        urlmap.connect('/status/{helper_type}', controller=StatusController,
                       action='show')
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/volumes')
        self.assertEquals(resp.code, 200)
        resp = self.request('/status/volume')  # no trailing s
        self.assertEquals(resp.code, 404)

    def test_status_conf(self):
        self.app = StorageWsgiApp(self.test_conf, urlmap, MockHelper())
        resp = self.request('/status/conf')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body, self.test_conf.values)


if __name__ == "__main__":
    unittest.main()
