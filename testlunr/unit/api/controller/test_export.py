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

from urllib2 import URLError

from lunr import db
from lunr.common import logger
from lunr.common.config import LunrConfig
from lunr.api.controller import base
from lunr.api.controller.export import ExportController
from lunr.api.server import ApiWsgiApp
from lunr.api.urlmap import urlmap

from testlunr.unit import patch, WsgiTestBase

# logger.configure(log_to_console=True, capture_stdio=False)


class MockRequest(object):

    def get_method(self):
        return 'PUT'

    def get_full_url(self):
        return 'https://localhost/'

    def getcode(self):
        return '200'


class MockUrlopen(object):

    def __init__(self, request, timeout=None):
        pass

    def read(self):
        return '{"name": "some_vol", "export": {"name": "some_iqn"}}'

    def getcode(self):
        return 200


class TestExportController(WsgiTestBase):
    """ Test lunr.api.controller.export.ExportController """
    def setUp(self):
        self.test_conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://'}})
        self.app = ApiWsgiApp(self.test_conf, urlmap)
        self.db = db.Session
        self.old_urlopen = base.urlopen
        base.urlopen = MockUrlopen
        self.old_db_close = self.db.close
        # This dirty little hack allows us to query the db after the request
        self.db.close = lambda: 42
        self.vtype = db.models.VolumeType('vtype')
        self.account = db.models.Account()
        self.node = db.models.Node('node', 10, volume_type=self.vtype,
                                   hostname='10.127.0.1', port=8080)
        self.volume = db.models.Volume(0, 'vtype', id='v1', node=self.node,
                                       account=self.account)
        self.export = db.models.Export(volume=self.volume)
        self.db.add_all([self.vtype, self.account, self.node, self.volume,
                         self.export])
        self.db.commit()

    def tearDown(self):
        base.urlopen = self.old_urlopen
        self.db.remove()
        self.db.close = self.old_db_close

    def test_create(self):
        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        resp = self.request(url, 'PUT')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'ATTACHING')

    def test_create_no_volume(self):
        url = "/v1.0/%s/volumes/missingvolume/export" % (self.account.id)
        resp = self.request(url, 'PUT')
        self.assertEquals(resp.code, 404)

    def test_create_with_ip(self):
        ip = '8.8.8.8'
        url = "/v1.0/%s/volumes/%s/export?ip=%s" % (
            self.account.id, self.volume.id, ip)
        resp = self.request(url, 'PUT')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'ATTACHING')
        self.assertEquals(resp.body['ip'], ip)

    def test_create_invalid_ip(self):
        ip = 'not.an.ip'
        url = "/v1.0/%s/volumes/%s/export?ip=%s" % (
            self.account.id, self.volume.id, ip)
        resp = self.request(url, 'PUT')
        self.assertEquals(resp.code, 412)
        self.assertIn('Invalid ip', resp.body['reason'])
        self.assertIn(ip, resp.body['reason'])

    def test_delete(self):
        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        resp = self.request(url, 'DELETE')
        self.assertEquals(resp.code // 100, 2)

    def test_delete_no_volume(self):
        url = "/v1.0/%s/volumes/doesntexist/export" % (self.account.id)
        resp = self.request(url, 'DELETE')
        self.assertEquals(resp.code, 404)

    def test_delete_no_export(self):
        self.db.delete(self.export)
        self.db.commit()
        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        resp = self.request(url, 'DELETE')
        self.assertEquals(resp.code, 404)

    def test_delete_node_error(self):
        def raise_exc(*args, **kwargs):
            e = base.NodeError(MockRequest(), URLError("somthing bad"))
            e.code = 400
            e.status = '400 something bad'
            raise e

        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        with patch(ExportController, 'node_request', raise_exc):
            resp = self.request(url, 'DELETE')
        self.assertEquals(resp.code, 400)

    def test_delete_node_404(self):
        def raise_exc(*args, **kwargs):
            e = base.NodeError(MockRequest(), URLError("Its gone!"))
            e.code = 404
            e.status = '404 Not Found'
            raise e

        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        with patch(ExportController, 'node_request', raise_exc):
            resp = self.request(url, 'DELETE')
        self.assertEquals(resp.code // 100, 2)

    def test_show(self):
        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        resp = self.request(url, 'GET')
        self.assertEquals(resp.code // 100, 2)

    def test_show_no_export(self):
        self.db.delete(self.export)
        self.db.commit()
        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        resp = self.request(url, 'GET')
        self.assertEquals(resp.code, 404)

    def test_show_nonexistent(self):
        url = "/v1.0/%s/volumes/nonexistent/export" % (self.account.id)
        resp = self.request(url, 'GET')
        self.assertEquals(resp.code, 404)

    def test_update(self):
        ip = '123.456.123.456'
        initiator = 'something.long.and.ugly.with.dots'

        def node_request(*args, **kwargs):
            return {
                'sessions': [{
                    'ip': ip,
                    'initiator': initiator
                }]
            }

        url = "/v1.0/%s/volumes/%s/export" % (self.account.id, self.volume.id)
        instance_id = 'someinstanceid'
        status = 'attaching'
        params = {'instance_id': instance_id, 'status': status}
        with patch(ExportController, 'node_request', node_request):
            resp = self.request(url, 'POST', params)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['instance_id'], instance_id)
        self.assertEquals(resp.body['status'], status)
        self.assertEquals(resp.body['session_ip'], ip)
        self.assertEquals(resp.body['session_initiator'], initiator)
        self.db.refresh(self.export)
        self.assertEquals(self.export.session_ip, ip)
        self.assertEquals(self.export.session_initiator, initiator)

    def test_update_404(self):
        url = "/v1.0/%s/volumes/nonexistent/export" % (self.account.id)
        params = {'instance_id': 'nothing', 'status': 'status'}
        resp = self.request(url, 'POST', params)
        self.assertEquals(resp.code, 404)


if __name__ == "__main__":
    unittest.main()
