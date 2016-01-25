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
import unittest
from uuid import uuid4
from collections import defaultdict

from lunr.common import logger
from lunr.storage.urlmap import urlmap
from lunr.storage.server import StorageWsgiApp
from lunr.storage.helper.export import DeviceBusy
from webob.exc import HTTPNotFound
from lunr.storage.helper.utils import execute, ProcessError

from testlunr.unit import patch, WsgiTestBase
from testlunr.unit.storage.helper.test_helper import BaseHelper


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


class TestExportController(WsgiTestBase, BaseHelper):

    def setUp(self):
        super(WsgiTestBase, self).setUp()
        super(BaseHelper, self).setUp()
        self.app = StorageWsgiApp(self.conf, urlmap)
        self.app.helper.cgroups = MockCgroupHelper()

        def mock_api_request(*args, **kwargs):
            pass
        self.app.helper.make_api_request = mock_api_request

    def tearDown(self):
        self.app = None
        super(BaseHelper, self).tearDown()
        super(WsgiTestBase, self).tearDown()

    def test_show(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        export = self.app.helper.exports.create(volume_id)
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['volume'], volume_id)

    def test_show_no_export(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url)
        self.assertEquals(resp.code, 404)

    def test_create_no_volume(self):
        volume_id = str(uuid4())
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 404)

    def test_create(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['volume'], volume_id)

    def test_create_exists(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        export = self.app.helper.exports.create(volume_id)
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code // 100, 2)

    def test_delete(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        export = self.app.helper.exports.create(volume_id)
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code // 100, 2)

    def test_delete_no_export(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 404)

    def test_delete_no_volume(self):
        volume_id = str(uuid4())
        url = "/volumes/%s/export" % volume_id
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 404)

    def test_delete_attached(self):
        volume_id = str(uuid4())
        volume = self.app.helper.volumes.create(volume_id)
        export = self.app.helper.exports.create(volume_id)
        session_path = os.path.join(self.scratch, 'proc_iet_session')
        with open(session_path, 'a+') as f:
            f.write('\tsid:1234 initiator:foo:01:01\n')
            f.write('\t\tcid:0 ip:127.0.0.1 state:active hd:none dd:none\n')
        export = self.app.helper.exports.get(volume_id)
        url = "/volumes/%s/export" % volume_id

        def raise_exc(*args, **kwargs):
            e = ProcessError('fake ietadm', '-1', 'error', 'SOS')
            raise DeviceBusy(e)
        with patch(self.app.helper.exports, 'ietadm', raise_exc):
            resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 409)


if __name__ == "__main__":
    unittest.main()
