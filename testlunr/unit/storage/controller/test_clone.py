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
import time

from lunr.storage.helper import volume
from lunr.storage.urlmap import urlmap
from lunr.common.config import LunrConfig
from lunr.storage.server import StorageWsgiApp
from lunr.storage.helper.utils.iscsi import ISCSIDevice, ISCSINotConnected

from testlunr.unit import WsgiTestBase, MockResourceLock
from testlunr.unit.storage.helper.test_helper import BaseHelper

# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=False)


class MockISCSIDevice(ISCSIDevice):
    def __init__(self, *args, **kwargs):
        super(MockISCSIDevice, self).__init__(*args, **kwargs)
        # FIXME. find a way to get a volume in here. :)
        self.device = '/dev/null'

    @property
    def connected(self):
        return True


class ExplodingISCSIDevice(ISCSIDevice):
    def __init__(self, *args, **kwargs):
        raise ISCSINotConnected('KABOOM!')


class ReadFailingISCSIDevice(ISCSIDevice):
    def copy_file_out(self, *args, **kwargs):
        raise ISCSINotConnected('read failed!')

    def connect(self, *args, **kwargs):
        pass

    @property
    def connected(self):
        return True


class TestCloneController(WsgiTestBase, BaseHelper):

    def setUp(self):
        self.origISCSIDevice = volume.ISCSIDevice
        volume.ISCSIDevice = MockISCSIDevice
        super(WsgiTestBase, self).setUp()
        super(BaseHelper, self).setUp()
        self.app = StorageWsgiApp(self.conf, urlmap)

        def fake_node_request(*args, **kwargs):
            pass
        self.app.helper.node_request = fake_node_request

        def fake_api_request(*args, **kwargs):
            pass
        self.app.helper.make_api_request = fake_api_request

        def fake_scrub(snapshot, volume):
            pass
        self.app.helper.volumes.scrub.scrub_snapshot = fake_scrub

        self.lock = MockResourceLock()
        self.sourcevol_id = 'sourcevol'
        self.sourcevol = self.app.helper.volumes.create(self.sourcevol_id,
                                                        lock=self.lock)

    def tearDown(self):
        self.app.helper.volumes.delete(self.sourcevol_id, lock=self.lock)
        self.app = None
        super(BaseHelper, self).tearDown()
        super(WsgiTestBase, self).tearDown()
        volume.ISCSIDevice = self.origISCSIDevice

    def test_create(self):
        targetvol_id = 'targetvol'
        iqn = 'iqnfoo'
        iscsi_ip = 'ipfoo'
        url = ('/volumes/%s/clones/%s?iscsi_ip=%s&iqn=%s'
               '&mgmt_host=%s&mgmt_port=%s' %
               (self.sourcevol_id, targetvol_id, iscsi_ip,
                iqn, 'fake_host', '42'))
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)
        snap = self.app.helper.volumes._get_snapshot(self.sourcevol_id)
        self.assertEquals(snap, None)

    def test_create_clone_fails(self):
        volume.ISCSIDevice = ExplodingISCSIDevice
        targetvol_id = 'targetvol'
        iqn = 'iqnfoo'
        iscsi_ip = 'ipfoo'
        url = ('/volumes/%s/clones/%s?iscsi_ip=%s&iqn=%s'
               '&mgmt_host=%s&mgmt_port=%s' %
               (self.sourcevol_id, targetvol_id, iscsi_ip,
                iqn, 'fake_host', '42'))
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 503)
        snap = self.app.helper.volumes._get_snapshot(self.sourcevol_id)
        self.assertEquals(snap, None)

    def test_clone_retry(self):
        volume.ISCSIDevice = ReadFailingISCSIDevice
        targetvol_id = 'targetvol'
        iqn = 'iqnfoo'
        iscsi_ip = 'ipfoo'
        url = ('/volumes/%s/clones/%s?iscsi_ip=%s&iqn=%s'
               '&mgmt_host=%s&mgmt_port=%s' %
               (self.sourcevol_id, targetvol_id, iscsi_ip,
                iqn, 'fake_host', '42'))
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)
        snap1 = self.app.helper.volumes._get_snapshot(self.sourcevol_id)
        self.assertNotEquals(snap1, None)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)
        snap2 = self.app.helper.volumes._get_snapshot(self.sourcevol_id)
        self.assertEquals(snap1, snap2)
        self.app.helper.volumes.delete(snap1['id'])


if __name__ == "__main__":
    unittest.main()
