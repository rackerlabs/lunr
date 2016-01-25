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

from lunr.storage.controller import backup
from lunr.storage.urlmap import urlmap
from lunr.common.config import LunrConfig
from lunr.storage.server import StorageWsgiApp

from testlunr.unit import WsgiTestBase, MockResourceLock
from testlunr.unit.storage.helper.test_helper import BaseHelper


class TestBackupController(WsgiTestBase, BaseHelper):

    def setUp(self):
        super(WsgiTestBase, self).setUp()
        super(BaseHelper, self).setUp()
        self.app = StorageWsgiApp(self.conf, urlmap)

        def fake_api_request(*args, **kwargs):
            pass
        self.app.helper.make_api_request = fake_api_request
        self.lock = MockResourceLock()
        self.volume1_id = str(uuid4())
        self.volume1 = self.app.helper.volumes.create(self.volume1_id,
                                                      lock=self.lock)

    def tearDown(self):
        self.app = None
        super(BaseHelper, self).tearDown()
        super(WsgiTestBase, self).tearDown()

    def test_index(self):
        url = '/volumes/thisdoesntexist/backups'
        resp = self.request(url)
        self.assertEquals(resp.code, 404)
        url = '/volumes/%s/backups' % self.volume1_id
        backup_id = str(uuid4())
        timestamp = int(time.time())
        snap = self.app.helper.volumes.create_snapshot(
            self.volume1_id, backup_id, timestamp)
        backup = self.app.helper.backups.create(snap, backup_id,
                                                lock=self.lock)
        resp = self.request(url)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body[backup_id], timestamp)

    def test_show(self):
        url = '/volumes/thisdoesntexist/backups/thisdoesntexisteither'
        resp = self.request(url)
        self.assertEquals(resp.code, 404)
        url = '/volumes/%s/backups/%s' % (self.volume1_id, 'doesntexist')
        resp = self.request(url)
        self.assertEquals(resp.code, 404)
        backup_id = str(uuid4())
        snap = self.app.helper.volumes.create_snapshot(self.volume1_id,
                                                       backup_id, 1)
        backup = self.app.helper.backups.create(snap, backup_id,
                                                lock=self.lock)
        url = '/volumes/%s/backups/%s' % (self.volume1_id, backup_id)
        resp = self.request(url)
        # Could mock the lock data stuff to make this 200
        # self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.code, 404)

    def test_create(self):
        backup1_id = str(uuid4())
        url = '/volumes/%s/backups/%s?timestamp=1' % (
                self.volume1_id, backup1_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'SAVING')

    def test_create_no_timestamp(self):
        url = "/volumes/foo/backups/foo"
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 400)

    def test_create_invalid_volume(self):
        invalid_volume_url = '/volumes/nonexistent/backups/foo?timestamp=1'
        resp = self.request(invalid_volume_url, method='PUT')
        self.assertEquals(resp.code, 404)

    def test_create_stacked_backup(self):
        backup1_id = str(uuid4())
        url = '/volumes/%s/backups/%s?timestamp=1' % (
                self.volume1_id, backup1_id)
        resp = self.request(url, method='PUT')
        self.assertEquals(resp.code, 200)
        backup2_id = str(uuid4())
        bad_backup_url = '/volumes/%s/backups/%s?timestamp=2' % (
                self.volume1_id, backup2_id)
        resp = self.request(bad_backup_url, method='PUT')
        self.assertEquals(resp.code, 409)

    def test_delete(self):
        backup_id = str(uuid4())
        snap = self.app.helper.volumes.create_snapshot(self.volume1_id,
                                                       backup_id, 1)
        backup = self.app.helper.backups.create(snap, backup_id,
                                                lock=self.lock)
        url = '/volumes/%s/backups/%s' % (self.volume1_id, backup_id)
        resp = self.request(url, method='DELETE')
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['id'], backup_id)
        self.assertEquals(resp.body['status'], 'DELETING')


if __name__ == "__main__":
    unittest.main()
