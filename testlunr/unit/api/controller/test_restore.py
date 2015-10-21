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


from lunr.common.config import LunrConfig
from lunr.api.server import ApiWsgiApp
from testlunr.unit import WsgiTestBase
from lunr.api.urlmap import urlmap
from lunr import db
import unittest


class TestRestoreController(WsgiTestBase):
    """ Test lunr.api.controller.restore.RestoreController """
    def setUp(self):
        conf = LunrConfig({'db': {'auto_create': True, 'url': 'sqlite://'}})
        self.app = ApiWsgiApp(conf, urlmap)
        self.db = db.Session

        self.account = db.models.Account()
        vtype = db.models.VolumeType('vtype')
        node = db.models.Node('node', 10, volume_type=vtype,
                              hostname='10.127.0.1', port=8080)

        # Simulate a volume that is being restored
        volume = db.models.Volume(0, 'vtype',
                                  id='v1', node=node, account=self.account)
        backup = db.models.Backup(volume, status='AVAILABLE')
        self.db.add_all([vtype, self.account, node, volume, backup])
        self.db.commit()
        # Assign the backup as the restore of the volume
        volume.restore_of = backup.id
        self.volume = dict(volume)
        self.backup = dict(backup)
        self.db.commit()

    def tearDown(self):
        self.db.remove()

    def test_list(self):
        resp = self.request(
            "/v1.0/admin/backups/%s/restores" % self.backup['id'])
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body[0]['id'], self.volume['id'])

    def test_get(self):
        resp = self.request("/v1.0/admin/backups/%s/restores/%s" %
                            (self.backup['id'], self.volume['id']))
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['id'], self.volume['id'])

    def test_delete(self):
        resp = self.request("/v1.0/admin/backups/%s/restores/%s" %
                            (self.backup['id'], self.volume['id']))
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['id'], self.volume['id'])

        # Delete the restore_of reference
        resp = self.request("/v1.0/admin/backups/%s/restores/%s" %
                            (self.backup['id'], self.volume['id']), 'DELETE')

        # Get restore returns 404
        resp = self.request("/v1.0/admin/backups/%s/restores/%s" %
                            (self.backup['id'], self.volume['id']))
        self.assertEquals(resp.code, 404)

        # List restore returns empty
        resp = self.request("/v1.0/admin/backups/%s/restores" %
                            self.backup['id'])
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body, [])
