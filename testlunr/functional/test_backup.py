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

# -------------------------------------------------------------------
# Change the API endpoint by defining the following in your environment

# export API_HOST='lunr-api.rackspace.corp'
# export API_PORT='8080'
# export API_VOLUME_TYPE='vtype'
# export API_SKIP_ADMIN='true'
# -------------------------------------------------------------------

from testlunr.functional import LunrTestCase, SkipTest, LunrApiService, \
    LunrStorageService
from lunr.storage.helper.export import ExportHelper
from lunr.common.config import LunrConfig
from lunr.db.models import VolumeType
from urllib import urlencode
from uuid import uuid4
from time import sleep
from lunr import db
import subprocess
import unittest
import json
import os


class LunrBackupTestCase(LunrTestCase):

    @classmethod
    def setUpClass(cls):
        conf = LunrConfig.from_api_conf()
        sess = db.configure(conf)
        # Change the min_size to 0, so we can
        # create volumes smaller than a gig
        query = sess.query(VolumeType).filter_by(name='vtype')
        # Save the original value
        cls._min_size = query.one().min_size
        # Set min_size to 0
        query.update({'min_size': 0})
        sess.commit()

    @classmethod
    def tearDownClass(cls):
        # Restore the original min_size
        db.Session.query(VolumeType).filter_by(name='vtype')\
            .update({'min_size': cls._min_size})
        db.Session.commit()

    def setUp(self):
        self.api = LunrApiService()
        self.storage = LunrStorageService()

        # setup our timeouts
        super(LunrBackupTestCase, self).setUp()

    def request(self, uri, *args, **kwargs):
        url = "http://%s:%s/v1.0/%s" % (self.api.host, self.api.port, uri)
        return self.urlopen(url, *args, **kwargs)

    def node_request(self, uri, *args, **kwargs):
        url = "http://%s:%s/%s" % (self.storage.host, self.storage.port, uri)
        return self.urlopen(url, *args, **kwargs)

    def wait_on_status(self, uri, status, node=False):
        for i in range(20):
            if node:
                resp = self.node_request(uri)
            else:
                resp = self.request(uri)
            try:
                if resp.body['status'] == status:
                    sleep(0.5)
                    return
            except KeyError:
                pass
            sleep(i)
        self.fail("%s never returned a 'status' of '%s'" % (uri, status))

    def wait_on_code(self, uri, code, node=False):
        for i in range(20):
            if node:
                resp = self.node_request(uri)
            else:
                resp = self.request(uri)
            if resp.code == code:
                return
            sleep(i)
        self.fail("%s never returned a code of '%s'" % (uri, code))

    def test_a_simple_backup_and_restore(self):
        volume_id = str(uuid4())

        # create a volume
        resp = self.request('dev/volumes/%s' % volume_id, 'PUT', {
                'size': 1,
                'volume_type_name': 'vtype'
            })
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'ACTIVE')

        # check the storage node to get path of my_vol
        resp = self.node_request('volumes/%s' % volume_id)
        self.assertCode(resp, 200)

        # Write 20k of 'G's to the volume
        with open(resp.body['path'], 'w') as f:
            for i in range(20):
                f.write('G' * 1024)
            f.flush()
            os.fsync(f.fileno())

        # Create an md5sum of the volume
        digest_my_vol = self.md5sum(resp.body['path'])

        backup_id = str(uuid4())
        # create a backup
        resp = self.request('dev/backups/%s' % backup_id, 'PUT', {
                'volume': volume_id
            })

        self.assertCode(resp, 200)
        # Status of the backup should be 'SAVING'
        self.assertEquals(resp.body['status'], 'SAVING')

        # The backup fork might not have started yet, wait until it does
        self.wait_on_status(
            'volumes/%s/backups/%s' % (volume_id, backup_id),
            'RUNNING', node=True)

        # Wait until status = 'AVAILABLE'
        self.wait_on_status('dev/backups/%s' % backup_id, 'AVAILABLE')

        # create a restore
        restore_id = str(uuid4())
        resp = self.request('dev/volumes/%s' % restore_id, 'PUT', {
                'backup': backup_id,
                'size': 1,
                'volume_type_name': 'vtype'
            })

        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'BUILDING')

        # Restore volume should show in backups/{id}/restores
        resp = self.request(
            'admin/backups/%s/restores/%s' % (backup_id, restore_id))
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'BUILDING')

        # Should not allow backup delete while restore is running
        resp = self.request('dev/backups/%s' % backup_id, 'DELETE')
        self.assertCode(resp, 409)

        # wait for restore
        self.wait_on_status('dev/volumes/%s' % restore_id, 'ACTIVE')

        # backups/{id}/restores/{restore_id} should return 404
        resp = self.request(
            'admin/backups/%s/restores/%s' % (backup_id, restore_id))
        self.assertCode(resp, 404)

        # check the storage node to get path of my_restore
        resp = self.node_request('volumes/%s' % restore_id)
        self.assertCode(resp, 200)

        # my_restore is a restore of my_vol
        digest_my_restore = self.md5sum(resp.body['path'])
        self.assertEquals(digest_my_restore, digest_my_vol)

        # clean up my_restore
        resp = self.request('dev/volumes/%s' % restore_id, method='DELETE')
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'DELETING')

        # restore is deleted
        self.wait_on_status('dev/volumes/%s' % restore_id, 'DELETED')

        # remove backup
        resp = self.request('dev/backups/%s' % backup_id, 'DELETE')
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'DELETING')

        # backup is deleted
        # Actual delete is done by orbit now. --cory
        # Wait just long enough for the manifest munging to happen.
        sleep(2)
        # self.wait_on_status('dev/backups/%s' % backup_id, 'DELETED')
        # sleep(2)
        # remove original volume
        resp = self.request('dev/volumes/%s' % volume_id, 'DELETE')
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'DELETING')

        # vol is deleted
        self.wait_on_status('dev/volumes/%s' % volume_id, 'DELETED')

    def test_all_the_things(self):
        volume_one = str(uuid4())
        # create vol1
        resp = self.request('dev/volumes/%s' % volume_one, 'PUT', {
                'size': 0,
                'volume_type_name': 'vtype'
            })
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'ACTIVE')

        # check the storage node to get path
        resp = self.node_request('volumes/%s' % volume_one)
        self.assertCode(resp, 200)
        vol1_path = resp.body['path']

        # calculate digest for init
        digest_init = self.md5sum(vol1_path)

        # take backup1
        backup_one = str(uuid4())
        resp = self.request('dev/backups/%s' % backup_one, 'PUT', {
                'volume': volume_one
            })
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'SAVING')

        # take backup2
        backup_two = str(uuid4())
        resp = self.request('dev/backups/%s' % backup_two, 'PUT', {
                'volume': volume_one
            })
        # Already snapshotted.
        self.assertEquals(resp.code, 409)

        # make a file system
        with open(os.devnull, 'w') as f:
            subprocess.Popen(['mkfs.ext3', vol1_path], stdout=f,
                             stderr=subprocess.STDOUT).wait()

        # calculate digest for ext3
        digest_ext3 = self.md5sum(vol1_path)

        # wait until backup1 is available
        self.wait_on_status('dev/backups/%s' % backup_one, 'AVAILABLE')

        # take backup2 (again)
        backup_two = str(uuid4())
        resp = self.request('dev/backups/%s' % backup_two, 'PUT', {
                'volume': volume_one
            })
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'SAVING')

        # create vol2 from backup1
        volume_two = str(uuid4())
        resp = self.request('dev/volumes/%s' % volume_two, 'PUT', {
                'backup': backup_one,
                'size': 0,
                'volume_type_name': 'vtype'
            })

        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'BUILDING')

        # wait until backup2 is available
        self.wait_on_status('dev/backups/%s' % backup_two, 'AVAILABLE')

        # create vol3 from backup2
        volume_three = str(uuid4())
        resp = self.request('dev/volumes/%s' % volume_three, 'PUT', {
                'backup': backup_two,
                'size': 0,
                'volume_type_name': 'vtype'
            })

        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'BUILDING')

        # delete backup1 from manifest
        resp = self.request('dev/backups/%s' % backup_one, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'DELETING')

        # Actual delete is done by orbit now. --cory
        # Wait just long enough for the manifest munging to happen.
        sleep(2.0)

        # delete original vol1
        resp = self.request('dev/volumes/%s' % volume_one, 'DELETE')
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'DELETING')

        # vol2 is active
        self.wait_on_status('dev/volumes/%s' % volume_two, 'ACTIVE')

        # check the storage node to get vol2_path
        resp = self.node_request('volumes/%s' % volume_two)
        self.assertEquals(resp.code // 100, 2)
        vol2_path = resp.body['path']

        # vol2 is a restore of backup1
        digest_vol2 = self.md5sum(vol2_path)
        self.assertEquals(digest_vol2, digest_init)

        # vol3 is active
        self.wait_on_status('dev/volumes/%s' % volume_three, 'ACTIVE')

        # check the storage node to get vol3_path
        resp = self.node_request('volumes/%s' % volume_three)
        self.assertEquals(resp.code // 100, 2)
        vol3_path = resp.body['path']

        # vol3 is a restore of backup2
        digest_vol3 = self.md5sum(vol3_path)
        self.assertEquals(digest_vol3, digest_ext3)

        # backup1 is deleted
        # Actual delete is done by orbit now. --cory
        # Wait just long enough for the manifest munging to happen.
        sleep(2.0)

        # create vol4 from backup2
        volume_four = str(uuid4())
        resp = self.request('dev/volumes/%s' % volume_four, 'PUT', {
                'backup': backup_two,
                'size': 0,
                'volume_type_name': 'vtype'
            })
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'BUILDING')

        # vol1 is deleted
        self.wait_on_status('dev/volumes/%s' % volume_one, 'DELETED')

        # check the storage node to verify vol1 is *really* deleted
        resp = self.node_request('volumes/%s' % volume_one)
        self.assertEquals(resp.code, 404)

        # vol4 is active
        self.wait_on_status('dev/volumes/%s' % volume_four, 'ACTIVE')

        # check the storage node to get vol4_path
        resp = self.node_request('volumes/%s' % volume_four)
        self.assertEquals(resp.code // 100, 2)
        vol4_path = resp.body['path']

        # vol4 is a restore of backup2
        digest_vol4 = self.md5sum(vol4_path)
        self.assertEquals(digest_vol4, digest_ext3)

        # clean up vol2
        resp = self.request('dev/volumes/%s' % volume_two, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'DELETING')

        # clean up vol3
        resp = self.request('dev/volumes/%s' % volume_three, 'DELETE')

        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'DELETING')

        # clean up vol4
        resp = self.request('dev/volumes/%s' % volume_four, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'DELETING')

        # vol2 is deleted
        self.wait_on_status('dev/volumes/%s' % volume_two, 'DELETED')

        # vol3 is deleted
        self.wait_on_status('dev/volumes/%s' % volume_three, 'DELETED')

        # vol4 is deleted
        self.wait_on_status('dev/volumes/%s' % volume_four, 'DELETED')


if __name__ == "__main__":
    unittest.main()
