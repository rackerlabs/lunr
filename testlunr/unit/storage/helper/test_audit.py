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


from lunr.storage.helper.base import Helper
from lunr.common.config import LunrConfig
from lunr.storage.helper import audit
import unittest
import textwrap


class TestAuditSnapshots(unittest.TestCase):

    def setUp(self):
        self.helper = Helper(LunrConfig())

        def mock_scan_volumes():
            return [
                {'backup_id': '33485eb3-5900-4068-93a1-2b72677fd699',
                 'device_number': '252:5',
                 'id': '33485eb3-5900-4068-93a1-2b72677fd699',
                 'origin': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
                 'path':
                 '/dev/lunr-volume/33485eb3-5900-4068-93a1-2b72677fd699',
                 'size': 3221225472,
                 'timestamp': 1399449283.0},
                {'clone_id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
                 'device_number': '252:9',
                 'id': '5304e6b7-986c-4d8b-9659-29a606f7c9e3',
                 'origin': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'path':
                 '/dev/lunr-volume/5304e6b7-986c-4d8b-9659-29a606f7c9e3',
                 'size': 5368709120},
                {'device_number': '252:8',
                 'id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
                 'size': 5368709120,
                 'volume': True}]
        self.helper.volumes._scan_volumes = mock_scan_volumes
        self.resp = []

        def mock_request(helper, url):
            return self.resp.pop(0)
        audit.request = mock_request

    def test_no_problems(self):
        self.resp = [{
            'id': '33485eb3-5900-4068-93a1-2b72677fd699',
            'status': 'SAVING',
            }, {
            'id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
            'status': 'CLONING',
            }]
        # Should not report any issue, there are 2 snapshots and API reports
        # they are both CLONING and SAVING
        self.assertEquals(audit.snapshots(self.helper), [])

    def test_bad_backup(self):
        self.resp = [{
            'id': '33485eb3-5900-4068-93a1-2b72677fd699',
            'status': 'AVAILABLE',
            }, {
            'id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
            'status': 'CLONING',
            }]
        # Should report 1 issue, the API reports the backup is AVAILABLE
        result = audit.snapshots(self.helper)
        self.assertEquals(result[0]['snapshot'],
                          '33485eb3-5900-4068-93a1-2b72677fd699')
        self.assertNotEqual(result[0]['msg'], None)

    def test_bad_clone(self):
        self.resp = [{
            'id': '33485eb3-5900-4068-93a1-2b72677fd699',
            'status': 'SAVING',
            }, {
            'id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
            'status': 'ACTIVE',
            }]
        # Should report 1 issue, the API reports the backup is AVAILABLE
        result = audit.snapshots(self.helper)
        self.assertEquals(result[0]['snapshot'],
                          '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b')
        self.assertNotEqual(result[0]['msg'], None)


class TestAuditVolumes(unittest.TestCase):
    def setUp(self):
        self.helper = Helper(LunrConfig())

        def mock_scan_volumes():
            return [
                {'clone_id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
                 'device_number': '252:9',
                 'id': '5304e6b7-986c-4d8b-9659-29a606f7c9e3',
                 'origin': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'path':
                 '/dev/lunr-volume/5304e6b7-986c-4d8b-9659-29a606f7c9e3',
                 'size': 5368709120},
                {'device_number': '252:1',
                 'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/91e81aec-b6da-40e9-82fb-f04a99a866eb',
                 'size': 3221225472,
                 'volume': True},
                {'device_number': '252:4',
                 'id': 'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
                 'size': 1073741824,
                 'volume': True},
                {'device_number': '252:0',
                 'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'size': 5368709120,
                 'volume': True}]

        self.helper.volumes._scan_volumes = mock_scan_volumes

        def mock_request(helper, url, **kwargs):
            return self.resp.pop(0)
        audit.request = mock_request

    def test_no_problems(self):

        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get list of active volumes
            [{
             'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
             'size': 5,
             'status': 'ACTIVE'},
             {
             'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
             'size': 3,
             'status': 'ACTIVE'},
             {
             'id': 'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
             'size': 1,
             'status': 'ACTIVE',
             }]
        ]
        # Should not report any issue, since the local volumes
        # are identical to the volumes returned by the API
        self.assertEquals(audit.volumes(self.helper), [])

    def test_deleted_but_still_exist(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get list of active volumes
            [{
             'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
             'size': 5,
             'status': 'ACTIVE'},
             {
             'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
             'size': 3,
             'status': 'ACTIVE'},
             {
             'id': 'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
             'size': 1,
             'status': 'DELETED',
             }]
        ]
        result = audit.volumes(self.helper)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0]['volume'],
                          'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98')
        self.assertNotEqual(result[0]['msg'], None)

    def test_missing_from_node(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get list of active volumes
            [{
             'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
             'size': 5,
             'status': 'ACTIVE'},
             {
             'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
             'size': 3,
             'status': 'ACTIVE'},
             {
             'id': 'e2c2591a-d6c6-11e3-8f58-080027bab7b1',
             'size': 1,
             'status': 'ACTIVE',
             }]
        ]
        result = audit.volumes(self.helper)
        self.assertEquals(len(result), 2)
        self.assertEquals(result[0]['volume'],
                          'e2c2591a-d6c6-11e3-8f58-080027bab7b1')
        self.assertNotEqual(result[0]['msg'], None)

    def test_missing_from_api(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get list of active volumes
            [{
             'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
             'size': 5,
             'status': 'ACTIVE'},
             {
             'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
             'size': 3,
             'status': 'ACTIVE'}]
        ]
        result = audit.volumes(self.helper)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0]['volume'],
                          'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98')
        self.assertNotEqual(result[0]['msg'], None)

    def test_sizes_do_not_match(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get list of active volumes
            [{
             'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
             'size': 5,
             'status': 'ACTIVE'},
             {
             'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
             'size': 2,
             'status': 'ACTIVE'},
             {
             'id': 'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
             'size': 1,
             'status': 'ACTIVE',
             }]
        ]

        result = audit.volumes(self.helper)
        self.assertEquals(len(result), 1)
        self.assertEquals(result[0]['volume'],
                          '91e81aec-b6da-40e9-82fb-f04a99a866eb')
        self.assertNotEqual(result[0]['msg'], None)


class TestNodeAudit(unittest.TestCase):
    def setUp(self):
        self.helper = Helper(LunrConfig())

        def mock_scan_volumes():
            return [
                {'clone_id': '70bde49f-0ca7-4fe1-bce4-b5a73f850b8b',
                 'device_number': '252:9',
                 'id': '5304e6b7-986c-4d8b-9659-29a606f7c9e3',
                 'origin': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'path':
                 '/dev/lunr-volume/5304e6b7-986c-4d8b-9659-29a606f7c9e3',
                 'size': 5368709120},
                {'device_number': '252:1',
                 'id': '91e81aec-b6da-40e9-82fb-f04a99a866eb',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/91e81aec-b6da-40e9-82fb-f04a99a866eb',
                 'size': 3221225472,
                 'volume': True},
                {'device_number': '252:4',
                 'id': 'c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/c21395aa-5a67-4d4e-ac70-d4ed8c6e7d98',
                 'size': 1073741824,
                 'volume': True},
                {'device_number': '252:0',
                 'id': 'c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'origin': '',
                 'path':
                 '/dev/lunr-volume/c270f302-9102-4327-bb0d-c8eaf9df872f',
                 'size': 5368709120,
                 'volume': True}]
        self.helper.volumes._scan_volumes = mock_scan_volumes

        def mock_request(helper, url, **kwargs):
            return self.resp.pop(0)
        audit.request = mock_request

        def mock_execute(*args, **kwargs):
            return self.execute_resp
        audit.execute = mock_execute

        self.execute_resp = textwrap.dedent("""
          --- Volume group ---
          VG Name               lunr-volume
          System ID
          Format                lvm2
          Metadata Areas        1
          Metadata Sequence No  952
          VG Access             read/write
          VG Status             resizable
          MAX LV                0
          Cur LV                5
          Open LV               1
          Max PV                0
          Cur PV                1
          Act PV                1
          VG Size               32984006656 B
          PE Size               4194304 B
          Total PE              7864
          Total PE              7864
          Alloc PE / Size       512 / 2147483648 B
          Free  PE / Size       7352 / 30836523008 B
          VG UUID               3UGgE7-hrkm-OF2e-dRLG-fOZw-waha-NHxWDr
        """)

    def test_no_problems(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get node details
            {
             'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
             'name': 'ubuntu',
             'size': 30,
             'status': 'ACTIVE',
             'storage_free': 21,
             'storage_used': 9}
        ]
        self.assertEquals(audit.node(self.helper), None)

    def test_storage_used_inconsistent(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get node details
            {
             'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
             'name': 'ubuntu',
             'size': 30,
             'status': 'ACTIVE',
             'storage_free': 21,
             'storage_used': 8}
        ]
        result = audit.node(self.helper)
        self.assertEquals(result, {
            'node': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
            'msg': 'API storage_used is inconsistent with node (8 != 9)'})

    def test_api_size_inconsistent(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get node details
            {
             'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
             'name': 'ubuntu',
             'size': 31,
             'status': 'ACTIVE',
             'storage_free': 21,
             'storage_used': 9}
        ]
        result = audit.node(self.helper)
        self.assertEquals(result, {
            'node': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
            'msg': "API 'size' is inconsistent with storage node (31 != 30)"})

    def test_api_storage_free_inconsistent(self):
        self.resp = [
            # Response for call to get node id
            [{
              'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
              'name': 'ubuntu'}],
            # Response for call to get node details
            {
             'id': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
             'name': 'ubuntu',
             'size': 30,
             'status': 'ACTIVE',
             'storage_free': 20,
             'storage_used': 9}
        ]
        result = audit.node(self.helper)
        self.assertEquals(result, {
            'node': '0ff84778-cdb8-4e5a-b897-f912641ca87c',
            'msg': "API 'storage_free' is inconsistent "
                   "with storage node  (20 != 21)"})
