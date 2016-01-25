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


from testlunr.integration import IetTest
from lunr.storage.helper.utils import execute
from lunr.storage.helper.export import ietadm
from lunr.common.config import LunrConfig
from lunr.storage.helper import base
from tempfile import mkdtemp
from uuid import uuid4
import unittest
from unittest import skip
import shutil
import os


class TestExportHelper(IetTest):

    def setUp(self):
        self.tempdir = mkdtemp()
        conf = self.config(self.tempdir)
        self.volume = 'volume-%s' % uuid4()
        help = base.Helper(conf)
        self.exports = help.exports
        self.volumes = help.volumes
        self.volumes.create(self.volume)
        self.exports.create(self.volume)
        self.host = '127.0.0.1'

    def tearDown(self):
        self.exports.delete(self.volume)
        shutil.rmtree(self.tempdir)

    def test_scan_exports(self):
        # Scan the current exports
        result = self.exports._scan_exports()
        # Find the export we created
        result = [v for v in result if v['volume'] == self.volume][0]
        # Assert it contains the information we need
        self.assertEquals(result['blocks'], '24576')
        self.assert_(self.volume in result['name'])
        self.assertEquals(result['blocksize'], '512')
        self.assertEquals(result['volume'], self.volume)
        self.assertEquals(result['state'], '0')
        self.assertEquals(result['iomode'], 'wt')
        self.assertEquals(result['path'], self.exports._lun_path(self.volume))
        self.assertEquals(result['iotype'], 'blockio')
        self.assertEquals(result['lun'], '0')
        self.assertTrue('tid' in result)

    def test_get_export_with_missing_lun(self):
        # Get the tid
        tid = self.exports._get_tid(self.volume)
        # Delete the lun from the export
        path = self.exports._lun_path(self.volume)
        ietadm(op='delete', tid=tid, lun=0, params={'Path': path})
        # Get the export information
        result = self.exports.get(self.volume)
        # Assert we can still see the export with the lun deleted
        self.assertEquals(result['volume'], self.volume)
        self.assert_(self.volume in result['name'])
        self.assertTrue('tid' in result)

    def test_scan_sessions(self):
        # Scan the current open sessions
        result = self.exports._scan_sessions()
        # Find the export we created
        result = [v for v in result if v['volume'] == self.volume][0]
        # Assert it contains basic information
        self.assert_(self.volume in result['name'])
        self.assertTrue('tid' in result)

    def test_session(self):
        # Get the session, assert is not 'Connected'
        sessions = self.exports._sessions(self.volume)
        self.assertEquals(len(sessions), 1)
        self.assertEquals(sessions[0]['connected'], False)

        # Connect to the exported volume
        export = self.exports.get(self.volume)
        execute('iscsiadm', mode='discovery', type='sendtargets',
                portal=self.host)
        execute('iscsiadm', mode='node', portal=self.host, login=None,
                targetname=export['name'])

        # Get the session, assert is 'Connected'
        sessions = self.exports._sessions(self.volume)
        self.assertEquals(len(sessions), 1)
        self.assertEquals(sessions[0]['connected'], True)
        self.assertEquals(sessions[0]['state'], 'active')
        self.assertEquals(sessions[0]['ip'], self.host)
        self.assertTrue('initiator' in sessions[0])

        # Logout of the exported volume
        execute('iscsiadm', mode='node', portal=self.host, logout=None,
                targetname=export['name'])

if __name__ == "__main__":
    unittest.main()
