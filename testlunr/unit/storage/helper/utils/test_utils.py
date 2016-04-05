#!/usr/bin/env python
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

import subprocess
import unittest

from StringIO import StringIO
from urllib2 import HTTPError
from urlparse import urlparse, parse_qs

from lunr.storage.helper import utils
from lunr.storage.helper.utils import APIError

from testlunr.unit import patch


class MockPopen(object):

    def __init__(self, returncode=0):
        self.returncode = returncode

    def communicate(self):
        return 'stuff', ''


class TestUtils(unittest.TestCase):

    def test_make_api_request_defaults(self):
        def mock_urlopen(req, data=None):
            expected = 'http://localhost:8080/v1.0/admin/nodes'
            self.assertEquals(req.get_full_url(), expected)
            mock_urlopen.called = True
        with patch(utils, 'urlopen', mock_urlopen):
            utils.make_api_request('nodes')
        self.assert_(mock_urlopen.called)

    def test_make_api_request_raise(self):
        def mock_urlopen(req, data=None):
            raise HTTPError(req.get_full_url(), 404, 'Not Found',
                            {}, StringIO('{"reason": "not found"}'))
        with patch(utils, 'urlopen', mock_urlopen):
            self.assertRaises(APIError, utils.make_api_request, 'not_found')

    def test_lookup_id(self):
        cinder_host = 'cinder_host'
        storage_vol_id = 'storage_vol_id'
        api_vol_id = 'storage_vol_id'

        def mock_make_api_request(resource, api_server=None):
            query_string = urlparse(resource).query
            params = parse_qs(query_string)
            self.assertEquals(params, {'name': [storage_vol_id],
                                       'cinder_host': [cinder_host]})
            volumes = ('[{"id": "%s", "status": "ACTIVE"},'
                       '{"id": "deleted", "status": "DELETED"}]' % api_vol_id)
            mock_make_api_request.called = True
            return StringIO(volumes)
        mock_make_api_request.called = False

        with patch(utils, 'make_api_request', mock_make_api_request):
            lookup_id = utils.lookup_id(storage_vol_id, 'unused', cinder_host)

        self.assertTrue(mock_make_api_request.called)
        self.assertEquals(lookup_id, api_vol_id)

    def test_lookup_id_deleted(self):
        cinder_host = 'cinder_host'
        storage_vol_id = 'storage_vol_id'
        api_vol_id = 'storage_vol_id'

        def mock_make_api_request(resource, data=None, api_server=None):
            query_string = urlparse(resource).query
            params = parse_qs(query_string)
            self.assertEquals(params, {'name': [storage_vol_id],
                                       'cinder_host': [cinder_host]})
            volumes = ('[{"id": "%s", "status": "DELETED"},'
                       '{"id": "deleted", "status": "DELETED"}]' % api_vol_id)
            mock_make_api_request.called = True
            return StringIO(volumes)
        mock_make_api_request.called = False

        with patch(utils, 'make_api_request', mock_make_api_request):
            try:
                utils.lookup_id(storage_vol_id, 'unused', cinder_host)
            except HTTPError, e:
                self.assertEquals(e.code, 404)
                self.assertTrue(mock_make_api_request.called)
            else:
                self.fail("Should be unreached")

    def test_lookup_id_409(self):
        cinder_host = 'cinder_host'
        storage_vol_id = 'storage_vol_id'

        def mock_make_api_request(resource, data=None, api_server=None):
            query_string = urlparse(resource).query
            params = parse_qs(query_string)
            self.assertEquals(params, {'name': [storage_vol_id],
                                       'cinder_host': [cinder_host]})
            volumes = ('[{"id": "deleting", "status": "DELETING"},'
                       '{"id": "building", "status": "BUILDING"}]')
            mock_make_api_request.called = True
            return StringIO(volumes)
        mock_make_api_request.called = False

        with patch(utils, 'make_api_request', mock_make_api_request):
            try:
                utils.lookup_id(storage_vol_id, 'unused', cinder_host)
            except HTTPError, e:
                self.assertEquals(e.code, 409)
                self.assertTrue(mock_make_api_request.called)
            else:
                self.fail("Should be unreached")

    def test_make_api_request_volume_id(self):
        volume_id = 'v1'
        volume_name = 'volume_name'
        cinder_host = 'node_cinder_host'

        def mock_urlopen(req, data=None):
            expected = 'http://localhost:8080/v1.0/admin/volumes/v1'
            self.assertEquals(req.get_full_url(), expected)
            mock_urlopen.called = True
        mock_urlopen.called = False

        def mock_lookup_id(id, api_server, cinder_host):
            self.assertEquals(id, volume_name)
            mock_lookup_id.called = True
            return volume_id
        mock_lookup_id.called = False

        data = {'cinder_host': cinder_host, 'foo': 'bar'}

        with patch(utils, 'urlopen', mock_urlopen):
            with patch(utils, 'lookup_id', mock_lookup_id):
                utils.make_api_request('volumes', volume_name, data=data)

        self.assertTrue(mock_urlopen.called)
        self.assertTrue(mock_lookup_id.called)

    def test_execute_sudo(self):
        execute_args = []

        def mock_popen(args, **kwargs):
            execute_args.extend(args)
            return MockPopen()

        with patch(subprocess, 'Popen', mock_popen):
            utils.execute('ls', '-r', _all=None, color='auto')

        self.assertEqual(5, len(execute_args))
        self.assertEqual(['sudo', 'ls', '-r', '--all', '--color=auto'],
                         execute_args)

    def test_execute_nosudo(self):
        execute_args = []

        def mock_popen(args, **kwargs):
            execute_args.extend(args)
            return MockPopen()

        with patch(subprocess, 'Popen', mock_popen):
            utils.execute('ls', '-r', _all=None, color='auto', sudo=False)

        self.assertEqual(4, len(execute_args))
        self.assertEqual(['ls', '-r', '--all', '--color=auto'], execute_args)


if __name__ == "__main__":
    unittest.main()
