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
