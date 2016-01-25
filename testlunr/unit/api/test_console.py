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


from json import dumps
from StringIO import StringIO
import unittest
import urllib2

from lunr.api import console
from lunr.api.console import Console, TypeConsole, NodeConsole, AccountConsole
from lunr.common.subcommand import SubCommandParser
from testlunr.unit import patch


class TestConsole(unittest.TestCase):
    def test_ip_to_url(self):
        obj = Console()
        self.assertEquals(obj.ip_to_url('localhost'), 'http://localhost:8080')
        self.assertEquals(obj.ip_to_url('localhost:2121'),
                          'http://localhost:2121')
        self.assertEquals(obj.ip_to_url('http://localhost:2121'),
                          'http://localhost:2121')


class TestConsoleNode(TestConsole):
    """
    Tests the NodeConsole class.
    """
    def test_deploy_all(self):
        def validate_update(req):
            self.assert_('node2' in req.get_full_url())
            self.assertEquals(req.get_method(), 'POST')
            return 200, {'id': 'node2', 'status': 'ACTIVE'}

        responses = iter([
            lambda req: (200, [
                {'id': 'node1', 'status': 'ACTIVE'},
                {'id': 'node2', 'status': 'PENDING'},
            ]),
            validate_update,
        ])

        def mock_open(req):
            response = responses.next()
            code, info = response(req)
            body = StringIO(dumps(info))
            return urllib2.addinfourl(body, {}, req.get_full_url(), code)

        with patch(console, 'urlopen', mock_open):
            parser = SubCommandParser([console.NodeConsole()])
            parser.run("node deploy --all".split())


if __name__ == "__main__":
    unittest.main()
