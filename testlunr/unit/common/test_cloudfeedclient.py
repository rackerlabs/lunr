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
from lunr.common import cloudfeedclient
from lunr.common.config import LunrConfig
from testlunr.unit import patch


class MockLog(object):
    msg = ""

    def info(self, msg):
        self.msg = msg

    def error(self, msg):
        self.msg = msg


class MockResponse(object):
    def __init__(self, body, code):
        self.body = body
        self.code = code

    def getcode(self):
        return self.code

    def read(self):
        return self.body


class TestBackupSuspects(unittest.TestCase):

    def setUp(self):
        self.conf = LunrConfig()
        self.log = MockLog()
        self.feed = cloudfeedclient.Feed(self.config, self.log,
                                         "http://no-host.com", "fake-token")

    def test_get_events_200(self):

        def urlopen(request, **kwargs):
            with open("test_terminatedfeedreader.xml") as fd:
                return MockResponse(fd.readl())
                    'in-use': True,
                    'uri': 'DELETE /volumes/ed209cdd-1317-41e8-8474-b0c0f6c3369c/'
                           'backups/a30a6e5b-2a96-489c-bde1-56f9c615ea1f',
                }), 200)

        with patch(cloudfeedclient, 'urlopen', urlopen):
                events = self.feed.get_events()
                self.assertEquals(len(events), 5)
