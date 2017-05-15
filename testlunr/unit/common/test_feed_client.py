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
import os
import unittest
from lunr.common import feed_client
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
        self.feed = cloudfeedclient.Feed(self.conf, self.log,
                                         "http://no-host.com", "fake-token")
        self.test_file = os.path.join(os.path.dirname(__file__),
                                      'test_feed_client.xml')

    def test_get_events_200(self):
        def urlopen(request, **kwargs):
            with open(self.test_file) as fd:
                return MockResponse(fd.read(), 200)

        with patch(cloudfeedclient, 'urlopen', urlopen):
                events = list(self.feed.get_events())
                self.assertEquals(len(events), 5)
                self.assertIn('product', events[0])
                self.assertIn('tenantId', events[0])
                self.assertEquals(events[0]['product']['status'], "TERMINATED")
                self.assertIn("id", events[0])

    def test_get_events_404(self):
        def urlopen(request, **kwargs):
            return MockResponse("", 404)

        with patch(cloudfeedclient, 'urlopen', urlopen):
            with self.assertRaises(cloudfeedclient.InvalidMarker):
                list(self.feed.get_events())

    def test_get_events_500(self):
        def urlopen(request, **kwargs):
            return MockResponse("", 500)

        with patch(cloudfeedclient, 'urlopen', urlopen):
            with self.assertRaises(cloudfeedclient.GetPageFailed):
                list(self.feed.get_events())

    def test_get_events_403(self):
        def urlopen(request, **kwargs):
            return MockResponse("", 403)

        with patch(cloudfeedclient, 'urlopen', urlopen):
            with self.assertRaises(cloudfeedclient.GetPageFailed):
                list(self.feed.get_events())
