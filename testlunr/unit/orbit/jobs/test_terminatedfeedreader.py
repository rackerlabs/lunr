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

from lunr.common.config import LunrConfig
from lunr.db.models import Error, Event, Marker
from lunr.common import cloudfeedclient
from lunr import db
from lunr.orbit.jobs.terminatedfeedreader import TerminatedFeedReader
from testlunr.unit import patch


class MockLog(object):
    msg = ""

    def info(self, msg):
        self.msg = msg

    def error(self, msg):
        self.msg = msg


class TestTerminatedFeedReader(unittest.TestCase):
    def setUp(self):
        self.conf = LunrConfig({'db': {'auto_create': True,
                                       'url': 'sqlite://'}})
        self.sess = db.configure(self.conf)
        self.log = MockLog()
        self.reader = TerminatedFeedReader(self.conf, self.sess)

    # def test_run(self):
    #    self.reader.run()

    def test_log_to_db_empty_event(self):
        error_msg = "Test Exception"
        self.reader.log_error_to_db(Exception(error_msg))
        obj = self.sess.query(Error).filter(Error.message == error_msg).first()
        self.assertEqual(obj.message, error_msg)
        self.assertEqual(obj.event_id, None)
        self.assertEqual(obj.tenant_id, None)

    def test_log_to_db_with_event(self):
        error_msg = "Test Exception"
        event = Event(tenant_id='123', id='345')
        self.reader.log_error_to_db(Exception(error_msg), event)
        obj = self.sess.query(Error).filter(Error.message == error_msg).first()
        self.assertEqual(obj.message, error_msg)
        self.assertEqual(obj.tenant_id, event.tenant_id)
        self.assertEqual(obj.event_id, event.event_id)

    def test_remove_errors(self):
        mock_error = Error(event_id="123", tenant_id="456",
                           message=str("Test Exception"), type="test")
        self.sess.add(mock_error)
        self.reader.remove_errors("test")
        obj = self.sess.query(Error).filter(Error.type == "test").first()
        self.assertEqual(obj, None)

    def test_save_event(self):
        mock_event = {'id': '123', 'tenantId': '456'}
        self.reader.save_event(mock_event)
        self.sess.commit()
        obj = self.sess.query(Event).filter(Event.event_id == mock_event['id']).first()
        self.assertEqual(obj.event_id, mock_event['id'])
        self.assertEqual(obj.tenant_id, mock_event['tenantId'])

    def test_save_marker(self):
        self.reader.marker = 'test-marker'
        self.reader.save_marker()
        self.sess.commit()
        obj = self.sess.query(Marker).first()
        self.assertEqual(obj.last_marker, self.reader.marker)

    def test_fetch_last_marker_empty_marker(self):
        self.reader.fetch_last_marker()
        self.assertEqual(self.reader.marker, None)

    def test_fetch_last_marker_with_marker(self):
        mock_marker = Marker(last_marker='test-marker')
        self.sess.add(mock_marker)
        self.sess.commit()
        self.reader.marker = self.reader.fetch_last_marker()
        self.assertIsNotNone(self.reader.marker)

    def test_fetch_events(self):
        # Mock cinder client request
        def fetch_token():
            return 'fake-token'

        # Mock feed server request
        def fetch_feed():
            return cloudfeedclient.Feed(self.conf, self.log,
                                        "http://no-host.com", "fake-token")

        with patch(self.reader, 'fetch_token', fetch_token):
            with patch(self.reader, 'fetch_feed', fetch_feed):
                events = self.reader.fetch_events()
                self.assertIsNotNone(events, "auth/feed retrieval is failing")

if __name__ == '__main__':
    unittest.main()
