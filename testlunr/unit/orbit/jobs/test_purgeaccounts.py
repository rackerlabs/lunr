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
from lunr.orbit.jobs.purgeaccounts import PurgeAccounts
from testlunr.unit import patch


class MockLog(object):
    msg = ""

    def info(self, msg):
        self.msg = msg

    def error(self, msg):
        self.msg = msg


class TestPurgeAccounts(unittest.TestCase):
    def setUp(self):
        self.conf = LunrConfig({'db': {'auto_create': True,
                                       'url': 'sqlite://'}})
        self.sess = db.configure(self.conf)
        self.log = MockLog()
        self.reader = PurgeAccounts(self.conf, self.sess)

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

