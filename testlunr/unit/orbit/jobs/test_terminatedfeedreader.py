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
from lunr.db.models import Error, Audit, Event, Marker
from lunr.common import cloudfeedclient
from lunr.cinder import cinderclient


class TestEvent(unittest.TestCase):
    def setUp(self):
        self.test_event = \
            {
                'dataCenter': 'GLOBAL',
                'eventTime': '2015-05-05T01:46:00.004Z',
                'xmlns': 'http://docs.rackspace.com/core/event',
                'xmlns:ns2': 'http://docs.rackspace.com/event/customer/access_policy',
                'region': 'GLOBAL',
                'product': {
                         'previousEvent': '',
                         'status': 'SUSPENDED',
                         'version': '1',
                         'serviceCode': 'CustomerService'
                },
                'tenantId': '6166031',
                'environment': 'PROD',
                'version': '2',
                'type': 'INFO',
                'id': 'fd1b60a4-d86d-48c1-81ae-8febac824f08'
             }

    def test_object_creation(self):
        event = Event(self.test_event)
        self.assertEqual(event.uuid, 'fd1b60a4-d86d-48c1-81ae-8febac824f08')


if __name__ == '__main__':
    unittest.main()
