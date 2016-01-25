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


from datetime import datetime, timedelta
from lunr.orbit import CronJob, CronError
import unittest


class TestSuspects(unittest.TestCase):

    def test_parser(self):
        job = CronJob()
        self.assertEquals(job.parse("hours=1"), timedelta(0, 3600))
        self.assertEquals(job.parse("hours = 1"), timedelta(0, 3600))
        self.assertEquals(job.parse("hours= 1"), timedelta(0, 3600))
        self.assertEquals(job.parse("hours=1, days=1"), timedelta(1, 3600))

    def test_parser_fail(self):
        job = CronJob()
        self.assertRaises(CronError, job.parse, "hours 1")
        self.assertRaises(CronError, job.parse, "")
        self.assertRaises(CronError, job.parse, "hour=1")
