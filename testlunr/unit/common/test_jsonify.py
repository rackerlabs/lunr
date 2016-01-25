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

import datetime
import decimal
from webob.multidict import MultiDict
from collections import Mapping
import uuid

from lunr.common import jsonify


class TestJsonify(unittest.TestCase):

    def test_jsonify_date(self):
        data = datetime.date(2013, 1, 1)
        json_data = jsonify.encode(data)
        self.assertEqual('"2013-01-01"', json_data)

    def test_jsonify_decimal(self):
        data = decimal.Decimal('5.15')
        json_data = jsonify.encode(data)
        self.assertEqual('5.15', json_data)

    def test_jsonify_multidict(self):
        data = MultiDict()
        data.add('a', 1)
        data.add('a', 2)
        data.add('b', 3)
        json_data = jsonify.encode(data)
        self.assertEqual('{"a": [1, 2], "b": 3}', json_data)

    def test_jsonify_mapping(self):
        # I don't understand this well enough to write
        # a test for it.
        pass

    def test_jsonify_uuid(self):
        data = uuid.uuid4()
        expected = '"' + str(data) + '"'
        json_data = jsonify.encode(data)
        self.assertEqual(expected, json_data)


if __name__ == "__main__":
    unittest.main()
