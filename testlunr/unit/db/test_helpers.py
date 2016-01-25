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
import datetime

import unittest
from urllib import urlencode
from webob import Request
from webob.exc import HTTPError, HTTPBadRequest

from lunr.db import helpers
from lunr.db import models


class TestHelpers(unittest.TestCase):

    def setUp(self):
        self.model = models.Account()

    def test_filter_empty_params(self):
        req = Request.blank('/')
        update_params, meta_params = \
            helpers.filter_update_params(req, self.model)
        self.assertEquals({}, update_params)
        self.assertEquals({}, meta_params)

    def test_filter_valid_params(self):
        request_params = {'status': 'ACTIVE'}
        req = Request.blank('/', method='POST',
                            body=urlencode(request_params))
        update_params, meta_params = \
            helpers.filter_update_params(req, self.model)
        self.assertEquals(request_params, update_params)
        self.assertEquals({}, meta_params)

    def test_filter_invalid_params(self):
        request_params = {'id': 'acct1', 'status': 'ACTIVE',
                          'invalid': 'bad_data'}
        req = Request.blank('/', method='POST',
                            body=urlencode(request_params))
        self.assertRaises(HTTPError, helpers.filter_update_params, req,
                          self.model)

    def test_filter_immutable_params(self):
        request_params = {'id': 'acct1'}
        req = Request.blank('/', method='POST',
                            body=urlencode(request_params))
        self.assertRaises(HTTPBadRequest, helpers.filter_update_params, req,
                          self.model)

        request_params = {'created_at': datetime.datetime.now().isoformat()}
        req = Request.blank('/', method='POST', body=urlencode(request_params))
        self.assertRaises(HTTPBadRequest, helpers.filter_update_params, req,
                          self.model)

    def test_filter_meta_params(self):
        request_params = {'x-meta-key1': 'val1', 'X-Meta-Key2': 'val2'}
        req = Request.blank('/', method='POST',
                            body=urlencode(request_params))
        update_params, meta_params = \
            helpers.filter_update_params(req, self.model)
        self.assertEquals({}, update_params)
        expected = {'key1': 'val1', 'Key2': 'val2'}
        self.assertEquals(expected, meta_params)

    def test_update_and_meta_params(self):
        request_params = {'status': 'ACTIVE',
                          'x-meta-key1': 'val1',
                          'X-Meta-Key2': 'val2'}
        req = Request.blank('/', method='POST',
                            body=urlencode(request_params))
        update_params, meta_params = \
            helpers.filter_update_params(req, self.model)
        expected_update = {'status': 'ACTIVE'}
        self.assertEquals(expected_update, update_params)
        expected_meta = {'key1': 'val1', 'Key2': 'val2'}
        self.assertEquals(expected_meta, meta_params)


if __name__ == "__main__":
    unittest.main()
