# Copyright (c) 2011-2015 Rackspace US, Inc.
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
import logging

from lunr.middleware import error_catcher
from lunr.common import logger


class TestStorageApp(unittest.TestCase):

    def test_catching_error(self):
        start_response_args = []

        def broken_app(env, start_response):
            raise Exception('THIS IS A BROKEN APP')

        def start_response(status, headers):
            start_response_args.append(status)
            start_response_args.append(headers)

        filtered_app = error_catcher.filter_factory({})(broken_app)
        response = ''.join(filtered_app({}, start_response))

        self.assert_(start_response_args[0].startswith('500'))
        self.assert_('an error occurred' in response.lower())
        self.assertEquals(dict(start_response_args[1])['content-type'],
                          'text/html')

    def test_no_error(self):
        start_response_args = []

        def working_app(env, start_response):
            start_response('200 OK', [])
            return ['EVERYTHING IS COOL']

        def start_response(status, headers):
            start_response_args.append(status)
            start_response_args.append(headers)

        filtered_app = error_catcher.filter_factory({})(working_app)
        response = ''.join(filtered_app({}, start_response))

        self.assert_(start_response_args[0].startswith('200'))
        self.assert_('everything is cool' in response.lower())


if __name__ == "__main__":
    unittest.main()
