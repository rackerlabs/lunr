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
from urllib2 import Request
from StringIO import StringIO
import json
import os
import errno
import httplib

from lunr.common import exc


class TestException(unittest.TestCase):

    def setUp(self):
        self.req = Request('http://localhost/path')

    def test_socket_timeout(self):
        error = exc.socket.timeout('timed out')
        e = exc.HTTPClientError(self.req, error)
        expected = 'GET on http://localhost/path failed with socket timeout'
        self.assertEquals(str(e), expected)

    def test_urllib_http_error(self):
        error = exc.urllib2.HTTPError(
            self.req.get_full_url(), 401,
            'Unauthorized', {}, StringIO("<h2>some error</h2>"))
        e = exc.HTTPClientError(self.req, error)
        expected = "GET on http://localhost/path returned " \
                   "'401' with '<h2>some error</h2>'"
        self.assertEquals(str(e), expected)

    def test_urllib_http_error_with_json(self):
        body = {
            'reason': 'Invalid Token'
        }
        data = json.dumps(body)
        error = exc.urllib2.HTTPError(self.req.get_full_url(), 401,
                                      'Unauthorized', {}, StringIO(data))
        e = exc.HTTPClientError(self.req, error)
        expected = "GET on http://localhost/path returned " \
                   "'401' with 'Invalid Token'"
        self.assertEquals(str(e), expected)
        body = {
            'message': 'You supplied an invalid token.'
        }
        data = json.dumps(body)
        error = exc.urllib2.HTTPError(self.req.get_full_url(), 401,
                                      'Unauthorized', {}, StringIO(data))
        e = exc.HTTPClientError(self.req, error)
        expected = "GET on http://localhost/path returned " \
                   "'401' with 'You supplied an invalid token.'"
        self.assertEquals(str(e), expected)

    def test_urllib_url_error(self):
        os_error = OSError(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED))
        socket_error = exc.socket.error(os_error)
        error = exc.urllib2.URLError(socket_error)
        e = exc.HTTPClientError(self.req, error)
        expected = "GET on http://localhost/path failed with '%s'" % os_error
        self.assertEquals(str(e), expected)

    def test_bad_status_line_error(self):
        error = httplib.BadStatusLine('')
        e = exc.HTTPClientError(self.req, error)
        expected = "GET on http://localhost/path failed " \
                   "with '%s'" % error.__class__.__name__
        self.assertEquals(str(e), expected)


if __name__ == "__main__":
    unittest.main()
