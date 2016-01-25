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
from uuid import uuid4
from webob import Request

from testlunr.unit import MockLogger

from lunr.middleware import trans_logger


def application(environ, start_response):
    start_response('200 OK', (('Content-Length', '13'),))
    return ['hello, world\n']


class TestResponseLoggerFilter(unittest.TestCase):

    def setUp(self):
        # swap out the module
        self._orig_logger = trans_logger.logger
        # for something else that responds to get_logger and returns a logger
        trans_logger.logger = MockLogger()
        # ensure "global" logger is named
        trans_logger.logger.get_logger('lunr')

    def tearDown(self):
        trans_logger.logger = self._orig_logger

    def test_not_logging_responses(self):
        request_id = 'test-%s' % uuid4()
        trans_logger.logger.debug('START')

        def start_response(status, headers):
            start_response.called = True
            self.assertEquals(status, '200 OK')
            self.assertEquals(headers, [('Content-Length', '13'),
                                        ('X-Request-Id', request_id)])

        filtered_app = trans_logger.filter_factory({})(application)
        req = Request.blank('/', headers={'x-request-id': request_id})
        body_iter = filtered_app(req.environ, start_response)
        self.assertEquals(start_response.called, True)
        self.assertEquals(body_iter, ['hello, world\n'])
        trans_logger.logger.info('FINISH')
        expected = """
lunr:DEBUG:START
lunr:INFO:- "GET /" 200 13
lunr:INFO:FINISH
""".lstrip()
        self.assertEquals(trans_logger.logger.pop_log_messages(), expected)

    def test_logging_responses(self):
        request_id = 'test-%s' % uuid4()
        trans_logger.logger.debug('START')

        def start_response(status, headers):
            start_response.called = True
            self.assertEquals(status, '200 OK')
            self.assertEquals(headers, [('Content-Length', '13'),
                                        ('X-Request-Id', request_id)])

        local_conf = {
            'echo': 'True',
            'level': 'WARNING',
        }
        _filter = trans_logger.filter_factory({}, **local_conf)
        filtered_app = _filter(application)
        req = Request.blank('/', headers={'x-request-id': request_id})
        body_iter = filtered_app(req.environ, start_response)
        self.assertEquals(start_response.called, True)
        self.assertEquals(body_iter, ['hello, world\n'])
        trans_logger.logger.info('FINISH')
        expected = """
lunr:DEBUG:START
lunr:WARNING:REQUEST:
GET / HTTP/1.0\r
Host: localhost:80\r
X-Request-Id: %(request_id)s
lunr:INFO:- "GET /" 200 13
lunr:WARNING:RESPONSE:
200 OK
Content-Length: 13
X-Request-Id: %(request_id)s

hello, world

lunr:INFO:FINISH
""".lstrip() % {'request_id': request_id}
        self.assertEquals(trans_logger.logger.pop_log_messages(), expected)


if __name__ == "__main__":
    unittest.main()
