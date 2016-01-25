#!/usr/bin/env python
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
import json
from StringIO import StringIO

from lunr.common.config import LunrConfig
from lunr.storage.helper.utils.client import swift
from testlunr.unit import patch

# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=True)


def sleep(secs):
    return


class Mock():
    def __init__(self):
        self.count = 0


class AuthFailAfter500(Mock):
    def __call__(self, url, token, http_conn=None):
        self.count += 1
        if self.count == 1:
            raise swift.ClientException("Some 500 Error", http_status=500)
        if self.count == 2:
            raise swift.ClientException("Some 401 Error", http_status=401)
        return ''


class SimulateAuthCacheFail(Mock):
    def __call__(self, url, token, http_conn=None):
        self.count += 1
        if self.count == 1:
            raise swift.ClientException("Some 401 Error", http_status=401)
        return ''


class Always500(Mock):
    def __call__(self, url, token, http_conn=None):
        self.count += 1
        raise swift.ClientException("Some 500 Error", http_status=500)


class Always401(Mock):
    def __call__(self, url, token, http_conn=None):
        self.count += 1
        raise swift.ClientException("Some 401 Error", http_status=401)


class Always408(Mock):
    def __call__(self, url, token, http_conn=None):
        self.count += 1
        raise swift.ClientException("Some 408 Error", http_status=408)


class Always400(Mock):
    def __call__(self, url, token, http_conn=None):
        self.count += 1
        raise swift.ClientException("Some 400 Error", http_status=400)


class TestSwiftClient(unittest.TestCase):

    def setUp(self):
        self.conn = swift.Connection('http:/localauth.com/auth1/',
                                     'user', 'key', 'USA', retries=5)
        self.conn.get_auth = lambda: (None, None)
        self.conn.http_connection = lambda: None

    def test_connect(self):
        conf = LunrConfig()
        conn = swift.connect(conf)
        self.assert_(conn)

    def test_fail_after_500(self):
        head_container = AuthFailAfter500()
        with patch(swift, 'sleep', sleep):
            self.conn._retry(None, head_container)
        self.assertEquals(head_container.count, 3)

    def test_2_auth_success(self):
        head_container = SimulateAuthCacheFail()
        with patch(swift, 'sleep', sleep):
            self.conn._retry(None, head_container)
        self.assertEquals(head_container.count, 2)

    def test_2_auth_fail(self):
        head_container = Always401()
        with patch(swift, 'sleep', sleep):
            self.assertRaises(swift.ClientException,
                              self.conn._retry, None, head_container)
        self.assertEquals(head_container.count, 2)

    def test_retry_408_errors(self):
        head_container = Always408()
        with patch(swift, 'sleep', sleep):
            self.assertRaises(swift.ClientException,
                              self.conn._retry, None, head_container)
        self.assertEquals(head_container.count, 6)

    def test_retry_500_errors(self):
        head_container = Always500()
        with patch(swift, 'sleep', sleep):
            self.assertRaises(swift.ClientException,
                              self.conn._retry, None, head_container)
        self.assertEquals(head_container.count, 6)

    def test_raise_non_500_errors(self):
        head_container = Always400()
        with patch(swift, 'sleep', sleep):
            self.assertRaises(swift.ClientException,
                              self.conn._retry, None, head_container)
        self.assertEquals(head_container.count, 1)


class MockResponse(object):

    def __init__(self, status=200, body='', headers=None):
        self.status = status
        self.headers = headers or {}
        self.body = body

    def read(self, size=None):
        if not size:
            return self.body
        if not hasattr(self, '_body_iter'):
            self._body_iter = StringIO(self.body)
        return self._body_iter.read(size)

    def getheaders(self):
        return self.headers.items()

    def getheader(self, key, *args, **kwargs):
        return self.headers.get(key, *args, **kwargs)


def _bad_service_auth_response():
        response_body = {
            'access': {
                "serviceCatalog": [{
                    "endpoints": [{
                        "internalURL": "http://snet-storagehost.com ",
                        "publicURL": "http://storagehost.com ",
                        "region": "USA",
                        "tenantId": "MossoCloudFS_aaaa-bbbb-cccc"}],
                    "name": "cloudFiles",
                    "type": "fog-store"}],
                "token": {
                    "expires": "2012-04-13T13:15:00.000-05:00",
                    "id": "aaaaa-bbbbb-ccccc-dddd"}}}
        return MockResponse(200, body=json.dumps(response_body))


def _bad_region_auth_response():
        response_body = {
            'access': {
                "serviceCatalog": [{
                    "endpoints": [{
                        "internalURL": "http://snet-storagehost.com ",
                        "publicURL": "http://storagehost.com ",
                        "region": "CHINA",
                        "tenantId": "MossoCloudFS_aaaa-bbbb-cccc"}],
                    "name": "cloudFiles",
                    "type": "object-store"}],
                "token": {
                    "expires": "2012-04-13T13:15:00.000-05:00",
                    "id": "aaaaa-bbbbb-ccccc-dddd"}}}
        return MockResponse(200, body=json.dumps(response_body))


def _bad_url_auth_response():
        response_body = {
            'access': {
                "serviceCatalog": [{
                    "endpoints": [{
                        "publicURL": "http://storagehost.com ",
                        "region": "USA",
                        "tenantId": "MossoCloudFS_aaaa-bbbb-cccc"}],
                    "name": "cloudFiles",
                    "type": "object-store"}],
                "token": {
                    "expires": "2012-04-13T13:15:00.000-05:00",
                    "id": "aaaaa-bbbbb-ccccc-dddd"}}}
        return MockResponse(200, body=json.dumps(response_body))


def _bad_tk_auth_response():
        response_body = {
            'access': {
                "serviceCatalog": [{
                    "endpoints": [{
                        "internalURL": "http://snet-storagehost.com ",
                        "publicURL": "http://storagehost.com ",
                        "region": "USA",
                        "tenantId": "MossoCloudFS_aaaa-bbbb-cccc"}],
                    "name": "cloudFiles",
                    "type": "object-store"}],
                "token": {
                    "expires": "2012-04-13T13:15:00.000-05:00"}}}
        return MockResponse(200, body=json.dumps(response_body))


def _stub_auth_response():
        response_body = {
            'access': {
                "serviceCatalog": [{
                    "endpoints": [{
                        "internalURL": "http://snet-storagehost.com ",
                        "publicURL": "http://storagehost.com ",
                        "region": "USA",
                        "tenantId": "MossoCloudFS_aaaa-bbbb-cccc"}],
                    "name": "cloudFiles",
                    "type": "object-store"}],
                "token": {
                    "expires": "2012-04-13T13:15:00.000-05:00",
                    "id": "aaaaa-bbbbb-ccccc-dddd"}}}
        return MockResponse(200, body=json.dumps(response_body))


class MockHTTPConnection(object):

    def __init__(self, host):
        pass

    def request(self, method, path, body, headers):
        try:
            validator = self.validators.pop(0)
        except (AttributeError, IndexError):
            pass
        else:
            validator(method, path, body, headers)
            validator.called = True

    def getresponse(self):
        resp = self.responses.pop(0)
        resp.consumed = True
        return resp


def mock_connection_factory(test_case):
    """
    define callable which returns a mock http conn with responses and
    validators lazyily loaded from the test case
    """
    def wrapped(*args, **kwargs):
        conn = MockHTTPConnection(*args, **kwargs)
        conn.responses = test_case.responses
        conn.validators = test_case.validators
        return conn
    return wrapped


class TestSwiftClientConnection(unittest.TestCase):

    def setUp(self):
        # give the factory a ref to test so it can lazy load responses
        swift.HTTPConnection = mock_connection_factory(self)

    def test_get_object_chunked(self):
        chunk_size = 10
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/chunk1')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        # stub mock responses
        full_body = ('a' * chunk_size) + ('b' * chunk_size)
        get_response = MockResponse(body=full_body)
        self.responses = [_stub_auth_response(), get_response]
        # create connection
        c = swift.Connection(
            'http://localauth.com/auth1/', 'user', 'key', 'USA')
        # request object get
        headers, body = c.get_object('vol1', 'chunk1',
                                     resp_chunk_size=chunk_size)
        # verify validators called and responses consumed
        self.assert_(get_object_validator.called)
        self.assert_(get_response.consumed)
        # body should be iterable
        first_chunk = body.next()
        self.assertEquals(first_chunk, 'a' * 10)
        second_chunk = body.next()
        self.assertEquals(second_chunk, 'b' * 10)
        self.assertRaises(StopIteration, body.next)
        self.assertEquals(full_body, first_chunk + second_chunk)

    def test_get_object_non_chunked(self):
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/chunk1')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        # stub mock responses
        full_body = 'full chunk 1 body'
        get_response = MockResponse(body=full_body)
        self.responses = [_stub_auth_response(), get_response]
        # create connection
        c = swift.Connection(
            'http://localauth.com/auth1/', 'user', 'key', 'USA')
        # request object get
        headers, body = c.get_object('vol1', 'chunk1')
        # verify validators called and responses consumed
        self.assert_(get_object_validator.called)
        self.assert_(get_response.consumed)
        # body should be string
        self.assertEquals(body, full_body)


class TestAuthConnection(unittest.TestCase):

    def setUp(self):
        # give the factory a ref
        swift.HTTPConnection = mock_connection_factory(self)

    def test_valid_auth(self):
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/block')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        full_body = 'the body'
        get_response = MockResponse(body=full_body)
        self.responses = [_stub_auth_response(), get_response]
        c = swift.Connection(
            'http://localauth.com/auth1/', 'user', 'key', 'USA')
        headers, body = c.get_object('vol1', 'block')
        self.assert_(get_object_validator.called)
        self.assert_(get_response.consumed)
        self.assertEquals(body, full_body)

    def test_bad_tk_auth(self):
        error_message = 'Inconsistent Service Catalog back from auth:'
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/block')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        full_body = 'the body'
        get_response = MockResponse(body=full_body)
        self.responses = [_bad_tk_auth_response(), get_response]
        try:
            c = swift.Connection(
                'http://localauth.com/auth1/', 'user', 'key', 'USA')
            headers, body = c.get_object('vol1', 'block')
        except swift.ClientException as e:
            self.assertEquals(e.msg[0:44], error_message)

    def test_bad_url_auth(self):
        error_message = 'Inconsistent Service Catalog back from auth:'
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/block')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        full_body = 'the body'
        get_response = MockResponse(body=full_body)
        self.responses = [_bad_url_auth_response(), get_response]
        try:
            c = swift.Connection(
                'http://localauth.com/auth1/', 'user', 'key', 'USA')
            headers, body = c.get_object('vol1', 'block')
        except swift.ClientException as e:
            self.assertEquals(e.msg[0:44], error_message)

    def test_bad_region_auth(self):
        error_message = 'Region USA not found'
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/block')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        full_body = 'the body'
        get_response = MockResponse(body=full_body)
        self.responses = [_bad_region_auth_response(), get_response]
        try:
            c = swift.Connection(
                'http://localauth.com/auth1/', 'user', 'key', 'USA')
            headers, body = c.get_object('vol1', 'block')
        except swift.ClientException as e:
            self.assertEqual(e.msg, error_message)

    def test_bad_service_auth(self):
        error_message = 'Service Type object-store not found'
        # stub request validators
        auth_validator = lambda *args: None

        def get_object_validator(method, path, body, headers):
            self.assertEquals(method, 'GET')
            self.assertEquals(path, '/vol1/block')
            self.assertEquals(body, '')
        self.validators = [lambda *args: None, get_object_validator]
        full_body = 'the body'
        get_response = MockResponse(body=full_body)
        self.responses = [_bad_service_auth_response(), get_response]
        try:
            c = swift.Connection(
                'http://localauth.com/auth1/', 'user', 'key', 'USA')
            headers, body = c.get_object('vol1', 'block')
        except swift.ClientException as e:
            self.assertEqual(e.msg, error_message)


if __name__ == "__main__":
    unittest.main()
