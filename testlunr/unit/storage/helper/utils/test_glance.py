#!/usr/bin/env python
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

import glanceclient.exc

from lunr.common.config import LunrConfig
from httplib import HTTPException
from lunr.storage.helper.utils import glance


class MockGlanceClient(object):
    @staticmethod
    def Client(*args, **kwargs):
        pass


class MockRetryGlanceClient(MockGlanceClient):
    class GlanceImages(object):
        def __init__(self, client):
            self.client = client

        def data(self, image_id):
            raise glanceclient.exc.ServiceUnavailable("Fail!")

        def get(self, image_id):
            raise glanceclient.exc.ServiceUnavailable("Fail!")

    def __init__(self, *args, **kwargs):
        self.called = 0
        self.images = self.GlanceImages(self)

    @staticmethod
    def Client(*args, **kwargs):
        return MockRetryGlanceClient()


class FakeHTTPException(HTTPException):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return "fake exception: %s" % self.code


class TestAuthenticate(unittest.TestCase):

    def setUp(self):
        self.orig_request = glance.request
        self.conf = LunrConfig({'glance': {'auth_strategy': 'keystone'}})

    def tearDown(self):
        glance.request = self.orig_request

    def test_success(self):
        token = {'access': {'token': {'id': 1, 'tenant': {'id': 'foo'}}}}
        responses = [token]

        def success(*args, **kwargs):
            return responses.pop(0)
        glance.request = success

        self.client = glance.GlanceClient(self.conf)

        self.assertEquals(self.client._token, 1)
        self.assertEquals(self.client.tenant_id, 'foo')
        self.assertEquals(responses, [])

    def test_500(self):
        responses = [FakeHTTPException(500)]

        def fail(*args, **kwargs):
            raise responses.pop(0)
        glance.request = fail

        self.assertRaises(glance.GlanceError, glance.GlanceClient, self.conf)

        self.assertEquals(responses, [])

    def test_401_retry(self):
        token = {'access': {'token': {'id': 1, 'tenant': {'id': 'foo'}}}}
        responses = [FakeHTTPException(401), token]

        def reauth(*args, **kwargs):
            resp = responses.pop(0)
            if isinstance(resp, dict):
                return resp
            raise resp
        glance.request = reauth

        self.client = glance.GlanceClient(self.conf)

        self.assertEquals(self.client._token, 1)
        self.assertEquals(self.client.tenant_id, 'foo')
        self.assertEquals(responses, [])

    def test_401_retry_fail(self):
        responses = [FakeHTTPException(401), FakeHTTPException(401)]

        def fail(*args, **kwargs):
            raise responses.pop(0)
        glance.request = fail

        self.assertRaises(glance.GlanceError, glance.GlanceClient, self.conf)

        self.assertEquals(responses, [])


class TestGlanceClient(unittest.TestCase):

    def setUp(self):
        self.orig_glanceclient = glance.glanceclient

    def tearDown(self):
        glance.glanceclient = self.orig_glanceclient

    def test_shuffle_glance_url(self):
        glance.glanceclient = MockGlanceClient

        conf = LunrConfig()
        glance_urls = ['url1:9292', 'url2:9292', 'url3:9292']
        urls_string = ', '.join(glance_urls)
        self.conf = LunrConfig({'glance': {'glance_urls': urls_string}})

        glance1 = glance.GlanceClient(self.conf)
        self.assertItemsEqual(glance1.glance_urls, glance_urls)

        shuffled = False
        for i in range(10):
            glance2 = glance.GlanceClient(self.conf)
            self.assertItemsEqual(glance2.glance_urls, glance_urls)
            if glance1.glance_urls != glance2.glance_urls:
                shuffled = True
                break

        self.assertTrue(shuffled)

    def test_client_retry(self):
        glance.glanceclient = MockRetryGlanceClient

        conf = LunrConfig()
        glance_urls = ['url1:9292', 'url2:9292', 'url3:9292']
        urls_string = ', '.join(glance_urls)
        self.conf = LunrConfig({'glance': {'glance_urls': urls_string}})

        glance1 = glance.GlanceClient(self.conf)
        self.assertRaises(glance.GlanceError, glance1.head, 'junk')
        self.assertEquals(glance1._glance_url_index, 3)

        glance2 = glance.GlanceClient(self.conf)
        self.assertRaises(glance.GlanceError, glance2.get, 'junk')
        self.assertEquals(glance2._glance_url_index, 3)


if __name__ == "__main__":
    unittest.main()
