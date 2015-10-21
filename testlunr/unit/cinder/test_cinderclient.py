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

import __builtin__

import json
from StringIO import StringIO
import urllib2

from lunr.cinder import cinderclient
from lunr.common.config import LunrConfig


setattr(__builtin__, '_', lambda x: x)


class MockResponse(object):

    def __init__(self, data='', code=200):
        self.data = data
        self.code = code

    def getcode(self):
        return self.code

    def read(self):
        body = json.dumps(self.data)
        return StringIO(body).read()


class MockUrllib(object):

    def urlopen(self, req):
        resp = self.responses.next()
        try:
            return resp(req)
        finally:
            resp.called = True
            if not hasattr(resp, 'count'):
                resp.count = 0
            resp.count += 1

    Request = urllib2.Request


def _stub_auth_response(req):
    return MockResponse({
        'access': {
            'token': {
                'id': 'fake',
                'tenant': {
                    'id': 'fake',
                }
            }
        }
    })


class TestCinderClient(unittest.TestCase):

    def setUp(self):
        self._orig_urllib2 = cinderclient.urllib2
        self.urllib2 = MockUrllib()
        cinderclient.urllib2 = self.urllib2
        _stub_auth_response.called = False

    def tearDown(self):
        cinderclient.urllib2 = self._orig_urllib2

    def test_rax_auth(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776', rax_auth=True)

        def auth_request(req):
            expected_url = 'http://auth:5000/v2.0/tokens'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'auth': {
                    'passwordCredentials': {
                        'username': 'fake',
                        'password': 'fake',
                    }
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'Accept': 'application/json',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse({
                'access': {
                    'token': {
                        'id': 'fake',
                        'tenant': {
                            'id': 'fake',
                        }
                    }
                }
            })

        def update_request(req):
            expected_url = 'http://cinder:8776/v1/fake/volumes/fake/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'os-reset_status': {
                    'status': 'available',
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([auth_request, update_request])

        client.update('volumes', 'fake', 'available')

        self.assert_(auth_request.called)
        self.assert_(update_request.called)

    def test_keystone_auth(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def auth_request(req):
            expected_url = 'http://auth:5000/v2.0/tokens'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'auth': {
                    'passwordCredentials': {
                        'username': 'fake',
                        'password': 'fake',
                    },
                    'tenantId': 'fake',
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'Accept': 'application/json',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse({
                'access': {
                    'token': {
                        'id': 'fake',
                        'tenant': {
                            'id': 'fake',
                        }
                    }
                }
            })

        def update_request(req):
            expected_url = 'http://cinder:8776/v1/fake/volumes/fake/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'os-reset_status': {
                    'status': 'available',
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([auth_request, update_request])

        client.update('volumes', 'fake', 'available')

        self.assert_(auth_request.called)
        self.assert_(update_request.called)

    def test_get_conn_from_config(self):
        conf = LunrConfig({
            'cinder': {
                'username': 'johnny',
                'password': 'admin',
                'auth_url': 'https://auth',
                'cinder_url': 'https:cinder',
                'rax_auth': 'true',
            }
        })
        client = cinderclient.get_conn(conf)
        self.assertEquals(client.username, 'johnny')
        self.assertEquals(client.password, 'admin')

    def test_update_volume(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def update_request(req):
            # should be volumes update
            expected_url = 'http://cinder:8776/v1/fake/volumes/vol1/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'os-reset_status': {
                    'status': 'available',
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response, update_request])

        client.update('volumes', 'vol1', 'available')

        self.assert_(update_request.called)

    def test_update_snapshot(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def update_request(req):
            expected_url = 'http://cinder:8776/v1/fake/snapshots/snap1/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'os-reset_status': {
                    'status': 'available',
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response, update_request])

        client.update('snapshots', 'snap1', 'available')

        self.assert_(update_request.called)

    def test_force_delete_volume(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def force_delete_request(req):
            expected_url = 'http://cinder:8776/v1/fake/volumes/vol2/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({'os-force_delete': {}})
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response,
                                       force_delete_request])

        client.force_delete('volumes', 'vol2')

        self.assert_(force_delete_request.called)

    def test_force_delete_snapshot(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def force_delete_request(req):
            expected_url = 'http://cinder:8776/v1/fake/snapshots/snap2/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({'os-force_delete': {}})
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response,
                                       force_delete_request])

        client.force_delete('snapshots', 'snap2')

        self.assert_(force_delete_request.called)

    def test_auth_retry(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def unauthorized_request(req):
            raise urllib2.HTTPError(req.get_full_url(), 401, 'Unauthorized',
                                    {}, StringIO())

        def update_request(req):
            return MockResponse()

        self.urllib2.responses = iter([unauthorized_request,
                                       _stub_auth_response, update_request])

        # set invalid token
        client.token = 'invalid'
        client.update('volumes', 'fake', 'available')
        self.assert_(unauthorized_request.called)
        self.assert_(update_request.called)
        # new token should be validated
        self.assertEquals(client.token, 'fake')

    def test_auth_retry_fails_after_five_attempts(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def unauthorized_request(req):
            raise urllib2.HTTPError(req.get_full_url(), 401, 'Unauthorized',
                                    {}, StringIO())

        self.urllib2.responses = iter([
            _stub_auth_response, unauthorized_request,
            _stub_auth_response, unauthorized_request,
            _stub_auth_response, unauthorized_request,
            _stub_auth_response, unauthorized_request,
            _stub_auth_response, unauthorized_request,
        ])
        self.assertRaises(cinderclient.CinderError, client.update,
                          'volumes', 'fake', 'available')
        self.assertEquals(unauthorized_request.count, 5)

    def test_auth_retry_unable_to_auth(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def unauthorized_request(req):
            raise urllib2.HTTPError(req.get_full_url(), 401, 'Unauthorized',
                                    {}, StringIO())

        self.urllib2.responses = iter([
            unauthorized_request, unauthorized_request,
            unauthorized_request, unauthorized_request,
            unauthorized_request, unauthorized_request,
        ])
        self.assertRaises(cinderclient.CinderError, client.update,
                          'volumes', 'fake', 'available')
        self.assertEquals(unauthorized_request.count, 5)

    def test_snapshot_progress(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)
        new_progress = "something%"

        def snapshot_progress_request(req):
            expected_url = 'http://cinder:8776/v1/fake/snapshots/snap2/action'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            data = json.dumps({'os-update_progress': new_progress})
            self.assertEquals(req.data, data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response,
                                       snapshot_progress_request])

        client.snapshot_progress('snap2', new_progress)

        self.assert_(snapshot_progress_request.called)

    def test_update_volume_metadata(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def metadata_request(req):
            expected_url = 'http://cinder:8776/v1/' \
                           'fake/volumes/volume_id/metadata'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'POST')
            expected_data = json.dumps({
                'metadata': {
                    'key': 'value',
                }
            })
            self.assertEquals(req.data, expected_data)
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response, metadata_request])

        client.update_volume_metadata('volume_id', {'key': 'value'})

        self.assert_(metadata_request.called)

    def test_delete_volume_metadata(self):
        client = cinderclient.CinderClient('fake', 'fake', 'http://auth:5000',
                                           'http://cinder:8776',
                                           tenant_id='fake', rax_auth=False)

        def metadata_request(req):
            expected_url = 'http://cinder:8776/v1/' \
                           'fake/volumes/volume_id/metadata/key'
            self.assertEquals(req.get_full_url(), expected_url)
            self.assertEquals(req.get_method(), 'DELETE')
            expected_headers = {
                'Content-type': 'application/json',
                'X-auth-token': 'fake',
            }
            self.assertEquals(req.headers, expected_headers)
            return MockResponse()

        self.urllib2.responses = iter([_stub_auth_response, metadata_request])

        client.delete_volume_metadata('volume_id', 'key')

        self.assert_(metadata_request.called)


if __name__ == "__main__":
    unittest.main()
