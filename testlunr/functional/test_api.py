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

# -------------------------------------------------------------------
# The functional tests here run against an internal
# WSGI server for each TestSuite
#
# If this is not desirable you can change the API endpoint by
# defining the following in your environment

# export API_HOST='lunr-api.rackspace.corp'
# export API_PORT='8080'
# export API_VOLUME_TYPE='vtype'
# export API_SKIP_ADMIN='true'
# -------------------------------------------------------------------

from testlunr.functional import LunrTestCase, SkipTest, LunrApiService
from lunr.common.config import Config
from lunr.storage.helper.utils import execute
from socket import gethostbyname
from lunr.common import config
from uuid import uuid4
from lunr import db
import unittest
import time
import os

from threading import Thread
from collections import defaultdict


class AsyncRequest(Thread):

    def __init__(self, request_func, *args, **kwargs):
        super(AsyncRequest, self).__init__()
        self.request = request_func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            self._result = self.request(*self.args, **self.kwargs)
            self._error = False
        except:
            self._error = True
            self._result = sys.exc_info()

    @property
    def result(self):
        if not hasattr(self, '_result'):
            raise Exception('_run has not been called')
        if self._error:
            raise self._result[0], self._result[1], self._result[2]
        return self._result


class LunrApiTestCase(LunrTestCase):

    def setUp(self):
        # Default the config to the local user and our current dir
        self.volume_type = os.environ.get('API_VOLUME_TYPE', 'vtype')
        self.skip_admin = Config.to_bool(os.environ.get('API_SKIP_ADMIN',
                                                        'false'))

        # Start the Lunr API Service if needed
        self.api = LunrApiService()

        # setup our timeouts
        super(LunrApiTestCase, self).setUp()

    def request(self, uri, *args, **kwargs):
        url = "http://%s:%s/v1.0/%s" % (self.api.host, self.api.port, uri)
        return self.urlopen(url, *args, **kwargs)

    def verify_active_test_volume_type(self):
        # verify active test_volume_type
        resp = self.request('admin/volume_types', 'GET',
                            {'status': 'ACTIVE', 'name': self.volume_type})
        if not resp.body:
            if self.skip_admin:
                raise SkipTest("Admin API tests disabled, "
                               "can't create test volume type")
            # create test_volume_type
            resp = self.request('admin/volume_types', 'POST',
                                {'name': self.volume_type})
            if resp.code // 100 != 2:
                raise Exception(
                    'Unable to automatically configure volume_type %s' %
                    self.volume_type)

    def assert200(self, code, body, action=None):
        err_msg = '%s: %s' % (code, body)
        if action:
            err_msg += ' when trying to %s' % action
        self.assertEquals(code // 100, 2, err_msg)


class TestAccountAPI(LunrApiTestCase):

    def test_account(self):
        # TODO: rewrite to assert auto create accounts

        # Test create
        acct_id = str(uuid4())
        resp = self.request('admin/accounts', 'POST', {'id': acct_id})
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], acct_id)
        acct_id2 = str(uuid4())
        resp = self.request('admin/accounts', 'POST', {'id': acct_id2})
        self.assertCode(resp, 200)
        # Test list
        resp = self.request('admin/accounts')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/accounts?id=%s' % acct_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(len(resp.body), 1)
        self.assertEquals(resp.body[0]['id'], acct_id)
        resp = self.request('admin/accounts?id=notfound')
        self.assertEquals(resp.code, 200)
        self.assertEquals(len(resp.body), 0)
        # Test show
        resp = self.request('admin/accounts/%s' % acct_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], acct_id)
        resp = self.request('admin/accounts/%s' % str(uuid4()))
        self.assertEquals(resp.code, 404)
        # Test update
        new_status = 'foo'
        resp = self.request('admin/accounts/%s' % acct_id, 'POST',
                            {'status': new_status})
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/accounts?status=%s' % 'bad_status')
        self.assertEquals(resp.code, 200)
        self.assertEquals(len(resp.body), 0)
        resp = self.request('admin/accounts?status=%s' % new_status)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body[0]['id'], acct_id)
        resp = self.request('admin/accounts/%s' % str(uuid4()), 'POST',
                            {'status': new_status})
        self.assertEquals(resp.code, 404)
        # Test delete
        resp = self.request('admin/accounts/%s' % acct_id, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/accounts?id=%s' % acct_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body[0]['status'], 'DELETED')
        resp = self.request('admin/accounts/%s' % acct_id2, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/accounts/%s' % str(uuid4()),
                            'DELETE')
        self.assertEquals(resp.code, 404)


class TestAdminAPI(LunrApiTestCase):

    def setUp(self):
        super(TestAdminAPI, self).setUp()
        if self.skip_admin:
            raise SkipTest("Admin API tests disabled")


class TestVolumeTypeAPI(TestAdminAPI):

    def setUp(self):
        super(TestVolumeTypeAPI, self).setUp()
        self.test_volume_type = 'garbage'
        resp = self.request('admin/volume_types', 'POST',
                            {'name': self.test_volume_type})
        if resp.code // 100 != 2:
            raise Exception('Unable to create volume_type %s' %
                            self.test_volume_type)

    def tearDown(self):
        resp = self.request('admin/volume_types/%s' % self.test_volume_type,
                            'DELETE')
        if resp.code // 100 != 2:
            raise Exception('Unable to delete volume_type %s' %
                            self.test_volume_type)

    def test_show(self):
        resp = self.request(
            'admin/volume_types/%s' % self.test_volume_type, 'GET')
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'ACTIVE')

    def test_update(self):
        resp = self.request(
            'admin/volume_types/%s' % self.test_volume_type, 'POST',
            {'status': 'FOOBAR'})
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'FOOBAR')


class TestNodeAPI(TestAdminAPI):

    def setUp(self):
        super(TestNodeAPI, self).setUp()
        self.verify_active_test_volume_type()

    def test_node(self):
        # Test create
        node_name = 'node-' + str(uuid4())
        volume_type_name = self.volume_type
        resp = self.request('admin/nodes', 'POST',
                            {'name': node_name, 'size': 1,
                             'volume_type_name': volume_type_name})
        self.assertEquals(resp.code // 100, 2)
        node_id = resp.body['id']
        self.assertEquals(resp.body['name'], node_name)
        node_name2 = 'node-' + str(uuid4())
        resp = self.request('admin/nodes', 'POST',
                            {'name': node_name2, 'size': 1,
                             'volume_type_name': volume_type_name})
        node_id2 = resp.body['id']
        resp = self.request('admin/nodes', 'POST')
        self.assertEquals(resp.code, 412)
        # Test list
        resp = self.request('admin/nodes')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/nodes?name=%s' % node_name)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(len(resp.body), 1)
        self.assertEquals(resp.body[0]['id'], node_id)
        # Test show
        resp = self.request('admin/nodes/%s' % node_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['name'], node_name)
        resp = self.request('admin/nodes/%s' % str(uuid4()))
        self.assertEquals(resp.code, 404)
        # Test update
        resp = self.request('admin/nodes/%s' % node_id, 'POST',
                            {'name': node_name + 'new'})
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/nodes?name=%s' % node_name)
        self.assertEquals(resp.code, 200)
        self.assertEquals(len(resp.body), 0)
        resp = self.request('admin/nodes?name=%s' % node_name + 'new')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body[0]['id'], node_id)
        node_name = node_name + 'new'
        resp = self.request('admin/nodes/%s' % str(uuid4()), 'POST',
                            {'name': node_name + 'new'})
        self.assertEquals(resp.code, 404)
        # Test delete
        resp = self.request('admin/nodes/%s' % node_id, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request('admin/nodes?name=%s' % node_name)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body[0]['status'], 'DELETED')
        resp = self.request('admin/nodes/%s' % node_id2, 'DELETE')
        resp = self.request('admin/nodes/%s' % str(uuid4()), 'DELETE')
        self.assertEquals(resp.code, 404)


class TestVolumeAPI(LunrApiTestCase):

    project_id = str(uuid4())

    def setUp(self):
        super(TestVolumeAPI, self).setUp()
        self.verify_active_test_volume_type()
        # verify active node for test_volume_type
        resp = self.request('admin/nodes', 'GET',
                            {'status': 'ACTIVE',
                             'volume_type_name': self.volume_type})
        self.node_id = None
        if not resp.body:
            if self.skip_admin:
                raise SkipTest("Admin API tests disabled, "
                               "can't create node")
            self.node_name = str(uuid4())
            params = {'name': self.node_name, 'size': 10,
                      'volume_type_name': self.volume_type}
            resp = self.request('admin/nodes', 'POST', params)
            self.node_id = resp.body['id']

    def tearDown(self):
        resp = self.request(self.project_id + '/volumes')
        for volume in resp.body:
            self.request(self.project_id + '/volumes/%s' % volume['id'],
                         'DELETE')
        if self.node_id:
            resp = self.request('admin/nodes/%s' % self.node_id, 'DELETE')
            self.assertEquals(resp.code // 100, 2)

    def test_volume(self):
        # Test create
        volume_id = str(uuid4())
        volume_type_name = self.volume_type
        path = self.project_id + '/volumes/%s' % volume_id
        resp = self.request(
            path, 'PUT', {'size': 1, 'volume_type_name': volume_type_name})
        self.assert200(resp.code, resp.body, 'create volume')
        self.assertEquals(resp.body['id'], volume_id)
        resp = self.request(
            path, 'PUT', {'size': 1, 'volume_type_name': volume_type_name})
        self.assertEquals(resp.code, 409)
        # missing params
        resp = self.request(self.project_id + '/volumes', 'POST')
        self.assertEquals(resp.code // 100, 4)
        volume_id2 = str(uuid4())
        path2 = self.project_id + '/volumes/%s' % volume_id2
        resp = self.request(
            path2, 'PUT', {'size': 1, 'volume_type_name': volume_type_name})
        self.assertEquals(resp.code // 100, 2)
        # Test list
        resp = self.request(self.project_id + '/volumes')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request(self.project_id + '/volumes?id=%s' % volume_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(len(resp.body), 1)
        self.assertEquals(resp.body[0]['id'], volume_id)
        # Test show
        resp = self.request(self.project_id + '/volumes/%s' % volume_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volume_id)
        resp = self.request(self.project_id + '/volumes/%s' % str(uuid4()))
        self.assertEquals(resp.code, 404)
        # Test update
        resp = self.request(
            self.project_id + '/volumes/%s' % volume_id, 'POST',
            {'status': 'CANARY'})
        self.assertEquals(resp.code // 100, 2)
        resp = self.request(
            self.project_id + '/volumes?status=%s' % 'CANARY')
        self.assertEquals(resp.code, 200)
        self.assertEquals(len(resp.body), 1)
        resp = self.request(self.project_id + '/volumes?id=%s' % volume_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(len(resp.body), 1)
        self.assertEquals(resp.body[0]['id'], volume_id)
        resp = self.request(
            self.project_id + '/volumes/%s' % str(uuid4()), 'POST',
            {'id': 'new-id'})
        self.assertEquals(resp.code, 400)
        resp = self.request(self.project_id + '/volumes/%s' % str(uuid4()),
                            'POST', {'status': 'invalidstatus'})
        self.assertEquals(resp.code, 404)
        # Test delete
        resp = self.request(
            self.project_id + '/volumes/%s' % volume_id, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        resp = self.request(self.project_id + '/volumes?id=%s' % volume_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(len(resp.body), 1)
        self.assertEquals(resp.body[0]['status'], 'DELETING')
        resp = self.request(
            self.project_id + '/volumes/%s' % volume_id2, 'DELETE')
        resp = self.request(
            self.project_id + '/volumes/%s' % str(uuid4()), 'DELETE')
        self.assertEquals(resp.code, 404)

    def test_delete_while_attached(self):
        volume_id = str(uuid4())
        host = gethostbyname(self.api.host)
        # Create a new volume
        path = 'test/volumes/%s' % volume_id
        resp = self.put(
            path, params={'size': 1, 'volume_type_name': self.volume_type})
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['id'], volume_id)

        path = path + '/export'
        resp = self.put(path, params={'ip': '127.0.0.1',
                                      'initiator': 'fake_initiator'})
        self.assertEquals(resp.code, 200)
        target = resp.body['target_name']

        # Connect to the volume
        execute('iscsiadm', mode='discovery', type='sendtargets',
                portal=host)
        execute('iscsiadm', mode='node', targetname=target,
                portal=host, login=None)

        # Attempt to delete the in-use volume
        resp = self.delete('test/volumes/%s' % volume_id)
        self.assertEquals(resp.code, 409)
        self.assertEquals(resp.reason, 'Conflict')
        self.assertIn("Cannot delete '%s' while export in use" % volume_id,
                      resp.body['reason'])

        # Assert the volume is still active
        resp = self.get('test/volumes/%s' % volume_id)
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'ACTIVE')

        # Logout of the exported volume
        execute('iscsiadm', mode='node', targetname=target,
                portal=host, logout=None)

        # Attempt to delete the volume while not in-use
        resp = self.delete('test/volumes/%s' % volume_id)
        self.assertEquals(resp.code, 200)

        # Volume should be deleting
        resp = self.get('test/volumes/%s' % volume_id)
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'DELETING')

        # API Should eventually report the volume as 'DELETED'
        for i in range(0, 30):
            time.sleep(2)
            resp = self.get('test/volumes/%s' % volume_id)
            self.assertEquals(resp.code, 200)
            if resp.body['status'] == 'DELETED':
                self.assert_(True)
                return
        self.fail("test/volumes/%s never returned a 'status' of 'DELETED'" %
                  volume_id)

    def test_create_volume_already_exists(self):
        volume_id = str(uuid4())
        # Create a new volume
        resp = self.put('test/volumes/%s' % volume_id, params={
            'size': 1, 'volume_type_name': self.volume_type})
        self.assert200(resp.code, resp.body, 'create volume')
        self.assertEquals(resp.body['id'], volume_id)

        # Create a new volume
        resp = self.put('test/volumes/%s' % volume_id, params={
                'size': 1, 'volume_type_name': self.volume_type})
        self.assertCode(resp, 409)
        self.assertIn("already exists", resp.body['reason'])

        resp = self.get('test/volumes/%s' % volume_id)
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'ACTIVE')

        # Attempt to delete the volume while not in-use
        resp = self.delete('test/volumes/%s' % volume_id)
        self.assertEquals(resp.code, 200)


class TestExportAPI(LunrApiTestCase):

    project_id = str(uuid4())

    def setUp(self):
        super(TestExportAPI, self).setUp()
        self.verify_active_test_volume_type()
        # verify active node for test_volume_type
        resp = self.request('admin/nodes', 'GET',
                            {'status': 'ACTIVE',
                             'volume_type_name': self.volume_type})
        self.node_id = None
        if not resp.body:
            if self.skip_admin:
                raise SkipTest("Admin API tests disabled, "
                               "can't create node")
            self.node_name = str(uuid4())
            params = {'name': self.node_name, 'size': 10,
                      'volume_type_name': self.volume_type}
            resp = self.request('admin/nodes', 'POST', params)
            self.node_id = resp.body['id']

    def tearDown(self):
        resp = self.request(self.project_id + '/volumes')
        for volume in resp.body:
            self.request(self.project_id + '/volumes/%s' % volume['id'],
                         'DELETE')
        if self.node_id:
            resp = self.request('admin/nodes/%s' % self.node_id, 'DELETE')
            self.assertEquals(resp.code // 100, 2)

    def test_export(self):
        volume_id = str(uuid4())
        # Create volume.
        path = self.project_id + '/volumes/%s' % volume_id
        resp = self.request(path, 'PUT', {
            'size': 1, 'volume_type_name': self.volume_type})
        self.assert200(resp.code, resp.body, 'create volume')
        self.assertEquals(resp.body['id'], volume_id)
        fake_initiator = 'monkeys'
        # Create export
        path = path + '/export'
        resp = self.request(path, 'PUT', {'ip': '127.0.0.1',
                                          'initiator': fake_initiator})
        target = resp.body['target_name']
        host = gethostbyname(self.api.host)
        self.assert200(resp.code, resp.body, 'create export')
        self.assertEquals(resp.body['status'], 'ATTACHING')
        self.assertEquals(resp.body['id'], volume_id)
        # Attach!
        execute('iscsiadm', mode='discovery', type='sendtargets',
                portal=host)
        execute('iscsiadm', mode='node', targetname=target,
                portal=host, login=None)
        resp = self.request(path, 'POST', {'status': 'ATTACHED',
                                           'mountpoint': '/some/thing',
                                           'instance_id': 'foo'})
        # Detach!
        execute('iscsiadm', mode='node', targetname=target,
                portal=host, logout=None)
        self.assert200(resp.code, resp.body, 'update export')
        self.assertNotEquals(resp.body['session_ip'], '')
        self.assertNotEquals(resp.body['session_initiator'], '')
        self.assertEquals(resp.body['status'], 'ATTACHED')
        self.assertEquals(resp.body['mountpoint'], '/some/thing')
        self.assertEquals(resp.body['instance_id'], 'foo')


class TestConcurrent(LunrApiTestCase):

    project_id = str(uuid4())

    def setUp(self):
        super(TestConcurrent, self).setUp()
        self.verify_active_test_volume_type()
        # verify active node for test_volume_type
        resp = self.request('admin/nodes', 'GET',
                            {'status': 'ACTIVE',
                             'volume_type_name': self.volume_type})
        self.node_id = None
        if not resp.body:
            if self.skip_admin:
                raise SkipTest("Admin API tests disabled, "
                               "can't create node")
            self.node_name = str(uuid4())
            params = {'name': self.node_name, 'size': 10,
                      'volume_type_name': self.volume_type}
            resp = self.request('admin/nodes', 'POST', params)
            self.node_id = resp.body['id']
        self.request_queue = {}

    def tearDown(self):
        resp = self.request(self.project_id + '/volumes')
        for volume in resp.body:
            self.request(self.project_id + '/volumes/%s' % volume['id'],
                         'DELETE')
        if self.node_id:
            resp = self.request('admin/nodes/%s' % self.node_id, 'DELETE')
            self.assertEquals(resp.code // 100, 2)

    def queue_request(self, *args, **kwargs):
        req_id = str(uuid4())
        self.request_queue[req_id] = AsyncRequest(self.request,
                                                  *args, **kwargs)
        return req_id

    def make_requests(self):
        for req in self.request_queue.values():
            req.start()
        result_map = {}
        for req_id, async_req in self.request_queue.items():
            async_req.join()
            result_map[req_id] = async_req.result
        return result_map

    def get_result_code_map(self):
        results = self.make_requests()
        result_code_map = defaultdict(list)
        for result in results.values():
            result_code_map[result.code].append(result)
        return result_code_map

    def test_concurrent_create(self):
        volume_id = str(uuid4())
        path = self.project_id + '/volumes/%s' % volume_id
        params = {'size': 1, 'volume_type_name': self.volume_type}
        self.queue_request(path, 'PUT', params)
        self.queue_request(path, 'PUT', params)
        result_code_map = self.get_result_code_map()
        self.assertEquals(len(result_code_map), 2)
        self.assertEquals(len(result_code_map[200]), 1)
        self.assertEquals(len(result_code_map[409]), 1)
        self.assertEquals(result_code_map[200][0].body['id'], volume_id)

    def wait_on_status(self, path, status, timeout=240):
        timeout = time.time() + timeout
        while True:
            resp = self.request(path, 'GET')
            if resp.code // 100 != 2:
                raise Exception("resource at '%s' returned %s: %s" % (
                    path, resp.code, resp.body['reason']))
            if resp.body['status'] == status:
                return resp
            if time.time() < timeout:
                time.sleep(5)
            else:
                break
        raise Exception("resource at '%s' did not enter status %s, "
                        "last status was %s" % (
                            path, status, resp.body['status']))

    def test_concurrent_recreate(self):
        volume_id = str(uuid4())
        path = self.project_id + '/volumes/%s' % volume_id
        params = {'size': 1, 'volume_type_name': self.volume_type}
        resp = self.request(path, 'PUT', params)
        self.assertEquals(resp.code // 100, 2)
        resp = self.request(path, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        self.wait_on_status(path, 'DELETED')
        # make two requests to recreate volume
        self.queue_request(path, 'PUT', params)
        self.queue_request(path, 'PUT', params)
        result_code_map = self.get_result_code_map()
        self.assertEquals(len(result_code_map), 2)
        self.assertEquals(len(result_code_map[200]), 1)
        self.assertEquals(len(result_code_map[409]), 1)
        self.assertEquals(result_code_map[200][0].body['id'], volume_id)


if __name__ == "__main__":
    unittest.main()
