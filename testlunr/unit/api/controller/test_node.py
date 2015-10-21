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

from urllib import urlencode
from uuid import uuid4
from webob import Request
from webob.exc import HTTPError, HTTPNotFound, HTTPPreconditionFailed, \
        HTTPConflict, HTTPBadRequest
from sqlalchemy.orm import sessionmaker

from lunr import db
from lunr.api.controller.node import NodeController as Controller
from lunr.common.config import LunrConfig


class MockApp(object):
    def __init__(self):
        self.conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://'}})
        # self.urlmap = urlmap
        self.helper = db.configure(self.conf)
        self.fill_percentage_limit = 0.5
        self.node_timeout = None


def make_request(method, data=None):
    req = Request.blank('', POST=data)
    return method(req)


def get_error_response(method, data=None):
    try:
        res = make_request(method, data)
    except HTTPError, e:
        return e
    raise AssertionError('HTTPError not raised by %s' % method.__name__)


class TestNodeController(unittest.TestCase):
    """ Test lunr.api.controller.node.NodeController """
    def setUp(self):
        self.mock_app = MockApp()
        self.db = self.mock_app.helper
        self.volume_type = db.models.VolumeType('vtype')
        self.db.add(self.volume_type)
        self.db.commit()

    def tearDown(self):
        db.Session.remove()

    def test_index(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(res.body, [])
        req = Request.blank('?name=test&size=1&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        res = c.create(req)
        req = Request.blank('?name=test2&size=1&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        res = c.create(req)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(len(res.body), 2)
        req = Request.blank('?name=test2')
        res = c.index(req)
        self.assertEqual(len(res.body), 1)
        self.assertEqual(res.body[0]['name'], 'test2')

    def test_create(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?name=test&size=a&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?name=test&size=1')
        self.assertRaises(HTTPBadRequest, c.create, req)
        req = Request.blank('?name=test&size=1&volume_type_name=notvtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?name=test&size=1&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        res = c.create(req)
        self.assertEqual(res.body['name'], 'test')
        req = Request.blank('?name=test&size=1&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        self.assertRaises(HTTPConflict, c.create, req)

    def test_delete(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test&size=1&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        id = c.create(req).body['id']
        c = Controller({'id': id}, self.mock_app)
        req = Request.blank('')
        res = c.delete(req)
        self.assertEqual(res.body['id'], id)
        self.assertEqual(res.body['status'], 'DELETED')
        c = Controller({'id': '00000000-0000-0000-0000-000000000000'},
                       self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.delete, req)

    def test_show_defaults(self):
        # make node named 'test'
        n = db.models.Node()
        self.db.add(n)
        self.db.commit()
        # create controller for resource with node's id
        c = Controller({'id': n.id}, self.mock_app)
        # make show request with no params
        details = make_request(c.show).body
        # assert defaults
        expected = dict(n)
        for k, v in expected.items():
            self.assertEquals(details[k], v)

    def test_show_with_storage_used(self):
        # create a new node
        n = db.models.Node(size=100)
        self.db.add(n)
        self.db.commit()
        # create controller for resource with node's id
        c = Controller({'id': n.id}, self.mock_app)
        # verify node is empty
        expected = {
            'size': 100,
            'storage_used': 0,
            'storage_free': 100,
        }
        details = make_request(c.show).body
        for k, v in expected.items():
            self.assertEquals(details[k], v)
        # add some volumes to the node
        a = db.models.Account()
        v1 = db.models.Volume(node=n, size=10, account=a)
        v2 = db.models.Volume(node=n, size=20, account=a)
        self.db.add_all([v1, v2, a])
        self.db.commit()
        # very available storage
        expected = {
            'size': 100,
            'storage_used': 30,
            'storage_free': 70,
        }
        details = make_request(c.show).body
        for k, v in expected.items():
            self.assertEquals(details[k], v)

    def test_show_for_missing_id(self):
        # make controller for id not in database
        c = Controller({'id': str(uuid4())}, self.mock_app)
        res = get_error_response(c.show)
        self.assertEquals(res.status_int, 404)

    def test_update(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test&size=1&volume_type_name=vtype&'
                            'hostname=127.0.0.1&port=8080')
        res = c.create(req)
        id = res.body['id']
        c = Controller({'id': id}, self.mock_app)
        req = Request.blank('?name=test2')
        res = c.update(req)
        self.assertEqual(res.body['id'], id)
        self.assertEqual(res.body['name'], 'test2')
        c = Controller({'id': '00000000-0000-0000-0000-000000000000'},
                       self.mock_app)
        req = Request.blank('?name=test2')
        self.assertRaises(HTTPNotFound, c.update, req)


if __name__ == "__main__":
    unittest.main()
