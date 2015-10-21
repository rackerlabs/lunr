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

from webob import Request
from webob.exc import HTTPNotFound
from sqlalchemy.orm import sessionmaker

from lunr import db
from lunr.api.controller.account import AccountController as Controller
from lunr.common.config import LunrConfig


class MockApp(object):
    def __init__(self):
        self.conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://'}})
        # self.urlmap = urlmap
        self.helper = db.configure(self.conf)
        self.fill_percentage_limit = 0.5


class TestAccountController(unittest.TestCase):
    """ Test lunr.api.controller.account.AccountController """
    def setUp(self):
        self.mock_app = MockApp()
        self.db = self.mock_app.helper

    def tearDown(self):
        db.Session.remove()

    def test_index(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(res.body, [])
        req = Request.blank('?id=test')
        res = c.create(req)
        req = Request.blank('?id=test2')
        res = c.create(req)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(len(res.body), 2)
        req = Request.blank('?id=test2')
        res = c.index(req)
        self.assertEqual(len(res.body), 1)
        self.assertEqual(res.body[0]['id'], 'test2')

    def test_create(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?id=test')
        res = c.create(req)
        self.assertEqual(res.body['id'], 'test')

    def test_delete(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?id=test')
        id = c.create(req).body['id']
        c = Controller({'id': '00000000-0000-0000-0000-000000000000'},
                       self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.delete, req)
        c = Controller({'id': id}, self.mock_app)
        req = Request.blank('')
        res = c.delete(req)
        self.assertEqual(res.body['id'], id)
        self.assertEqual(res.body['status'], 'DELETED')

    def test_show(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?id=test')
        id = c.create(req).body['id']
        c = Controller({'id': '00000000-0000-0000-0000-000000000000'},
                       self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.show, req)
        c = Controller({'id': id}, self.mock_app)
        req = Request.blank('')
        res = c.show(req)
        self.assertEqual(res.body['id'], id)

    def test_update(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('')
        res = c.create(req)
        id = res.body['id']
        c = Controller({'id': '00000000-0000-0000-0000-000000000000'},
                       self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.update, req)
        c = Controller({'id': id}, self.mock_app)
        req = Request.blank('?status=test')
        res = c.update(req)
        self.assertEqual(res.body['id'], id)
        self.assertEqual(res.body['status'], 'test')

if __name__ == "__main__":
    unittest.main()
