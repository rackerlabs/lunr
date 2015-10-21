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
from webob.exc import HTTPNotFound, HTTPPreconditionFailed
from sqlalchemy.orm import sessionmaker

from lunr import db
from lunr.api.controller.volume_type import VolumeTypeController as Controller
from lunr.common.config import LunrConfig


class MockApp(object):
    def __init__(self):
        self.conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://'}})
        # self.urlmap = urlmap
        self.helper = db.configure(self.conf)
        self.fill_percentage_limit = 0.5


class TestVolumeTypeController(unittest.TestCase):
    """ Test lunr.api.controller.volumetype.VolumeTypeController """
    def setUp(self):
        self.mock_app = MockApp()

    def tearDown(self):
        db.Session.remove()

    def test_index(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(res.body, [])
        req = Request.blank('?name=test')
        res = c.create(req)
        req = Request.blank('?name=test2')
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
        req = Request.blank('?name=test')
        res = c.create(req)
        self.assertEqual(res.body['name'], 'test')
        req = Request.blank('?name=test')
        res = c.create(req)
        self.assertEqual(res.body['name'], 'test')

    def test_create_min_max(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test&min_size=42&max_size=200')
        res = c.create(req)
        self.assertEqual(res.body['name'], 'test')
        self.assertEqual(res.body['min_size'], 42)
        self.assertEqual(res.body['max_size'], 200)

    def test_create_limits(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test')
        res = c.create(req)
        self.assertEqual(res.body['name'], 'test')
        self.assertEqual(res.body['read_iops'], 0)
        self.assertEqual(res.body['write_iops'], 0)
        req = Request.blank('?name=test&read_iops=100&write_iops=150')
        res = c.create(req)
        self.assertEqual(res.body['name'], 'test')
        self.assertEqual(res.body['read_iops'], 100)
        self.assertEqual(res.body['write_iops'], 150)
        req = Request.blank('?name=test&read_iops=asdf&write_iops=150')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?name=test&read_iops=-42&write_iops=150')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_delete(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test')
        name = c.create(req).body['name']
        c = Controller({'name': 'NoVolumeType'}, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.delete, req)
        c = Controller({'name': name}, self.mock_app)
        req = Request.blank('')
        res = c.delete(req)
        self.assertEqual(res.body['name'], name)
        self.assertEqual(res.body['status'], 'DELETED')

    def test_show(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test')
        name = c.create(req).body['name']
        c = Controller({'name': 'NoVolumeType'}, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.show, req)
        c = Controller({'name': 'test'}, self.mock_app)
        req = Request.blank('')
        res = c.show(req)
        self.assertEqual(res.body['name'], 'test')

    def test_update(self):
        c = Controller({}, self.mock_app)
        req = Request.blank('?name=test')
        res = c.create(req)
        name = res.body['name']
        c = Controller({'name': 'NotVolumeType'}, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.update, req)
        c = Controller({'name': name}, self.mock_app)
        req = Request.blank('?status=change')
        res = c.update(req)
        self.assertEqual(res.body['name'], name)
        self.assertEqual(res.body['status'], 'change')

if __name__ == "__main__":
    unittest.main()
