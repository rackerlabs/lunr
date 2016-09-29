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


import datetime
import unittest
import tempfile
import os
import json
from urllib2 import URLError
import urlparse
from httplib import BadStatusLine
import socket

from sqlalchemy.exc import OperationalError
from webob import Request
from webob.exc import HTTPBadRequest, HTTPServiceUnavailable, HTTPNotFound,\
        HTTPPreconditionFailed, HTTPBadRequest, HTTPConflict, HTTPError,\
        HTTPInsufficientStorage

from lunr import db
from lunr.api import server
from lunr.api.controller.base import BaseController
from lunr.api.controller.volume import VolumeController as Controller
from lunr.api.controller.account import AccountController
from lunr.api.controller import base
from lunr.api.server import ApiWsgiApp
from lunr.api.urlmap import urlmap
from lunr.common.config import LunrConfig


from testlunr.functional import Struct
from testlunr.unit import patch, WsgiTestBase


# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=False)


class MockRequest(object):

    def get_method(self):
        return 'PUT'

    def get_full_url(self):
        return 'https://localhost/'


class MockResponse(object):

    def __init__(self, resp_code, resp_body):
        self.resp_code = resp_code
        self.resp_body = resp_body

    def getcode(self):
        return self.resp_code

    def read(self):
        return self.resp_body


class MockUrlopen(object):
    def __init__(self, request, timeout=None):
        pass

    def read(self):
        return '{"name": "some_vol", "status": "ACTIVE"}'

    def getcode(self):
        return 200


class MockUrlopenWithImage(object):
    def __init__(self, request, timeout=None):
        pass
        # I'm not really happy with this.
        # It'd be nice if it were some kind of assertion.
        data = urlparse.parse_qs(request.data)
        self.image_id = data['image_id']

    def read(self):
        self.called = True
        return '{"name": "some_vol", "status": "IMAGING"}'

    def getcode(self):
        return 200


class MockUrlopenBlowup(object):
    def __init__(self, request, timeout=None):
        raise URLError('blah')


class MockApp(object):
    def __init__(self):
        self.conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://', 'echo': False}})
        # self.urlmap = urlmap
        self.helper = db.configure(self.conf)
        self.fill_percentage_limit = 0.5
        self.fill_strategy = 'broad_fill'
        self.node_timeout = None
        self.image_convert_limit = 3


class TestVolumeController(unittest.TestCase):
    """ Test lunr.api.controller.volume.VolumeController """
    def setUp(self):
        self.mock_app = MockApp()
        self.old_urlopen = base.urlopen
        base.urlopen = MockUrlopen
        self.db = self.mock_app.helper
        self.vtype = db.models.VolumeType('vtype')
        self.db.add(self.vtype)
        for i in range(3):
            n = db.models.Node('node%s' % i, 10 + i, volume_type=self.vtype,
                               hostname='10.127.0.%s' % i, port=8080 + i)
            setattr(self, 'node%s' % i, n)
            self.db.add(n)
        self.assertEquals(3, self.db.query(db.models.Node).count())
        self.account = db.models.Account(id='someaccount')
        self.account2 = db.models.Account(id='someotheraccount')
        self.db.add(self.account)
        self.db.add(self.account2)
        self.db.commit()
        self.account_id = self.account.id
        self.account_id2 = self.account2.id

    def tearDown(self):
        base.urlopen = self.old_urlopen
        self.db.remove()

    def test_index_empty(self):
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(res.body, [])

    def test_index_populated(self):
        c = Controller({'account_id':  self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        res = c.create(req)
        c = Controller({'account_id':  self.account_id, 'id': 'test2'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        res = c.create(req)

        c = Controller({'account_id':  self.account_id}, self.mock_app)
        req = Request.blank('')
        res = c.index(req)
        self.assertEqual(len(res.body), 2)
        for v in res.body:
            self.assert_(v['id'].startswith('test'))

    def test_index_filtered(self):
        c = Controller({'account_id':  self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        res = c.create(req)
        c = Controller({'account_id':  self.account_id, 'id': 'test2'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        res = c.create(req)

        c = Controller({'account_id':  self.account_id}, self.mock_app)
        req = Request.blank('?id=test2')
        res = c.index(req)
        self.assertEqual(len(res.body), 1)
        self.assertEqual(res.body[0]['id'], 'test2')

    def test_index_filtered_by_restore_of(self):
        volume = db.models.Volume(node=None, account=self.account,
                                  status='ACTIVE', size=1, restore_of='foo')
        self.db.add(volume)
        self.db.commit()
        self.assertEqual(volume.restore_of, 'foo')
        c = Controller({'account_id': self.account_id, 'id': volume.id},
                       self.mock_app)
        req = Request.blank('?restore_of=foo')
        res = c.index(req)
        self.assertEqual(len(res.body), 1)
        self.assertEqual(res.body[0]['id'], volume.id)
        self.assertEqual(res.body[0]['restore_of'], 'foo')

    def test_index_name_and_cinder_host(self):
        cinder_host = 'somehost'
        name = 'somehostvolume'
        n = db.models.Node('somehostnode', 1000, volume_type=self.vtype,
                           hostname='10.127.0.99', port=8099,
                           cinder_host=cinder_host)
        # Two volumes, same name
        v1 = db.models.Volume(node=self.node0, account=self.account,
                              status='ACTIVE', size=1, name=name)
        v2 = db.models.Volume(node=n, account=self.account,
                              status='ACTIVE', size=1, name=name)
        # One one the right node, different name
        v3 = db.models.Volume(node=n, account=self.account,
                              status='ACTIVE', size=1)
        self.db.add_all([n, v1, v2])
        self.db.commit()

        # Query by name
        c = Controller({'account_id':  self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?name=%s' % name)
        res = c.index(req)
        self.assertEqual(len(res.body), 2)
        vol_ids = (v1.id, v2.id,)
        self.assertEqual((res.body[0]['id'], res.body[1]['id']), vol_ids)

        # Query by cinder_host
        req = Request.blank('?cinder_host=%s' % cinder_host)
        res = c.index(req)
        self.assertEqual(len(res.body), 2)
        vol_ids = (v2.id, v3.id,)
        self.assertEqual((res.body[0]['id'], res.body[1]['id']), vol_ids)

        # Query by both
        req = Request.blank('?cinder_host=%s&name=%s' % (cinder_host, name))
        res = c.index(req)
        self.assertEqual(len(res.body), 1)
        self.assertEqual(res.body[0]['id'], v2.id)

    def test_create_success(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        res = c.create(req)
        self.assertEqual(res.body['id'], 'test')
        self.assert_(res.body['node_id'])
        self.assert_(res.body['status'], 'ACTIVE')
        # For use with the volume manager.
        self.assert_(res.body['cinder_host'])

    def test_create_missing_params(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPBadRequest, c.create, req)
        req = Request.blank('?size=a&volume_type_name=vtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?size=1')
        self.assertRaises(HTTPBadRequest, c.create, req)
        req = Request.blank('?size=1&volume_type_name=garbage')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_create_inactive_volume_type(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        inactive = db.models.VolumeType('inactive', status="INACTIVE")
        self.db.add(inactive)
        self.db.flush()
        req = Request.blank('?size=1&volume_type_name=inactive')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_create_duplicate_fails(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        res = c.create(req)
        self.assertEqual(res.body['id'], 'test')
        self.assert_(res.body['node_id'])
        # test create duplicate
        req = Request.blank('?size=1&volume_type_name=vtype')
        self.assertRaises(HTTPConflict, c.create, req)

    def test_create_fail(self):
        base.urlopen = MockUrlopenBlowup
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1000&volume_type_name=vtype')
        self.assertRaises(HTTPError, c.create, req)
        try:
            c.create(req)
        except HTTPError, e:
            self.assertEquals(e.code // 100, 5)

    def test_create_from_image(self):
        image_id = 'my_image'
        base.urlopen = MockUrlopenWithImage
        c = Controller({'account_id': self.account_id, 'id': 'test1'},
                       self.mock_app)
        self.mock_called = False

        def mock_get_recommended_nodes(*args, **kwargs):
            self.assert_('imaging' in kwargs)
            self.assertTrue(kwargs['imaging'])
            self.mock_called = True
            return [self.node0]
        c.get_recommended_nodes = mock_get_recommended_nodes

        req = Request.blank('?size=2&volume_type_name=vtype&image_id=%s' %
                            image_id)
        res = c.create(req)

        self.assertTrue(self.mock_called)
        self.assertEqual(res.body['id'], 'test1')
        self.assertEqual(res.body['size'], 2)
        self.assertEqual(res.body['status'], 'IMAGING')
        self.assertEqual(res.body['image_id'], image_id)

    def test_create_from_backup_with_new_size(self):
        volume = db.models.Volume(node=self.node0, account=self.account,
                                  status='ACTIVE', size=1)
        self.db.add(volume)
        backup = db.models.Backup(volume, status='AVAILABLE')
        self.db.add(backup)
        self.db.commit()

        c = Controller({'account_id': self.account_id, 'id': 'test1'},
                       self.mock_app)
        req = Request.blank('?size=2&volume_type_name=vtype&backup=%s' %
                            backup.id)
        res = c.create(req)
        self.assertEqual(res.body['id'], 'test1')
        self.assertEqual(res.body['size'], 2)
        self.assert_(res.body['node_id'])
        self.assert_(res.body['status'], 'ACTIVE')

    def test_create_backup_validation(self):
        volume = db.models.Volume(node=self.node0, account=self.account,
                                  status='ACTIVE', size=1)
        self.db.add(volume)
        self.db.commit()
        c = Controller({'account_id': self.account_id, 'id': 'test1'},
                       self.mock_app)
        req = Request.blank('?size=2&volume_type_name=vtype&backup=%s' %
                            'backupnotfound')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        backup = db.models.Backup(volume, status='NOTAVAILABLE')
        self.db.add(backup)
        self.db.commit()
        req = Request.blank('?size=2&volume_type_name=vtype&backup=%s' %
                            backup.id)
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_create_size_validation(self):
        self.vtype.min_size = 2
        self.vtype.max_size = 4
        self.db.add(self.vtype)
        self.db.commit()
        c = Controller({'account_id': self.account_id, 'id': 'test1'},
                       self.mock_app)
        req = Request.blank('?volume_type_name=vtype')
        self.assertRaises(HTTPBadRequest, c.create, req)
        req = Request.blank('?size=monkeys&volume_type_name=vtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?size=1&volume_type_name=vtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?size=5&volume_type_name=vtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

        volume = db.models.Volume(node=self.node0, account=self.account,
                                  status='ACTIVE', size=3)
        self.db.add(volume)
        self.db.commit()
        req = Request.blank(
            '?size=2&volume_type_name=vtype&source_volume=%s' % volume.id)
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

        backup = db.models.Backup(volume, status='AVAILABLE')
        self.db.add(backup)
        self.db.commit()
        req = Request.blank('?size=2&volume_type_name=vtype&backup=%s' %
                            backup.id)
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_create_source_validation(self):
        c = Controller({'account_id': self.account_id, 'id': 'test1'},
                       self.mock_app)
        req = Request.blank('?size=2&volume_type_name=vtype&source_volume=%s' %
                            'backupnotfound')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        source_vol = db.models.Volume(node=self.node0, account=self.account,
                                      status='NOTACTIVE', size=1)
        self.db.add(source_vol)
        self.db.commit()
        req = Request.blank('?size=2&volume_type_name=vtype&source_volume=%s' %
                            source_vol.id)
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        source_vol.status = 'ACTIVE'
        source_vol.node = None
        self.db.add(source_vol)
        self.db.commit()
        req = Request.blank('?size=2&volume_type_name=vtype&source_volume=%s' %
                            source_vol.id)
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        source_vol = db.models.Volume(node=self.node0, account=self.account,
                                      status='NOTACTIVE', size=1)
        self.db.add(source_vol)
        self.db.commit()
        req = Request.blank('?size=2&volume_type_name=vtype&source_volume=%s' %
                            source_vol.id)
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_create_source_success(self):
        c = Controller({'account_id': self.account_id, 'id': 'test1'},
                       self.mock_app)
        source_vol = db.models.Volume(node=self.node0, account=self.account,
                                      status='ACTIVE', size=1)
        self.db.add(source_vol)
        self.db.commit()
        req = Request.blank('?size=2&volume_type_name=vtype&source_volume=%s' %
                            source_vol.id)
        res = c.create(req)
        self.assertEqual(res.body['id'], 'test1')
        self.assertEqual(res.body['size'], 2)
        self.assert_(res.body['node_id'], self.node0.id)
        self.assert_(res.body['status'], 'CLONING')

    def test_transfer_success(self):
        volume = db.models.Volume(node=self.node0, account=self.account,
                                  status='ACTIVE', size=1)
        self.db.add(volume)
        self.db.commit()
        c = Controller({'account_id': self.account_id, 'id': volume.id},
                       self.mock_app)
        new_account_id = 'new_account_id'
        req = Request.blank('?account_id=%s' % new_account_id)
        res = c.update(req)
        self.assertEqual(res.body['id'], volume.id)
        self.assertEqual(res.body['account_id'], new_account_id)

    def test_transfer_404(self):
        c = Controller({'account_id': self.account_id, 'id': 'missing'},
                       self.mock_app)
        new_account_id = 'new_account_id'
        req = Request.blank('?account_id=%s' % new_account_id)
        self.assertRaises(HTTPNotFound, c.update, req)

    def test_validate_name(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('')
        valid_name = c._validate_name(req.params)
        # default to id
        self.assertEqual(valid_name, 'test')
        name = 'aSDf'
        req = Request.blank('?name=%s' % name)
        valid_name = c._validate_name(req.params)
        self.assertEqual(valid_name, name)
        # only allow alphanumeric and dashes
        name = 'asdf_42'
        req = Request.blank('?name=%s' % name)
        self.assertRaises(HTTPPreconditionFailed, c._validate_name, req.params)
        # only allow alphanumeric and dashes
        name = 'asdf*+42'
        req = Request.blank('?name=%s' % name)
        self.assertRaises(HTTPPreconditionFailed, c._validate_name, req.params)
        name = 'asdf  2'
        req = Request.blank('?name=%s' % name)
        self.assertRaises(HTTPPreconditionFailed, c._validate_name, req.params)

    def test_validate_volume_type_limits(self):
        n = db.models.Node('bignode', 10000, volume_type=self.vtype,
                           hostname='10.127.0.72', port=8152)
        self.db.add(n)
        self.vtype.min_size = 100
        self.vtype.max_size = 1000
        self.db.add(self.vtype)
        self.db.commit()
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1234&volume_type_name=vtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?size=12&volume_type_name=vtype')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        req = Request.blank('?size=123&volume_type_name=vtype')
        res = c.create(req)
        self.assertEqual(res.body['id'], 'test')
        self.assertEqual(res.body['size'], 123)

    def test_validate_force_node(self):
        n = db.models.Node('mynode', 100, volume_type=self.vtype,
                           hostname='10.127.0.72', port=8152)
        self.db.add(n)
        self.db.commit()

        c = Controller({'account_id': self.account_id, 'id': 'test'},
                self.mock_app)
        req = Request.blank('?force_node=%s' % n.id)
        force_node = c._validate_force_node(req.params)
        self.assertEquals(force_node, n.id)
        req = Request.blank('?force_node=%s' % n.name)
        force_node = c._validate_force_node(req.params)
        self.assertEquals(force_node, n.name)
        req = Request.blank('?force_node=garbage')
        self.assertRaises(HTTPPreconditionFailed, c._validate_force_node,
                          req.params)


    def test_validate_affinity(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                self.mock_app)
        req = Request.blank('?somethingelse=2')
        affinity = c._validate_affinity(req.params)
        self.assertEquals(affinity, '')
        req = Request.blank('?affinity=badformat')
        self.assertRaises(HTTPPreconditionFailed, c._validate_affinity,
                          req.params)
        req = Request.blank('?affinity=badtypeof_affinity:abcde')
        self.assertRaises(HTTPPreconditionFailed, c._validate_affinity,
                          req.params)
        req = Request.blank('?affinity=different_node:volume_id')
        affinity = c._validate_affinity(req.params)
        self.assertEquals(affinity, 'different_node:volume_id')
        req = Request.blank('?affinity=different_group:volume_id')
        affinity = c._validate_affinity(req.params)
        self.assertEquals(affinity, 'different_group:volume_id')

    def test_delete(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        id = c.create(req).body['id']
        c = Controller({'account_id': self.account_id, 'id': id},
                       self.mock_app)
        req = Request.blank('')
        res = c.delete(req)
        self.assertEqual(res.body['id'], id)
        self.assertEqual(res.body['status'], 'DELETING')
        self.assert_(res.body['deleted_at'])
        self.assertIsInstance(res.body['deleted_at'], datetime.datetime)
        c = Controller(
                {
                    'account_id': self.account_id,
                    'id': '00000000-0000-0000-0000-000000000000',
                }, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.delete, req)

    def test_delete_different_name(self):
        volume_id = 'test'
        volume_name = 'nottest'
        c = Controller({'account_id': self.account_id, 'id': volume_id},
                       self.mock_app)
        req = Request.blank(
            '?size=1&volume_type_name=vtype&name=%s' % volume_name)
        c.create(req)
        c = Controller({'account_id': self.account_id, 'id': volume_id},
                       self.mock_app)
        req = Request.blank('')

        node_request_path = []
        def raise_exc(self, node, method, path, **kwargs):
            node_request_path.append(path)
            raise base.NodeError(MockRequest(), URLError("something bad"))

        with patch(Controller, 'node_request', raise_exc):
            self.assertRaises(base.NodeError, c.delete, req)

        self.assertEquals(str(node_request_path[0]),
                          '/volumes/%s' % volume_name)


    def test_delete_no_node_restore_of(self):
        volume = db.models.Volume(node=None, account=self.account,
                                  status='ACTIVE', size=1, restore_of='foo')
        self.db.add(volume)
        self.db.commit()
        self.assertEqual(volume.restore_of, 'foo')
        c = Controller({'account_id': self.account_id, 'id': volume.id},
                       self.mock_app)
        req = Request.blank('')
        res = c.delete(req)
        self.assertEqual(res.body['id'], volume.id)
        self.assertEqual(res.body['status'], 'DELETED')
        self.assertEqual(res.body['restore_of'], None)
        self.assertEqual(volume.restore_of, None)

    def test_delete_node_404_restore_of(self):
        def raise_exc(*args, **kwargs):
            e = base.NodeError(MockRequest(), URLError("fake 404"))
            e.code = 404
            raise e

        volume = db.models.Volume(node=self.node0, account=self.account,
                                  status='ACTIVE', size=1, restore_of='foo')
        self.db.add(volume)
        self.db.commit()
        self.assertEqual(volume.restore_of, 'foo')
        c = Controller({'account_id': self.account_id, 'id': volume.id},
                       self.mock_app)
        req = Request.blank('')
        with patch(Controller, 'node_request', raise_exc):
            self.assertRaises(base.NodeError, c.delete, req)

        self.assertEqual(volume.restore_of, None)

    def test_show(self):
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        c.create(req)
        req = Request.blank('')
        res = c.show(req)
        self.assertEqual(res.body['id'], 'test')
        c = Controller(
                {
                    'account_id': self.account_id,
                    'id': '00000000-0000-0000-0000-000000000000',
                }, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.show, req)

    def test_show_fails_after_account_is_deleted(self):
        # create volume
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('?size=1&volume_type_name=vtype')
        c.create(req)
        # delete account
        self.account.status = 'DELETED'
        self.db.add(self.account)
        self.db.commit()
        # should raise not found
        c = Controller({'account_id': self.account_id, 'id': 'test'},
                       self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.show, req)
        # admin still works
        c = Controller({'account_id': 'admin', 'id': 'test'},
                       self.mock_app)
        req = Request.blank('')
        res = c.show(req)
        self.assertEqual(res.body['id'], 'test')

    def test_update(self):
        pass

    def test_get_recommended_nodes_is_random(self):
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        size = 1
        nodes1 = c.get_recommended_nodes(self.vtype.name, size)
        attempts = 0
        while True:
            nodes2 = c.get_recommended_nodes(self.vtype.name, size)
            try:
                self.assertNotEquals(nodes1, nodes2)
            except AssertionError:
                if attempts >= 3:
                    raise
            else:
                break
            attempts += 1
        self.assertEquals(len(nodes1), len(nodes2))
        nodes1.sort()
        nodes2.sort()
        self.assertEqual(nodes1, nodes2)

    def test_get_recommended_ignores_deleted_volumes(self):
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        vtype = db.models.VolumeType('something_else')
        n = db.models.Node('newnode', 10, volume_type=vtype,
                           hostname='10.127.0.42', port=8242)
        self.db.add_all([vtype, n])
        self.db.commit()
        nodes = c.get_recommended_nodes('something_else', 1)
        self.assertNotEquals([], nodes)
        self.assertEquals(n.id, nodes[0].id)
        # Fill it to the gills.
        v = db.models.Volume(10, 'something_else', node=n,
                             account_id=self.account_id)
        self.db.add(v)
        self.db.commit()
        self.assertRaises(HTTPError, c.get_recommended_nodes,
                          'something_else', 1)
        v.status = 'DELETED'
        self.db.add(v)
        self.db.commit()
        nodes = c.get_recommended_nodes('something_else', 1)
        self.assertNotEquals([], nodes)
        self.assertEquals(n.id, nodes[0].id)

    def test_dont_recommend_full_nodes(self):
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        size = 2
        # (10 - (10 - 3 - 2)) / 10 == .5, IN!
        self.db.add(db.models.Volume(3, 'vtype', node=self.node0,
                                     account_id=self.account_id))
        # (11 - (11 - 4 - 2)) / 11 > .5, OUT!
        self.db.add(db.models.Volume(4, 'vtype', node=self.node1,
                                     account_id=self.account_id))
        # (12 - (12 - 20 - 2)) / 12 > .5, OUT!
        self.db.add(db.models.Volume(5, 'vtype', node=self.node2,
                                     account_id=self.account_id))
        nodes1 = c.get_recommended_nodes(self.vtype.name, size)
        self.assertEquals(nodes1, [self.node0])

    def test_recommend_nodes_ordered_by_volumes(self):
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        n = db.models.Node('node3', 13, volume_type=self.vtype,
                           hostname='10.127.0.3', port=8083)
        self.db.add(n)
        self.db.add(db.models.Volume(1, 'vtype', node=self.node1,
                                     account_id=self.account_id,
                                     volume_type=self.vtype))
        self.db.add(db.models.Volume(1, 'vtype', node=n,
                                     account_id=self.account_id))
        n.calc_storage_used()
        self.db.commit()
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        # node0/2 have 0 volumes, they should be included.
        # node1/3 have 1 volume. 3 should be included because it's bigger.
        node_ids = [node.id for node in nodes]
        expected_ids = [self.node0.id, self.node2.id, n.id]
        self.assertEqual(sorted(node_ids), sorted(expected_ids))

    def test_deep_recommended_nodes(self):
        self.mock_app.fill_strategy = 'deep_fill'
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        node_ids = [node.id for node in nodes]
        # Our test nodes are in order by size, 0 = 10, 1 = 11, etc.
        expected_ids = [self.node0.id, self.node1.id, self.node2.id]
        self.assertEqual(node_ids, expected_ids)
        # Add Volume from a different account,
        self.db.add(db.models.Volume(1, 'vtype', node=self.node0,
                                     account_id=self.account_id2,
                                     volume_type=self.vtype))
        # node0 should still be preferred
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        self.assertEquals(3, len(nodes))
        self.assertEquals(nodes[0].id, self.node0.id)
        # Now add a volume for this account
        self.db.add(db.models.Volume(1, 'vtype', node=self.node0,
                                     account_id=self.account_id,
                                     volume_type=self.vtype))
        # Should prefer the second node
        nodes = c.get_recommended_nodes(self.vtype.name, 3)
        self.assertEquals(3, len(nodes))
        self.assertEquals(nodes[0].id, self.node1.id)
        # Add a volume to the second node
        self.db.add(db.models.Volume(3, 'vtype', node=self.node1,
                                     account_id=self.account_id,
                                     volume_type=self.vtype))
        # Should prefer the last node
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        self.assertEquals(3, len(nodes))
        self.assertEquals(nodes[0].id, self.node2.id)

    def test_recommended_nodes_for_image(self):
        self.mock_app.fill_strategy = 'deep_fill'
        self.mock_app.image_convert_limit = 1
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        node_ids = [node.id for node in nodes]
        # Our test nodes are in order by size, 0 = 10, 1 = 11, etc.
        expected_ids = [self.node0.id, self.node1.id, self.node2.id]
        self.assertEqual(node_ids, expected_ids)
        # Add an imaging volume to node0
        self.db.add(db.models.Volume(1, 'vtype', node=self.node0,
                                     account_id=self.account_id2,
                                     volume_type=self.vtype, status='IMAGING'))
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        self.assertEquals(3, len(nodes))
        node_ids = [node.id for node in nodes]
        self.assertIn(self.node0.id, node_ids)
        # Now try recommendations for imaging
        nodes = c.get_recommended_nodes(self.vtype.name, 1, imaging=True)
        self.assertEquals(2, len(nodes))
        node_ids = [node.id for node in nodes]
        self.assertNotIn(self.node0.id, node_ids)

    def test_recommended_nodes_affinity_node(self):
        self.mock_app.fill_strategy = 'deep_fill'
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        node_ids = [node.id for node in nodes]
        # Our test nodes are in order by size, 0 = 10, 1 = 11, etc.
        expected_ids = [self.node0.id, self.node1.id, self.node2.id]
        self.assertEqual(node_ids, expected_ids)
        # Now add a volume for this account
        v1 = db.models.Volume(1, 'vtype', node=self.node0,
            account_id=self.account_id, volume_type=self.vtype)
        v2 = db.models.Volume(2, 'vtype', node=self.node1,
            account_id=self.account_id, volume_type=self.vtype)
        v3 = db.models.Volume(2, 'vtype', node=self.node2,
            account_id=self.account_id, volume_type=self.vtype)
        self.db.add_all([v1, v2, v3])
        self.db.commit()
        # Should prefer the second node
        affinity = 'different_node:%s' % v1.id
        nodes = c.get_recommended_nodes(self.vtype.name, 3,
                                        affinity=affinity)
        self.assertEquals(2, len(nodes))
        self.assertEquals(nodes[0].id, self.node1.id)
        # Add a volume to the second node
        # Should prefer the last node
        affinity = 'different_node:%s,%s' % (v1.id, v2.id)
        nodes = c.get_recommended_nodes(self.vtype.name, 1,
                                        affinity=affinity)
        self.assertEquals(1, len(nodes))
        self.assertEquals(nodes[0].id, self.node2.id)
        # Recommendation fails.
        affinity = 'different_node:%s,%s,%s' % (v1.id, v2.id, v3.id)
        self.assertRaises(HTTPInsufficientStorage, c.get_recommended_nodes,
                          self.vtype.name, 1, affinity=affinity)

    def test_recommended_nodes_affinity_cab(self):
        self.mock_app.fill_strategy = 'deep_fill'
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        nodes = c.get_recommended_nodes(self.vtype.name, 1)
        node_ids = [node.id for node in nodes]
        # Our test nodes are in order by size, 0 = 10, 1 = 11, etc.
        expected_ids = [self.node0.id, self.node1.id, self.node2.id]
        self.assertEqual(node_ids, expected_ids)
        # Set up cabs.
        self.node0.affinity_group = 'cab1'
        self.node1.affinity_group = 'cab2'
        self.node2.affinity_group = 'cab1'
        # Now add some volumes to these cabs.
        # v1,v3 = cab1, v2 = cab2
        self.db.add_all([self.node0, self.node1, self.node2])
        v1 = db.models.Volume(1, 'vtype', node=self.node0,
            account_id=self.account_id, volume_type=self.vtype)
        v2 = db.models.Volume(1, 'vtype', node=self.node1,
            account_id=self.account_id, volume_type=self.vtype)
        v3 = db.models.Volume(1, 'vtype', node=self.node2,
            account_id=self.account_id, volume_type=self.vtype)
        self.db.add_all([v1, v2, v3])
        self.db.commit()
        # Should only show node1 (cab2)
        affinity = 'different_group:%s' % v1.id
        nodes = c.get_recommended_nodes(self.vtype.name, 3,
                                        affinity=affinity)
        self.assertEquals(1, len(nodes))
        self.assertEquals(nodes[0].id, self.node1.id)
        # Should show node0 and node2 (cab1)
        affinity = 'different_group:%s' % v2.id
        nodes = c.get_recommended_nodes(self.vtype.name, 1,
                                        affinity=affinity)
        self.assertEquals(2, len(nodes))
        self.assertEquals(nodes[0].id, self.node0.id)
        self.assertEquals(nodes[1].id, self.node2.id)
        # Recommendation fails.
        affinity = 'different_group:%s,%s' % (v1.id, v2.id)
        self.assertRaises(HTTPInsufficientStorage, c.get_recommended_nodes,
                          self.vtype.name, 1, affinity=affinity)

    def test_recommended_nodes_force_node(self):
        self.mock_app.fill_strategy = 'deep_fill'
        c = Controller({'account_id':  self.account_id}, self.mock_app)
        node = self.node0

        nodes = c.get_recommended_nodes(self.vtype.name, 1,
                                        force_node=node.name)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, node.id)

        nodes = c.get_recommended_nodes(self.vtype.name, 1,
                                        force_node=node.id)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, node.id)


class TestVolumeApi(WsgiTestBase):

    def setUp(self):
        self.test_conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://'}})
        self.app = ApiWsgiApp(self.test_conf, urlmap)
        self.vtype = db.models.VolumeType('vtype')
        db.Session.add(self.vtype)
        self.node = db.Session.add(db.models.Node('somenode', 100000000000,
                                                  volume_type=self.vtype,
                                                  port=8080,
                                                  hostname='127.0.0.1'))
        self.account = db.models.Account(id='someaccount')
        db.Session.add(self.account)
        db.Session.commit()

    def tearDown(self):
        db.Session.remove()

    def test_index(self):
        resp = self.request("/v1.0/account/volumes")
        self.assertEqual(resp.code, 200)
        self.assertEqual(resp.body, [])

    def test_create_different_name(self):
        volume_id = 'test'
        volume_name = 'nottest'
        node_request_path = []
        def raise_exc(self, node, method, path, **kwargs):
            node_request_path.append(path)
            raise base.NodeError(MockRequest(), URLError("something bad"))

        with patch(Controller, 'node_request', raise_exc):
            resp = self.request("/v1.0/account/volumes/%s" % volume_id, 'PUT',
                                {'size': 1, 'volume_type_name': 'vtype',
                                 'name': volume_name})

        self.assertEquals(str(node_request_path[0]),
                          '/volumes/%s' % volume_name)

    def test_create_no_storage_nodes_avail(self):
        def return_empty(*args, **kwargs):
            return []

        with patch(BaseController, 'get_recommended_nodes', return_empty):
            resp = self.request("/v1.0/account/volumes/test", 'PUT',
                                {'size': 1, 'volume_type_name': 'vtype'})

        self.assertEquals(resp.code, 503)
        self.assertEquals(
            resp.body['reason'], "No available storage nodes for type 'vtype'")

        resp = self.request("/v1.0/account/volumes/test")
        self.assertEquals(resp.code, 404)

    def test_create_storage_node_req_fail(self):
        def raise_exc(*args, **kwargs):
            raise base.NodeError(MockRequest(), URLError("something bad"))

        with patch(Controller, 'node_request', raise_exc):
            resp = self.request("/v1.0/account/volumes/test", 'PUT',
                                {'size': 1, 'volume_type_name': 'vtype'})

        self.assertEquals(resp.code, 503)
        self.assert_("something bad" in resp.body['reason'])

        resp = self.request("/v1.0/account/volumes/test")
        self.assertEquals(resp.body['status'], 'DELETED')

    def test_create_storage_node_failover(self):
        self.node2 = db.Session.add(
            db.models.Node('somenode2', 100000000000, volume_type=self.vtype,
                           port=8081, hostname='127.0.0.1'))
        self.node3 = db.Session.add(
            db.models.Node('somenode3', 100000000000, volume_type=self.vtype,
                           port=8082, hostname='127.0.0.1'))
        db.Session.commit()

        def fail_response(*args, **kwargs):
            raise socket.timeout("too slow!")

        def success_response(*args, **kwargs):
            data = {'status': 'ACTIVE'}
            return MockResponse(200, json.dumps(data))
        self.responses = [fail_response, fail_response, success_response]

        def mock_urlopen(*args, **kwargs):
            func = self.responses.pop(0)
            return func(*args, **kwargs)
        with patch(base, 'urlopen', mock_urlopen):
            resp = self.request("/v1.0/account/volumes/test", 'PUT',
                                {'size': 1, 'volume_type_name': 'vtype'})
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'ACTIVE')
        self.assertEqual(self.responses, [])

    def test_admin_cant_create(self):
        resp = self.request("/v1.0/admin/volumes/test", 'PUT',
                            {'size': 1, 'volume_type_name': 'vtype'})
        self.assertEquals(resp.code, 405)

    def test_db_goes_away_recovery(self):
        attempts = [0]
        original = getattr(db.Session, 'get_or_create_account')

        def raise_exc(account):
            if attempts[0] == 0:
                attempts[0] += 1
                raise OperationalError('', '', Struct(args=[2006]))
            return original(account)

        with patch(base, 'urlopen', MockUrlopen):
            with patch(server, 'sleep', lambda i: True):
                with patch(db.Session, 'get_or_create_account', raise_exc):
                    resp = self.request("/v1.0/account/volumes/thrawn", 'PUT',
                                        {'size': 1,
                                         'volume_type_name': 'vtype'})
        self.assertEquals(resp.code, 200)

    def test_create_volume_exists(self):
        vol_id = "v1"
        db.Session.add(db.models.Volume(0, 'vtype', id=vol_id,
                                        account_id=self.account.id))
        db.Session.commit()
        url = "/v1.0/%s/volumes/%s" % (self.account.id, vol_id)
        resp = self.request(url, 'PUT',
                            {'size': 1, 'volume_type_name': 'vtype'})
        self.assertEquals(resp.code, 409)

    def test_create_volume_exists_on_other_account(self):
        vol_id = "v1"
        db.Session.add(db.models.Volume(0, 'vtype', id=vol_id,
                                        account_id=self.account.id))
        db.Session.commit()
        url = "/v1.0/%s/volumes/%s" % ('not' + self.account.id, vol_id)
        resp = self.request(url, 'PUT',
                            {'size': 1, 'volume_type_name': 'vtype'})
        self.assertEquals(resp.code, 409)

    def test_create_fails_on_deleted_account(self):
        self.account.status = 'DELETED'
        db.Session.add(self.account)
        db.Session.commit()
        resp = self.request("/v1.0/%s/volumes/test" % self.account.id,
                            'PUT', {'size': 1, 'volume_type_name': 'vtype'})
        self.assertEquals(resp.code, 404)
        self.assert_("Account is not ACTIVE" in resp.body['reason'])


if __name__ == "__main__":
    unittest.main()
