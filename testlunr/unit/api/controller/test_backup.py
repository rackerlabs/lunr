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


from StringIO import StringIO
import unittest
from urllib import urlencode
from urllib2 import HTTPError
from webob import Request
from webob.exc import HTTPNotFound, HTTPUnprocessableEntity, \
    HTTPPreconditionFailed
import json

from lunr import db
from lunr.api.controller import base
from lunr.api.controller.account import AccountController
from lunr.api.controller.backup import BackupController as Controller
from lunr.api.controller.base import BaseController
from lunr.api.controller.volume import VolumeController
from lunr.api.server import ApiWsgiApp
from lunr.api.urlmap import urlmap
from lunr.common.config import LunrConfig
from testlunr.unit import patch, WsgiTestBase


class MockApp(object):
    def __init__(self):
        self.conf = LunrConfig(
            {'db': {'auto_create': True, 'url': 'sqlite://'}})
        # self.urlmap = urlmap
        self.helper = db.configure(self.conf)
        self.fill_percentage_limit = 0.5
        self.node_timeout = None


class MockResponse(object):

    def __init__(self, body=''):
        self.body_file = StringIO(body)

    def read(self, *args, **kwargs):
        return self.body_file.read(*args, **kwargs)

    def get_method(self):
        return 'PUT'

    def get_full_url(self):
        return 'https://localhost/'

    def getcode(self):
        return 200


class MockUrlopen(object):
    def __init__(self):
        pass

    def wrap_callback(self, callback):
        """
        Add a called property to the callback which will be set to False and
        automatically updated to True once the method is called.

        :param callback: a callable

        :returns: a wrapped callable with a new property called
        """
        def wrapper(*args, **kwargs):
            wrapper.called = True
            return callback(*args, **kwargs)
        wrapper.called = False
        return wrapper

    @property
    def resp(self):
        if not hasattr(self, '_resp'):
            def gen():
                while True:
                    yield '{}'
            self._resp = gen()
        return self._resp

    @resp.setter
    def resp(self, resp):
        """Set next resp from urlopen"""
        if isinstance(resp, basestring) or isinstance(resp, Exception):
            resp = [resp]
        self._resp = (r for r in resp)

    @property
    def request_callback(self):
        """
        Callable which will accept and optionally inspect a urllib2.Request
        """
        if not hasattr(self, '_callback'):
            self._callback = self.wrap_callback(lambda *args, **kwargs: None)
        return self._callback

    @request_callback.setter
    def request_callback(self, callback):
        """
        Set callack to inspect a urllib2.Request

        :params callback: callable which will accept a urllib2.Request and
                          optionally make asserts aginst it
        """
        self._callback = self.wrap_callback(callback)

    def __call__(self, request, timeout=None):
        self.request_callback(request)
        resp = self.resp.next()
        if isinstance(resp, Exception):
            raise resp
        if not hasattr(resp, 'read'):
            resp = MockResponse(resp)
        return resp


class TestBackupController(unittest.TestCase):
    """ Test lunr.api.controller.backup.BackupController """
    def setUp(self):
        self.mock_app = MockApp()
        self._orig_urlopen = base.urlopen
        base.urlopen = self.mock_urlopen = MockUrlopen()
        self.db = self.mock_app.helper
        self.volume_type = db.models.VolumeType('vtype')
        # create node
        self.node = db.models.Node('lunr1', 10, volume_type=self.volume_type)
        # create account
        self.account = db.models.Account()
        self.db.add_all([self.volume_type, self.node, self.account])
        self.db.commit()

    def create_volume(self, status='NEW', **kwargs):
        # create volume
        volume = db.models.Volume(node=self.node, account=self.account,
                                  status=status, **kwargs)
        self.db.add(volume)
        self.db.commit()
        return volume

    def tearDown(self):
        base.urlopen = self._orig_urlopen
        db.Session.remove()

    def test_list_empty_backups(self):
        # create controller
        c = Controller({'account_id': self.account.id}, self.mock_app)
        req = Request.blank('')
        resp = c.index(req)
        self.assertEquals(len(resp.body), 0)

    def test_list_populated_backups(self):
        # create a volume directly in the db
        volume = self.create_volume('ACTIVE')
        # use the api to create a few backups
        for i in range(3):
            backup_id = 'backup%s' % i
            c = Controller({'account_id': self.account.id, 'id': backup_id},
                           self.mock_app)
            params = {
                'volume': volume.id,
            }
            req = Request.blank('?%s' % urlencode(params))
            resp = c.create(req)
            self.assertEqual(resp.body['id'], backup_id)
            self.assertEquals(resp.body['volume_id'], volume.id)
            self.assertEquals(resp.body['status'], 'SAVING')
        # get the index/listing of the newly created backups
        c = Controller({'account_id': self.account.id}, self.mock_app)
        req = Request.blank('')
        resp = c.index(req)
        # make sure everything looks to be there
        self.assertEquals(len(resp.body), 3)
        for backup in resp.body:
            self.assert_(backup['id'].startswith('backup'))

    def test_filtered_backups(self):
        vol1 = self.create_volume('ACTIVE')
        vol2 = self.create_volume('ACTIVE')
        # vol1/backup1
        req = Request.blank('?%s' % urlencode({'volume': vol1.id}))
        c = Controller({'id': 'backup1', 'account_id': self.account.id},
                       self.mock_app)
        resp = c.create(req)
        # vol2/backup2
        req = Request.blank('?%s' % urlencode({'volume': vol2.id}))
        c = Controller({'id': 'backup2', 'account_id': self.account.id},
                       self.mock_app)
        resp = c.create(req)
        # vol1/backup3
        req = Request.blank('?%s' % urlencode({'volume': vol1.id}))
        c = Controller({'id': 'backup3', 'account_id': self.account.id},
                       self.mock_app)
        resp = c.create(req)
        # un-filtered backups
        req = Request.blank('')
        resp = c.index(req)
        self.assertEquals(len(resp.body), 3)
        # filter backups for vol2 (should just be backup2)
        req = Request.blank('?volume_id=%s' % vol2.id)
        resp = c.index(req)
        self.assertEquals(len(resp.body), 1)
        backup2 = resp.body[0]
        self.assertEquals(backup2['volume_id'], vol2.id)
        # filter backups for vol1 (should have backup1 & backup3)
        req = Request.blank('?volume_id=%s' % vol1.id)
        resp = c.index(req)
        self.assertEquals(len(resp.body), 2)
        for backup in resp.body:
            self.assertEquals(backup['volume_id'], vol1.id)

    def test_get_single_backup(self):
        # create a volume directly in the db
        volume = self.create_volume('ACTIVE')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        # use the api to create a backup
        params = {
            'volume': volume.id,
        }
        req = Request.blank('?%s' % urlencode(params))
        resp = c.create(req)
        self.assertEquals(resp.body['id'], 'backup1')
        self.assertEquals(resp.body['volume_id'], volume.id)
        self.assertEquals(resp.body['status'], 'SAVING')
        # make a new request against show for the name of backup
        c = Controller({'account_id': self.account.id,
                        'id': 'backup1'}, self.mock_app)
        req = Request.blank('')
        resp = c.show(req)
        self.assertEquals(resp.body['id'], 'backup1')
        self.assertEquals(resp.body['volume_id'], volume.id)
        self.assertEquals(resp.body['status'], 'SAVING')

    def test_get_backup_not_found(self):
        # create controller
        c = Controller({'account_id': self.account.id,
                        'id': 'backup1'}, self.mock_app)
        req = Request.blank('')
        self.assertRaises(HTTPNotFound, c.show, req)

    def test_create_backup_invalid_params(self):
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        # test missing name
        req = Request.blank('')
        self.assertRaises(HTTPPreconditionFailed, c.create, req)
        # test missing volume
        params = {}
        req = Request.blank('?%s' % urlencode(params))
        self.assertRaises(HTTPPreconditionFailed, c.create, req)

    def test_create_backup_volume_not_found(self):
        volume_id = 'volume-not-found'
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        params = {
            'volume': volume_id,
        }
        req = Request.blank('?%s' % urlencode(params))
        self.assertRaises(HTTPUnprocessableEntity, c.create, req)
        try:
            c.create(req)
        except HTTPUnprocessableEntity, e:
            self.assert_(volume_id in e.detail)

    def test_create_backup_invalid_volume(self):
        # create volume in non-ACTIVE state
        volume = self.create_volume('ERROR')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        params = {
            'volume': volume.id,
        }
        req = Request.blank('?%s' % urlencode(params))
        self.assertRaises(HTTPUnprocessableEntity, c.create, req)
        try:
            c.create(req)
        except HTTPUnprocessableEntity, e:
            self.assert_(volume.id in e.detail)

    def test_create_backup_imaging_scrub(self):
        volume = self.create_volume('IMAGING_SCRUB')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        params = {
            'volume': volume.id,
        }
        req = Request.blank('?%s' % urlencode(params))
        # success returns dict
        resp = c.create(req)
        self.assertEquals(resp.body['account_id'], self.account.id)
        self.assertEquals(resp.body['volume_id'], volume.id)
        self.assertEquals(resp.body['status'], 'SAVING')

    def test_create_backup_success(self):
        volume = self.create_volume('ACTIVE')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        params = {
            'volume': volume.id,
        }
        req = Request.blank('?%s' % urlencode(params))
        # success returns dict
        resp = c.create(req)
        self.assertEquals(resp.body['account_id'], self.account.id)
        self.assertEquals(resp.body['volume_id'], volume.id)
        self.assertEquals(resp.body['status'], 'SAVING')

    def test_create_stacked_backup(self):
        # create volume in db
        volume = self.create_volume('ACTIVE')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        # setup backup params
        params = {
            'volume': volume.id,
        }
        req = Request.blank('?%s' % urlencode(params))
        # prime urlopen response with Exception
        error = json.dumps({'reason': 'failed'})
        self.mock_urlopen.resp = [HTTPError('', 409, 'already snapped!', {},
                                            StringIO(error))]
        self.assertRaises(base.NodeError, c.create, req)
        # Node error should not leave a backup record around
        backup = self.db.query(db.models.Backup).get('backup1')
        self.assertEquals(backup, None)

    def test_create_backup_fails(self):
        # create volume in db
        volume = self.create_volume('ACTIVE')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        # setup backup params
        params = {
            'volume': volume.id,
        }
        # backup request
        req = Request.blank('?%s' % urlencode(params))
        # prime urlopen response with Exception
        error = json.dumps({'reason': 'failed'})
        self.mock_urlopen.resp = [HTTPError('', 500, 'kaboom!', {},
                                            StringIO(error))]
        self.assertRaises(base.NodeError, c.create, req)
        # Node error should not leave a backup record around
        backup = self.db.query(db.models.Backup).get('backup1')
        self.assertEquals(backup, None)

    def test_delete_backup_success(self):
        volume = self.create_volume('ACTIVE')
        # create controller
        c = Controller({'account_id': self.account.id, 'id': 'backup1'},
                       self.mock_app)
        params = {
            'volume': volume.id,
        }
        req = Request.blank('?%s' % urlencode(params))
        # success returns dict
        resp = c.create(req)
        self.assertEquals(resp.body['account_id'], self.account.id)
        self.assertEquals(resp.body['volume_id'], volume.id)
        self.assertEquals(resp.body['status'], 'SAVING')
        # create controller for named request
        c = Controller({'account_id': self.account.id,
                        'id': 'backup1'}, self.mock_app)
        # make delete request
        resp = c.delete(Request.blank(''))
        self.assertEquals(resp.body['account_id'], self.account.id)
        self.assertEquals(resp.body['volume_id'], volume.id)
        self.assertEquals(resp.body['status'], 'DELETING')

    def test_delete_backup_not_found(self):
        # create controller for named request
        c = Controller({'account_id': self.account.id,
                        'id': 'backup1'}, self.mock_app)
        # make delete request
        self.assertRaises(HTTPNotFound, c.delete, Request.blank(''))

    def test_delete_backup_already_deleted(self):
        v = self.create_volume()
        b = db.models.Backup(v, status='DELETED')
        self.db.add(b)
        self.db.commit()
        c = Controller({'account_id': self.account.id,
                        'id': b.id}, self.mock_app)
        self.assertRaises(HTTPNotFound, c.delete, Request.blank(''))

    def test_delete_backup_waiting_for_audit(self):
        v = self.create_volume()
        b = db.models.Backup(v, status='AUDITING')
        self.db.add(b)
        self.db.commit()
        c = Controller({'account_id': self.account.id,
                        'id': b.id}, self.mock_app)
        self.assertRaises(HTTPNotFound, c.delete, Request.blank(''))

    def test_delete_backup_node_request_fails(self):
        v = self.create_volume()
        b = db.models.Backup(v, status='AVAILABLE')
        self.db.add(b)
        self.db.commit()
        c = Controller({'account_id': self.account.id,
                        'id': b.id}, self.mock_app)
        error = json.dumps({'reason': 'failed'})
        self.mock_urlopen.resp = [HTTPError('', 500, 'kaboom!', {},
                                            StringIO(error))]
        self.assertRaises(base.NodeError, c.delete, Request.blank(''))
        self.assertEquals(b.status, 'AVAILABLE')


class TestBackupApi(WsgiTestBase):

    def setUp(self):
        self.test_conf = LunrConfig(
            {'auto_create': True, 'db': {'url': 'sqlite://'}})
        self.app = ApiWsgiApp(self.test_conf, urlmap)

    def test_admin_cant_create(self):
        resp = self.request("/v1.0/admin/backups/test", 'PUT', {
                'volume': 'foo'
            })
        self.assertEquals(resp.code, 405)


if __name__ == "__main__":
    unittest.main()
