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


from urllib2 import HTTPError, URLError
from httplib import BadStatusLine
from StringIO import StringIO

from lunr.api.controller.base import BaseController, NodeError
from lunr.common.config import LunrConfig
from testlunr.functional import Struct
from lunr.api.controller import base
from testlunr.unit import patch
from lunr import db

import unittest


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

    def setUp(self):
        self.node = Struct(hostname='localhost', port='8080')
        self.app = MockApp()

    def test_node_request_exception(self):
        def raise_exc(*args, **kwargs):
            raise BadStatusLine("something bad")

        controller = BaseController({}, self.app)
        with patch(base, 'urlopen', raise_exc):
            with self.assertRaises(NodeError) as cm:
                controller.node_request(self.node, 'PUT', '/volumes/vol-01')

        # Assert the exception details are correct
        self.assertEquals(cm.exception.detail,
                          "PUT on http://localhost:8080/volumes/vol-01 "
                          "failed with 'BadStatusLine'")
        self.assertEquals(cm.exception.code, 503)
        self.assertEquals(cm.exception.reason, "something bad")

    def test_node_request_urllib2_httperror(self):
        def raise_exc(*args, **kwargs):
            fp = StringIO('{"reason": "something bad"}')
            raise HTTPError('http://localhost/volumes/vol-01', 500,
                            'Internal Error', {}, fp)

        controller = BaseController({}, self.app)
        with patch(base, 'urlopen', raise_exc):
            with self.assertRaises(NodeError) as cm:
                controller.node_request(self.node, 'PUT', '/volumes/vol-01')

        # Assert the exception details are correct
        self.assertEquals(cm.exception.detail,
                          "PUT on http://localhost:8080/volumes/vol-01 "
                          "returned '500' with 'something bad'")
        self.assertEquals(cm.exception.code, 500)
        self.assertEquals(cm.exception.reason, "something bad")

    def test_node_request_urllib2_urlerror(self):
        def raise_exc(*args, **kwargs):
            raise URLError("something bad")

        controller = BaseController({}, self.app)
        with patch(base, 'urlopen', raise_exc):
            with self.assertRaises(NodeError) as cm:
                controller.node_request(self.node, 'PUT', '/volumes/vol-01')

        # Assert the exception details are correct
        self.assertEquals(cm.exception.detail,
                          "PUT on http://localhost:8080/volumes/vol-01 "
                          "failed with 'something bad'")
        self.assertEquals(cm.exception.code, 503)
        self.assertEquals(cm.exception.reason, "something bad")


class TestFillStrategy(unittest.TestCase):

    def setUp(self):
        self.mock_app = MockApp()
        self.db = self.mock_app.helper
        self.vtype = db.models.VolumeType('vtype')
        self.db.add(self.vtype)
        self.nodes = {}
        for i in range(10):
            n = db.models.Node('node%s' % i, 100, volume_type=self.vtype,
                               hostname='10.127.0.%s' % i, port=8080 + i)
            self.nodes[i] = n
            self.db.add(n)
        self.account = db.models.Account(id='someaccount')
        self.db.add(self.account)
        self.db.commit()
        self.controller = BaseController({}, self.mock_app)
        self.controller._account_id = self.account.id

    def tearDown(self):
        pass

    def test_broad_fill_by_account(self):
        account1 = self.account
        account2 = db.models.Account()
        limit = 3
        self.db.add(account2)
        # Give account1 a volume on 7/10 nodes.
        for i in range(7):
            node = self.nodes[i]
            self.db.add(db.models.Volume(i, 'vtype', node=node,
                                         account_id=account1.id,
                                         volume_type=self.vtype))
        self.db.commit()
        nodes = self.controller.broad_fill('vtype', 1, limit).all()
        node_ids = [x[0].id for x in nodes]
        # It shouldl definitely pick the 3 empty nodes.
        self.assertItemsEqual(
            node_ids, (self.nodes[7].id, self.nodes[8].id, self.nodes[9].id))
        # Fill up the other 3 nodes with account2 volumes.
        for i in range(7, 10):
            node = self.nodes[i]
            # Lots more full than the other nodes!
            self.db.add(db.models.Volume(20, 'vtype', node=node,
                                         account_id=account2.id,
                                         volume_type=self.vtype))
        self.db.commit()
        nodes = self.controller.broad_fill('vtype', 1, limit).all()
        node_ids = [x[0].id for x in nodes]
        # We STILL want the 3 nodes account1 doesn't have volumes on.
        self.assertItemsEqual(
            node_ids, (self.nodes[7].id, self.nodes[8].id, self.nodes[9].id))
        # Put account1 volumes on two of those
        self.db.add(db.models.Volume(10, 'vtype', node=self.nodes[8],
                                     account_id=account1.id,
                                     volume_type=self.vtype))
        self.db.add(db.models.Volume(10, 'vtype', node=self.nodes[9],
                                     account_id=account1.id,
                                     volume_type=self.vtype))
        self.db.commit()
        nodes = self.controller.broad_fill('vtype', 1, limit).all()
        node_ids = [x[0].id for x in nodes]
        # 0 & 1 should be preferred (least used)
        # and 7 still doesn't have any volumes for account1
        self.assertItemsEqual(
            node_ids, (self.nodes[0].id, self.nodes[1].id, self.nodes[7].id))

    def test_sort_imaging_nodes(self):
        # Number of volumes imaging is going to be the #1 sort criteria now.
        shim_account = db.models.Account()
        self.db.add(shim_account)
        # Add one image to every node.
        for i in xrange(10):
            volume = db.models.Volume(1, 'vtype', node=self.nodes[i],
                                      account_id=shim_account.id,
                                      volume_type=self.vtype,
                                      status='IMAGING')
            self.db.add(volume)

        # These nodes will now be preferred by deep fill.
        for i in range(3):
            volume = db.models.Volume(10, 'vtype', node=self.nodes[i],
                                      account_id=shim_account.id,
                                      volume_type=self.vtype)
            self.db.add(volume)

        # Just slightly less preferred.
        for i in range(3, 6):
            volume = db.models.Volume(9, 'vtype', node=self.nodes[i],
                                      account_id=shim_account.id,
                                      volume_type=self.vtype)
            self.db.add(volume)

        # Even slightly less preferred.
        volume = db.models.Volume(8, 'vtype', node=self.nodes[7],
                                  account_id=shim_account.id,
                                  volume_type=self.vtype)
        self.db.add(volume)
        self.db.commit()

        limit = 3
        results = self.controller.deep_fill('vtype', 1, limit).all()
        nodes = [r.Node for r in results]
        expected = [self.nodes[0], self.nodes[1], self.nodes[2]]
        self.assertItemsEqual(nodes, expected)
        # Add a volume in IMAGING to node0,1,3,4
        for i in (0, 1, 3, 4):
            volume = db.models.Volume(1, 'vtype', node=self.nodes[i],
                                      account_id=shim_account.id,
                                      volume_type=self.vtype,
                                      status='IMAGING')
            self.db.add(volume)
        self.db.commit()
        q = self.controller.deep_fill('vtype', 1, limit, imaging=True)
        results = q.all()
        self.assertEquals(3, len(results))
        nodes = [r.Node for r in results]
        expected = [self.nodes[2], self.nodes[5], self.nodes[7]]
        self.assertItemsEqual(nodes, expected)

    def test_imaging_limit(self):
        shim_account = db.models.Account()
        self.db.add(shim_account)
        # These nodes will now be preferred by deep fill.
        for i in range(3):
            volume = db.models.Volume(10, 'vtype', node=self.nodes[i],
                                      account_id=shim_account.id,
                                      volume_type=self.vtype)
            self.db.add(volume)
        # Add two volumes in IMAGING to node0
        for i in xrange(3):
            volume = db.models.Volume(1, 'vtype', node=self.nodes[0],
                                      account_id=shim_account.id,
                                      volume_type=self.vtype,
                                      status='IMAGING')
            self.db.add(volume)
        self.db.commit()

        limit = 3
        results = self.controller.deep_fill('vtype', 1, limit).all()
        nodes = [r.Node for r in results]
        expected = [self.nodes[0], self.nodes[1], self.nodes[2]]

        # Add one more!
        volume = db.models.Volume(1, 'vtype', node=self.nodes[0],
                                  account_id=shim_account.id,
                                  volume_type=self.vtype,
                                  status='IMAGING')
        self.db.add(volume)
        self.db.commit()

        results = self.controller.deep_fill('vtype', 1, limit,
                                            imaging=True).all()
        nodes = [r.Node for r in results]
        self.assertNotIn(self.nodes[0], nodes)

if __name__ == "__main__":
    unittest.main()
