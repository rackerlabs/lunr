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

import hashlib

from lunr import db
from lunr.db import models
from sqlalchemy.exc import IntegrityError

from lunr.common.config import LunrConfig


class ModelTest(unittest.TestCase):

    def setUp(self):
        self.db = db.configure(
            LunrConfig({'db': {'auto_create': True, 'url': 'sqlite://'}}))

    def tearDown(self):
        db.Session.remove()


class TestNode(ModelTest):

    def setUp(self):
        ModelTest.setUp(self)
        self.volume_type = models.VolumeType('lunr')
        self.default_volume_type = models.VolumeType('vtype')
        self.db.add_all([self.volume_type, self.default_volume_type])
        self.db.commit()

    def test_create_default(self):
        n = models.Node()
        self.assert_(isinstance(dict(n), dict))
        self.db.add(n)
        self.db.commit()
        n = self.db.query(models.Node).first()
        self.assert_(isinstance(dict(n), dict))
        expected = {
            'size': 0,
            'port': 8081,
            'hostname': 'localhost',
            'storage_hostname': 'localhost',
            'storage_port': 3260,
            'volume_type_name': models.DEFAULT_VOLUME_TYPE,
            'affinity_group': '',
            'weight': 100,
        }
        for k, v in expected.items():
            err_msg = '%s != %s (%s)' % (v, getattr(n, k), k)
            self.assertEquals(v, getattr(n, k), err_msg)

    def test_default_empty_meta(self):
        n = models.Node('lunr', 12, volume_type=self.volume_type,
                        hostname='127.0.0.1', port=8080)
        self.assertEquals({}, n.meta)
        self.db.add(n)
        self.db.commit()
        self.assertEquals({}, n.meta)

    def test_add_meta_on_create(self):
        n = models.Node('lunr', 12, volume_type=self.volume_type,
                        hostname='127.0.0.1', port=8080, meta={'key1': 'val1'})
        expected = {'key1': 'val1'}
        self.assertEquals(expected, n.meta)
        self.db.add(n)
        self.db.commit()
        n = self.db.query(models.Node).filter_by(name='lunr').first()
        self.assertEquals(expected, n.meta)

    def test_set_meta_after_init(self):
        n = models.Node('lunr', 12, volume_type=self.volume_type,
                        hostname='127.0.0.1', port=8080)
        n.meta = {'key1': 'val1'}
        expected = {'key1': 'val1'}
        self.assertEquals(expected, n.meta)
        self.db.add(n)
        self.db.commit()
        n = self.db.query(models.Node).filter_by(name='lunr').first()
        self.assertEquals(expected, n.meta)

    def test_update_meta_after_commit(self):
        n = models.Node('lunr', 8, volume_type=self.volume_type,
                        hostname='test', port=8080)
        self.db.add(n)
        self.db.commit()
        n = self.db.query(models.Node).filter_by(name='lunr').first()
        # use the _meta property to update isntance meta
        n._meta.update(key1='val1')
        self.assertEquals({'key1': 'val1'}, n.meta)
        self.db.commit()
        n = self.db.query(models.Node).filter_by(name='lunr').first()
        self.assertEquals({'key1': 'val1'}, n.meta)

    def test_meta_is_immuatable_after_commit(self):
        n = models.Node('lunr', 12, volume_type=self.volume_type,
                        hostname='127.0.0.1', port=8080)
        # this works here because the instance is new
        n.meta.update(key1='val1')
        expected = {'key1': 'val1'}
        self.assertEquals(expected, n.meta)
        self.db.add(n)
        self.db.commit()
        n = self.db.query(models.Node).filter_by(name='lunr').first()
        self.assertEquals(expected, n.meta)
        # when you load an object from the database the meta attribute is
        # not directly mutable, you have to use _meta
        try:
            n.meta.update(key2='val2')
        except Exception:
            pass
        else:
            raise AssertionError('node meta should not support item '
                                 'assignment directly')
        # assignment still works
        n.meta = dict(n.meta)
        n.meta.update(key2='val2')
        self.db.commit()
        n = self.db.query(models.Node).filter_by(name='lunr').first()
        expected = {'key1': 'val1', 'key2': 'val2'}
        self.assertEquals(expected, n.meta)

    def test_calculate_storage_used(self):
        a = models.Account()
        n = models.Node('lunr', 12, volume_type=self.volume_type,
                        hostname='127.0.0.1', port=8080)
        v = models.Volume(account=a, size=1, volume_type=self.volume_type,
                          node=n)
        self.db.add_all([a, n, v])
        self.db.commit()
        n.calc_storage_used()
        self.assertEquals(1, n.storage_used)
        v.status = 'DELETED'
        self.db.add(v)
        n.calc_storage_used()
        self.assertEquals(0, n.storage_used)


class TestAccount(ModelTest):

    def test_placeholder(self):
        self.assert_(True)


class TestVolume(ModelTest):

    def test_default_id(self):
        a = models.Account()
        vtype = models.VolumeType('vtype')
        v = models.Volume(account=a, volume_type_name=vtype.name)
        self.db.add_all([a, vtype, v])
        self.db.commit()
        self.assert_(v.id)

    def test_default_name(self):
        a = models.Account()
        vtype = models.VolumeType('vtype')
        v = models.Volume(account=a, volume_type_name=vtype.name)
        self.db.add_all([a, vtype, v])
        self.db.commit()
        self.assert_(v.name)
        self.assertEquals(v.name, v.id)

    def test_name(self):
        a = models.Account()
        vtype = models.VolumeType('vtype')
        name = 'mylittlevolume'
        v = models.Volume(account=a, volume_type_name=vtype.name, name=name)
        self.db.add_all([a, vtype, v])
        self.db.commit()
        self.assert_(v.id)
        self.assertEquals(v.name, name)

    def test_foreign_keys(self):
        a = models.Account()
        v = models.Volume(account=a, volume_type_name='thisdoesntexist')
        self.db.add(v)
        self.assertRaises(IntegrityError, self.db.commit)

    def test_active_backup_count(self):
        vt = models.VolumeType('lunr')
        a = models.Account()
        n = models.Node('lunr', 10, volume_type=vt, hostname='127.0.0.1',
                        port=8080)
        v = models.Volume(account=a, size=1, volume_type=vt, node=n)
        b1 = models.Backup(volume=v, status='AVAILABLE')
        b2 = models.Backup(volume=v, status='AVAILABLE')
        b3 = models.Backup(volume=v, status='NOTAVAILABLE')
        b4 = models.Backup(volume=v, status='SOMETHING')
        b5 = models.Backup(volume=v, status='AVAILABLE')
        b6 = models.Backup(volume=v, status='AUDITING')
        b7 = models.Backup(volume=v, status='DELETED')
        self.db.add_all([a, n, v, b1, b2, b3, b4, b5, b6, b7])
        self.db.commit()
        self.db.refresh(v)
        self.assertEquals(5, v.active_backup_count())


class TestExport(ModelTest):

    def test_placeholder(self):
        self.assert_(True)


class TestBackup(ModelTest):

    def test_placeholder(self):
        self.assert_(True)


class TestVolumeType(ModelTest):

    def test_create(self):
        vt = models.VolumeType('foo')
        expected = {
            'name': 'foo',
            'status': 'ACTIVE',
            'min_size': 1,
            'max_size': 1024,
        }
        self.db.add(vt)
        self.db.commit()
        for k, v in expected.items():
            attr = getattr(vt, k)
            err_msg = '%s != %s (%s)' % (v, attr, k)
            self.assertEquals(v, attr, err_msg)

    def test_placeholder(self):
        self.assert_(True)


class TestExportType(ModelTest):

    def test_placeholder(self):
        self.assert_(True)

if __name__ == "__main__":
    unittest.main()
