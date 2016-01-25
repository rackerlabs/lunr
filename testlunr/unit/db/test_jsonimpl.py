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


from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import unittest

from lunr.db import jsonimpl


class TestFrozenDict(unittest.TestCase):

    def test_acts_like_dict(self):
        d = jsonimpl.FrozenDict(key1='val1', key2='val2')
        expected = {'key1': 'val1', 'key2': 'val2'}
        self.assertEquals(expected, d)
        self.assert_('key1' in d)
        self.assertEquals(['key1', 'key2'], sorted(d.keys()))
        self.assertEquals('val1', d['key1'])
        self.assertEquals('val1', d.get('key1'))
        self.assertEquals(None, d.get('missing_key'))
        self.assertEquals(sorted(expected.items()), sorted(d.items()))
        self.assertEquals(repr({}), repr(jsonimpl.FrozenDict()))

    def test_acts_not_like_dict(self):
        d = jsonimpl.FrozenDict(key1='val1', key2='val2')
        expected = {'key1': 'val1', 'key2': 'val2'}
        try:
            d['key3'] = 'val3'
        except TypeError:
            pass
        else:
            raise AssertionError('FrozenDict should raise TypeError for '
                                 'item assigment')
        try:
            del d['key1']
        except TypeError:
            pass
        else:
            raise AssertionError('FrozenDict should raise TypeError for '
                                 'item deletion')
        self.assertFalse(hasattr(d, 'update'))

engine = create_engine('sqlite://')
Base = declarative_base(bind=engine)


class User(Base):
    __tablename__ = 'user'

    name = Column(String, primary_key=True)
    meta = Column(jsonimpl.JsonEncodedDict())

Session = sessionmaker()


class TestJsonImpl(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        Base.metadata.bind = engine
        Base.metadata.create_all()
        self.db = Session(bind=engine)

    def test_create(self):
        u = User(name='john', meta={'last_id': 'doe'})
        self.assertEquals({'last_id': 'doe'}, u.meta)
        self.db.add(u)
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        self.assertEquals({'last_id': 'doe'}, u.meta)

    def test_update(self):
        u = User(name='john')
        u.meta = {'last_id': 'doe'}
        self.assertEquals({'last_id': 'doe'}, u.meta)
        self.db.add(u)
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        self.assertEquals({'last_id': 'doe'}, u.meta)
        # assigning attribute to a new object will mark it dirty in the session
        u.meta = dict(u.meta)
        u.meta.update(age=30)
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        self.assertEquals({'last_id': 'doe', 'age': 30}, u.meta)

    def test_delete(self):
        u = User(name='john', meta={'last_id': 'doe', 'age': 30})
        self.assertEquals({'last_id': 'doe', 'age': 30}, u.meta)
        self.db.add(u)
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        self.assertEquals({'last_id': 'doe', 'age': 30}, u.meta)
        # assigning attribute to a new object will mark it dirty in the session
        u.meta = dict(u.meta)
        del u.meta['age']
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        self.assertEquals({'last_id': 'doe'}, u.meta)

    def test_update_meta_fails(self):
        # first establish that updates wouldn't work if we tried
        u = User(name='john', meta={'last_id': 'doe'})
        self.assertEquals({'last_id': 'doe'}, u.meta)
        self.db.add(u)
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        self.assertEquals({'last_id': 'doe'}, u.meta)
        # save a copy of meta for later
        orig_meta = dict(u.meta)
        # update the underlying dict since we can't change meta directly
        u.meta._d.update(age=30)
        # u.meta reflects change
        self.assertEquals({'last_id': 'doe', 'age': 30}, u.meta)
        # but the attribute didn't get flagged for update
        self.assertFalse(u in self.db.dirty)
        self.db.commit()
        u = self.db.query(User).filter_by(name='john').first()
        # no update, meta is still equal to the orignal meta
        self.assertEquals(orig_meta, u.meta)

    def test_update_meta_not_supported(self):
        # to avoid this, the meta object should not support direct updates
        u = User(name='john', meta={'last_id': 'doe'})
        self.assertEquals({'last_id': 'doe'}, u.meta)
        self.db.add(u)
        self.db.commit()
        try:
            u.meta['age'] = 30
        except Exception:
            pass
        else:
            raise AssertionError('meta object should not support '
                                 'item assignment')

    def test_none_is_encoded_as_empty_dict(self):
        u = User(name='john')
        # meta is not set
        self.assertEquals(None, u.meta)
        self.db.add(u)
        self.db.commit()
        result = self.db.execute('select meta from user where name=:name',
                                 {'name': 'john'})
        row = [x for x in result][0]
        # but it gets json encoded as an empty dict before going to the db
        self.assertEquals(row[0], '{}')

    def test_null_is_decoded_as_empty_dict(self):
        # if we manually insert a null value
        self.db.execute('insert into user (name) values (:name)',
                        {'name': 'bob'})
        result = self.db.execute('select meta from user where name=:name',
                                 {'name': 'bob'})
        row = [x for x in result][0]
        self.assertEquals(row[0], None)
        # the type engine will still hydrate it as an empty dict
        u = self.db.query(User).filter_by(name='bob').first()
        self.assertEquals({}, u.meta)

if __name__ == "__main__":
    unittest.main()
