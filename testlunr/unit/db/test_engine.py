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

import sqlalchemy
from sqlalchemy import pool
from shutil import rmtree
import os
from tempfile import mkdtemp
from testlunr.unit import patch
from lunr.common.config import LunrConfig
from lunr import db


class TestEngine(unittest.TestCase):

    def setUp(self):
        if db.Session:
            db.Session.remove()
        db._engine = None

    def test_create_in_memory_sqlite(self):
        db.configure(LunrConfig({'db': {'url': 'sqlite://'}}))
        self.assertEquals(str(db.Session.bind.url), 'sqlite://')
        self.assert_(isinstance(db.Session.bind.pool,
                                pool.SingletonThreadPool))

    def test_create_default_file_db(self):
        temp = mkdtemp()
        try:
            conf = LunrConfig(
                {'default': {'lunr_dir': temp}, 'db': {'auto_create': True}})
            db.configure(conf)
            self.assertEquals(str(db.Session.bind.url),
                              'sqlite:///' + temp + '/lunr.db')
            self.assert_(isinstance(db.Session.bind.pool, pool.NullPool))
        finally:
            rmtree(temp)

    def test_auto_create_false(self):
        class FakeLogger(object):
            def __init__(self):
                self.warned = False

            def warn(self, msg):
                self.warned = True
                self.msg = msg

        logger = FakeLogger()
        temp = mkdtemp()
        try:
            conf = LunrConfig({
                'default': {'lunr_dir': temp},
                'db': {'auto_create': False},
            })
            with patch(db, 'logger', logger):
                db.configure(conf)
            self.assert_(logger.warned)
            self.assert_('not version controlled' in logger.msg)
        finally:
            rmtree(temp)

    def test_non_default_poolclass(self):
        db.configure(LunrConfig({'db': {'auto_create': True,
                                        'url': 'sqlite://',
                                        'poolclass': 'StaticPool'}}))
        self.assert_(isinstance(db.Session.bind.pool, pool.StaticPool))

    def test_invalid_poolclass(self):
        self.assertRaises(ValueError, db.configure,
                          LunrConfig({'db': {'poolclass': 'LunrPool'}}))

    def test_echo_true(self):
        db.configure(LunrConfig({'db': {'auto_create': True,
                                        'url': 'sqlite://',
                                        'echo': 'true'}}))
        self.assertTrue(db.Session.bind.echo)

    def test_echo_false(self):
        db.configure(LunrConfig({'db': {'auto_create': True,
                                        'url': 'sqlite://',
                                        'echo': 'false'}}))
        self.assertFalse(db.Session.bind.echo)

    def test_unable_to_connect(self):
        tmp_dir = mkdtemp()
        os.rmdir(tmp_dir)
        self.assertFalse(os.path.exists(tmp_dir))
        url = 'sqlite:///' + tmp_dir + '/lunr.db'
        self.assertRaises(db.DBError, db.configure,
                          LunrConfig({'db': {'auto_create': True,
                                             'url': url}}))
        self.assertRaises(db.DBError, db.configure,
                          LunrConfig({'db': {'url': url}}))


if __name__ == "__main__":
    unittest.main()
