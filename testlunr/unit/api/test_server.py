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

from routes.mapper import Mapper
from webob import Request, Response
import simplejson

from lunr.common.config import LunrConfig
from lunr.api.server import ApiWsgiApp
from lunr.common.wsgi import LunrServeCommand
from lunr.db.models import Account
from lunr import db

from testlunr.unit.common.test_wsgi import BaseServeApp


TEST_CONF = LunrConfig({'db': {'auto_create': True, 'url': 'sqlite://'}})


class HappyController(object):
    def __init__(self, route, app):
        self.db = app.helper
        self.route = route
        self.app = app

    def happy_db(self, request):
        a = Account(id='happy account')
        self.db.add(a)
        self.db.commit()
        self.db.refresh(a)
        return Response(dict(a))

    def lazy_db(self, request):
        a = Account(id='lazy account')
        self.db.add(a)
        return Response('non commital')


class SadController(object):
    def __init__(self, route, app):
        self.db = app.helper
        self.route = route
        self.app = app

    def fail_db(self, request):
        a = Account(id='happy account')
        self.db.add(a)
        raise Exception('dbfail')

urlmap = Mapper()
urlmap.connect('/db/happy', controller=HappyController, action="happy_db")
urlmap.connect('/db/lazy', controller=HappyController, action="lazy_db")
urlmap.connect('/db/fail', controller=SadController, action="fail_db")


class TestServer(unittest.TestCase):
    def setUp(self):
        self.app = ApiWsgiApp(TEST_CONF, urlmap)

    def tearDown(self):
        db.Session.remove()

    def test_happydb(self):
        request = Request.blank('/db/happy')
        res = self.app(request)
        self.assertEquals(res.status_int // 1, 200)
        self.assertEquals(simplejson.loads(res.body)['id'], 'happy account')

    def test_lazydb(self):
        request = Request.blank('/db/lazy')
        res = self.app(request)
        self.assertEquals(res.status_int // 1, 200)
        # Check that the entry got recorded
        try:
            session = db.Session()
            q = session.query(Account)
            q.filter_by(id='lazy account')
            r = q.all()
            self.assertEquals(len(r), 1)
        finally:
            session.close()

    def test_faildb(self):
        request = Request.blank('/db/fail')
        res = self.app(request)
        self.assertEquals(res.status_int, 500)
        # Failure should have been rolled back, so test db still works
        request = Request.blank('/db/happy')
        res = self.app(request)
        self.assertEquals(res.status_int // 1, 200)


class TestServeApiApp(BaseServeApp):

    config_filename = 'api-server.conf'
    use_app = "egg:lunr#api_server"

    def test_serve_api(self):
        def serve(app):
            self.assert_(isinstance(app, ApiWsgiApp))
        self.serve = serve
        cmd = LunrServeCommand('api-server')
        cmd.run([self.config_file])
        self.assert_(serve.called)


if __name__ == "__main__":
    unittest.main()
