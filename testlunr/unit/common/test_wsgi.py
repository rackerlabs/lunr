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

import logging
import os
from paste.script import serve
from routes.mapper import Mapper
from shutil import rmtree
import simplejson
from string import Template
from tempfile import mkdtemp
from textwrap import dedent
from webob import Request, Response
from webob.exc import HTTPServerError

from lunr.common import wsgi, logger
from lunr.common.config import LunrConfig


TEST_CONF = LunrConfig()


class HappyController(object):
    def __init__(self, route, app):
        self.route = route
        self.helper = app.helper
        self.app = app

    def happy_method(self, request):
        return Response('happy')

    def happy_response(self, request):
        return Response()


class SadController(object):
    def __init__(self, route, app):
        self.route = route
        self.helper = app.helper
        self.app = app

    def five_hundred(self, request):
        raise HTTPServerError

    def fail(self, request):
        raise Exception('fail')


class MyWsgiApp(wsgi.LunrWsgiApp):
    def __init__(self, conf, urlmap, helper=None):
        self.conf = conf
        self.urlmap = urlmap
        self.helper = helper

    def call(self, request):
        # Match the Request URL to an action
        action = self.match(request)
        return action(request)


urlmap = Mapper()
urlmap.connect('/happy', controller=HappyController, action="happy_method")
urlmap.connect('/happyresponse', controller=HappyController,
               action="happy_response")
urlmap.connect('/notimplemented', controller=HappyController,
               action="not_implemented")
urlmap.connect('/fivehundred', controller=SadController, action="five_hundred")
urlmap.connect('/fail', controller=SadController, action="fail")


class TestiLunrWsgiApp(unittest.TestCase):
    def setUp(self):
        self.app = MyWsgiApp(TEST_CONF, urlmap)

    def tearDown(self):
        pass

    def test_happypath(self):
        request = Request.blank('/happy')
        res = self.app(request)
        self.assertEquals(res.status_int // 1, 200)
        self.assertEquals(simplejson.loads(res.body), 'happy')

    def test_happyresponse(self):
        request = Request.blank('/happyresponse')
        res = self.app(request)
        self.assertEquals(res.status_int // 1, 200)

    def test_404(self):
        request = Request.blank('/notfound')
        res = self.app(request)
        self.assertEquals(res.status_int, 404)

    def test_501(self):
        request = Request.blank('/notimplemented')
        res = self.app(request)
        self.assertEquals(res.status_int, 501)

    def test_500(self):
        request = Request.blank('/fivehundred')
        res = self.app(request)
        self.assertEquals(res.status_int, 500)

    def test_fail(self):
        request = Request.blank('/fail')
        res = self.app(request)
        self.assertEquals(res.status_int, 500)


class BaseServeApp(unittest.TestCase):

    config_filename = 'lunr-server.conf'
    config_filebody = Template(dedent(
        """
        [DEFAULT]
        lunr_dir = ${lunr_dir}

        [app:main]
        use = ${use_app}

        [storage]
        run_dir = ${run_dir}
        """
    ))
    use_app = ""  # e.g. "egg:lunr#storage_server"

    @staticmethod
    def serve(app):
        pass

    def _mock_loadserver(self, *args, **kwargs):
        def wrapped(*args, **kwargs):
            rv = self.serve(*args, **kwargs)
            self.serve.called = True
            return rv

        return wrapped

    def setUp(self):
        self.orig_loadserver = serve.loadserver
        serve.loadserver = self._mock_loadserver
        self.orig_logger_configure = logger.configure
        # logger.configure = partial(logger.configure, capture_stdio=False)
        logger.configure = lambda *args, **kwargs: logging.basicConfig()
        self.serve.called = False
        self.scratch = mkdtemp()
        self.config_file = os.path.join(self.scratch, self.config_filename)
        config_options = {
            'lunr_dir': self.scratch,  # not needed?
            'run_dir': self.scratch,
            'use_app': self.use_app,
        }
        with open(self.config_file, 'w') as f:
            f.write(self.config_filebody.substitute(config_options))

    def tearDown(self):
        serve.loadserver = self.orig_loadserver
        logger.configure = self.orig_logger_configure
        self.serve = BaseServeApp.serve
        self.serve.called = False
        rmtree(self.scratch)

if __name__ == "__main__":
    unittest.main()
