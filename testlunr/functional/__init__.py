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


try:
    from nose import SkipTest
except ImportError:
    SkipTest = Exception

from lunr.common.jsonify import loads
from tempfile import mkdtemp
from shutil import rmtree
from time import sleep
import hashlib
import inspect
import unittest
import urllib2
import urllib
import os
import sys


class TemporaryDirectory(object):

    def __enter__(self):
        self.path = mkdtemp()
        return self.path

    def __exit__(self, *args):
        # rmtree(self.path)
        pass


class Struct(object):
    def __init__(self, **attrs):
        for key, value in attrs.items():
            setattr(self, key, value)


class LunrStorageService(object):

    def __init__(self):
        # In case we want to run the functional tests against a remote box
        self.host = os.environ.get('STORAGE_HOST', 'localhost')
        # FIXME: Change to port 0, when we get config fixed
        self.port = int(os.environ.get('STORAGE_PORT', '8081'))

    def stop(self):
        pass


class LunrApiService(object):

    def __init__(self):
        # In case we want to run the functional tests against a remote box
        self.host = os.environ.get('API_HOST', 'localhost')
        # FIXME: Change to port 0, when we get config fixed
        self.port = int(os.environ.get('API_PORT', '8080'))

    def stop(self):
        pass


class LunrTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def request(self, url, *args, **kwargs):
        """ Subclasses should override request() to make
            a convenience call for the subclass user

            def request(self, url, **kw):
                return self.urlopen("http://localhost:8080/%s" % uri, **kw)
        """
        return self.urlopen(url, *args, **kwargs)

    def get(self, url, **kwargs):
        return self.request(url, 'GET', **kwargs)

    def put(self, url, **kwargs):
        return self.request(url, 'PUT', **kwargs)

    def post(self, url, **kwargs):
        return self.request(url, 'POST', **kwargs)

    def delete(self, url, **kwargs):
        return self.request(url, 'DELETE', **kwargs)

    def urlopen(self, url, method='GET', params=None, headers=None):
        params = params or {}
        headers = headers or {}
        data = urllib.urlencode(params)

        if method in ('GET', 'HEAD', 'DELETE') and data:
            url += '?' + data

        req = urllib2.Request(url, data, headers)
        req.get_method = lambda *args, **kwargs: method

        attempts = 0
        while True:
            try:
                resp = urllib2.urlopen(req)
            except urllib2.HTTPError, e:
                resp = e
            except urllib2.URLError:
                attempts += 1
                if attempts > 3:
                    raise
                # exponential backoff
                sleep(2 ** attempts)
                continue
            break

        try:
            # read in the entire response
            body = loads(''.join(resp.readlines()))
            return Struct(code=resp.code, body=body, reason=resp.msg)
        except urllib2.URLError, e:
            sys.stderr.write('Caught URLError - %s (%s)' % (resp.code, e))

    def unlink(self, file):
        try:
            os.unlink(file)
        except OSError:
            pass

    def assertCode(self, resp, code, action=''):
        msg = "expected code '%s' got '%s' : %s"\
                % (code, resp.code, resp.body)
        if action:
            msg += " when trying to %s" % action
        self.assertEquals(resp.code, code, msg)

    def md5sum(self, path):
        page_size = 4096
        hasher = hashlib.md5()
        with open(path) as f:
            page = f.read(page_size)
            while page:
                hasher.update(page)
                page = f.read(page_size)
        return hasher.hexdigest()

    def shortDescription(self):
        # Get the file name of the test class, change from .pyc to .py
        file = inspect.getfile(self.__class__)[:-1]
        return "%s:%s.%s" % (file, self.__class__.__name__,
                             self._testMethodName)
