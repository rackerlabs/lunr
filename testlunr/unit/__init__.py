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

from tempfile import NamedTemporaryFile
from testlunr.functional import Struct
from contextlib import contextmanager
from webob import Request
import unittest
import urllib
import json
import os
import logging
from StringIO import StringIO

from lunr.common.logger import LunrLoggerAdapter, local


@contextmanager
def temp_disk_file(body=''):
    path = None
    try:
        with NamedTemporaryFile('w', delete=False) as f:
            path = f.name
            f.write(body)
        yield path
    finally:
        if path:
            os.unlink(path)


@contextmanager
def patch(target, attr, new):
    """
    Run in context with patched attribute on target.

    :param target: real object to patch
    :param attr: name of attribute to patch, a string
    :param new: mock or stub to use in place
    """
    original = getattr(target, attr)
    setattr(target, attr, new)
    try:
        yield
    finally:
        setattr(target, attr, original)


class WsgiTestBase(unittest.TestCase):

    def request(self, uri, method='GET', params=None):
        encoded = urllib.urlencode(params or {})
        body = ''

        req = Request.blank(uri)

        if method in ('PUT', 'POST'):
            body = encoded
            req.content_type = 'application/x-www-form-urlencoded'
        else:
            uri = "%s?%s" % (uri, encoded)

        req.method = method
        req.body = body

        resp = self.app(req)
        return Struct(code=resp.status_int, body=json.loads(resp.body))


class MockLogger(object):

    def __init__(self):
        self.local = local
        self.log_file = StringIO()
        self.logger = None

    def get_logger(self, name):
        if not self.logger:
            logger = logging.getLogger(name)
            logger.setLevel(1)  # caputure everything
            handler = logging.StreamHandler(self.log_file)
            handler.setFormatter(
                logging.Formatter('%(name)s:%(levelname)s:%(message)s'))
            logger.addHandler(handler)
            self.logger = LunrLoggerAdapter(logger)
        return self.logger

    def pop_log_messages(self):
        rv = self.log_file.getvalue()
        self.log_file.seek(0)
        self.log_file.truncate()
        return rv

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            # prevent recursive lookup
            logger = object.__getattribute__(self, 'logger')
            if hasattr(logger, name):
                return getattr(logger, name)
            raise


class MockResourceLock(object):
    def acquire(self, info):
        pass

    def remove(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, trace):
        pass
