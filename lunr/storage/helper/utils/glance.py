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


import json
from random import shuffle
import urllib2, ssl

import glanceclient
import glanceclient.exc as glance_exc

from lunr.common.exc import HTTPClientError
from lunr.common import logger


class GlanceError(Exception):
    pass


def request(method, path, headers=None, data=None):
    """
    How many times must I write this function before I just add requests
    to our pip requires?
    """
    req = urllib2.Request(path, headers=headers, data=data)
    req.get_method = lambda *args: method
    resp = urllib2.urlopen(req, context=ssl._create_unverified_context())
    data = resp.read()
    logger.debug(
        "%s on %s succeeded with %s" %
        (req.get_method(), req.get_full_url(), resp.getcode()))
    if data:
        return json.loads(data)


class GlanceClient(object):

    def __init__(self, conf, **kwargs):
        self.username = kwargs.get('username',
                                   conf.string('glance', 'username', 'demo'))
        self.password = kwargs.get('password',
                                   conf.string('glance', 'password', 'demo'))
        self.auth_url = kwargs.get('auth_url',
                                   conf.string('glance', 'auth_url',
                                               'http://localhost:5000'))
        glance_urls = conf.list('glance', 'glance_urls',
                                'http://localhost:9292')
        self.glance_urls = kwargs.get('glance_urls') or glance_urls
        shuffle(self.glance_urls)
        self._glance_url_index = -1
        self.insecure = kwargs.get('insecure',
                                   conf.bool('glance', 'insecure', False))
        self.version = kwargs.get('version',
                                  conf.int('glance', 'version', 1))
        self.timeout = kwargs.get('timeout',
                                  conf.int('glance', 'timeout', 60))
        self.auth_strategy = kwargs.get('auth_strategy',
                                        conf.string('glance',
                                                    'auth_strategy', 'noauth'))
        self.tenant_id = kwargs.get('tenant_id')
        self._token = None
        self._init_client()

    @property
    def glance_url(self):
        return self.glance_urls[self._glance_url_index]

    def next_glance_url(self):
        self._glance_url_index += 1
        try:
            return self.glance_urls[self._glance_url_index]
        except IndexError:
            raise GlanceError('No more glance urls to try!')

    def _init_client(self):
        try:
            self.client = glanceclient.Client(self.version,
                                              self.next_glance_url(),
                                              insecure=self.insecure,
                                              token=self.token,
                                              timeout=self.timeout)
        except (glance_exc.BaseException, glance_exc.HTTPException) as e:
            raise GlanceError(
                "Error initializing client, host: %s, error: %s" %
                (self.glance_url, str(e)))

    def authenticate(self):
        path = self.auth_url + '/v2.0/tokens'
        headers = {'content-type': 'application/json',
                   'accept': 'application/json'}
        body = {
            'auth': {
                'passwordCredentials': {
                    'username': self.username,
                    'password': self.password,
                }
            }
        }
        if self.tenant_id and not self.auth_strategy == 'rax':
            # in keystone request a token scoped to the tenant
            body['auth']['tenantId'] = self.tenant_id
        data = json.dumps(body)
        path = self.auth_url + '/v2.0/tokens'
        try:
            token_info = request('POST', path, headers=headers, data=data)
        except HTTPClientError.exceptions, e:
            # One retry for auth.
            if e.code != 401:
                raise GlanceError(str(e))
            try:
                token_info = request('POST', path, headers=headers,
                                     data=data)
            except HTTPClientError.exceptions, e:
                raise GlanceError(str(e))

        self.token = token_info['access']['token']['id']
        try:
            self.tenant_id = token_info['access']['token']['tenant']['id']
        except KeyError:
            # an unscoped keystone token won't have a tenant_id
            pass

    @property
    def token(self):
        if self.auth_strategy == 'noauth':
            return ''
        if not self._token:
            self.authenticate()
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    def get(self, image_id):
        try:
            return self.client.images.data(image_id)
        except (glance_exc.BaseException, glance_exc.HTTPException) as e:
            logger.warning(
                "Exception in glance.get, host: %s, id: %s, error: %s" %
                (self.glance_url, image_id, e))
            self._init_client()
            return self.get(image_id)

    def head(self, image_id):
        try:
            return self.client.images.get(image_id)
        except (glance_exc.BaseException, glance_exc.HTTPException) as e:
            logger.warning(
                "Exception in glance.head, host: %s, id: %s, error: %s" %
                (self.glance_url, image_id, e))
            self._init_client()
            return self.head(image_id)


def get_conn(conf, **kwargs):
    return GlanceClient(conf, **kwargs)
