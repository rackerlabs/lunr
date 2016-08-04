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


import urllib2
import json

from lunr.common.exc import HTTPClientError
from lunr.common import logger

RETRIES = 5


class CinderError(HTTPClientError):
    pass


def request(method, path, headers=None, data=None):
    """
    How many times must I write this function before I just add requests
    to our pip requires?
    """
    req = urllib2.Request(path, headers=headers, data=data)
    req.get_method = lambda *args: method
    try:
        resp = urllib2.urlopen(req)
        data = resp.read()
    except HTTPClientError.exceptions as e:
        raise CinderError(req, e)
    logger.debug(
        "%s on %s succeeded with %s" %
        (req.get_method(), req.get_full_url(), resp.getcode()))
    if data:
        return json.loads(data)


class CinderClient(object):

    def __init__(self, username, password, auth_url, cinder_url,
                 tenant_id=None, admin_tenant_id=None, rax_auth=True):
        self.username = username
        self.password = password
        self.auth_url = auth_url.rstrip('/')
        self.cinder_url = cinder_url.rstrip('/')
        self.tenant_id = tenant_id or admin_tenant_id
        self.admin_tenant_id = admin_tenant_id or tenant_id
        self._token = None
        self.rax_auth = rax_auth
        self.retries = RETRIES

    def request(self, method, path, headers=None, data=None):
        try:
            return request(method, path, headers=headers, data=data)
        except CinderError as e:
            if e.code == 401:
                if self.retries <= 1:
                    self.retries = RETRIES
                    raise
                self.retries -= 1
                self.token = None
                headers['X-Auth-Token'] = self.token
                return self.request(method, path, headers=headers, data=data)
            raise

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
        if self.tenant_id and not self.rax_auth:
            # in keystone request a token scoped to the tenant
            body['auth']['tenantId'] = self.tenant_id
        data = json.dumps(body)
        path = self.auth_url + '/v2.0/tokens'
        token_info = self.request('POST', path, headers=headers, data=data)
        self.token = token_info['access']['token']['id']
        try:
            self.tenant_id = token_info['access']['token']['tenant']['id']
        except KeyError:
            # an unscoped keystone token won't have a tenant_id
            pass

    @property
    def token(self):
        if not self._token:
            self.authenticate()
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    def update(self, resource_type, resource_id, status):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%(cinder_url)s/v1/%(tenant_id)s/' \
               '%(resource_type)s/%(resource_id)s/action' % {
                   'cinder_url': self.cinder_url,
                   'tenant_id': self.tenant_id,
                   'resource_type': resource_type,
                   'resource_id': resource_id,
               }
        body = {'os-reset_status': {'status': status}}
        data = json.dumps(body)
        return self.request('POST', path, headers, data)

    def force_delete(self, resource_type, resource_id):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%(cinder_url)s/v1/%(tenant_id)s/' \
            '%(resource_type)s/%(resource_id)s/action' % {
                'cinder_url': self.cinder_url,
                'tenant_id': self.tenant_id,
                'resource_type': resource_type,
                'resource_id': resource_id,
            }
        body = {'os-force_delete': {}}
        data = json.dumps(body)
        return self.request('POST', path, headers, data)

    def terminate_connection(self, volume_id, ip=None,
                             initiator=None, host=None):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        body = {
            'os-terminate_connection': {
                'connector': {
                    'ip': ip,
                    'initiator': initiator,
                    'host': host,
                }
            }
        }
        data = json.dumps(body)
        path = '%s/v1/%s/volumes/%s/action' % (self.cinder_url,
                                               self.tenant_id,
                                               volume_id)
        return self.request('POST', path, headers, data)

    def detach(self, volume_id, instance=None):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        body = {'os-detach': None}
        data = json.dumps(body)
        path = '%s/v1/%s/volumes/%s/action' % (self.cinder_url,
                                               self.tenant_id,
                                               volume_id)
        return self.request('POST', path, headers, data)

    def snapshot_progress(self, snapshot_id, progress):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%(cinder_url)s/v1/%(tenant_id)s/' \
            '%(resource_type)s/%(resource_id)s/action' % {
                'cinder_url': self.cinder_url,
                'tenant_id': self.tenant_id,
                'resource_type': 'snapshots',
                'resource_id': snapshot_id,
            }
        body = {'os-update_progress': progress}
        data = json.dumps(body)
        return self.request('POST', path, headers, data)

    def update_volume_metadata(self, volume_id, metadata):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/volumes/%s/metadata' % (self.cinder_url,
                                                 self.tenant_id, volume_id)
        body = {'metadata': metadata}
        return self.request('POST', path, headers, json.dumps(body))

    def delete_volume_metadata(self, volume_id, key):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/volumes/%s/metadata/%s' % (self.cinder_url,
                                                    self.tenant_id, volume_id,
                                                    key)
        return self.request('DELETE', path, headers, None)

    def delete_volume(self, volume_id):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/volumes/%s' % (self.cinder_url, self.tenant_id,
                                        volume_id)
        return self.request('DELETE', path, headers, None)

    def get_volume(self, volume_id):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/volumes/%s' % (self.cinder_url, self.tenant_id,
                                        volume_id)
        return self.request('GET', path, headers, None)

    def list_volumes(self):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/volumes/detail' % (self.cinder_url, self.tenant_id)
        return self.request('GET', path, headers, None)

    def list_snapshots(self):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/snapshots/detail' % (self.cinder_url, self.tenant_id)
        return self.request('GET', path, headers, None)

    def get_snapshot(self, snapshot_id):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/snapshots/%s' % (self.cinder_url, self.tenant_id, snapshot_id)
        return self.request('GET', path, headers, None)

    def delete_snapshot(self, snapshot_id):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/snapshots/%s' % (self.cinder_url, self.tenant_id, snapshot_id)
        return self.request('DELETE', path, headers, None)

    def quota_defaults(self):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        # GET http://lunr:8776/v1/account1/os-quota-sets/123/defaults
        path = '%s/v1/%s/os-quota-sets/defaults' % (self.cinder_url, self.tenant_id)
        return self.request('GET', path, headers, None)

    def quota_get(self):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/os-quota-sets/%s' % (self.cinder_url, self.admin_tenant_id, self.tenant_id)
        return self.request('GET', path, headers, None)

    def quota_update(self, **kwargs):
        headers = {'content-type': 'application/json',
                   'X-Auth-Token': self.token}
        path = '%s/v1/%s/os-quota-sets/%s' % (self.cinder_url, self.admin_tenant_id, self.tenant_id)
        return self.request('PUT', path, headers, None)


def get_args(conf):
    return {
        'username': conf.string('cinder', 'username', 'demo'),
        'password': conf.string('cinder', 'password', 'demo'),
        'auth_url': conf.string('cinder', 'auth_url',
                                'http://localhost:5000'),
        'cinder_url': conf.string('cinder', 'cinder_url',
                                  'http://localhost:8776'),
        'rax_auth': conf.bool('cinder', 'rax_auth', True),
    }


def get_conn(conf, **kwargs):
    cinder_args = get_args(conf)
    cinder_args.update(kwargs)
    return CinderClient(**cinder_args)
