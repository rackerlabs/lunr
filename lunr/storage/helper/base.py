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

import hashlib
import socket
from time import sleep
from urllib2 import URLError, HTTPError
from urlparse import urlparse

from lunr.cinder import cinderclient
from lunr.common import logger
from lunr.common.jsonify import loads
from lunr.storage.helper.volume import VolumeHelper
from lunr.storage.helper.export import ExportHelper
from lunr.storage.helper.backup import BackupHelper
from lunr.storage.helper.cgroup import CgroupHelper
from lunr.storage.helper.utils import make_api_request, node_request, \
    ServiceUnavailable, APIError


def bytes_to_gibibytes(b):
    return int(float(b) / 2 ** 30)


def my_ip(admin_url):
    netloc = urlparse(admin_url).netloc

    try:
        host, port = netloc.split(':')
    except ValueError:
        host, port = netloc, 8080

    port = int(port)

    try:
        sock = socket.socket(socket.AF_INET)
        sock.connect((host, port))
        my_addr, port = sock.getsockname()
        sock.close()
    except socket.error:
        my_addr = '127.0.0.1'
    return my_addr


def get_registration_exceptions(local_info, node_info):
    exceptions = {}
    for k, v in local_info.items():
        if 'hostname' in k and node_info[k] != v:
            try:
                node_value = socket.gethostbyname(node_info[k])
            except socket.error:
                # skip hostname translation on failure
                pass
        else:
            try:
                node_value = node_info[k]
            except KeyError, e:
                logger.error("During registration; missing '%s' key in api "
                             "server response" % k)
                continue
        if node_value != v:
            logger.warning("Invalid '%s' registered "
                           "as %r != %r" % (k, node_value, v))
            exceptions[k] = v
        else:
            logger.info("Verified '%s' registered as '%s'" % (k, v))
    return exceptions


class NotRegistered(ServiceUnavailable):
    pass


class Helper(object):

    def __init__(self, conf):
        self.volumes = VolumeHelper(conf)
        self.exports = ExportHelper(conf)
        self.backups = BackupHelper(conf)
        self.cgroups = CgroupHelper(conf)
        self.api_server = conf.string('storage', 'api_server',
                                      "http://localhost:8080")
        self.api_retry = conf.int('storage', 'api_retry', 1)

        # name of node registration
        self.name = conf.string('storage', 'name', socket.gethostname())
        self.affinity_group = conf.string('storage', 'affinity_group', '')
        self.maintenance_zone = conf.string('storage', 'maintenance_zone', '')

        # management interface
        self.management_host = conf.string('server:main', 'host', '0.0.0.0')
        if self.management_host == '0.0.0.0':
            self.management_host = my_ip(self.api_server)
        self.management_port = conf.int('server:main', 'port', 8081)

        # storage interface
        self.storage_host = conf.string('storage', 'host', '127.0.0.1')
        self.storage_port = conf.int('storage', 'port', 3260)
        self.volume_type = conf.string('storage', 'volume_type', 'vtype')

        # cinder
        self.cinder_args = cinderclient.get_args(conf)
        self.rax_auth = conf.bool('cinder', 'rax_auth', True)
        if self.rax_auth:
            self.client = cinderclient.CinderClient(**self.cinder_args)
        self.cinder_host = conf.string('storage', 'cinder_host',
                                       self.management_host)

    def make_api_request(self, *args, **kwargs):
        kwargs['api_server'] = kwargs.pop('api_server', self.api_server)
        kwargs['cinder_host'] = kwargs.pop('cinder_host', self.cinder_host)
        attempt = 0
        while True:
            attempt += 1
            try:
                return make_api_request(*args, **kwargs)
            except APIError, e:
                # don't retry client errors
                if e.code // 100 == 4:
                    raise
            if attempt > self.api_retry:
                raise
            # logger.debug('retrying api request', exc_info=True)
            sleep(2 ** attempt)

    def node_request(self, *args, **kwargs):
        return node_request(*args, **kwargs)

    def api_status(self):
        resp = self.make_api_request('nodes?name=%s' % self.name)
        data = loads(resp.read())
        if not data:
            raise NotRegistered(
                "Unable to find node entry for name '%s'" % self.name)
        elif len(data) > 1:
            raise ServiceUnavailable(
                "Duplicate node entry for name '%s'" % self.name)
        else:
            node_resp = self.make_api_request('nodes', data[0]['id'])
            node = loads(node_resp.read())
        return node

    def _local_info(self):
        return {
            'name': self.name,
            'hostname': self.management_host,
            'port': self.management_port,
            'storage_hostname': self.storage_host,
            'storage_port': self.storage_port,
            'volume_type_name': self.volume_type,
            'size': bytes_to_gibibytes(self.volumes.status()['vg_size']),
            'cinder_host': self.cinder_host,
            'affinity_group': self.affinity_group,
            'maintenance_zone': self.maintenance_zone,
        }

    def _register(self, node_id=None, data=None):
        if node_id:
            path = 'nodes/%s' % node_id
        else:
            path = 'nodes'
        if not data:
            data = self._local_info()
            data['status'] = 'PENDING'
        resp = self.make_api_request(path, method='POST',
                                     data=data)
        return loads(resp.read())

    def check_registration(self):
        try:
            node_info = self.api_status()
        except NotRegistered:
            logger.info("Registering new node '%s'" % self.name)
            self._register()
            return
        # check api node_info against local stats
        local_info = self._local_info()
        data = get_registration_exceptions(local_info, node_info)
        if not data:
            logger.info("Verfied registration, "
                        "node status is '%s'" % node_info['status'])
            return
        if node_info['status'] == 'ACTIVE':
            data['status'] = 'PENDING'
        logger.info("Node status is '%s', updating registration: %r" % (
            node_info['status'], data))
        # racy if node just came out of maintenance?
        self._register(node_info['id'], data=data)

    def get_cinder(self, account=None):
        if self.rax_auth:
            return self.client
        else:
            return cinderclient.CinderClient(tenant_id=account,
                                             **self.cinder_args)
