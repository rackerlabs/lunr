#!/usr/bin/env python
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


"""
Command Line Interface to LunR Admin API
"""

from lunr.common.subcommand import SubCommand, SubCommandParser,\
    opt, noargs, SubCommandError, confirm, Displayable
from urllib2 import urlopen, Request, HTTPError, URLError
from lunr.db import models
from lunr.common.exc import HTTPClientError
from lunr.common.config import LunrConfig
from httplib import HTTPException
from lunr.common import logger
from urlparse import urlparse
from urllib import urlencode
from functools import wraps
from json import loads
import sys
import os


class Console(SubCommand):
    def __init__(self):
        # let the base class setup methods in our class
        SubCommand.__init__(self)
        # Add global arguments for this subcommand
        self.opt('-c', '--config', default=None,
                 help="config file (default: /etc/lunr/storage-server.conf)")
        self.opt('-v', '--verbose', action='count',
                 help="be verbose (-vv is more verbose)")
        self.opt('-u', '--url', default=None, help="lunr admin api url")

    def node_request(self, id, uri, **kwargs):
        warn_on_errors = kwargs.pop('warn_on_errors', True)
        node = self.request('/nodes/%s' % id)
        try:
            url = "http://%s:%s%s" % (node['hostname'], node['port'], uri)
            return (self.urlopen(url, **kwargs), node)
        except HTTPClientError, e:
            node['error'] = e
            if warn_on_errors:
                print "** %s" % e
            return (False, node)

    def request(self, uri, **kwargs):
        if not self.url:
            conf = self.load_conf(self.config)
            self.url = conf.string('storage', 'api_server',
                                   'http://localhost:8080')
        url = self.ip_to_url(self.url)
        return self.urlopen("%s/v1.0/admin%s" % (url, uri), **kwargs)

    def ip_to_url(self, url):
        """
        Allows the user to specify a host or host:port
        example: -u 192.168.5.1 or -u 192.168.5.1:8080
        """
        uri = urlparse(url)
        # If no scheme, assume it's an ip/hostname
        if not uri.scheme:
            return self.ip_to_url('http://%s' % url)
        # If no port, assume we want port 8080
        if not uri.port:
            return self.ip_to_url('%s:8080' % url)
        return url

    def urlopen(self, url, method='GET', params=None, headers=None):
        params = params or {}
        headers = headers or {}
        data = urlencode(params)

        if method in ('GET', 'HEAD', 'DELETE') and data:
            url += '?' + data

        req = Request(url, data, headers)
        req.get_method = lambda *args, **kwargs: method

        try:
            if self.verbose:
                print "-- %s on %s with %s "\
                    % (req.get_method(), req.get_full_url(), params)
            resp = urlopen(req)

            return loads(''.join(resp.readlines()))
        except (HTTPError, URLError, HTTPException), e:
            raise HTTPClientError(req, e)

    def unused(self, _dict):
        """ Remove unused parameters from the dict """
        result = {}
        for key, value in _dict.items():
            if value is not None:
                result[key] = value
        return result

    def only(self, _dict, allowed):
        """ Return a dict of only the allowed keys """
        result = {}
        for key, value in _dict.items():
            if key in allowed:
                result[key] = value
        return result

    def load_conf(self, file):
        try:
            return LunrConfig.from_conf(file or LunrConfig.lunr_storage_config)
        except IOError, e:
            if file or self.verbose:
                print 'Warning: %s' % e
            return LunrConfig()


class TypeConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'type'

    @noargs
    def list(self):
        """ List all volume types """
        resp = self.request('/volume_types')
        self.display(resp, ['name', 'status', 'min_size', 'max_size',
                     'read_iops', 'write_iops'])

    @opt('name', help="name of the volume type")
    def get(self, name=None):
        """ Display details of a volume type """
        resp = self.request('/volume_types/%s' % name)
        self.display(resp)

    @opt('--min-size', help="minimum volume size")
    @opt('--max-size', help="maximum volume size")
    @opt('-r', '--read-iops', help="read iops")
    @opt('-w', '--write-iops', help="write iops")
    @opt('name', help="name of the volume type")
    def create(self, args):
        """ Create a new volume type  """
        # Only these args are parameters
        params = self.only(args, models.VolumeType.get_mutable_column_names())
        # Remove any parameters that are None
        params = self.unused(params)
        resp = self.request('/volume_types', method='POST', params=params)
        self.display(resp)

    @opt('name', help="name of the volume type")
    def delete(self, name):
        """ Mark a volume type as deleted  """
        resp = self.request('/volume_types/%s' % name,
                            method='DELETE')
        self.display(resp)


class NodeConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'node'

    @noargs
    def list(self):
        """ List all nodes """
        resp = self.request('/nodes')
        self.display(resp, ['id', 'name', 'status', 'volume_type_name',
                     'hostname', 'storage_hostname'])

    @opt('id', help="id of the node")
    def get(self, id):
        """ Display details of a given node  """
        result = self.request('/nodes/%s' % id)
        self.display(result)

    @opt('-H', '--hostname', required=True, help='api hostname')
    @opt('-P', '--port', required=True, help='api port')
    @opt('-S', '--storage-hostname', required=True, help='storage hostname')
    @opt('-t', '--volume-type-name', required=True,
         help='type of storage (volume_type_name)')
    @opt('-s', '--size', required=True, help='size in GB')
    @opt('--status', default='PENDING',
         help="status of node (Default is 'PENDING'")
    @opt('name', help="name of the new node")
    def create(self, args):
        """ Create a new node """
        # Only these args are parameters
        params = self.only(args, models.Node.get_mutable_column_names())
        # Remove any parameters that are None
        params = self.unused(params)
        resp = self.request('/nodes', method='POST', params=params)
        self.display(resp)

    @opt('-H', '--hostname', help='api hostname')
    @opt('-P', '--port', help='api port')
    @opt('-S', '--storage-hostname', help='storage hostname')
    @opt('-t', '--volume-type-name',
         help='type of storage (volume_type_name)')
    @opt('-s', '--size', help='size in GB')
    @opt('--status', help='status of node')
    @opt('id', help="id of the node to update")
    def update(self, id, args):
        """ Update a node """
        # Only these args are parameters
        params = self.only(args, models.Node.get_mutable_column_names())
        # Remove any parameters that are None
        params = self.unused(params)
        # Post the update parameters
        resp = self.request('/nodes/%s' % id, method='POST', params=params)
        self.display(resp)

    @opt('-a', '--all', action='store_true', help='deploy all nodes')
    @opt('id', nargs='?', help="id of the node to deploy")
    def deploy(self, id=None, all=None):
        """ Mark a PENDING node(s) ACTIVE  """
        if not all and not id:
            return self._parser.print_help()

        if all:
            nodes = self.request('/nodes')
            nodes = [n for n in nodes if n['status'] == 'PENDING']
        else:
            node = self.request('/nodes/%s' % id)
            if node['status'] != 'PENDING':
                if not confirm("Node '%s' status is '%s' set to 'ACTIVE'"
                               % (node['id'], node['status'])):
                    return 1
            nodes = [node]

        results = []
        for node in nodes:
            resp = self.request('/nodes/%s' % node['id'], method='POST',
                                params={'status': 'ACTIVE'})
            results.append(resp)

        if results:
            self.display(results, ['id', 'status'])
            return 0
        print "No nodes in 'PENDING' status"

    @opt('id', help="id of the node to delete")
    def delete(self, id):
        """ Mark a given node as deleted """
        resp = self.request('/nodes/%s' % id, method='DELETE')
        self.display(resp)


class AccountConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'account'

    @noargs
    def list(self):
        """ List all accounts for everyone """
        resp = self.request('/accounts')
        self.display(resp, ['id', 'status'])

    @opt('account', help="account to get (id or name)")
    def get(self, account):
        """ List details for a specific account """
        self.display(self.request('/accounts/%s' % account))


class VolumeConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'volume'

    @opt('-s', '--status', help="Filter the list by status")
    @opt('-a', '--account-id', help="Filter the list by account_id")
    @opt('-n', '--node-id', help="Filter the list by node_id")
    @opt('-i', '--id', help="Filter the list by volume id")
    @opt('-r', '--restore-of', help="Filter the list by restore_of")
    def list(self, args):
        """ List all volumes for everyone """
        filters = self.remove(args, ['config', 'verbose', 'url'])
        resp = self.request('/volumes', params=self.unused(filters))
        self.display(resp, ['id', 'status', 'size', 'volume_type_name'])

    @opt('id', help="id of the volume to get")
    def get(self, id):
        """ List details for a specific volume """
        # Get volume info
        resp = self.request('/volumes/%s' % id)
        self.display(resp)
        # Get the node info for this volume
        print "\n-- Node %s --" % resp['node_id']
        resp = self.request('/nodes/%s' % resp['node_id'])
        self.display(resp)

    @opt('id', help="id of the volume to delete")
    def delete(self, id):
        """ Delete a specific volume """
        resp = self.request('/volumes/%s' % id, method='DELETE')
        self.display(resp)


class ExportConsole(Console, Displayable):
    _name = 'export'

    @opt('id', help="id of the volume to get export")
    def get(self, id):
        """ List export details for a specific volume """
        # Get export info
        resp = self.request('/volumes/%s/export' % id)
        self.display(resp)

    @opt('id', help="id of the volume to create export")
    def create(self, id):
        """Create an export for a specific volume"""
        self.request('/volumes/%s/export' % id, method='PUT')

    @opt('id', help="id of the volume to delete export")
    def delete(self, id):
        """Delete an export for a specific volume"""
        self.request('/volumes/%s/export' % id, method='DELETE')


class BackupConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'backup'

    @opt('-a', '--account-id', help='filter results by account id')
    @opt('-V', '--volume-id', help='filter results by volume id')
    def list(self, args):
        """ List all backups for everyone """
        # Only these args are parameters
        params = self.only(args, models.Backup.get_mutable_column_names())
        # Remove any parameters that are None
        params = self.unused(params)

        resp = self.request('/backups', params=params)
        self.display(resp, ['id', 'status', 'size', 'volume_id'])

    @opt('id', help="id of the backup to get")
    def get(self, id):
        """ get details for a specific backup """
        resp = self.request('/backups/%s' % id)
        self.display(resp)

    @opt('id', help="id of the backup to delete")
    def delete(self, id):
        """ Delete a specific backup """
        resp = self.request('/backups/%s' % id, method='DELETE')
        self.display(resp)


class ToolConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'tools'

    def _is_connected(self, payload):
        if not payload:
            return '(error)'
        sessions = payload.get('sessions', [])
        ips = []
        for session in sessions:
            ip = session.get('ip', False)
            if ip:
                ips.append(ip)
        if ips:
            return ', '.join(ips)
        return '(not connected)'

    def _iqn(self, payload):
        if not payload:
            return '(error)'
        return payload.get('name', '(not exported)')

    @opt('-d', '--deleted', action='store_true',
         help="include deleted volumes in the listing")
    @opt('account', help="account id")
    def account(self, account, deleted=None):
        """
        Display all available information about the account
        and the status of it's volumes
        """

        results = []
        volumes = self.request('/volumes', params={'account_id': account})
        # Get a list of all volumes for this account
        for volume in volumes:
            if volume['status'] == 'DELETED' and deleted is None:
                continue
            # Get the status of the volume from the storage node
            (payload, node) = self.node_request(volume['node_id'],
                                                '/volumes/%s' % volume['id'])
            results.append({'volume_id': volume['id'],
                            'volume': volume['name'],
                            'size': volume['size'],
                            'node': "http://%s:%s" % (node['hostname'],
                                                      node['port']),
                            'in-use': self._is_connected(payload),
                            'status': volume['status']})
        self.display(self.request('/accounts/%s' % account),
                     ['name', 'status', 'last_modified', 'created_at'])
        if results:
            return self.display(results, ['volume', 'status', 'size',
                                'node', 'in-use'])
        print "-- This account has no active volumes --"

    @opt('id', help="volume id")
    def volume(self, id):
        """ Display all available information about the volume """
        volume = self.request('/volumes/%s' % id)
        (payload, node) = self.node_request(volume['node_id'],
                                            '/volumes/%s/export' %
                                            volume['id'],
                                            warn_on_errors=False)
        if 'error' in node:
            if node['error'].code == 404:
                # no export, fill payload
                payload = {'connected': False}
            else:
                print '** %s' % node['error']
        volume['node-url'] = "http://%s:%s/volumes/%s" % (node['hostname'],
                                                          node['port'],
                                                          volume['id'])
        volume['in-use'] = self._is_connected(payload)
        volume['iqn'] = self._iqn(payload)
        self.display(volume, ['account_id', 'status', 'size', 'node_id',
                              'node-url', 'in-use', 'iqn', 'created_at',
                              'last_modified'])


def main(argv=sys.argv[1:]):
    logger.configure(log_to_console=True, level=logger.DEBUG,
                     lunr_log_level=logger.DEBUG, capture_stdio=False)

    # Create the top-level parser
    parser = SubCommandParser([
        TypeConsole(), NodeConsole(), AccountConsole(), VolumeConsole(),
        BackupConsole(), ExportConsole(), ToolConsole(),
    ], desc=__doc__.strip())
    # execute the command requested
    try:
        return parser.run(argv)
    except HTTPClientError, e:
        print str(e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
