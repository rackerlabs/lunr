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

import os
import socket
import subprocess

from json import loads
from httplib import HTTPException
from StringIO import StringIO
from urllib import urlencode
from urllib2 import urlopen, Request, HTTPError, URLError
from uuid import uuid4

from lunr.common import logger
from lunr.common.exc import HTTPClientError, NodeError
from lunr.storage.helper.utils.client import get_conn


class StorageError(Exception):
    """
    Baseclass for Helper Errors
    """


class NotFound(StorageError):
    """
    Public exception for resource not found.
    """

    def __init__(self, *args, **kwargs):
        self._id = kwargs.pop('id', None)
        super(NotFound, self).__init__(*args, **kwargs)

    def __getattribute__(self, name):
        if name.endswith('_id'):
            name = '_id'
        return super(Exception, self).__getattribute__(name)


class AlreadyExists(StorageError):
    """
    Public exception for resource already exists.
    """


class InvalidImage(StorageError):
    """
    Public exception for image couldn't be found.
    """


class ResourceBusy(StorageError):
    """
    Public exception for resourse busy.
    """


class ServiceUnavailable(StorageError):
    """
    Public exception for service unavailable.
    """


class ProcessError(OSError, StorageError):
    """
    Base exception raised by execute, each command that lunr uses should
    try to define subclasses of this Exception that are more specific to
    their return codes and error messages.
    """

    def __init__(self, cmd, out, err, errcode):
        self.cmd = cmd
        self.out = out.rstrip()
        self.err = err.rstrip()
        self.errcode = errcode
        Exception.__init__(self, str(self))

    def __str__(self):
        return '\n$ %s\nSTDOUT:%s\nSTDERR:%s\n\nERRORCODE: %s' % (
            self.cmd, self.out, self.err, self.errcode)


class APIError(HTTPClientError):
    """
    Public exception Errors Contacting the Lunr API
    """


def format_value(value):
    if value is None:
        return ''
    else:
        if hasattr(value, 'items'):
            value = ','.join(['%s=%s' % (k, v) for k, v in value.items()])
        elif hasattr(value, '__iter__') and not isinstance(value, basestring):
            value = ','.join(value)
        return '=%s' % value


def execute(cmd, *args, **kwargs):
    sudo = kwargs.pop('sudo', True)
    if sudo:
        args = ['sudo', cmd] + list(args)
    else:
        args = [cmd] + list(args)

    for k, v in kwargs.items():
        if k.startswith('_'):
            k = k[1:]
        args.append('--%s%s' % (k, format_value(v)))

    logger.debug("execute: %s" % args)
    p = subprocess.Popen(args, close_fds=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    logger.debug('returned: %s' % p.returncode)
    if p.returncode:
        raise ProcessError(' '.join(args), out, err, p.returncode)
    return out.rstrip()


def lookup_id(id, api_server, cinder_host):
    params = {'name': id, 'cinder_host': cinder_host}
    resource = 'volumes?%s' % urlencode(params)
    resp = make_api_request(resource, api_server=api_server)
    volumes = loads(resp.read())
    found = None
    for volume in volumes:
        if volume['status'] !='DELETED':
            if found:
                raise HTTPError('unused', 409, 'Conflict',
                                {}, StringIO('{"reason": "conflict"}'))
            found = volume
    if not found:
        raise HTTPError('unused', 404, 'Not Found',
                        {}, StringIO('{"reason": "not found"}'))
    return found['id']


def make_api_request(resource, id=None, data=None, method=None,
                     api_server='http://localhost:8080/', cinder_host=None):
    admin_url = api_server.rstrip('/') + '/v1.0/admin/'
    if resource == 'volumes' and id:
        # Translate from lv_name to API volume id
        id = lookup_id(id, api_server, cinder_host)
    resource += '/%s' % id if id else ''
    if data is not None:
        data = urlencode(data)
    request = Request(admin_url + resource, data=data)
    if method:
        request.get_method = lambda *args, **kwargs: method.upper()
    try:
        return urlopen(request)
    except (HTTPError, URLError), e:
        raise APIError(request, e)


def node_request(node_ip, node_port, method, path, **kwargs):
    url = 'http://%s:%s%s' % (node_ip, node_port, path)
    req_id = getattr(logger.local, 'request_id', None)
    # This uuid() call can hang after a fork. libuuid reads from the wrong fd.
    # This is a workaround for when node_request is used in the storage clone
    # callback. We already have a request_id in that case.
    if not req_id:
        req_id = 'lunr-%s' % uuid4()
    headers = {'X-Request-Id': req_id}

    if method in ('GET', 'HEAD', 'DELETE'):
        url += '?' + urlencode(kwargs)

    req = Request(url, urlencode(kwargs), headers=headers)
    req.get_method = lambda *args, **kwargs: method
    try:
        # FIXME. Magic number.
        resp = urlopen(req, timeout=120)
        logger.debug(
            "%s on %s succeeded with %s" %
            (req.get_method(), req.get_full_url(), resp.getcode()))
        return loads(resp.read())
    except (socket.timeout, HTTPError, URLError, HTTPException), e:
        raise NodeError(req, e)
