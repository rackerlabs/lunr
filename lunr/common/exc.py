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

from lunr.common.jsonify import loads
from httplib import HTTPException, BadStatusLine
from webob import exc
import socket
import urllib2


class HTTPClientError(exc.HTTPError):
    """
    Base exception class for make_api_request() and node_request()
    """

    # Catch IOError to handle uncaught SSL Errors
    exceptions = (urllib2.URLError, HTTPException, urllib2.HTTPError, IOError)

    title = exc.HTTPServiceUnavailable.title
    code = exc.HTTPServiceUnavailable.code
    explanation = exc.HTTPServiceUnavailable.explanation

    def __init__(self, req, e):
        self.method = req.get_method()
        self.url = req.get_full_url()
        detail = "%s on %s " % (self.method, self.url)

        if type(e) is socket.timeout:
            detail += "failed with socket timeout"
            self.reason = detail

        if type(e) is urllib2.HTTPError:
            raw_body = ''.join(e.fp.read())
            self.reason = raw_body  # most basic reason
            try:
                body = loads(raw_body)
            except ValueError:
                pass
            else:
                # json body has more info
                if 'reason' in body:
                    self.reason = body['reason']
                elif 'message' in body:
                    self.reason = body['message']
            detail += "returned '%s' with '%s'" % (e.code, self.reason)
            self.title = e.msg
            self.code = e.code

        if type(e) is urllib2.URLError:
            detail += "failed with '%s'" % e.reason
            self.reason = e.reason

        if type(e) is IOError:
            detail += "failed with '%s'" % e
            self.reason = str(e)

        if isinstance(e, HTTPException):
            # work around urllib2 bug, it throws a
            # BadStatusLine without an explaination.
            if isinstance(e, BadStatusLine):
                detail += "failed with '%s'" % e.__class__.__name__
            else:
                detail += "failed with '%s'" % e
            self.reason = str(e)

        super(HTTPClientError, self).__init__(detail=detail)

    def __str__(self):
        return self.detail


class ClientException(Exception):
    def __init__(self, msg, code=404):
        self.http_status = code
        Exception.__init__(self, msg)


class NodeError(HTTPClientError):
    """
    Public exception Errors Contacting the Storage Node API
    """
    pass
