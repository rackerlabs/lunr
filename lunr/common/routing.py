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


from routes import Mapper
from webob.exc import HTTPMethodNotAllowed


def _allowed(*methods):
    return lambda env, _: env.get('REQUEST_METHOD', '') not in methods


class NotAllowed(object):

    def __init__(self, *args, **kwargs):
        pass

    def not_allowed(self, request):
        # TODO(thrawn01) Add the 'allow' header as stated in RFC2616
        # Section 14.7
        raise HTTPMethodNotAllowed("Method %s not allowed for path '%s'" %
                                   (request.method, request.path))


def lunr_connect(urlmap, path, controller, mapping):
    for k, v in mapping.iteritems():
        urlmap.connect(path, controller=controller, action=v,
                       conditions=dict(method=[k]))
    urlmap.connect(path, controller=NotAllowed, action='not_allowed',
                   conditions=dict(function=_allowed(*mapping.keys())))
