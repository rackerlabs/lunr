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


from StringIO import StringIO
from time import sleep

from lunr.common import exc


GLOBAL_CONNECTION = None


ClientException = exc.ClientException


class Connection(object):

    ClientException = exc.ClientException

    def __init__(self):
        self.data = {}

    def put_object(self, container, name, body, **kwargs):
        if container not in self.data:
            raise self.ClientException('container does not exist %s' %
                                       container)
        if hasattr(body, 'read'):
            contents = body.read()
        else:
            contents = body
        self.data[container][name] = contents

    def put_container(self, container, *args, **kwargs):
        self.data[container] = {}

    def get_object(self, container, name, resp_chunk_size=None, **kwargs):
        try:
            contents = self.data[container][name]
        except KeyError:
            raise self.ClientException(
                'object not found %r/%r' % (container, name))
        if resp_chunk_size:
            # return generator
            f = StringIO(contents)

            def body():
                buf = f.read(resp_chunk_size)
                while buf:
                    yield buf
                    buf = f.read(resp_chunk_size)
            return {}, body()
        else:
            return {}, contents

    def delete_object(self, container, name, **kwargs):
        try:
            del self.data[container][name]
        except KeyError:
            raise self.ClientException(
                'object not found %r/%r' % (container, name))

    def get_container(self, container, **kwargs):
        marker = kwargs.get('marker', None)
        if container not in self.data:
            raise self.ClientException(
                'container does not exist %s' % container)
        listing = []
        for obj in self.data[container].keys():
            if obj > marker:
                listing.append({'name': obj})
        return {}, listing

    def delete_container(self, container):
        if self.data[container]:
            raise self.ClientException('Container not empty', 409)
        try:
            del self.data[container]
        except KeyError:
            pass

    def head_account(self):
        return {
            'containers': len(self.data),
            'objects': sum([len(c.values()) for c in self.data.values()])
        }

    def head_container(self, container):
        if container not in self.data:
            raise self.ClientException('%r does not exist' % container)

        object_count = len(self.data[container].keys())
        bytes_used = sum(len(obj) for obj in self.data[container].values())

        return {
            'x-container-object-count': object_count,
            'x-container-bytes-used': bytes_used
        }


def connect(conf):
    global GLOBAL_CONNECTION
    if not GLOBAL_CONNECTION:
        GLOBAL_CONNECTION = Connection()
    return GLOBAL_CONNECTION


def reset():
    global GLOBAL_CONNECTION
    GLOBAL_CONNECTION = None
