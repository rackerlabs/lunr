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


import errno
import os

from lunr.common import exc

DEFAULT_CHUNK_READ_SIZE = 65536


def obj_name(container, name):
    return '/'.join((container, name))


def get_writer(file_name):
    def g():
        with open(file_name, 'w') as f:
            chunk = yield
            while chunk:
                f.write(chunk)
                chunk = yield
    writer = g()
    # prime the pump
    writer.next()
    return writer

ClientException = exc.ClientException


class Connection(object):

    ClientException = exc.ClientException

    def __init__(self, path):
        self.dir = os.path.abspath(path)
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)

    def path(self, container, name=None):
        args = [self.dir, container]
        if name:
            args.append(name)
        return os.path.join(*args)

    def put_object(self, container, name, body, **kwargs):
        if not os.path.exists(self.path(container)):
            raise self.ClientException('container does not exist %s' %
                                       container)
        file_name = self.path(container, name)
        try:
            # There could be slashes in name.
            # These dirs wont be removed by delete_object, sorry.
            os.makedirs(os.path.dirname(file_name))
        except OSError, e:
            # ignore File exists
            if e.errno != errno.EEXIST:
                raise
        try:
            writer = get_writer(file_name)
        except IOError, e:
            if e.errno == errno.ENOENT:
                # container does not exist
                raise self.ClientException('container does not exist %s' %
                                           container)
            else:
                raise

        if hasattr(body, 'read'):
            chunk = body.read(DEFAULT_CHUNK_READ_SIZE)
            while chunk:
                writer.send(chunk)
                chunk = body.read(DEFAULT_CHUNK_READ_SIZE)
        else:
            writer.send(body)

    def put_container(self, container, *args, **kwargs):
        try:
            os.mkdir(self.path(container))
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise self.ClientException(str(e), 500)

    def delete_container(self, container, *args, **kwargs):
        try:
            os.rmdir(self.path(container))
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise self.ClientException(str(e), 409)

    def get_object(self, container, name, resp_chunk_size=None, **kwargs):
        obj = self.path(container, name)
        try:
            f = open(obj, 'r')
        except IOError, e:
            if e.errno == errno.ENOENT:
                raise self.ClientException('object not found %s' %
                                           obj_name(container, name))
            raise
        if resp_chunk_size:
            # return generator
            def body():
                try:
                    buf = f.read(resp_chunk_size)
                    while buf:
                        yield buf
                        buf = f.read(resp_chunk_size)
                finally:
                    f.close()
            return {}, body()
        else:
            try:
                return {}, f.read()
            finally:
                f.close()

    def head_object(self, container, name, **kwargs):
        obj = self.path(container, name)
        if not os.path.exists(obj):
            raise self.ClientException('object not found %s' %
                                       obj_name(container, name))
        # TODO(clayg): return something?
        return {}

    def delete_object(self, container, name, **kwargs):
        obj = self.path(container, name)
        if not os.path.exists(obj):
            raise self.ClientException('object not found %s' %
                                       obj_name(container, name))
        os.remove(obj)

    def get_container(self, container, **kwargs):
        marker = kwargs.get('marker', None)
        try:
            listing = os.listdir(self.path(container))
        except OSError, e:
            raise self.ClientException(str(e))
        return {}, [{'name': obj} for obj in listing if obj > marker]

    def head_account(self):
        if not os.path.exists(self.dir):
            raise self.ClientException(
                "Backup storage path '%s' does not exist." % self.dir)
        stat = os.statvfs(self.dir)
        free_bytes = stat.f_bfree * stat.f_bsize
        status = {
            'path': self.dir,
            'free': free_bytes,
        }
        if status['free'] <= 0:
            raise self.ClientException("No free space on storage device")
        return status

    def head_container(self, container):
        object_count = 0
        bytes_used = 0
        for root, dirs, files in os.walk(self.path(container)):
            object_count += len(files)
            bytes_used += sum(os.path.getsize(os.path.join(root, name))
                              for name in files)
        return {
            'x-container-object-count': object_count,
            'x-container-bytes-used': bytes_used
        }


def connect(conf):
    path = conf.string('disk', 'path', conf.path('backups'))
    return Connection(path)
