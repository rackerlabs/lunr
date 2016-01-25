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

import errno
import fcntl
import os
import json


class NullResource(object):
    """ Implments the lock interface for spawn. """
    def __init__(self, *args, **kwargs):
        self.owned = False

    def remove(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        pass

    def acquire(self, info):
        pass


class LockFile(object):
    """ Manages locking and unlocking an open file handle
    can also be used as a context manager
    """

    def __init__(self, fd, lock_operation=fcntl.LOCK_EX,
                 unlock_operation=fcntl.LOCK_UN):
        self.fd = fd
        self.file_name = None
        if type(fd) != int:
            self.fd = self.open(fd)
            self.file_name = fd
        self.lock_operation = lock_operation
        self.unlock_operation = unlock_operation

    def __enter__(self):
        self.lock(self.lock_operation)
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.unlock(self.unlock_operation)
        return False

    def lock(self, operation=fcntl.LOCK_EX):
        fcntl.flock(self.fd, operation)

    def unlock(self, operation=fcntl.LOCK_UN):
        fcntl.flock(self.fd, operation)

    def write(self, data):
        os.lseek(self.fd, 0, os.SEEK_SET)
        os.ftruncate(self.fd, 0)
        os.write(self.fd, data)
        os.fsync(self.fd)

    def read(self):
        size = os.lseek(self.fd, 0, os.SEEK_END)
        os.lseek(self.fd, 0, os.SEEK_SET)
        return os.read(self.fd, size)

    def close(self):
        try:
            os.close(self.fd)
        except TypeError, OSError:
            pass
        self.fd = None

    def unlink(self):
        self.close()
        try:
            os.unlink(self.file_name)
        except OSError, e:
            pass

    def _createdir(self, file_name):
        try:
            dir = os.path.dirname(file_name)
            os.makedirs(dir)
        except OSError, e:
            # ignore if already exists
            if e.errno != errno.EEXIST:
                raise

    def open(self, file_name):
        for i in range(0, 2):
            try:
                # Attempt to create the file
                return os.open(file_name, os.O_RDWR | os.O_CREAT)
            except OSError, e:
                # No such file or directory
                if e.errno == errno.ENOENT:
                    # create the dir and try again
                    self._createdir(file_name)
                    continue
                # Unknown error
                raise
        raise RuntimeError("failed to create '%s'" % file_name)


class JsonLockFile(LockFile):
    """ Manages a lock file that contains json """

    def update(self, info):
        data = self.read()
        data.update(info)
        self.write(data)

    def get(self, key, default=None):
        try:
            data = self.read()
            return data[key]
        except KeyError:
            return default

    def write(self, data):
        super(JsonLockFile, self).write(json.dumps(data))

    def read(self):
        try:
            return json.loads(super(JsonLockFile, self).read())
        except ValueError, e:
            return {}


class ResourceFile(JsonLockFile):
    """ Manages ownership of a resource file,
    can also be used as a context manager
    """

    def __init__(self, file_name):
        self.file_name = file_name
        self.owned = False
        self.fd = None

    def __enter__(self):
        self.fd = self.open(self.file_name)
        super(ResourceFile, self).lock()
        return self

    def __exit__(self, exc_type, exc_value, trace):
        super(ResourceFile, self).unlock()
        self.close()
        return False

    def used(self):
        """ Returns true if the resource file is in use by someone """
        info = self.read()
        # If pid is alive, the volume is owned by someone else
        if 'pid' in info and self.alive(info['pid']):
            return info
        return False

    def alive(self, pid):
        try:
            os.kill(pid, 0)
            return True
        except OSError, e:
            return False

    def acquire(self, info):
        """ Acquire ownership of the file by writing our pid information """
        self.update(info)
        if 'pid' in info:
            # We own the resource
            self.owned = True

    def remove(self):
        if self.owned:
            self.unlink()
