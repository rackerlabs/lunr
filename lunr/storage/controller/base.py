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

from lunr.common.lock import ResourceFile
from webob.exc import HTTPConflict, HTTPAccepted
from os import path
import os
import signal


class BaseController(object):
    def __init__(self, route, app):
        self.route = route
        self.helper = app.helper
        self.app = app

        # Add a couple of shortcuts
        self.volume_id = route.get('volume_id', None)
        self.id = route.get('id', None)


def inspect(obj, request, lock_file_path):
    run_dir = obj.app.helper.volumes.run_dir

    result = {
        'volume_id': obj.volume_id,
        'id': obj.id,
        'uri': "%s %s" % (request.method, request.path),
        'pid': os.getpid(),
        'interruptible': False
    }
    # Expand any %(volume_id)s or %(id)s in the path
    fragment = lock_file_path % result
    # Combine the result with the run_directory and return
    result['lock_file'] = path.join(run_dir, fragment)
    return result


def claim(resource, info):
    with resource as lock:
        # If the resource is in use
        used = lock.used()
        if used:
            # if the operation using the resource matches ours
            if info['uri'] == used['uri']:
                raise HTTPAccepted("Operation accepted; already in-progress")
            # if the operation is interruptible, kill it and steal the lock
            if used.get('interruptible'):
                try:
                    os.kill(used['pid'], signal.SIGTERM)
                    lock.acquire(info)
                    return
                except OSError:
                    pass
            # else, some other operation started this request
            raise HTTPConflict(
                "Request conflicts with in-progress '%s'" % used['uri'])
        # else, claim ownership of the volume
        lock.acquire(info)


def lock(lock_file_path):

    def wrap(func):
        def new_func(self, request):
            # Get some info about the controller call
            info = inspect(self, request, lock_file_path)
            # create a ResourceFile object to manage volume ownership
            resource = ResourceFile(info['lock_file'])

            try:
                # Attempt to claim exclusive ownership of the volume
                claim(resource, info)
                # Execute the controller method
                return func(self, request, resource)
            finally:
                # Remove the resource file if we own it
                resource.remove()
        return new_func
    return wrap
