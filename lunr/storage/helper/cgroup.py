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
import os
import fcntl
import errno

from collections import defaultdict

from lunr.common import logger
from lunr.storage.helper.utils import ServiceUnavailable


class CgroupFs(object):

    def __init__(self, path):
        self.path = path

    def read(self, param):
        try:
            with open(os.path.join(self.path, param)) as f:
                for line in f.readlines():
                    yield line.split()
        except IOError, e:
            msg = "Cgroup read error: %s/%s: %s" % (self.path, param, e)
            logger.error(msg)

    def write(self, param, value):
        try:
            with open(os.path.join(self.path, param), 'w') as f:
                f.write(value)
                logger.info("Writing cgroup: %s: %s" % (param, value))
        except IOError, e:
            msg = "Cgroup write error: %s/%s: %s" % (self.path, param, e)
            logger.error(msg)


class CgroupHelper(object):
    cgroup_parameters = [
        'blkio.throttle.read_iops_device',
        'blkio.throttle.write_iops_device',
    ]

    def __init__(self, conf):
        sys_fs_path = conf.string('cgroup', 'cgroup_path',
                                  '/sys/fs/cgroup/blkio/sysdefault')
        self.cgroup_fs = CgroupFs(sys_fs_path)
        run_dir = conf.string('storage', 'run_dir', conf.path('run'))
        self.cgroups_path = os.path.join(run_dir, 'cgroups')

    def _updates_path(self):
        return os.path.join(self.cgroups_path, 'updates')

    def all_cgroups(self):
        data = defaultdict(dict)
        for name in self.cgroup_parameters:
            for device, throttle in self.cgroup_fs.read(name):
                data[name][device] = throttle
        return data

    def get(self, volume):
        device = volume['device_number']
        data = {}
        cgroups = self.all_cgroups()
        for name, device_map in cgroups.items():
            if device in device_map:
                data[name] = device_map[device]
        return data

    def set_read_iops(self, volume, throttle):
        self.set(volume, throttle, 'blkio.throttle.read_iops_device')

    def set_write_iops(self, volume, throttle):
        self.set(volume, throttle, 'blkio.throttle.write_iops_device')

    def set(self, volume, throttle, param=None):
        if not param:
            params = self.cgroup_parameters
        else:
            params = [param]
        throttle = int(throttle)
        if throttle < 0:
            raise ValueError("Throttle cannot be negative")
        device = volume['device_number']
        for name in params:
            value = "%s %s" % (device, throttle)
            self.cgroup_fs.write(name, value)
            self.save_update(volume, name, throttle)

    def save_update(self, volume, name, throttle):
        try:
            os.makedirs(self.cgroups_path)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        try:
            with open(self._updates_path(), 'a') as f:
                fcntl.lockf(f.fileno(), fcntl.LOCK_EX)
                entry = '%s %s %s\n' % (volume['id'], name, throttle)
                f.write(entry)
        except IOError, e:
            logger.error('Failed writing cgroup update: %s' % e)

    def _read_initial_cgroups(self):
        cgroups = defaultdict(dict)
        try:
            for line in open(self._updates_path(), 'r+'):
                try:
                    volume_id, name, throttle = line.split()
                    cgroups[name][volume_id] = throttle
                except ValueError:
                    pass
        except IOError, e:
            logger.info('Failed reading cgroup updates: %s' % e)
        return cgroups

    def load_initial_cgroups(self, volumes):
        cgroups = self._read_initial_cgroups()
        volume_map = {}
        for volume in volumes:
            volume_map[volume['id']] = volume['device_number']
        for cgroup_name, volume_throttle in cgroups.items():
            for volume_id, throttle in volume_throttle.items():
                if volume_id not in volume_map:
                    continue
                device = volume_map[volume_id]
                line = "%s %s" % (device, throttle)
                self.cgroup_fs.write(cgroup_name, line)
