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

import unittest
import os
from tempfile import mkdtemp
from textwrap import dedent
from shutil import rmtree

from lunr.common.config import LunrConfig
from lunr.storage.helper.utils import ProcessError, ServiceUnavailable
from lunr.storage.helper import cgroup


class MockCgroupFs(object):

    def __init__(self, empty=False):
        if empty:
            self.data = {
                'blkio.throttle.write_iops_device': [],
                'blkio.throttle.read_iops_device': []
            }
        else:
            self.data = {
                'blkio.throttle.write_iops_device': [
                    ['1:0', '2'],
                    ['1:1', '1'],
                    ['2:0', '0'],
                ],
                'blkio.throttle.read_iops_device': [
                    ['1:0', '0'],
                    ['1:1', '1'],
                    ['2:0', '2'],
                ]
            }

    def read(self, param):
        return self.data[param]

    def write(self, param, value):
        device, throttle = value.split()
        entry = [device, throttle]
        for line in self.data[param]:
            if line[0] == device:
                self.data[param].remove(line)
                break
        self.data[param].append(entry)


class TestCgroupFs(unittest.TestCase):

    def setUp(self):
        self.scratch = mkdtemp()

    def tearDown(self):
        rmtree(self.scratch)

    def test_read(self):
        value = "something"
        with open(os.path.join(self.scratch, value), 'w') as f:
            f.write("foo 1\n")
            f.write("bar 2\n")
            f.write("baz 3\n")
        cgroup_fs = cgroup.CgroupFs(self.scratch)
        self.assertEquals(list(cgroup_fs.read(value)),
                          [['foo', '1'],
                           ['bar', '2'],
                           ['baz', '3']])

    def test_read_fail(self):
        cgroup_fs = cgroup.CgroupFs(self.scratch)
        self.assertEquals(list(cgroup_fs.read('nonexistent')), [])

    def test_write(self):
        value = "something"
        cgroup_fs = cgroup.CgroupFs(self.scratch)
        cgroup_fs.write(value, "foo")
        with open(os.path.join(self.scratch, value), 'r') as f:
            contents = f.read()
        self.assertEquals(contents, "foo")

    def test_write_fail(self):
        badscratch = os.path.join(self.scratch, 'badpath')
        cgroup_fs = cgroup.CgroupFs(badscratch)
        cgroup_fs.write('garbage', "foo")
        self.assertFalse(os.path.exists(badscratch))


class TestCgroupHelper(unittest.TestCase):

    def setUp(self):
        self.scratch = mkdtemp()
        self.cgroups_path = os.path.join(self.scratch, 'cgroups')
        self.conf = LunrConfig({
            'storage': {
                'run_dir': self.scratch
            }
        })
        self.helper = cgroup.CgroupHelper(self.conf)
        self.helper.cgroup_fs = MockCgroupFs()

    def tearDown(self):
        rmtree(self.scratch)

    def test_all_cgroups(self):
        data = self.helper.all_cgroups()
        writes = data['blkio.throttle.write_iops_device']
        self.assertEquals(writes['1:0'], '2')
        self.assertEquals(writes['1:1'], '1')
        self.assertEquals(writes['2:0'], '0')
        reads = data['blkio.throttle.read_iops_device']
        self.assertEquals(reads['1:0'], '0')
        self.assertEquals(reads['1:1'], '1')
        self.assertEquals(reads['2:0'], '2')

    def test_get(self):
        v1 = {'id': 'v1', 'device_number': '1:0'}
        data = self.helper.get(v1)
        self.assertEquals(data, {'blkio.throttle.write_iops_device': '2',
                                 'blkio.throttle.read_iops_device': '0'})

    def test_set(self):
        v1 = {'id': 'v1', 'device_number': '1:0'}
        v2 = {'id': 'v2', 'device_number': '1:1'}
        self.helper.set(v1, '10')
        self.helper.set(v2, '100', 'blkio.throttle.read_iops_device')
        data = self.helper.get(v1)
        self.assertEquals(data, {'blkio.throttle.write_iops_device': '10',
                                 'blkio.throttle.read_iops_device': '10'})
        updates_file = os.path.join(self.cgroups_path, "updates")
        with open(updates_file, 'r') as f:
            line = f.readline()
            self.assertEquals(line, "v1 blkio.throttle.read_iops_device 10\n")
            line = f.readline()
            self.assertEquals(line, "v1 blkio.throttle.write_iops_device 10\n")
            line = f.readline()
            self.assertEquals(line, "v2 blkio.throttle.read_iops_device 100\n")
            line = f.readline()
            self.assertEquals(line, "")

    def test_set_negative(self):
        self.assertRaises(ValueError, self.helper.set, '1:1', '-42')

    def test_set_nonint(self):
        self.assertRaises(ValueError, self.helper.set, '1:1', 'monkey')

    def test_set_zero(self):
        v1 = {'id': 'v1', 'device_number': '42:42'}
        self.helper.set(v1, '42')
        data = self.helper.get(v1)
        self.assertEquals(data, {'blkio.throttle.write_iops_device': '42',
                                 'blkio.throttle.read_iops_device': '42'})
        # 0 gets written to cgroupfs
        self.helper.set(v1, '0')
        data = self.helper.get(v1)
        self.assertEquals(data, {'blkio.throttle.write_iops_device': '0',
                                 'blkio.throttle.read_iops_device': '0'})

    def test_load_initial_cgroups(self):
        self.helper.cgroup_fs = MockCgroupFs(True)
        volumes = [
                {'id': 'v1', 'device_number': '1:1'},
                {'id': 'v2', 'device_number': '1:2'},
                {'id': 'v3', 'device_number': '1:3'},
                {'id': 'v4', 'device_number': '1:4'},
        ]
        os.mkdir(self.cgroups_path)
        updates_file = os.path.join(self.cgroups_path, "updates")
        with open(updates_file, 'w') as f:
            f.write('v1 blkio.throttle.read_iops_device huh\n')
            f.write('v1 blkio.throttle.read_iops_device 72\n')
            f.write('v1 blkio.throttle.read_iops_device 100\n')
            f.write('v2 blkio.throttle.read_iops_device 200\n')
            f.write('v3 blkio.throttle.read_iops_device 300\n')
            f.write('v1 blkio.throttle.write_iops_device 101\n')
            f.write('v2 blkio.throttle.write_iops_device 202\n')
            f.write('v3 blkio.throttle.write_iops_device 303\n')
            f.write('v5 blkio.throttle.write_iops_device boom\n')

        self.helper.load_initial_cgroups(volumes)

        data = self.helper.all_cgroups()
        reads = data['blkio.throttle.read_iops_device']
        self.assertEquals(reads['1:1'], '100')
        self.assertEquals(reads['1:2'], '200')
        self.assertEquals(reads['1:3'], '300')
        writes = data['blkio.throttle.write_iops_device']
        self.assertEquals(writes['1:1'], '101')
        self.assertEquals(writes['1:2'], '202')
        self.assertEquals(writes['1:3'], '303')

    def test_load_initial_cgroups_missing(self):
        self.helper.cgroup_fs = MockCgroupFs(True)
        self.helper.load_initial_cgroups([])

        data = self.helper.all_cgroups()
        self.assertEquals(data, {})


if __name__ == "__main__":
    unittest.main()
