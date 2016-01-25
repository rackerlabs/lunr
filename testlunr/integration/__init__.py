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

from lunr.storage.helper.utils import execute, directio
from lunr.common.config import LunrConfig
from subprocess import call, check_output
from os import path
import unittest
import os


class IetTest(unittest.TestCase):
    _ramdisk = None
    vgname = 'iet-test'

    @staticmethod
    def sudo(cmd):
        print "-- sudo " + cmd
        return call("sudo " + cmd + "> /dev/null", shell=True)

    @staticmethod
    def check_sudo(cmd):
        print "-- sudo " + cmd
        return check_output("sudo " + cmd, shell=True)

    @classmethod
    def setUpClass(cls):
        cls._ramdisk = cls.find_ram_disk()
        cls.wipe_device(cls._ramdisk)
        cls.sudo("pvcreate %s --metadatasize=1024k" % cls._ramdisk)
        cls.sudo("vgcreate %s %s" % (cls.vgname, cls._ramdisk))

    @classmethod
    def tearDownClass(cls):
        cls.sudo("vgremove -f %s" % cls.vgname)
        cls.sudo("pvremove -ff %s -y" % cls._ramdisk)

    def setUp(self):
        self._ramdisk = self.find_ram_disk()
        self.wipe_device(self._ramdisk)
        self.sudo("pvcreate %s --metadatasize=1024k" % self._ramdisk)
        self.vgname = 'iet-test-%s' % os.path.basename(self._ramdisk)
        self.sudo("vgcreate %s %s" % (self.vgname, self._ramdisk))

    def tearDown(self):
        # Remove the volume group
        self.sudo("vgremove -f %s" % self.vgname)
        # Remove the physical volume
        self.sudo("pvremove -ff %s -y" % self._ramdisk)

    @classmethod
    def wipe_device(cls, device):
        size = directio.size(device)
        # Create a 32k block
        block = '\0' * 32768
        with directio.open(device) as file:
            # Divide the size into 32k chunks
            for i in xrange(0, size / 32768):
                # Write a block of nulls
                file.write(block)

    @classmethod
    def find_ram_disk(cls, inUse=False):
        # Get a list of devices lvm is using
        stdout = cls.check_sudo("pvs --noheadings --options=pv_name")
        used = [disk.strip() for disk in stdout.splitlines()]
        if inUse:
            return used

        # Find a Ram disk that is not in use by lvm
        for index in xrange(0, 15):
            disk = "/dev/ram%d" % index
            if disk in used:
                continue
            # if the block device is less than 60MB, skip it
            if (directio.size(disk) / 1048576) < 60:
                continue
            return disk
        return None

    def config(self, dir):
        conf = {
            'default': {},
            'export': {
                'ietd_config': os.path.join(dir, 'ietd.conf'),
                'iqn_export': 'iqn.2012.com.ietests',
                'device_prefix': '/dev'
            },
            'storage': {
                'run_dir': os.path.join(dir, 'backups'),
                'skip_fork': True
            },
            'backup': {
                'client': 'disk',
            },
            'volume': {
                'volume_group': self.vgname,
                'device_prefix': '/dev',
            },
            'disk': {
                'path': os.path.join(dir, 'backups')
            }
        }
        return LunrConfig(conf)

    @classmethod
    def md5sum(cls, file):
        hasher = hashlib.md5()
        with directio.open(file) as f:
            while True:
                block = f.read(32768)
                if len(block) == 0:
                    break
                hasher.update(block)
        return hasher.hexdigest()
