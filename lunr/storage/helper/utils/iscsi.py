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


import json

import logging
from lunr.common import logger
from lunr.storage.helper.utils import execute, ProcessError

import os
import time
from time import sleep
from uuid import uuid4

log = logger.get_logger()


class ISCSIDeviceNotFound(Exception):
    pass


class ISCSILoginFailed(Exception):
    pass


class ISCSILogoutFailed(Exception):
    pass


class ISCSICopyFailed(Exception):
    pass


class ISCSINotConnected(Exception):
    pass


class ISCSIDevice(object):

    def __init__(self, iqn, ip, port):
        self.iqn = iqn
        self.ip = ip
        self.port = port
        self.device = "/dev/disk/by-path/ip-%s:%s-iscsi-%s-lun-0" % \
                      (self.ip, self.port, self.iqn)

    @property
    def connected(self):
        for attempts in range(1, 4):
            if os.path.exists(self.device):
                return True
            sleep(attempts ** 2)
        return False

    def connect(self):
        """ login or open an iscsi connection """
        try:
            portal = "%s:%s" % (self.ip, self.port)
            execute('iscsiadm', mode='discovery', type='sendtargets',
                    portal=self.ip)
            execute('iscsiadm', mode='node', targetname=self.iqn,
                    portal=self.ip, login=None)
        except ProcessError, e:
            logger.exception("iscsi login failed with '%s'" % e)
            raise ISCSILoginFailed()
        if not self.connected:
            raise ISCSINotConnected("ISCSI device doesn't exist")

    def copy_file_out(self, path, callback=None):
        """ copy file to the iscsi device """
        try:
            self.copy_volume(path, self.device, callback=callback)
        except IOError, e:
            logger.exception("copy_file_out failed with '%s'" % e)
            raise ISCSICopyFailed()

    @staticmethod
    def copy_volume(src_volume, dest_volume, block_size=4194304,
                    callback=None):
        src, dest = None, None
        try:
            src = os.open(src_volume, os.O_RDONLY)
            dest = os.open(dest_volume, os.O_WRONLY)
            # Get the size of the source volume
            size = os.lseek(src, 0, os.SEEK_END)
            # Seek back to the beginning of the source device
            os.lseek(src, 0, os.SEEK_SET)

            for block in xrange(0, size, block_size):
                if block % 100 == 0:  # Every 1000 Blocks
                    percent = float(block) / size * 1000
                    logger.debug('cur_pos = %s (%d%%)' % (block, percent))
                    if callback:
                        callback(percent)
                os.write(dest, os.read(src, block_size))
        finally:
            if dest:
                try:
                    os.fsync(dest)
                except OSError:
                    # Unit tests do not use a device that supports fsync()
                    pass
                os.close(dest)
            if src:
                os.close(src)

    def disconnect(self):
        """ logout or close the iscsi connection """
        try:
            execute('iscsiadm', mode='node', targetname=self.iqn,
                    portal=self.ip, logout=None)
        except ProcessError, e:
            logger.exception("iscsi logout failed with '%s'" % e)
            raise ISCSILogoutFailed()
