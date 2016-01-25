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

from lunr.storage.helper.utils import directio, ProcessError, execute
from timeit import default_timer as Timer
from struct import unpack_from
from lunr.common import logger
from tempfile import mkdtemp
from shutil import rmtree
from mmap import mmap

import time
import os
import re

log = logger.get_logger()


class ScrubError(RuntimeError):
    pass


class Scrub(object):

    def __init__(self, conf):
        self._display_only = conf.bool('scrub', 'display-only', False)
        self._display_exceptions = conf.bool('scrub',
                                             'display-exceptions', False)
        # Throttle speed is in MB/s
        self._throttle_speed = conf.float('scrub', 'throttle_speed', 0)
        self.scrub_buf = ''

    def run(self, cmd, *args, **kwargs):
        for attempts in range(0, 3):
            try:
                return execute(cmd, *args, **kwargs)
            except ProcessError, e:
                log.error("Command '%s' returned non-zero exit status" % e.cmd)
                log.error("stdout: %s" % e.out)
                log.error("stderr: %s" % e.err)
        raise ScrubError("Aborted after 3 attempts to execute '%s'" % cmd)

    def write(self, fd, offset, buf):
        # Seek to the offset
        if fd.seek(offset, os.SEEK_SET) == -1:
            raise ScrubError("Unable to seek to offset '%d'" % offset)
        try:
            return fd.write(buf)
        except (OSError, IOError), e:
            raise ScrubError("Write on '%s' offset '%d' failed with '%s'"
                             % (fd.raw.path, offset, e))

    def read(self, fd, offset, length):
        # Seek to the offset
        if fd.seek(offset, os.SEEK_SET) == -1:
            raise ScrubError("Unable to seek to offset '%d'" % offset)
        try:
            return fd.read(length)
        except (OSError, IOError), e:
            raise ScrubError("Read on '%s' failed: %s" % (fd.raw.path, e))

    def read_exception_metadata(self, fd, chunk_size, index):
        # exception = { uint64 old_chunk, uint64 new_chunkc }
        # if the size of each exception metadata is 16 bytes,
        # exceptions_per_chunk is how many exceptions can fit in one chunk
        exceptions_per_chunk = chunk_size / 16
        # Offset where the exception metadata store begins
        # 1 + for the header chunk, then + 1 to take into
        # account the exception metadata chunk
        store_offset = 1 + ((exceptions_per_chunk + 1) * index)
        # seek to the begining of the exception metadata store
        # and read the entire store
        store = self.read(fd, chunk_size * store_offset, chunk_size)
        if not self._display_only:
            log.debug("Scrubbing metadata at %d" % (chunk_size * store_offset))
            self.write(fd, chunk_size * store_offset, self.scrub_buf)
        exception = 0
        while exception < exceptions_per_chunk:
            # Unpack 1 exception metadata from the store
            (old_chunk, new_chunk) = unpack_from('<QQ', store, exception * 16)
            # Yields the offset where the exception exists in the cow
            yield new_chunk * chunk_size
            # Increment to the next exception in the metatdata store
            exception = exception + 1

    def read_header(self, fd):
        SECTOR_SHIFT = 9
        SNAPSHOT_DISK_MAGIC = 0x70416e53
        SNAPSHOT_DISK_VERSION = 1
        SNAPSHOT_VALID_FLAG = 1

        # Read the cow metadata
        header = unpack_from("<IIII", self.read(fd, 0, 16))

        if header[0] != SNAPSHOT_DISK_MAGIC:
            raise ScrubError(
                "Invalid COW device; header magic doesn't match")

        if header[1] != SNAPSHOT_VALID_FLAG:
            log.warning(
                "Inactive COW device; valid flag not set '%d' got '%d'"
                % (SNAPSHOT_VALID_FLAG, header[1]))

        if header[2] != SNAPSHOT_DISK_VERSION:
            raise ScrubError(
                "Unknown metadata version; expected '%d' got '%d' "
                % (SNAPSHOT_DISK_VERSION, header[2]))

        log.info("Magic: %X" % header[0])
        log.info("Valid: %d" % header[1])
        log.info("Version: %d" % header[2])
        log.info("Chunk Size: %d" % header[3])

        header = list(header)
        # Chunk size is byte aligned to 512 bytes
        # (0 << SECTOR_SHIFT) == 512
        return header[3] << SECTOR_SHIFT

    def scrub_cow(self, cow_path):
        try:
            log.info("Opening Cow '%s'" % cow_path)
            # Open the cow block device
            fd = directio.open(cow_path, mode='+o', buffered=32768)
        except OSError, e:
            raise ScrubError("Failed to open cow '%s'" % e)

        # Read the meta data header
        chunk_size = self.read_header(fd)
        if not self._display_only:
            log.debug("Scrubbing cow header")
            self.write(fd, 0, '\0' * 16)

        # Create a buffer of nulls the size of the chunk
        self.scrub_buf = '\0' * chunk_size

        store, count = (0, 0)
        while True:
            # Iterate through all the exceptions
            for offset in self.read_exception_metadata(fd, chunk_size, store):
                # zero means we reached the last exception
                if offset == 0:
                    if self._display_only:
                        log.info("Counted '%d' exceptions" % count)
                    else:
                        log.info("Scrubbed '%d' exceptions" % count)
                    return fd.close()
                if self._display_exceptions:
                    log.debug("Exception: %s",
                              self.read(fd, offset, chunk_size))
                count = count + 1
                if not self._display_only:
                    # Write a chunk full of NULL's at 'offset'
                    self.write(fd, offset, self.scrub_buf)
            # Seek the next store
            store = store + 1

    def _dash(self, value):
        """ When dev-mapper creates symlinks in /dev/mapper it
            replaces all occurances of '-' with '--'. Presumably
            to make parsing the pattern 'name-volume-type' easier
        """
        return re.sub('-', '--', value)

    def get_writable_cow(self, snapshot, volume):
        """Remove the COWing from the volume so we can scrub it.

        Change the vg-vol device from snapshot-origin to linear.
        Remove the vg-snapshot device.
        Remove the vg-vol-real.
        Only the vg-vol linear and vg-snap-cow linear devices remain.
        """
        path, vol = os.path.split(snapshot['path'])
        path, vg = os.path.split(path)
        path, dev = os.path.split(path)
        snap_name = "%s-%s" % (self._dash(vg), self._dash(vol))
        snap_path = os.path.join(os.sep, dev, 'mapper', snap_name)
        cow_name = snap_name + "-cow"
        cow_path = os.path.join(os.sep, dev, 'mapper', cow_name)

        if self._display_only:
            return (cow_name, cow_path)

        if not os.path.exists(cow_path):
            raise ScrubError(
                "non-existant cow '%s'; invalid snapshot volume?" % cow_path)

        # If the snap device is gone, we've already been here before.
        if not os.path.exists(snap_path):
            return (cow_name, cow_path)

        path, vol = os.path.split(volume['path'])
        path, vg = os.path.split(path)
        path, dev = os.path.split(path)
        vol_name = "%s-%s" % (self._dash(vg), self._dash(vol))
        vol_real_name = vol_name + "-real"

        try:
            real_table = self.run(
                '/sbin/dmsetup', 'table', vol_real_name).rstrip()
            log.info("real_table: %s" % real_table)
        except ProcessError, e:
            raise ScrubError("dmsetup failed '%s'; not running as root?" % e)

        tmpdir = mkdtemp()
        tmpfile = os.path.join(tmpdir, 'table')

        try:
            with open(tmpfile, 'w') as f:
                f.write(real_table)

            self.run('/sbin/dmsetup', 'suspend', vol_name)
            try:
                self.run('/sbin/dmsetup', 'load', vol_name, tmpfile)
            finally:
                self.run('/sbin/dmsetup', 'resume', vol_name)
            self.run('/sbin/dmsetup', 'remove', snap_name)
            self.run('/sbin/dmsetup', 'remove', vol_real_name)
        finally:
            rmtree(tmpdir)

        return (cow_name, cow_path)

    def remove_cow(self, cow_name):
        log.info("Removing cow'%s'" % cow_name)
        self.run('/sbin/dmsetup', 'remove', cow_name, '-f')

    def scrub_snapshot(self, snapshot, volume):
        (cow_name, cow_path) = self.get_writable_cow(snapshot, volume)
        if not os.path.exists(cow_path):
            raise ScrubError("snapshot '%s' has no cow" % snapshot['name'])

        # scrub the cow
        self.scrub_cow(cow_path)

        if not self._display_only:
            self.remove_cow(cow_name)

    def scrub_volume(self, volume, byte='\x00'):
        CHUNKSIZE = 4 * 1024 ** 2  # 4 MB
        chunk = mmap(-1, CHUNKSIZE)
        chunk.write(byte * CHUNKSIZE)
        log.debug('Chunk Size: %d' % CHUNKSIZE)

        fd = os.open(volume, os.O_DIRECT | os.O_SYNC | os.O_WRONLY)
        try:
            # Get the size of the block device
            size = os.lseek(fd, 0, os.SEEK_END)
            # Seek back to the beginning of the device
            os.lseek(fd, 0, os.SEEK_SET)

            sample_size = 15
            target_sleep, start = 0, Timer()
            # If config included a throttle speed for scrubbing
            if self._throttle_speed > 0:
                # microseconds it takes to transfer 1 MB at our
                # throttle speed, multiplied by the MB in our sample size
                target_sleep = (1 / self._throttle_speed) * \
                    ((sample_size * CHUNKSIZE) / 1048576.0)

            # TODO: this math only works if CHUNKSIZE is == lvm PE size
            for block_num in xrange(0, size, CHUNKSIZE):
                os.write(fd, chunk)

                # Sample scrub progress every 'sample_size' ( in blocks )
                if (block_num % sample_size) == 0 and block_num != 0:
                    elapsed = (Timer() - start)
                    log.debug("Throughput %.3fMB/s POS: %s (%d%%)" %
                              (float(((sample_size * CHUNKSIZE) / 1048576.0) /
                                     elapsed), block_num,
                               float(block_num) / size * 100))
                    # If we are throttling our scrub
                    if target_sleep != 0:
                        # Calculate how long we must sleep to
                        # achieve our target throughput
                        time.sleep(abs(target_sleep - elapsed))
                    start = Timer()
        finally:
            os.fsync(fd)
            os.close(fd)
