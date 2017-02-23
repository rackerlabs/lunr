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


import distutils.version
import errno
import fcntl
import os
import re
from time import time, sleep
from os.path import join, exists
import json
from random import shuffle
from setproctitle import setproctitle
from shutil import rmtree
import socket
import subprocess
from tempfile import mkdtemp
import uuid

from lunr.common import logger
from lunr.common.lock import ResourceFile, NullResource
from lunr.storage.helper.utils import execute, NotFound, \
    ProcessError, AlreadyExists, InvalidImage, ServiceUnavailable
from lunr.storage.helper.utils.glance import GlanceError, \
    get_conn as get_glance_conn

from lunr.cinder.cinderclient import CinderError
from lunr.storage.helper.utils.jobs import spawn
from lunr.storage.helper.utils.worker import Worker
from lunr.storage.helper.utils.scrub import Scrub, ScrubError
from lunr.storage.helper.utils.iscsi import ISCSIDevice, ISCSILoginFailed, \
    ISCSICopyFailed, ISCSINotConnected, ISCSILogoutFailed


def decode_tag(tag):
    parts = tag.split('.')
    # Is a volume that is being scrubed
    if parts[0] == 'zero':
        return {'zero': True}
    # Is a Volume that is being created from backup
    if parts[0] == 'restore':
        return {'backup_source_volume_id': parts[1], 'backup_id': parts[2]}
    # Is a snapshot undergoing a backup
    if parts[0] == 'backup':
        return {'timestamp': float(parts[1]), 'backup_id': parts[2]}
    if parts[0] == 'clone':
        return {'clone_id': parts[1]}
    if parts[0] == 'convert':
        return {'image_id': parts[1]}
    # Regular volume
    return {'volume': True}


def encode_tag(backup_source_volume_id=None, backup_id=None, timestamp=None,
               zero=False, clone_id=None, image_id=None, **kwargs):
    if zero:
        return 'zero'
    if timestamp and backup_id:
        return 'backup.%d.%s' % (int(timestamp), backup_id)
    if backup_source_volume_id and backup_id:
        return 'restore.%s.%s' % (backup_source_volume_id, backup_id)
    if clone_id:
        return 'clone.%s' % clone_id
    if image_id:
        return 'convert.%s' % image_id
    return 'volume'


class VolumeHelper(object):

    LVS_OPTIONS = [
        'lv_name',
        'lv_size',
        'origin',
        'origin_size',
        'lv_tags',
        'lv_kernel_major',
        'lv_kernel_minor',
    ]
    NEW_MKFS_VERSION = '1.42.8'

    def __init__(self, conf):
        self.volume_group = conf.string('volume', 'volume_group',
                                        'lunr-volume')
        self.device_prefix = conf.string('volume', 'device_prefix', '/dev')
        self.run_dir = conf.string('storage', 'run_dir', conf.path('run'))
        self.convert_gbs = conf.int('glance', 'convert_gbs', 100)
        self.glance_mgmt_urls = conf.list('glance', 'glance_mgmt_urls', None)
        self.glance_base_multiplier = conf.float('glance',
                                                 'base_convert_multiplier',
                                                 2.0)
        self.glance_custom_multiplier = conf.float('glance',
                                                   'custom_convert_multiplier',
                                                   4.0)
        self.skip_fork = conf.bool('storage', 'skip_fork', False)
        self.scrub = Scrub(conf)
        self.conf = conf
        self.max_snapshot_bytes = conf.int('volume', 'max_snapshot_bytes',
                                           None)
        if self.max_snapshot_bytes:
            self.sector_size = conf.int('volume', 'sector_size', 512)
            max_bytes = (self.max_snapshot_bytes -
                         self.max_snapshot_bytes % self.sector_size)
            if max_bytes != self.max_snapshot_bytes:
                logger.info("Setting max_snapshot_size to %s" % max_bytes)
                self.max_snapshot_bytes = max_bytes
        self.has_old_mkfs = self.old_mkfs()

    def check_config(self):
        # To understand the volume group name size
        # limit you must keep the following in mind
        #
        #  * The max length of a dev-mapper or lvm name is 128 characters
        #  * lvm expands any name with an '-' to '--'
        #  * dev-mapper tracks files by using '-' as a separator
        #       like so (volgroup-volname-voltype) [Reserve 2 chars]
        #  * We internally use '.' as a separator
        #       like so (z.volume_id) [Reserve 2 chars]
        #  * If we reserve 30 charaters for the volume group
        #       that will leave 94 characters for the volume name
        expanded_name = self.volume_group.replace('-', '--')
        if len(expanded_name) > 30:
            raise RuntimeError(
                "'volume_group' option '%s' cannot exceed 30 "
                "characters in length" % expanded_name)

    def old_mkfs(self):
        new_version = distutils.version.StrictVersion(self.NEW_MKFS_VERSION)
        version_info = self._mkfs_version()
        version_line, _ = version_info.strip().split('\n')
        mkfs, version, _ = version_line.split()
        if mkfs != 'mke2fs':
            raise ValueError
        return distutils.version.StrictVersion(version) < new_version

    def _mkfs_version(self):
        args = ['/sbin/mkfs.ext4', '-V']
        logger.debug("execute: %s" % args)
        p = subprocess.Popen(args, close_fds=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        logger.debug('returned: %s' % p.returncode)
        if p.returncode:
            raise ProcessError(' '.join(args), out, err, p.returncode)
        return err.rstrip()

    def _resource_file(self, id):
        return join(self.run_dir, 'volumes/%s/resource' % id)

    def _stats_file(self, id):
        return join(self.run_dir, 'volumes/%s/stats' % id)

    def _get_path(self, id):
        return join(self.device_prefix, self.volume_group, id)

    def _parse_volume(self, line):
        data = {}
        values = line.split(':')
        for i, key in enumerate(self.LVS_OPTIONS):
            data[key] = values[i]
        volume = decode_tag(data['lv_tags'])
        volume.update({
            'id': data['lv_name'],
            'size': int(data['lv_size'][:-1]),
            'path': self._get_path(data['lv_name']),
            'origin': data['origin'],
            'device_number': '%s:%s' % (data['lv_kernel_major'],
                                        data['lv_kernel_minor']),

        })
        if volume['origin']:
            volume['size'] = int(data['origin_size'][:-1])
        return volume

    def _get_volume(self, volume_id):
        volume_name = '%s/%s' % (self.volume_group, volume_id)
        out = execute('lvs', volume_name, noheadings=None, separator=':',
                      units='b', options=','.join(self.LVS_OPTIONS))
        out = out.strip()
        return self._parse_volume(out)

    def _scan_volumes(self):
        out = execute('lvs', self.volume_group, noheadings=None, separator=':',
                      units='b', options=','.join(self.LVS_OPTIONS))
        volumes = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            volume = self._parse_volume(line)
            volumes.append(volume)
        return volumes

    def _get_snapshot(self, id):
        for volume in self._scan_volumes():
            if volume['origin'] == id:
                return volume
        return None

    def list(self):
        return self._scan_volumes()

    def _in_use(self, volume_id):
        resource_file = self._resource_file(volume_id)
        if not exists(resource_file):
            return False

        with ResourceFile(resource_file) as lock:
            return lock.used()

    def get(self, volume_id):
        try:
            volume = self._get_volume(volume_id)
        except ProcessError, e:
            raise NotFound('No volume named %s.' % volume_id, id=volume_id)

        volume['realpath'] = os.path.realpath(volume['path'])
        if 'backup_id' in volume:
            used = self._in_use(volume['id'])
            if used:
                volume['status'] = 'BUILDING'
                volume.update(used)
                return volume
            volume['status'] = 'ERROR'
        if 'zero' in volume:
            used = self._in_use(volume['id'])
            if used:
                volume['status'] = 'DELETING'
                volume.update(used)
                return volume
            volume['status'] = 'ERROR'
        return volume

    def restore(self, dest_volume, backup_source_volume_id,
                backup_id, size, cinder):
        op_start = time()
        logger.rename('lunr.storage.helper.volume.restore')
        setproctitle("lunr-restore: " + dest_volume['id'])
        job_stats_path = self._stats_file(dest_volume['id'])
        worker = Worker(backup_source_volume_id, conf=self.conf,
                        stats_path=job_stats_path)
        try:
            worker.restore(backup_id, dest_volume['path'],
                           dest_volume['id'], cinder)
        finally:
            os.unlink(job_stats_path)
        self.update_tags(dest_volume, {})
        duration = time() - op_start
        logger.info('STAT: Restore %r from %r. '
                    'Size: %r GB Time: %r s Speed: %r MB/s' %
                    (dest_volume['id'], backup_id, size, duration,
                     size * 1024 / duration))

    def write_raw_image(self, glance, image, path):
        op_start = time()
        with open(path, 'wb') as f:
            # Try until we run out of glances.
            while True:
                try:
                    chunks = glance.get(image.id)
                except GlanceError, e:
                    logger.warning("Error fetching glance image: %s" % e)
                    raise

                try:
                    for chunk in chunks:
                        f.write(chunk)
                    break
                # Glanceclient doesn't handle socket timeouts for chunk reads.
                except (GlanceError, socket.timeout) as e:
                    continue
        duration = time() - op_start
        mbytes = image.size / 1024 / 1024
        logger.info('STAT: glance.get %r. Size: %r MB Time: %r Speed: %r' %
                    (image.id, mbytes, duration, mbytes / duration))

    def get_oldstyle_vhd(self, path):
        old_style = os.path.join(path, 'image.vhd')
        if os.path.exists(old_style):
            return old_style
        return None

    def get_vhd_chain(self, path):
        i = 0
        chain = []
        while True:
            vhd = os.path.join(path, '%d.vhd' % i)
            if not os.path.exists(vhd):
                break
            chain.append(vhd)
            i = i + 1
        return chain

    def reparent_vhd_chain(self, chain):
        chain = list(chain)
        while len(chain) > 1:
            child = chain.pop(0)
            parent = chain[0]
            # Because someone hates us, these vhds don't actually point to
            # their real parent file name.
            execute('vhd-util', 'modify', '-n', child, '-p', parent,
                    sudo=False)

    def repair_vhd_chain(self, chain):
        # nova FIXME(cory) It seems the vhd footer can get corrupted
        # when creating the image. This makes the resize blow up.
        # This is supposed to fix it.
        for link in chain:
            execute('vhd-util', 'repair', '-n', link, sudo=False)

    def get_coalesced_vhd(self, path):
        # Check for old style, image.vhd
        old_style = self.get_oldstyle_vhd(path)
        if old_style:
            return old_style

        op_start = time()

        chain = self.get_vhd_chain(path)
        if len(chain) == 0:
            raise ValueError('Invalid image. Bad vhd chain.')

        journal = os.path.join(path, 'vhdjournal')

        self.reparent_vhd_chain(chain)
        self.repair_vhd_chain(chain)

        while len(chain) > 1:
            child = chain.pop(0)
            parent = chain[0]
            child_size = execute('vhd-util', 'query', '-n', child, '-v',
                                 sudo=False)
            parent_size = execute('vhd-util', 'query', '-n', parent, '-v',
                                  sudo=False)
            if child_size != parent_size:
                execute('vhd-util', 'resize', '-n', parent,
                        '-s', child_size, '-j', journal, sudo=False)
            execute('vhd-util', 'coalesce', '-n', child, sudo=False)

        duration = time() - op_start
        logger.info('STAT: get_coalesced_vhd Time: %r.' % duration)
        return chain[0]

    def untar_image(self, path, image):
        tarball = os.path.join(path, 'image')
        op_start = time()
        execute('tar', '-C', path, '-zxf', tarball, sudo=False)
        execute('rm','-rf', path, sudo=False)
        duration = time() - op_start
        mbytes = image.size / 1024 / 1024
        uncompressed = 0
        for f in os.listdir(path):
            # No fair counting twice.
            if f == 'image':
                continue
            fpath = os.path.join(path, f)
            if os.path.isfile(fpath):
                uncompressed += os.path.getsize(fpath)
        uncompressed = uncompressed / 1024 / 1024
        logger.info('STAT: tar %r. Compressed Size: %r MB '
                    'Uncompressed Size: %r MB '
                    'Time: %r Speed: %r' %
                    (image.id, mbytes, uncompressed,
                     duration, uncompressed / duration))

    def prepare_tmp_vol(self, tmp_vol):
        if not tmp_vol:
            raise ValueError("No tmp_vol")
        if not os.path.exists(tmp_vol['path']):
            raise ValueError("tmp_vol doesn't exist")

        if self.has_old_mkfs:
            execute('/sbin/mkfs.ext4', tmp_vol['path'], sudo=False)
        else:
            execute('/sbin/mkfs.ext4', '-E', 'root_owner',  tmp_vol['path'],
                    sudo=False)

        mount_dir = mkdtemp()
        execute('mount', '-t', 'ext4', '-o', 'loop', tmp_vol['path'],
                mount_dir)
        return mount_dir

    def cleanup_tmp_vol(self, tmp_vol, convert_dir, scrub_callback):
        if not tmp_vol:
            raise ValueError("No tmp_vol")
        if not os.path.exists(tmp_vol['path']):
            raise ValueError("tmp_vol doesn't exist")
        if convert_dir:
            execute('umount', convert_dir)
            rmtree(convert_dir)
        spawn(NullResource(), self.remove_lvm_volume, tmp_vol,
              callback=scrub_callback, skip_fork=self.skip_fork)

    def copy_image(self, volume, image, glance, tmp_vol, scrub_callback):
        logger.rename('lunr.storage.helper.volume.copy_image')
        setproctitle("lunr-copy-image: " + volume['id'])
        copy_image_start = time()
        convert_dir = None
        try:
            if image.disk_format == 'raw':
                self.write_raw_image(glance, image, volume['path'])
                return

            convert_dir = self.prepare_tmp_vol(tmp_vol)

            if not os.path.exists(convert_dir):
                raise ValueError("Convert dir doesn't exist!")

            try:
                path = mkdtemp(dir=convert_dir)
                logger.info("Image convert tmp dir: %s" % path)
                image_file = os.path.join(path, 'image')
                self.write_raw_image(glance, image, image_file)

                if (image.disk_format == 'vhd' and
                        image.container_format == 'ovf'):
                    self.untar_image(path, image)
                    image_file = self.get_coalesced_vhd(path)

                op_start = time()
                out = execute('qemu-img', 'convert', '-O', 'raw', image_file,
                              volume['path'])
                duration = time() - op_start
                mbytes = os.path.getsize(image_file) / 1024 / 1024
                logger.info('STAT: image convert %r. Image Size: %r MB '
                            'Time: %r Speed: %r' %
                            (image.id, mbytes, duration, mbytes / duration))
            except Exception, e:
                logger.exception("Exception in image conversion")
                raise

        except Exception, e:
            # We have to clean this up no matter what happened.
            # Delete volume syncronously. Clean up db in callback.
            logger.exception('Unhandled exception in copy_image')
            self.remove_lvm_volume(volume)
        finally:
            self.cleanup_tmp_vol(tmp_vol, convert_dir, scrub_callback)
            duration = time() - copy_image_start
            logger.info('STAT: copy_image %r. Time: %r ' %
                        (image.id, duration))

    def _do_create(self, volume_id, size_str, tag,
                   backup_source_volume_id=None):
        try:
            out = execute('lvcreate', self.volume_group,
                          name=volume_id, size=size_str, addtag=tag)
        except ProcessError, e:
            if not e.errcode == 5 and 'already exists' not in e.err:
                raise
            # We ran out of space on the storage node!
            if "Insufficient free extents" in e.err \
                    or "insufficient free space" in e.err:
                logger.error(e.err)
                raise ServiceUnavailable("LVM reports insufficient "
                                         "free space on drive")
            # If we are requesting a restore, and the existing volume is this
            # same failed restore, it's not an error.
            if backup_source_volume_id and backup_source_volume_id == \
                    self.get(volume_id).get('backup_source_volume_id', False):
                logger.info("Restarting failed restore on '%s'" % volume_id)
            else:
                raise AlreadyExists("Unable to create a new volume named "
                                    "'%s' because one already exists." %
                                    volume_id)

    def create_convert_scratch(self, image, size):
        volume_id = uuid.uuid4()
        size_str = self._get_size_str(size)
        tag = encode_tag(image_id=image.id)
        self._do_create(volume_id, size_str, tag)
        return self.get(volume_id)

    def _get_size_str(self, size):
        if size:
            return '%sG' % size
        return '12M'

    def _get_scratch_multiplier(self, image):
        if image.properties.get('image_type') == 'base':
            return self.glance_base_multiplier
        return self.glance_custom_multiplier

    def create(self, volume_id, size=None, backup_source_volume_id=None,
               backup_id=None, image_id=None, callback=None, lock=None,
               account=None, cinder=None, scrub_callback=None):

        op_start = time()

        size_str = self._get_size_str(size)
        tmp_vol = None
        snet_glance = None

        if image_id:
            mgmt_glance = get_glance_conn(self.conf, tenant_id=account,
                                          glance_urls=self.glance_mgmt_urls)
            snet_glance = get_glance_conn(self.conf, tenant_id=account)
            try:
                glance_start = time()
                image = mgmt_glance.head(image_id)
                logger.info('STAT: glance.head %r. Time: %r' %
                            (image_id, time() - glance_start))
                status = getattr(image, 'status', 'ACTIVE')
                if status.upper() != 'ACTIVE':
                    raise InvalidImage("Non-active image status: %s" % status)
                min_disk = getattr(image, 'min_disk', 0)
                if min_disk:
                    multiplier = self._get_scratch_multiplier(image)
                    convert_gbs = int(min_disk * multiplier)
                else:
                    convert_gbs = self.convert_gbs
                tmp_vol = self.create_convert_scratch(image, convert_gbs)
            except GlanceError, e:
                logger.warning("Error fetching glance image: %s" % e)
                raise InvalidImage("Error fetching image: %s" % image_id)

        # Create a tag to apply to the lvm volume
        tag = encode_tag(backup_source_volume_id=backup_source_volume_id,
                         backup_id=backup_id)

        try:
            self._do_create(volume_id, size_str, tag, backup_source_volume_id)
        except Exception, e:
            # If we ran out of space due to the tmp_vol
            logger.error('Failed to create volume: %s' % e)
            # Update cinder immediately.
            if callback:
                callback()
            if tmp_vol:
                spawn(NullResource(), self.remove_lvm_volume, tmp_vol,
                      callback=scrub_callback, skip_fork=self.skip_fork)
            raise

        def log_duration():
            duration = time() - op_start

            parts = ['STAT: Create Volume']
            if volume_id:
                parts.append('Volume_ID: %s' % (volume_id,))
            if backup_id:
                parts.append('Backup_ID: %s' % (backup_id,))
            if backup_source_volume_id:
                parts.append('Backup_Source_Volume_ID: %s' % (backup_id,))
            if image_id:
                parts.append('Image_ID: %s' % (image_id,))
            parts.append('Size: %s' % size)
            parts.append('Duration: %s' % duration)

            logger.info(' '.join(parts))

        def callback_wrap(*args, **kwargs):
            try:
                log_duration()
            finally:
                if callback:
                    return callback(*args, **kwargs)

        if any((backup_source_volume_id, backup_id)):
            # TODO: clean up this volume if the spawn fails
            dest_volume = self.get(volume_id)
            size = (dest_volume['size'] / 1024 / 1024 / 1024)
            spawn(lock, self.restore, dest_volume,
                  backup_source_volume_id, backup_id, size, cinder,
                  callback=callback_wrap, skip_fork=self.skip_fork)
        elif image_id:
            # TODO: clean up this volume if the spawn fails
            dest_volume = self.get(volume_id)
            spawn(lock, self.copy_image, dest_volume, image, snet_glance,
                  tmp_vol, scrub_callback, callback=callback_wrap,
                  skip_fork=self.skip_fork)
        else:
            log_duration()

    def _max_snapshot_size(self, bytes):
        chunk_size = 4096
        exception_size = 16
        origin_chunks = ((bytes - 1) // chunk_size) + 1
        exceptions_per_chunk = ((chunk_size - 1) // exception_size) + 1
        # +1 for rounding up, +1 for the last allocated (unused) chunk
        exception_chunks = ((origin_chunks - 1) // exceptions_per_chunk) + 2
        # +1 for the header chunk
        max_overhead_bytes = (exception_chunks + 1) * chunk_size
        total_bytes = bytes + max_overhead_bytes
        if self.max_snapshot_bytes:
            return min(self.max_snapshot_bytes, total_bytes)
        return total_bytes

    def create_snapshot(self, volume_id, snapshot_id, timestamp=None,
                        type_='backup', clone_id=None):
        snapshot = self._get_snapshot(volume_id)
        if snapshot:
            if type_ == 'backup':
                if snapshot['id'] == snapshot_id:
                    return snapshot
            elif type_ == 'clone':
                if snapshot.get('clone_id') == clone_id:
                    return snapshot
            raise AlreadyExists(
                "Volume %s already has a snapshot." % volume_id)

        origin = self.get(volume_id)
        # TODO: support size as kwarg or % of origin.size?
        sizestr = '%sB' % self._max_snapshot_size(origin['size'])

        if type_ == 'backup':
            # TODO: should we prevent create snapshot if timestamp is too old?
            timestamp = int(timestamp or time())
            tag = encode_tag(backup_id=snapshot_id, timestamp=timestamp)
        elif type_ == 'clone':
            tag = encode_tag(clone_id=clone_id)
        else:
            raise ValueError("Invalid snapshot type: %s" % type_)

        try:
            # Create an lvm snapshot
            execute('lvcreate', origin['path'], name=snapshot_id,
                    size=sizestr, snapshot=None, addtag=tag)
            return self.get(snapshot_id)
        except ProcessError, e:
            if e.errcode != 5 or 'already exists' not in e.err:
                raise
            raise AlreadyExists("snapshot id '%s' already in use" % id)

    def _copy_clone(self, snapshot, clone_id, size, iscsi_device, cinder=None):
        def progress_callback(percent):
            try:
                if cinder:
                    cinder.update_volume_metadata(
                        clone_id, {'clone-progress': "%.2f%%" % percent})
            except CinderError, e:
                logger.warning(
                    "Error updating clone-progress metadata: %s" % e)

        op_start = time()
        logger.rename('lunr.storage.helper.volume._copy_clone')
        setproctitle("lunr-clone: %s %s" % (snapshot['origin'], clone_id))
        try:
            iscsi_device.copy_file_out(snapshot['path'],
                                       callback=progress_callback)
        except (ISCSINotConnected, ISCSICopyFailed), e:
            logger.error("copy_file_out failed: %s" % str(e))
            raise
        try:
            iscsi_device.disconnect()
        except ISCSILogoutFailed:
            raise ServiceUnavailable("Unable to disconnect")
        duration = time() - op_start
        logger.info('STAT: Clone %r to %r. '
                    'Size: %r GB Time: %r s Speed: %r MB/s' %
                    (snapshot['origin'], clone_id, size, duration,
                     size * 1024 / duration))
        self.delete(snapshot['id'])

    def create_clone(self, volume_id, clone_id, iqn, iscsi_ip, iscsi_port,
                     callback=None, lock=None, cinder=None):
        volume = self.get(volume_id)
        size = volume['size'] / 1024 / 1024 / 1024
        logger.info("Cloning source '%s' to volume '%s'" %
                    (volume_id, clone_id))
        snapshot_name = uuid.uuid4()
        snapshot = self.create_snapshot(volume_id, snapshot_name,
                                        clone_id=clone_id, type_='clone')
        logger.info("Snapshot to clone id: '%s'" % snapshot['id'])
        try:
            new_volume = ISCSIDevice(iqn, iscsi_ip, iscsi_port)
            new_volume.connect()
        except (ISCSILoginFailed, ISCSINotConnected):
            msg = "Unable to open iscsi connection to %s:%s - %s" % \
                  (iscsi_ip, iscsi_port, iqn)
            logger.error(msg)
            self.delete(snapshot['id'])
            raise ServiceUnavailable(msg)

        spawn(lock, self._copy_clone, snapshot, clone_id, size, new_volume,
              cinder, callback=callback, skip_fork=self.skip_fork)

    def update_tags(self, vol_info, tags):
        # Build the old tag, so we can delete them
        deltag = encode_tag(**vol_info)
        # Build the new tag, so we can add it
        addtag = encode_tag(**tags)
        # call lvchange to replace the current tags
        self.lvchange(vol_info['path'], deltag=deltag, addtag=addtag)

    def lvchange(self, path, **kwargs):
        try:
            return execute('lvchange', path, **kwargs)
        except ProcessError, e:
            if e.errcode == 5 and 'not found' in e.err:
                raise NotFound('No volume named %s.' % id)
            raise

    def _dash(self, value):
        """Copied from scrub."""
        return re.sub('-', '--', value)

    def remove_lvm_snapshot(self, snapshot):
        try:
            op_start = time()
            volume = self.get(snapshot['origin'])
            logger.rename('lunr.storage.helper.volume.remove_lvm_snapshot')
            self.scrub.scrub_snapshot(snapshot, volume)
            self.remove(snapshot['path'])
            # TODO: Failure to scrub a snapshot is un-acceptable
            # If we catch an exception, we should mark the snapshot
            # Or make this as recoverable as possible
            duration = time() - op_start
            logger.info("STAT: remove_lvm_snapshot(%r) Time: %r" %
                        (volume['path'],
                         duration))
        except Scrub, e:
            logger.exception(
                "scrub snapshot failed with '%r' after %r seconds" %
                (e, time() - op_start))
        except ProcessError, e:
            logger.exception(
                "delete snapshot failed with '%r' after %r seconds" %
                (e, time() - op_start))
        except Exception, e:
            logger.exception(
                "unknown exception caught '%r' after %s seconds" %
                (e, time() - op_start))

    def remove_lvm_volume(self, volume):
        try:
            op_start = time()
            size = volume['size'] / 1024 / 1024 / 1024
            logger.rename('lunr.storage.helper.volume.remove_lvm_volume')
            setproctitle("lunr-remove: " + volume['id'])
            # Scrub the volume
            self.scrub.scrub_volume(volume['path'])
            # Remove the device
            self.remove(volume['path'])
            duration = time() - op_start
            logger.info('STAT: remove_lvm_volume(%r) '
                        'Size: %r GB Time: %r s Speed: %r MB/s' %
                        (volume['path'],
                         size, duration,  size * 1024 / duration))
        except ProcessError, e:
            logger.exception(
                "delete volume failed with '%r' after %r seconds" %
                (e, time() - op_start))
        except Scrub, e:
            logger.exception(
                "scrub volume failed with '%r' after %r seconds" %
                (e, time() - op_start))
        except Exception, e:
            logger.exception(
                "unknown exception caught '%r' after %r seconds" %
                (e, time() - op_start))

    def remove(self, path):
        for i in range(0, 10):
            try:
                return execute('lvremove', path, force=None)
            except ProcessError:
                sleep(1)
                continue
        logger.error("Failed to delete volume '%s' after 10 tries" % path)
        raise

    def delete(self, volume_id, callback=None, lock=None):
        volume = self.get(volume_id)
        # If origin exists, this volume is a snapshot
        if volume['origin']:
            # scrub and remove snapshots synchronously
            self.remove_lvm_snapshot(volume)
            return

        # This is a normal volume delete
        if 'zero' not in volume['id']:
            # Does this volume have a snapshot?
            if self._get_snapshot(volume['id']):
                raise ServiceUnavailable(
                    "Refusing to delete volume '%s' "
                    "with an active backup" % volume['id'])
            try:
                self.update_tags(volume, {'zero': True})
            except NotFound:
                pass
            volume = self.get(volume_id)
        spawn(lock, self.remove_lvm_volume, volume,
              callback=callback, skip_fork=self.skip_fork)

    def status(self):
        options = ('vg_size', 'vg_free', 'lv_count')
        try:
            out = execute('vgs', self.volume_group, noheadings=None, unit='b',
                          options=','.join(options), separator=':')
        except ProcessError, e:
            if e.errcode == 5 and 'not found' in e.err:
                raise ServiceUnavailable("Volume group '%s' not found." %
                                         self.volume_group)
            logger.exception("Unknown error trying to query status of "
                             "volume group '%s'" % self.volume_group)
            raise ServiceUnavailable("[Errno %d] %s" % (e.errcode, e.err))

        status = {'volume_group': self.volume_group}
        values = (int(i.rstrip('B')) for i in out.split(':'))
        for opt, v in zip(options, values):
            status[opt] = v
        return status
