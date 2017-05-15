#!/usr/bin/env python
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


"""
Command Line Interface to LunR Storage Helper
"""


from lunr.storage.helper.utils.manifest import read_local_manifest,\
    ManifestEmptyError
from lunr.storage.helper.utils import NotFound, ProcessError, execute
from lunr.common.subcommand import SubCommand, SubCommandParser,\
    opt, noargs, confirm, Displayable
from lunr.storage.helper.utils.iscsi import ISCSIDevice
from lunr.storage.helper.utils.scrub import Scrub, ScrubError
from lunr.storage.helper.utils.worker import BLOCK_SIZE
from lunr.common.config import LunrConfig
from lunr.storage.helper.base import Helper
from lunr.storage.helper.utils import StorageError
from lunr.storage.helper import audit
from lunr.common import logger
from lunr.common.lock import NullResource
from os.path import exists, join
from time import time, sleep

import logging
import random
import signal
import errno
import uuid
import sys
import os
import re


log = logger.get_logger()


class Console(SubCommand):
    def __init__(self):
        # let the base class setup methods in our class
        SubCommand.__init__(self)
        # Add global arguments for this subcommand
        self.opt('-c', '--config', default=LunrConfig.lunr_storage_config,
                 help="config file (default: /etc/lunr/storage-server.conf)")
        self.opt('-v', '--verbose', action='count',
                 help="be verbose (-vv is more verbose)")

    def load_conf(self, file):
        try:
            conf = LunrConfig.from_conf(file)
            return Helper(conf)
        except IOError, e:
            print 'Error: %s' % e
            sys.exit(1)


class VolumeConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'volume'

    @noargs
    def list(self):
        """ List all volumes """
        helper = self.load_conf(self.config)
        self.display(helper.volumes.list(), ['id', 'path', 'size', 'origin'])

    @opt('volume', help="the volume id to get")
    def get(self, volume):
        """ Display details of volume """
        helper = self.load_conf(self.config)
        self.display(helper.volumes.get(volume))

    @opt('--id', help="id of the new volume (defaults to uuid)")
    @opt('size', type=int, help='size of new volume in GB')
    def create(self, size=None, id=None):
        """ Create a new volume, if no  """
        helper = self.load_conf(self.config)
        id = id or str(uuid.uuid4())
        helper.volumes.create(id, size=size)
        print "created '%s'" % id

    @opt('-t', '--timestamp', help="timestamp to create the "
         "snapshot for (defaults to current time)")
    @opt('--src', required=True, help="source volume id the snapshot is for")
    @opt('id', nargs='?', help="id of the new snapshot")
    def snapshot(self, id=None, src=None, timestamp=None):
        """
        Create a snapshot of a given volume.
        You can use these snapshots with `backups save`
        """

        helper = self.load_conf(self.config)
        timestamp = timestamp or time()
        id = id or str(uuid.uuid4())
        helper.volumes.create_snapshot(src, id, timestamp)
        print "created Snapshot '%s'" % id

    @opt('-s', '--scrub', action='store_true',
         help='scrub volume before deleting it!')
    @opt('id', help="id of the volume to delete")
    def delete(self, id=None, scrub=None):
        """ Remove a volume on disk  """
        helper = self.load_conf(self.config)
        if scrub:
            # delete will scrub the volume before deletion
            lock = NullResource()
            helper.volumes.delete(id, lock=lock)
        else:
            volume = helper.volumes.get(id)
            # remove the volume without scrubing
            helper.volumes.remove(volume['path'])
        print "deleted '%s'" % id

    @opt('dest', help="name of destination volume")
    @opt('src', help="name of source volume")
    def rename(self, src, dest):
        """ Rename 'src' to 'dest' """
        try:
            helper = self.load_conf(self.config)
            execute('lvrename', helper.volumes.volume_group, src, dest)
            print "Renamed '%s' to '%s'" % (src, dest)
        except ProcessError, e:
            if e.errcode == 5 and 'not found' in e.err:
                print "No volume named '%s'" % src
                return 1
            print "Unknown Error %s" % e
            return 1

    @opt('dest-vol', help="volume id to restore the backup to")
    @opt('backup-id', help="id of the backup")
    @opt('src-vol', help="source volume the backup is from")
    def restore(self, src_vol=None, backup_id=None, dest_vol=None):
        """ Restore a backup to a destination volume """
        helper = self.load_conf(self.config)
        volume = helper.volumes.get(dest_vol)
        helper.volumes.restore(volume, src_vol, backup_id)
        print "restored backup '%s' to volume '%s'" % (backup_id, dest_vol)


class ExportConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'export'

    @noargs
    def list(self):
        """ List all exports  """
        helper = self.load_conf(self.config)
        self.display(helper.exports.list(), ['tid', 'lun', 'name', 'path'])

    @opt('id', help="volume to display ")
    def get(self, id=None):
        """ Display details of an export for a given volume """
        helper = self.load_conf(self.config)
        self.display(helper.exports.get(id))

    @opt('-a', '--all', action='store_true',
         help='recreate exports for all regular volumes')
    @opt('id', nargs='?', help="id of the volume to create an export for")
    def create(self, id=None, all=None):
        """ Create an export for a given volume """
        helper = self.load_conf(self.config)
        if not all:
            helper.exports.create(id)
            print "export created '%s'" % id
            return 0

        for volume in helper.volumes.list():
            try:
                # Only export volumes, not snapshots
                if not volume.get('volume', False):
                    continue
                helper.exports.get(volume['id'])
                print "export exists '%s'" % volume['id']
            except NotFound:
                helper.exports.create(volume['id'])
                print "export created '%s'" % volume['id']

    @opt('-f', '--force', action='store_true', help='force close session')
    @opt('id', help="id of the volume to delete en export for")
    def delete(self, id=None, force=None):
        """ Remove an export for a given volume """
        helper = self.load_conf(self.config)
        helper.exports.delete(id, force=force)
        print "export deleted '%s'" % id


class BackupConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'backup'

    def _get_backups(self, helper, id, file, deleted=False):
        results = []
        try:
            # Attempt to get a listing of the backups for this volume
            volume = helper.volumes.get(id)
            for key, value in helper.backups.list(volume).items():
                # For each of the backups create an row
                results.append({'volume': volume['id'],
                               'backup': key, 'timestamp': value})
            return results
        except NotFound:
            # Include deleted volumes in the list?
            if not deleted:
                return []

        try:
            # Read the manifest file of the deleted volume
            backups = read_local_manifest(file).backups
        except ManifestEmptyError:
            backups = {'(manifest is empty)': '0'}

        # For each of the backups create an row
        for key, value in backups.items():
            results.append({'volume': id + " (deleted)",
                           'backup': key, 'timestamp': value})
        return results

    @opt('-i', '-a', '--include-deleted', action='store_true',
         help="include backups even if the source volume was deleted")
    def list(self, include_deleted=None):
        """ List all backups on the storage node """
        helper = self.load_conf(self.config)
        # build a path to backup manifests
        path = join(helper.backups.run_dir, 'volumes')
        print "Searching '%s' for backups" % path
        # list all the volumes in this directory
        results = []
        for dir in os.listdir(path):
            file = join(path, dir, 'manifest')
            # If the manifest exists
            if exists(file):
                # Get a listing of all the backups for this manifest file
                backups = self._get_backups(helper, dir, file, include_deleted)
                results.extend(backups)
        self.display(results)

    @opt('--src', required=True, help="source volume id the backup is for")
    @opt('backup_id', help="id of the backup")
    def get(self, backup_id=None, src=None):
        """ List details for a specific backup """
        try:
            helper = self.load_conf(self.config)
            volume = helper.volumes.get(src)
            self.display(helper.backups.get(volume, backup_id))
        except NotFound, e:
            print str(e)
            return 1

    @opt('-s', '--scrub', action='store_true',
         help='scrub volume before deleting it!')
    @opt('-f', '--force', action='store_true',
         help='automatically confirm all prompts')
    @opt('-t', '--timestamp', help='timestamp')
    @opt('--src', help="source volume id the backup is for")
    @opt('backup_id', help="id of the backup")
    def save(self, backup_id=None, src=None, timestamp=None,
             force=None, scrub=None):
        """
        Create a new backup for a given volume or snapshot

        If you provide a valid --src and backup_id, a snapshot can
        be created for you.

        If can also omit --src if you provide the backup_id of a
        valid snapshot.  The `--timestamp` option is ignored if in this
        case.

        You can specify `--scrub` to wipe the snapshot volume,
        otherwise you will be prompted to simply remove it (you can
        bypass prompts with `--force`)

        WARNING: running this command does not LOCK the volume while
        the save is in progress
        """
        helper = self.load_conf(self.config)
        helper.volumes.skip_fork = True

        volume = helper.volumes.get(src or backup_id)
        if not src and not volume['origin']:
            print "--src is required if backup_id isn't a snapshot"
            return 1

        # The id passed was NOT a snapshot id
        if not volume['origin']:
            if not confirm("Create new snapshot of '%s' for backup '%s'"
                           % (volume['id'], backup_id), force=force):
                return
            # Create a new snapshot
            volume = helper.volumes.create_snapshot(volume['id'],
                                                    backup_id,
                                                    timestamp=timestamp)
        else:
            # snapshot id was passed
            backup_id = volume['backup_id']

        # Start the backup job
        helper.backups.save(volume, backup_id)
        if scrub:
            helper.volumes.delete(volume)
        elif confirm('Remove snapshot', force=force):
            helper.volumes.remove(volume['path'])

    # alias create command
    create = save

    @opt('--src', required=True, help="source volume id the backup is for")
    @opt('backup_id', help="id of the backup")
    def prune(self, backup_id=None, src=None):
        """ Remove backup from manifest and clean out unreferenced blocks """
        helper = self.load_conf(self.config)
        volume = helper.volumes.get(src)
        helper.backups.prune(volume, backup_id)

    # alias the delete command
    delete = prune

    @opt('id', help="id of the volume")
    def audit(self, id=None):
        """ Look for unreferenced blocks in the container for a volume """
        helper = self.load_conf(self.config)
        volume = helper.volumes.get(id)
        helper.backups.audit(volume)


class ToolsConsole(Console, Displayable):
    def __init__(self):
        # let the base class setup methods in our class
        Console.__init__(self)
        # Give our sub command a name
        self._name = 'tools'

    @opt('-t', '--throttle', type=int, default=0,
         help='specify iops throttle (IE: -t 1000)')
    @opt('-p', '--path', default='/sys/fs/cgroup/blkio/sysdefault',
         help='path to blkio cgroup to use')
    @opt('-d', '--display', action='store_true', help='display only')
    @opt('volumes', nargs='*', help="list of volumes to apply the limits to")
    def cgroup(self, volumes=None, throttle=None, display=None, path=None):
        """ Apply write_iops and read_iops device throttle to volume[s].  """
        helper = self.load_conf(self.config)
        volumes = [helper.volumes.get(vol) for vol in volumes]
        volumes = volumes or helper.volumes.list()
        if not throttle:
            display = True
        if display:
            data = helper.cgroups.all_cgroups()

        results = []
        for volume in volumes:
            if display:
                count = 0
                for name, dev_map in data.items():
                    if not volume['device_number'] in dev_map:
                        continue
                    limit = "%s %s" % (name, dev_map[volume['device_number']])
                    results.append({'volume': volume['id'], 'limits': limit})
                    count += 1
                if not count:
                    results.append({'volume': volume['id'],
                                   'limits': 'no-limits'})
                continue
            if volume.get('zero', False) or volume.get('backup_id', False):
                continue
            for name in ('blkio.throttle.write_iops_device',
                         'blkio.throttle.read_iops_device'):
                helper.cgroups.set(volume, throttle, name)
                value = "%s %s" % (volume['device_number'], throttle)
                results.append({'volume': volume['id'],
                                'limits': "%s %s" % (name, value)})
        self.display(results)

    @opt('-d', '--display', const=True, action='store_const',
         help="Do not scrub the cow, display cow stats & exit; implies -v")
    @opt('path', help="Path to the cow (/dev/mapper/do-have-a-cow)")
    def scrub_cow(self, path=None, display=None):
        """
        Scrub or display details for a cow device

        Example:
            %(prog)s (cmd)s scrub-cow -d /dev/mapper/lunr--volume-my--snap-cow
        """
        config = {}
        if self.verbose == 1:
            log.setLevel(logging.INFO)
        if self.verbose > 1:
            log.setLevel(logging.DEBUG)
            config['display-exceptions'] = True
        if display:
            if self.verbose < 2:
                log.setLevel(logging.INFO)
            log.info("Display Only, Not Scrubbing")
            config['display-only'] = True

        try:
            # Init the Scrub object with our command line options
            scrub = Scrub(LunrConfig({'scrub': config}))
            # Scrub the cow
            scrub.scrub_cow(path)
        except ScrubError, e:
            log.error(str(e))
            return 1

    @opt('dest', help="destinace volume id, or the path to a destination")
    @opt('src', help="source volume id, or the path to source")
    def copy(self, src, dest):
        """
        Copy all data from one volume to another
            > lunr-storage-admin tools copy \
                /dev/lunr-volume/8275d343-365b-4966-a652-43271adbe9e5 \
                /dev/lunr-volume/b206deda-df9f-473c-8a72-b3df09dd3b1d
        """
        helper = self.load_conf(self.config)

        if not os.path.exists(src):
            src = helper.volumes.get(src)['path']

        if not os.path.exists(dest):
            dest = helper.volumes.get(dest)['path']

        if not confirm("Copy from '%s' to '%s'" % (src, dest)):
            return

        ISCSIDevice.copy_volume(src, dest)

    @opt('-b', '--byte', default='00',
         help='byte to use for scrubbing instead of zero')
    @opt('volume', help="volume id, or the path to a volume")
    def scrub(self, volume=None, byte=None):
        """
        Scrub a volume with 0x03:
            > lunr-storage-admin tools scrub -b 03 -v \
                /dev/lunr-volume/8275d343-365b-4966-a652-43271adbe9e5
        """
        byte = byte.decode('hex')
        if byte != '\x00':
            if not confirm('write %s to disk' % repr(byte)):
                return

        helper = self.load_conf(self.config)
        if os.path.exists(volume):
            path = volume
        else:
            path = helper.volumes.get(volume)['path']
        helper.volumes.scrub.scrub_volume(path, byte=byte)

    @opt('-p', '--percent', default=50, type=int,
         help='percentage of chunks to write to.')
    @opt('-w', '--write-size', default=4096, type=int,
         help='byptes to write to each chunk')
    @opt('volume', help="volume id, or the path to a volume")
    def randomize(self, volume=None, write_size=None, percent=None):
        """ Write random data to a percentage of the volume """
        if os.path.exists(volume):
            with open(volume) as f:
                size = os.lseek(f.fileno(), 0, os.SEEK_END)
            volume = {'path': volume, 'size': size}
        else:
            helper = self.load_conf(self.config)
            volume = helper.volumes.get(volume)

        total_num_blocks = int(volume['size'] / BLOCK_SIZE)
        num_writes = int(total_num_blocks * percent * 0.01)
        offsets = sorted(random.sample(xrange(total_num_blocks), num_writes))
        print "Writing '%s' bytes of urandom to the start of "\
            "'%s' chunks in %s" % (write_size, num_writes, volume['path'])
        with open(volume['path'], 'w') as f:
            for offset in offsets:
                if self.verbose:
                    print "writing block '%s'" % offset
                f.seek(offset * BLOCK_SIZE)
                f.write(os.urandom(write_size))

    @opt('-s', '--status', default='available',
         help='status')
    # TODO(could look this up in the api)
    @opt('-a', '--account', default=None,
         help='account_id')
    @opt('snapshot', help="snapshot id")
    def callback_snapshot(self, snapshot, status='available', account=None):
        """Fire a callback to cinder"""
        helper = self.load_conf(self.config)
        client = helper.get_cinder(account=account)
        client.update('snapshots', snapshot, status)

    @opt('id', help="The volume uuid of the clone (dest volume)")
    def cancel_clone(self, id):
        def clone_pid(id):
            for line in execute('ps', 'aux').split('\n'):
                if re.search('lunr-clone.*%s' % id, line):
                    return int(re.split('\s*', line)[1])
            raise RuntimeError("Unable to find pid for lunr-clone '%s'" % id)

        def dm_device(pid):
            for line in execute('lsof', '-p', str(pid)).split('\n'):
                if re.search('/dev/dm-', line):
                    return re.split('\s*', line)[8].lstrip('/dev/')
            raise RuntimeError("DM for lunr-clone pid '%s' not found" % pid)

        def snapshot_uuid(dm):
            for dir_path, dir_names, file_names in os.walk("/dev/mapper"):
                for file in file_names:
                    try:
                        path = os.path.join(dir_path, file)
                        link = os.readlink(path)
                        if re.search(dm, link):
                            return re.sub('--', '-', file)\
                                .lstrip('lunr-volume-')
                    except OSError:
                        pass
            raise RuntimeError("Unable to find snapshot_uuid for DM '%s'" % dm)

        def from_iscsi_connection(id):
            for line in execute('iscsiadm', '-m', 'session').split('\n'):
                if re.search(id, line):
                    conn = re.split('\s*', line)
                    ip, port, _ = re.split(':|,', conn[2])
                    print conn[3], ip, port
                    return ISCSIDevice(conn[3], ip, port)
            raise RuntimeError("Unable to find iscsi connection for '%s'" % id)

        def is_running(pid):
            try:
                os.kill(pid, 0)
                return True
            except OSError as err:
                if err.errno == errno.ESRCH:
                    return False
                raise

        helper = self.load_conf(self.config)
        # Find the clone process
        pid = clone_pid(id)
        # Find the snapshots device mapper name
        dm = dm_device(pid)
        # Find the uuid of the snapshot from the DM
        snapshot_id = snapshot_uuid(dm)
        # Kill the process
        attempts = 0
        while is_running(pid):
            print "-- PID is running: %d" % pid
            os.kill(pid, signal.SIGTERM)
            print "-- Killed PID: %d" % pid
            attempts += 1
            sleep(1)
            if attempts > 5:
                print "Attempted to kill '%d'; trying `kill -9`"
                os.kill(pid, signal.SIGKILL)

        # Disconnect from the clone volume
        iscsi = from_iscsi_connection(id)
        iscsi.disconnect()
        # Get the snapshot info
        snapshot = helper.volumes.get(snapshot_id)
        print "-- Snapshot to Scrub and Delete: %s" % snapshot['id']
        # Remove the lvm snapshot
        helper.volumes.remove_lvm_snapshot(snapshot)
        # Mark the clone volume as active
        print "-- Marking clone volume '%s' in cinder as available" % id
        client = helper.get_cinder()
        client.update('volumes', id, 'available')
        print "-- Marking clone volume '%s' in lunr as ACTIVE" % id
        helper.make_api_request('volumes', id, data={'status': 'ACTIVE'})

    @opt('-C', '--clean', default=False, action='store_const',
         const=True, help='Scrub and remove any non transient snapshots')
    def audit_snapshots(self, clean):
        """
        Display a list of snapshots that should not exist and
        need to be cleaned up
        """
        helper = self.load_conf(self.config)
        first = audit.snapshots(helper, self.verbose)
        if not len(first):
            return 0

        # Sleep for a second, then check again
        sleep(1)
        second = audit.snapshots(helper)

        # If the lists do not match, something must have
        # changed while we were sleeping
        if not audit.compare_lists(first, second, key='snapshot'):
            return self.audit_snapshots(clean=clean)

        # At this point we know the snapshots in the first list
        # Are verifiably bad, and not apart of a transient backup,
        # or clone operation
        print "\nThe following Snapshots should not exist, " \
            "and need to be cleaned up (Use --clean to remove these snapshots)"
        self.display(first)
        if clean:
            lvs = [lv for lv in helper.volumes._scan_volumes()
                   if lv['origin'] != '']
            for snap in first:
                for lv in lvs:
                    if lv[snap['id']] == snap['snapshot']:
                        helper.volumes.remove_lvm_snapshot(lv)
        return 1

    @noargs
    def audit_volumes(self):
        """
        Report any inconsistencies with node
        volumes and what the API reports about the volumes
        """
        helper = self.load_conf(self.config)
        first = audit.volumes(helper)
        if not len(first):
            return 0
        sleep(1)
        second = audit.volumes(helper)

        # If the lists do not match, something must have
        # changed while we were sleeping
        if not audit.compare_lists(first, second, key='volume'):
            return self.audit_volumes()

        print "\n The following Volumes failed to pass audit"
        self.display(first)
        return 1

    @noargs
    def audit_node(self):
        """
        Report any inconsistencies with node capacity
        and what the API reports about the capacity
        """
        helper = self.load_conf(self.config)
        first = audit.node(helper)
        if not first:
            return 0
        sleep(1)
        second = audit.node(helper)
        # If the results do not match, something must have
        # changed while we were sleeping
        if first != second:
            return self.audit_node()

        print "\n The Node failed to pass audit"
        self.display(first)
        return 1


def main(argv=sys.argv[1:]):
    logger.configure(log_to_console=True, level=logger.DEBUG,
                     lunr_log_level=logger.DEBUG, capture_stdio=False)

    # Create the top-level parser
    parser = SubCommandParser([VolumeConsole(), ExportConsole(),
                              BackupConsole(), ToolsConsole()],
                              desc=__doc__.strip())
    # execute the command requested
    try:
        return parser.run(argv)
    except StorageError, e:
        if parser.command.verbose:
            raise
        return '%s: %s' % (e.__class__.__name__, e)


if __name__ == "__main__":
    sys.exit(main())
