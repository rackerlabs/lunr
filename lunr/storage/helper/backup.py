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
from functools import partial
import json
import os
from os.path import join, exists
from setproctitle import setproctitle
from time import time

from lunr.common import logger, exc
from lunr.common.lock import ResourceFile

from lunr.storage.helper.utils import get_conn, NotFound, ServiceUnavailable
from lunr.storage.helper.utils.manifest import Manifest, read_local_manifest, \
    ManifestEmptyError
from lunr.storage.helper.utils.jobs import spawn
from lunr.storage.helper.utils.worker import BLOCK_SIZE, Worker


class BackupHelper(object):

    def __init__(self, conf):
        self.run_dir = conf.string('storage', 'run_dir', conf.path('run'))
        self.skip_fork = conf.bool('storage', 'skip_fork', False)
        self.conf = conf

    def _resource_file(self, id):
        return join(self.run_dir, 'volumes/%s/resource' % id)

    def _stats_file(self, id):
        return join(self.run_dir, 'volumes/%s/stats' % id)

    def _in_use(self, volume_id):
        resource_file = self._resource_file(volume_id)
        if not exists(resource_file):
            return False

        with ResourceFile(resource_file) as lock:
            # If the file is not in use
            return lock.used()

    def _is_a_backup_running(self, volume_id):
        used = self._in_use(volume_id)
        if not used:
            return False
        if 'stats' in used and 'volume_id' in used:
            return used
        return False

    def _backup_is_running(self, volume_id, backup_id):
        used = self._in_use(volume_id)
        if not used:
            return False
        if 'id' not in used or used['id'] != backup_id:
            return False
        return used

    def list(self, volume):
        """
        Find all manifest in local cache, and running backups
        """
        results = {}

        # Might be a backup running for this volume not yet in the manifest
        running = self._is_a_backup_running(volume['id'])
        if running:
            results.update({running['id']: 'RUNNING'})

        try:
            manifest_file = Worker.build_lock_path(self.run_dir, volume['id'])
            manifest = read_local_manifest(manifest_file)
        except ManifestEmptyError:
            return results

        for backup_id in manifest.backups:
            if self._backup_is_running(volume['id'], backup_id):
                job = self.get(volume, backup_id)
                job['ts'] = manifest.backups.get(backup_id)
                manifest.backups[backup_id] = job

        results.update(manifest.backups)
        return results

    def get(self, volume, backup_id):
        running = self._backup_is_running(volume['id'], backup_id)
        if not running:
            raise NotFound("no active backup running on '%s' called '%s'"
                           % (volume['id'], backup_id))
        stats_file = self._stats_file(volume['id'])
        with ResourceFile(stats_file) as lock:
            stats = lock.read()

        return {'lock': self._resource_file(volume['id']),
                'status': 'RUNNING',
                'stats': stats}

    def save(self, snapshot, backup_id, cinder):
        job_stats_path = self._stats_file(snapshot['origin'])
        logger.rename('lunr.storage.helper.backup.save')
        setproctitle("lunr-save: " + backup_id)
        size = snapshot['size'] / 1024 / 1024 / 1024

        try:
            op_start = time()
            worker = Worker(snapshot['origin'], conf=self.conf,
                            stats_path=job_stats_path)
        except exc.ClientException, e:
            if e.http_status != 404:
                raise
            op_start = time()
            conn = get_conn(self.conf)
            conn.put_container(snapshot['origin'])
            logger.warning("failed to retrieve manifest;"
                           " first time backup for this volume?")
            # TODO: write the block_size on the manifest at create?
            block_count, remainder = divmod(snapshot['size'], BLOCK_SIZE)
            if remainder:
                block_count += 1
            # initial backup is the only time the we need to worry about
            # creating a new manifest for the worker
            worker = Worker(snapshot['origin'], conf=self.conf,
                            manifest=Manifest.blank(block_count),
                            stats_path=job_stats_path)
        try:
            worker.save(snapshot['path'], backup_id,
                        timestamp=snapshot['timestamp'], cinder=cinder)
        finally:
            os.unlink(job_stats_path)
        duration = time() - op_start
        logger.info('STAT: worker save for backup_id %r on %r. '
                    'Size: %r GB Time: %r s Speed: %r MB/s' %
                    (backup_id, snapshot['path'], size,
                     duration, size * 1024 / duration))

    def create(self, snapshot, backup_id,
               callback=None, lock=None, cinder=None, error_callback=None):
        spawn(lock, self.save, snapshot, backup_id, cinder,
              callback=callback, error_callback=error_callback,
              skip_fork=self.skip_fork)

    def remove_container(self, volume):
        for attempt in range(0, 2):
            try:
                logger.info("Removing container '%s'" % volume['id'])
                conn = get_conn(self.conf)
                return conn.delete_container(volume['id'])
            except exc.ClientException, e:
                if e.http_status == 404:
                    return
                # Audit the backups, and try again
                self.audit(volume)

    def prune(self, volume, backup_id):
        logger.rename('lunr.storage.helper.backup.prune')
        setproctitle("lunr-prune: " + backup_id)

        try:
            op_start = time()
            worker = Worker(volume['id'], self.conf)
        except exc.ClientException, e:
            # If the manifest doesn't exist, We consider the backup deleted.
            # If anything else happens, we bail.
            if e.http_status != 404:
                raise
            logger.warning('No manifest found pruning volume: %s' %
                           volume['id'])
            return
        try:
            history = worker.delete(backup_id)
        except NotFound, e:
            logger.warning("backup_id: '%s' missing from manifest in prune" %
                           backup_id)
            return
        duration = time() - op_start
        logger.info('STAT: pruning %r. Time: %r s' % (backup_id, duration))

    def delete(self, volume, backup_id, callback=None, lock=None):
        spawn(lock, self.prune, volume, backup_id,
              callback=callback, skip_fork=self.skip_fork)

    def audit(self, volume):
        logger.rename('lunr.storage.helper.backup.audit')
        setproctitle("lunr-audit: " + volume['id'])
        try:
            op_start = time()
            worker = Worker(volume['id'], self.conf)
        except exc.ClientException, e:
            if e.http_status != 404:
                raise
            op_start = time()
            conn = get_conn(self.conf)
            conn.put_container(volume['id'])
            logger.warning("failed to retrieve manifest;"
                           " auditing volume with no backups")
            # creating a blank manifest for the worker
            worker = Worker(volume['id'], conf=self.conf,
                            manifest=Manifest.blank(0))
        worker.audit()
        duration = time() - op_start
        logger.info('STAT: auditing %r. Time: %r s ' % (volume['id'],
                                                        duration))

    def run_audit(self, volume, lock=None, callback=None):
        spawn(lock, self.audit, volume, callback=callback,
              skip_fork=self.skip_fork, interruptible=True)

    def status(self):
        conn = get_conn(self.conf)
        status = {'client': conn.__module__.rsplit('.', 1)[-1]}
        try:
            status.update(conn.head_account())
        except conn.ClientException, e:
            raise ServiceUnavailable(e)
        basic_types = (type(None), bool, int, basestring)
        for k, v in vars(conn).items():
            if k == 'key':
                continue
            if isinstance(v, basic_types):
                status[k] = v
        return status
