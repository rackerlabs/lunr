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


import hashlib
import logging
import lz4
import multiprocessing
import os
import Queue
import sys
import threading
from collections import defaultdict
from contextlib import contextmanager
from setproctitle import setproctitle, getproctitle
from StringIO import StringIO
from time import time

import simplejson

from lunr.cinder.cinderclient import CinderError
from lunr.common import logger
from lunr.common.lock import JsonLockFile
from lunr.storage.helper.utils import get_conn
from lunr.storage.helper.utils.manifest import Manifest, load_manifest, \
    save_manifest, delete_manifest, DuplicateBackupIdError

# TODO(clayg): need ability to override these from config
BLOCK_SIZE = 4 * 1024 ** 2  # 4 MB
NUM_WORKERS = 10
NUM_RESTORE_WORKERS = 5


class BlockReadFailed(Exception):
    pass


class SaveFailedInvalidCow(Exception):
    pass


def body_method(name):
    """
    Wrapper to expose a method on a Block object's compressed_body
    """
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, '_compressed_body'):
            self._compress_body()
        wrapped_method = getattr(self._compressed_body, name)
        return wrapped_method(*args, **kwargs)
    return wrapper


class Block(object):

    def __init__(self, block_dev, blockno, salt):
        self.path = block_dev
        self.blockno = blockno
        self.salt = salt
        self.stats = defaultdict(int)

    @contextmanager
    def timeit(self, key):
        before = time()
        yield
        self.stats[key] += time() - before

    def restored(self):
        self.stats['restored'] = 1

    def uploaded(self):
        self.stats['uploaded'] = 1

    def skipped(self):
        self.stats['skipped'] = 1

    def ignored(self):
        self.stats['ignored'] = 1

    @contextmanager
    def open(self, *args, **kwargs):
        try:
            with open(self.path, *args, **kwargs) as f:
                f.seek(self.blockno * BLOCK_SIZE)
                yield f
        except IOError, e:
            msg = (e.args, self, self.path, self.blockno,
                   self.blockno * BLOCK_SIZE)
            raise Exception('%s' % repr(msg))

    def _compress_body(self):
        decompressed = self.decompressed_body
        with self.timeit('compress'):
            data = lz4.compress(decompressed)
        self._compressed_body = StringIO(data)

    def _hydrate(self):
        """Populate hash and decompressed body attributes for block
        """
        hasher = hashlib.md5()
        read_amount = BLOCK_SIZE
        data = ''
        with self.open('rb') as fp:
            with self.timeit('read'):
                try:
                    data = fp.read(BLOCK_SIZE)
                except IOError:
                    raise BlockReadFailed()
            with self.timeit('hash'):
                hasher.update(data)
        with self.timeit('hash'):
            hasher.update(self.salt)
            self._hash = hasher.hexdigest()
        self._decompressed_body = data

    @property
    def decompressed_body(self):
        try:
            return self._decompressed_body
        except AttributeError:
            self._hydrate()
        return self._decompressed_body

    @property
    def hash(self):
        try:
            return self._hash
        except AttributeError:
            self._hydrate()
        return self._hash

    def __len__(self):
        orig_pos = self.tell()
        self.seek(0, os.SEEK_END)  # byte 0 realtive to end of file
        length = self.tell()
        self.seek(orig_pos)
        return length

    read = body_method('read')
    tell = body_method('tell')
    seek = body_method('seek')


def reinit_logging():
    # NOTE: explict close of syslog handler to force reconnect and suppress
    # traceback when the next log message goes and finds it's sockets fd is
    # inexplictly no longer valid, this is obviously jank
    # Manually nuking the logging global lock is the best thing ever.
    logging._lock = threading.RLock()
    log = logger.get_logger()
    root = getattr(log, 'logger', log).root
    for handler in root.handlers:
        try:
            # Re-create log handlers RLocks incase we forked during a locked
            # write operation; Not doing this may result in a deadlock the
            # next time we write to a log handler
            handler.createLock()
            handler.close()
        except AttributeError:
            pass


class StatsProcess(multiprocessing.Process):
    def __init__(self, cinder, cinder_id, stat_queue, block_count, stats_lock,
                 update_interval=1000):
        multiprocessing.Process.__init__(self)
        self.cinder = cinder
        self.cinder_id = cinder_id
        self.stat_queue = stat_queue
        self.block_count = block_count
        self.stats_lock = stats_lock
        self.update_interval = update_interval

    def update_cinder(self, percent):
        raise NotImplementedError()

    def handle_task(self, task):
        raise NotImplementedError()

    def get_stats(self):
        raise NotImplementedError()

    def get_percentage(self):
        raise NotImplementedError()

    def run(self):
        setproctitle("%s %s" % (self.__class__.__name__, getproctitle()))
        reinit_logging()
        first = True
        while True:
            task = self.stat_queue.get()
            if task is None:
                logger.debug("StatsProcess exiting...")
                break

            blocks_handled = self.handle_task(task)

            if first or blocks_handled % self.update_interval == 0:
                first = False
                percent = self.get_percentage()
                self.update_cinder(percent)
                stats = self.get_stats()
                if self.stats_lock:
                    try:
                        # Lock the file and write to it
                        with self.stats_lock as lock:
                            lock.update(stats)
                    except OSError, e:
                        logger.error('Failed updating stats: %s' % e)


class StatsSaveProcess(StatsProcess):
    def __init__(self, cinder, cinder_id, stat_queue, block_count, stats_lock,
                 update_interval=1000):
        super(StatsSaveProcess, self).__init__(cinder, cinder_id, stat_queue,
                                               block_count, stats_lock,
                                               update_interval)
        self.blocks_uploaded = 0
        self.blocks_read = 0
        self.upload_count = block_count

    @property
    def blocks_handled(self):
        return self.blocks_read + self.blocks_uploaded

    def handle_task(self, task):
        which, count = task
        if which == 'uploaded':
            self.blocks_uploaded += count
        elif which == 'read':
            self.blocks_read += count
        elif which == 'upload_count':
            self.upload_count = count
        return self.blocks_handled

    def get_percentage(self):
        all_blocks = self.block_count + self.upload_count
        return self.blocks_handled * 100.0 / all_blocks

    def get_stats(self):
        return {
            'blocks_read': self.blocks_read,
            'blocks_uploaded': self.blocks_uploaded,
            'block_count': self.block_count,
            'upload_count': self.upload_count,
            'progress': self.get_percentage(),
        }

    def update_cinder(self, percent):
        if self.cinder:
            try:
                self.cinder.snapshot_progress(self.cinder_id,
                                              "%.2f%%" % percent)
            except CinderError, e:
                logger.warning('Error updating snapshot progress: %s'
                               % e)


class StatsRestoreProcess(StatsProcess):
    def __init__(self, cinder, cinder_id, stat_queue, block_count, stats_lock,
                 update_interval=1000):
        super(StatsRestoreProcess, self).__init__(cinder,
                                                  cinder_id,
                                                  stat_queue,
                                                  block_count,
                                                  stats_lock,
                                                  update_interval)
        self.blocks_restored = 0

    def handle_task(self, task):
        which, count = task
        if which == 'restored':
            self.blocks_restored += count
        return self.blocks_restored

    def get_percentage(self):
        return self.blocks_restored * 100.0 / self.block_count

    def get_stats(self):
        return {
            'block_count': self.block_count,
            'blocks_restored': self.blocks_restored,
            'progress': self.get_percentage(),
        }

    def update_cinder(self, percent):
        if self.cinder:
            try:
                self.cinder.update_volume_metadata(
                    self.cinder_id,
                    {'restore-progress': "%.2f%%" % percent})
            except CinderError, e:
                logger.warning('Error updating restore-progress metadata: %s'
                               % e)


class RestoreProcess(multiprocessing.Process):
    def __init__(self, conf, volume_id, block_queue, result_queue, stat_queue,
                 salt):
        multiprocessing.Process.__init__(self)
        self.conn = get_conn(conf)
        self.volume_id = volume_id
        self.block_queue = block_queue
        self.result_queue = result_queue
        self.stat_queue = stat_queue
        self.stats = defaultdict(int)
        self.errors = defaultdict(int)
        self.salt = salt

    @property
    def empty_block_hash(self):
        try:
            return self._empty_block_hash
        except AttributeError:
            hasher = hashlib.md5()
            hasher.update(self.empty_block)
            hasher.update(self.salt)
            self._empty_block_hash = hasher.hexdigest()
        return self._empty_block_hash

    @property
    def empty_block(self):
        # alloc as needed
        return '\x00' * BLOCK_SIZE

    def _write_empty_block(self, hash_, block):
        logger.debug('Writing empty block %s - %s' % (
            block.blockno, hash_))
        with block.open('w+b') as f:
            with block.timeit('write'):
                f.write(self.empty_block)
        return block.stats

    def _restore_block(self, hash_, block):
        logger.debug("restore block: %s, hash: %s, empty_block_hash: %s" %
                     (block.blockno, hash_, self.empty_block_hash))
        if hash_ == self.empty_block_hash:
            return self._write_empty_block(hash_, block)

        with block.timeit('network_read'):
            _headers, body = self.conn.get_object(self.volume_id, hash_)

        with block.open('w+b') as f:
            with block.timeit('decompress'):
                decompressed = lz4.decompress(body)
            with block.timeit('write'):
                f.write(decompressed)
        block.restored()
        logger.debug('Restored Block "%s/%s"' % (self.volume_id, hash_))

    def run(self):
        setproctitle("%s %s" % (self.__class__.__name__, getproctitle()))
        reinit_logging()
        while True:
            task = self.block_queue.get()
            if task is None:
                self.result_queue.put((self.stats, self.errors))
                logger.debug('%s: exiting...' % self.name)
                self.block_queue.task_done()
                break
            try:
                block_dev, blockno, hash_ = task
                block = Block(block_dev, blockno, self.salt)
                self._restore_block(hash_, block)
                for stat in block.stats:
                    self.stats[stat] += block.stats[stat]
                stat_task = ('restored', 1)
                self.stat_queue.put(stat_task)
                logger.debug('Finished block write %s - %s : %s' % (
                    block.blockno, hash_, simplejson.dumps(block.stats)))
            except Exception:
                t, v, tb = sys.exc_info()
                self.errors[t.__name__] += 1
                logger.error('Error in block #%s' % block.blockno,
                             exc_info=(t, v, tb))

            self.block_queue.task_done()


class SaveProcess(multiprocessing.Process):
    def __init__(self, conf, volume_id, block_queue, result_queue, stat_queue):
        multiprocessing.Process.__init__(self)
        self.conn = get_conn(conf)
        self.volume_id = volume_id
        self.block_queue = block_queue
        self.result_queue = result_queue
        self.stat_queue = stat_queue
        self.stats = defaultdict(int)
        self.errors = defaultdict(int)

    def run(self):
        setproctitle("%s %s" % (self.__class__.__name__, getproctitle()))
        reinit_logging()
        while True:
            block = self.block_queue.get()
            if block is None:
                self.result_queue.put((self.stats, self.errors))
                logger.debug('%s: exiting...' % self.name)
                self.block_queue.task_done()
                break
            try:
                content_length = len(block)
                with block.timeit('network_write'):
                    self.conn.put_object(self.volume_id, block.hash, block,
                                         content_length=content_length)
                block.uploaded()
                block.stats['network_bytes'] += content_length
                for stat in block.stats:
                    self.stats[stat] += block.stats[stat]
                stat_task = ('uploaded', 1)
                self.stat_queue.put(stat_task)
                logger.debug('Finished upload %s - %s : %s' % (
                    block.blockno, block.hash, simplejson.dumps(block.stats)))
            except Exception:
                t, v, tb = sys.exc_info()
                self.errors[t.__name__] += 1
                logger.error('Error in block #%s' % block.blockno,
                             exc_info=(t, v, tb))

            self.block_queue.task_done()


class Worker(object):

    manifest_lock_path = 'volumes/%(volume_id)s/manifest'

    def __init__(self, volume_id, conf, manifest=None, stats_path=None):
        self.run_dir = conf.string('storage', 'run_dir', conf.path('run'))
        self.conf = conf
        self.conn = get_conn(conf)
        self.id = volume_id
        self.stats_lock = None
        if stats_path:
            with JsonLockFile(stats_path) as lock:
                self.stats_lock = lock

        self.block_queue = multiprocessing.JoinableQueue(NUM_WORKERS)
        self.result_queue = multiprocessing.Queue()
        self.stat_queue = multiprocessing.Queue()
        self.update_interval = conf.int('storage',
                                        'stats_update_interval', 1000)

        if manifest is None:
            self.manifest = load_manifest(self.conn, self.id,
                                          self._lock_path())
        else:
            self.manifest = manifest

    @classmethod
    def build_lock_path(cls, run_dir, volume_id):
        return os.path.join(run_dir, cls.manifest_lock_path % {
            'volume_id': volume_id})

    def _lock_path(self):
        return self.build_lock_path(self.run_dir, self.id)

    @property
    def hash_set(self):
        try:
            return self._hash_set
        except AttributeError:
            self._hash_set = self.manifest.block_set
        return self._hash_set

    @property
    def empty_block_hash(self):
        try:
            return self._empty_block_hash
        except AttributeError:
            hasher = hashlib.md5()
            hasher.update(self.empty_block)
            hasher.update(self.manifest.salt)
            self._empty_block_hash = hasher.hexdigest()
        return self._empty_block_hash

    @property
    def empty_block(self):
        # alloc as needed
        return '\x00' * BLOCK_SIZE

    def _needs_upload(self, block, manifest_head, manifest_diff):
        upload = True
        if block.hash == self.empty_block_hash:
            logger.debug('Skipping empty block %s - %s' % (
                block.blockno, block.hash))
            block.skipped()
            upload = False
        elif block.hash in self.hash_set:
            logger.debug('Skipping upload %s - %s' % (
                block.blockno, block.hash))
            block.skipped()
            upload = False
        elif manifest_head[block.blockno] == block.hash:
            logger.debug('Ignoring unchanged block %s - %s' % (
                block.blockno, block.hash))
            block.ignored()
            upload = False

        # Tell the manifest about the new block
        manifest_diff[block.blockno] = block.hash
        # Don't upload this one again
        self.hash_set.add(block.hash)

        return upload

    def wait_for_stats(self, process, queue):
        # Wait for the stats process to finish.
        while process.is_alive():
            process.join(5)  # Five seconds each.
            logger.info('Waiting for stats process to die, qsize: %d',
                        queue.qsize())

    def save(self, block_dev, backup_id, timestamp=None, cinder=None):
        try:
            diff = self.manifest.create_backup(backup_id, timestamp=timestamp)
        except DuplicateBackupIdError, e:
            logger.warning('Duplicate request to create existing backup.')
            return

        head = self.manifest.replay()

        processes = []
        for i in xrange(NUM_WORKERS):
            process = SaveProcess(self.conf, self.id, self.block_queue,
                                  self.result_queue, self.stat_queue)
            processes.append(process)

        stats_process = StatsSaveProcess(
            cinder, backup_id, self.stat_queue, self.manifest.block_count,
            self.stats_lock, update_interval=self.update_interval)
        processes.append(stats_process)

        for process in processes:
            process.start()

        total_results = defaultdict(int)
        block_failed = False
        blocks_to_upload = 0
        for blockno in xrange(self.manifest.block_count):
            block = Block(block_dev, blockno, self.manifest.salt)

            try:
                needs_upload = self._needs_upload(block, head, diff)
                stat_task = ('read', 1)
                self.stat_queue.put(stat_task)
            except BlockReadFailed:
                logger.error("BlockReadFailed blockno: %s" % blockno)
                block_failed = True
                break
            except:
                logger.exception("Unknown exception in worker.save")
                block_failed = True
                break

            if needs_upload:
                blocks_to_upload += 1
                self.block_queue.put(block)

            logger.debug("Block stats: %s" %
                         simplejson.dumps(block.stats))
            for stat in block.stats:
                total_results[stat] += block.stats[stat]

        stat_task = ('upload_count', blocks_to_upload)
        self.stat_queue.put(stat_task)

        # None is the special task to tell them to quit.
        for i in xrange(NUM_WORKERS):
            self.block_queue.put(None)

        # Waits until all tasks have been marked done.
        self.block_queue.join()

        # Now kill the stat worker
        self.stat_queue.put(None)

        logger.debug("Reading results")
        for i in xrange(NUM_WORKERS):
            result = self.result_queue.get()
            stats, errors = result
            logger.debug("Worker stats: %s" % simplejson.dumps(stats))
            logger.debug("Worker errors: %s" % simplejson.dumps(errors))
            for stat in stats:
                total_results[stat] += stats[stat]

        logger.info('Save job stats: %s' % simplejson.dumps(total_results))
        # Any block read that throws an IOError will cause the backup to fail
        # completely and have the snap removed by the controller.
        if block_failed:
            raise SaveFailedInvalidCow()

        save_manifest(self.manifest, self.conn, self.id, self._lock_path())

        self.wait_for_stats(stats_process, self.stat_queue)

    def _iterblocks(self):
        """
        Iterate over object listing for container yielding out the object name
        (block hash) for the blocks stored in the data store.
        """
        _headers, listing = self.conn.get_container(self.id)
        while listing:
            for obj in listing:
                if obj['name'] == 'manifest':
                    continue
                yield obj['name']
            _headers, listing = self.conn.get_container(self.id,
                                                        marker=obj['name'])

    def audit(self):
        block_set = set(self.manifest.block_set)
        ts = time()
        for hash_ in self._iterblocks():
            if hash_ not in block_set:
                logger.debug('Found Unreferenced Block "%s/%s.%s"' % (
                    self.id, hash_, ts))
                self.conn.delete_object(self.id, hash_,
                                        headers={'X-Timestamp': ts})

    def delete(self, backup_id):
        """Delete a backup.

        This used to actually delete the blocks. Now it only deletes the
        backup entry in the manifest. Audit does the actual block delete.
        """
        orig_block_set = set(self.manifest.block_set)
        # after this point a block could become valid for a new backup
        ts = time()
        self.manifest.delete_backup(backup_id)
        if self.manifest.history:
            new_block_set = set(self.manifest.block_set)
            save_manifest(self.manifest, self.conn, self.id, self._lock_path())
        else:
            new_block_set = set()
            delete_manifest(self.conn, self.id, self._lock_path())
        # Caller may want to know how many backups remain
        return self.manifest.history

    def restore(self, backup_id, block_dev, volume_id=None, cinder=None):
        logger.debug('Restoring %s/%s to %s' % (self.id, backup_id, block_dev))
        if not os.path.exists(block_dev):
            raise Exception('ENOENT on %s' % block_dev)
        backup = self.manifest.get_backup(backup_id)
        self.update_volume_metadata(cinder, volume_id,
                                    {'restore-progress': "%.2f%%" % 0})

        processes = []
        for i in xrange(NUM_RESTORE_WORKERS):
            process = RestoreProcess(self.conf, self.id, self.block_queue,
                                     self.result_queue, self.stat_queue,
                                     self.manifest.salt)
            processes.append(process)

        stats_process = StatsRestoreProcess(
            cinder, volume_id, self.stat_queue, self.manifest.block_count,
            self.stats_lock, update_interval=self.update_interval)
        processes.append(stats_process)

        for process in processes:
            process.start()

        total_results = defaultdict(int)
        blocks_to_upload = 0
        for blockno, hash_ in enumerate(backup):
            task = (block_dev, blockno, hash_)
            self.block_queue.put(task)

        # None is the special task to tell them to quit.
        for i in xrange(NUM_RESTORE_WORKERS):
            self.block_queue.put(None)

        # Waits until all tasks have been marked done.
        self.block_queue.join()

        # Now kill the stat worker
        self.stat_queue.put(None)

        for i in xrange(NUM_RESTORE_WORKERS):
            result = self.result_queue.get()
            stats, errors = result
            logger.debug("Worker stats: %s" % simplejson.dumps(stats))
            logger.debug("Worker errors: %s" % simplejson.dumps(errors))
            for stat in stats:
                total_results[stat] += stats[stat]

        logger.info('Restore job stats: %s' % simplejson.dumps(total_results))

        self.wait_for_stats(stats_process, self.stat_queue)

    def update_volume_metadata(self, cinder, volume_id, metadata):
        try:
            if volume_id and cinder:
                cinder.update_volume_metadata(volume_id, metadata)
        except CinderError, e:
            logger.warning('Error updating restore-progress metadata: %s' % e)
