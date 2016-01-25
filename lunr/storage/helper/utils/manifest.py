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

from bisect import bisect_left, bisect_right
import errno
import fcntl
from time import time
import json
from StringIO import StringIO
import os
from uuid import UUID

from lunr.common import logger
from lunr.storage.helper.utils import NotFound

MOST_RECENT = float('inf')

EMPTY_BLOCK = ''  # TODO: import this from block hash/salt magic


def convert_numeric_keys(d):
    n = {}
    for k, v in d.iteritems():
        try:
            n[int(k)] = v
        except ValueError:
            # luckily, int('0.0') raises a ValueError instead of truncating
            try:
                n[float(k)] = v
            except ValueError:
                n[k] = v
    return n


def _clear_cache(f):
    def wrapper(self, *args, **kwargs):
        if hasattr(self, '_history'):
            del self._history
        return f(self, *args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


class InvalidTimestampError(Exception):
    pass


class DuplicateBackupIdError(Exception):
    pass


class ManifestParseError(Exception):
    pass


class ManifestEmptyError(Exception):
    pass


class ManifestSaltError(Exception):
    pass


class ManifestVersionError(Exception):
    pass


class Manifest(dict):
    """
    A manifest is a description of how to replay a collection of
    backup chunks to rebuild a complete volume to a point in time.

    It's implimented as a mapping.

    The majority of keys are timestamps which map to a sparse diff of
    the chunks which are different from the current backup and the
    previous backup.

    There are four special keys.

    The first is the 'version' key, which identifies the version of the
    manifest.

    The second is the 'salt' key, which is used to hash the objects.

    The smallest timestamp key, is the "base" - it is a special value,
    represented as list rather than a mapping, which contains the hash
    values for all of the chunks on the volume at it's earliest
    restorable time.

    There is also a key 'backups' which maps backup_ids to timestamps.

    Example::

        {
            100: ['x00', 'x00', 'x00', 'x00', 'x00', 'x00', 'x00', 'x00'],
            101: {0: 'x01', 4: 'x01'},
            103: {0: 'x02', 5: 'x01'},
            107: {0: 'x03', 1: 'x02'},
            'backups': {'id0': 100, 'id1': 101, 'id2': 103, 'id3': 107},
        }

    To construct a full volume before a given time start with the list
    of chunks in the base and "replay" the diffs up until that time in
    accending order.

    """

    VERSION = '1.0'
    NAMED_KEYS = ['backups', 'version', 'salt']

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        if 'version' not in self:
            self['version'] = self.VERSION
        if 'salt' not in self:
            # Using os.random to avoid the "libuuid closed fd" bug.
            self['salt'] = str(UUID(bytes=os.urandom(16)))

    @property
    def version(self):
        try:
            return self['version']
        except KeyError:
            raise ManifestVersionError('Manifest has no version')

    @property
    def salt(self):
        try:
            return self['salt']
        except KeyError:
            raise ManifestSaltError('Manifest has no salt')

    @property
    def backups(self):
        """Shortcut to key 'backups'."""
        try:
            return self['backups']
        except KeyError:
            self['backups'] = {}
        return self['backups']

    @property
    def history(self):
        """
        Return the current sorted list of timestamps for all restorable states.

        The current sorted list is cached in the _history attribute, which will
        be deleted on calls to __setitem__ or __delitem__.
        """
        try:
            return self._history
        except AttributeError:
            self._history = sorted(k for k in self if k not in self.NAMED_KEYS)
        return self._history

    __setitem__ = _clear_cache(dict.__setitem__)
    __delitem__ = _clear_cache(dict.__delitem__)

    @property
    def base(self):
        """Shortcut to key 'base'."""
        try:
            return self[self.history[0]]
        except IndexError:
            try:
                self.base = [EMPTY_BLOCK for b in xrange(self._block_count)]
            except AttributeError:
                raise AttributeError('Manifest has block_count.')
        return self[self.history[0]]

    @base.setter
    def base(self, value):
        if len(self.history) > 1:
            raise AttributeError("Can't change base once the manifest has a "
                                 "backup history.")
        if not self.history:
            self[0] = value
        else:
            self[self.history[0]] = value

    @property
    def block_count(self):
        try:
            return len(self.base)
        except IndexError:
            return self._block_count

    @block_count.setter
    def block_count(self, value):
        if self.history:
            raise AttributeError("Can't change block_count on manifest with "
                                 "base of length %s" % self.block_count)
        self._block_count = value

    @property
    def block_set(self):
        # TODO(clayg): mash into replay for backups?  leave out for restores?
        blocks = set(self.base)
        for ts in self.history[1:]:
            for hash_ in self[ts].itervalues():
                blocks.add(hash_)
        return blocks

    def replay(self, until=MOST_RECENT):
        """
        Replay diffs until timestamp.

        :params until: a timestamp, stops playback, replay all if omitted
        """
        blocks = list(self.base)
        for ts in self.history[1:]:
            if ts > until:
                break
            for blockno, hash_ in self[ts].iteritems():
                blocks[blockno] = hash_
        return blocks

    def get_backup(self, backup_id):
        """
        Get the chunk list of the volume for a given backup

        :params backup_id: id of backup

        :returns: a list of hashes
        """
        try:
            try:
                ts = self.backups[backup_id]
            except KeyError:
                if not backup_id.isdigit():
                    raise
                ts = self.backups[int(backup_id)]
        except KeyError:
            if backup_id != 'base':
                raise
            ts = 0
        return self.replay(ts)

    def squash(self, start, until):
        """
        Squash the backups from start up to until and remove them.

        :params start: a timestamp, start time
        :params until: a timestamp, until time

        The timestamp params start and until do not need to be in
        valid keys or exist in history, they may be given as any
        number, interpreted as a unix timestamp to specify a arbitrary
        point in time.

        e.g.

        time.mktime((datetime.now() - timedelta(days=30)).timetuple())

        As a convenience you may dynamically specify the last timestamp in
        history by passing in MOST_RECENT (i.e. float('inf')) for until.

        """
        if until <= start:
            if until < start:
                raise ValueError('The value of util most be larger '
                                 'than start.')
            # == is noop
            return
        elif until == MOST_RECENT:
            try:
                until = self.history[-1]
            except IndexError:
                self.base
                until = self.history[-1]
        # find sorted insertion point before start if its in the list
        idx1 = bisect_left(self.history, start)

        if idx1 >= len(self.history):
            # some joker wants to squash diffs beyond the end of history?
            self[until] = {}
            return

        # find sorted insertion point after until if its in the list
        idx2 = bisect_right(self.history, until)

        ts = self.history[idx1]
        # use the first backup after start to use a basis for the squash
        squash_base = self[ts]
        # optomize out first update when squash_base is in in history
        del self[ts]
        # bring in idx2 because history is dynamic
        idx2 -= 1

        squash_diffs = self.history[idx1:idx2]

        if squash_diffs:
            # if squash_base is the manifest base, we can't dict.update
            try:
                updater = squash_base.update
            except AttributeError:
                def updater(diff):
                    for blockno, hash_ in diff.iteritems():
                        squash_base[blockno] = hash_

            # iterate over remaining diffs in the squash and update squash_base
            for ts in squash_diffs:
                updater(self[ts])
                del self[ts]

        # add squash_base at squash point
        self[until] = squash_base

    def delete_backup(self, backup_id):
        """
        Delete's a backup.  Squash the diff and remove the backup_id from
        backups.

        :params backup_id: id of backup
        """
        try:
            try:
                start = self.backups[backup_id]
            except KeyError:
                if not backup_id.isdigit():
                    raise
                backup_id = int(backup_id)
                start = self.backups[backup_id]
        except KeyError:
            raise NotFound('No backup %s' % backup_id)
        try:
            next = self.history[bisect_right(self.history, start)]
        except IndexError:
            # There is no next backup
            del self[start]
        else:
            self.squash(start, next)
        del self.backups[backup_id]

    def create_backup(self, backup_id, timestamp=None):
        """
        Create a new backup, and populate backup_id references.

        :params backup_id: id of backup
        """
        timestamp = float(timestamp or time())
        if backup_id in self.backups:
            raise DuplicateBackupIdError('A backup with id %s already exists' %
                                         backup_id)
        if not self.history:
            backup = self[timestamp] = [EMPTY_BLOCK for b in
                                        xrange(self._block_count)]
        else:
            if timestamp <= self.history[-1]:
                raise InvalidTimestampError(
                    "Backup timestamp can't be at the same time or before "
                    "the most recent backup.")
            backup = self[timestamp] = {}
        self.backups[backup_id] = timestamp
        return backup

    @classmethod
    def blank(cls, size):
        m = cls()
        m.block_count = size
        return m

    @classmethod
    def load(cls, raw_json_fp):
        content = json.load(raw_json_fp, object_hook=convert_numeric_keys)
        try:
            if content['version'] != cls.VERSION:
                raise ManifestVersionError(
                    'Unknown manifest version: %s' % content['version'])
        except KeyError:
            raise ManifestVersionError('Manifest has no version')
        # compat for old manifests
        if 'salt' not in content:
            content['salt'] = ''
        return cls(content)

    @classmethod
    def loads(cls, raw_json_string):
        content = json.loads(raw_json_string, object_hook=convert_numeric_keys)
        try:
            if content['version'] != cls.VERSION:
                raise ManifestVersionError(
                    'Unknown manifest version: %s' % content['version'])
        except KeyError:
            raise ManifestVersionError('Manifest has no version')
        # compat for old manifests
        if 'salt' not in content:
            content['salt'] = ''
        return cls(content)

    def dump(self, f):
        return json.dump(self, f)

    def dumps(self):
        return json.dumps(self)


LOCK_MAP = {}


def aquire_lock(local_cache_filename, type_=fcntl.LOCK_EX):
    try:
        fd = os.open(local_cache_filename, os.O_RDWR | os.O_CREAT)
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise
        os.makedirs(os.path.dirname(local_cache_filename))
        fd = os.open(local_cache_filename, os.O_RDWR | os.O_CREAT)
    fcntl.lockf(fd, type_)
    LOCK_MAP[local_cache_filename] = fd
    return fd


def release_lock(local_cache_filename, close=True):
    fd = LOCK_MAP.pop(local_cache_filename)
    fcntl.lockf(fd, fcntl.LOCK_UN)
    if close:
        os.close(fd)


def read_local_manifest(local_cache_filename):
    try:
        with open(local_cache_filename) as f:
            raw_json_string = f.read()
    except IOError, e:
        if e.errno != errno.ENOENT:
            raise
        raw_json_string = None
    if not raw_json_string:
        raise ManifestEmptyError('Manifest file at %s contained no content' %
                                 local_cache_filename)
    try:
        return Manifest.loads(raw_json_string)
    except ValueError:
        msg = 'Unable to parse manifest %s' % local_cache_filename
        logger.exception(msg + ':\n"""%s"""' % raw_json_string)
        raise ManifestParseError(msg + '.')


def load_manifest(conn, volume_id, lock_file):
    fd = aquire_lock(lock_file)
    op_start = time()
    _headers, raw_json_string = conn.get_object(volume_id, 'manifest',
                                                newest=True)
    duration = time() - op_start
    logger.info("STAT: load_manifest for %r Duration: %r" % (volume_id,
                                                             duration))
    manifest = Manifest.loads(raw_json_string)
    os.write(fd, raw_json_string)
    return manifest


def save_manifest(manifest, conn, volume_id, lock_file):
    fd = aquire_lock(lock_file)
    raw_json_string = manifest.dumps()
    os.ftruncate(fd, 0)
    os.write(fd, raw_json_string)
    op_start = time()
    conn.put_object(volume_id, 'manifest', raw_json_string)
    duration = time() - op_start
    logger.info("STAT: save_manifest for %r Duration: %r" % (volume_id,
                                                             duration))
    release_lock(lock_file)


def delete_manifest(conn, volume_id, lock_file):
    fd = aquire_lock(lock_file)
    op_start = time()
    conn.delete_object(volume_id, 'manifest')
    duration = time() - op_start
    logger.info("STAT: delete_manifest for %r Duration: %r" % (volume_id,
                                                               duration))
    os.remove(lock_file)
    release_lock(lock_file)


def main():
    from optparse import OptionParser
    from lunr.common.config import LunrConfig
    from lunr.db.console import DBConsole as ManifestConsole
    from lunr.storage.helper.utils.client import get_conn
    parser = OptionParser('%prog [options] volume_id')
    parser.add_option('-C', '--config', default=LunrConfig.lunr_storage_config,
                      help="override config file")
    options, args = parser.parse_args()

    try:
        conf = LunrConfig.from_conf(options.config)
    except IOError, e:
        return 'ERROR: %s' % e

    try:
        volume_id = args.pop(0)
    except IndexError:
        return 'ERROR: Specify volume_id'

    conn = get_conn(conf)
    _headers, raw_json_string = conn.get_object(volume_id, 'manifest',
                                                newest=True)
    manifest = Manifest.loads(raw_json_string)

    banner = "manifest for volume %s available as 'm'" % volume_id
    c = ManifestConsole(banner=banner, locals={'m': manifest})
    return c()

if __name__ == "__main__":
    import sys
    sys.exit(main())
