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


from datetime import datetime, timedelta
from httplib import HTTPException
from json import loads
import re
import socket
from time import mktime, time
from urllib import urlencode
from urllib2 import Request, urlopen, URLError, HTTPError

from sqlalchemy.sql import func
from sqlalchemy.exc import OperationalError

from lunr.common.exc import HTTPClientError
from lunr.db.models import Backup, Volume
from lunr.orbit import CronJob
from lunr.common import logger


log = logger.get_logger('orbit.suspects')


class EmptyResponse():
    def __init__(self, code):
        self.code = code

    def getcode(self):
        return self.code


class Suspects(CronJob):

    def __init__(self, conf):
        CronJob.__init__(self)
        self.span = conf.string('suspects', 'span', 'hours=1')
        self.interval = conf.string('suspects', 'interval', 'seconds=5')
        self.timeout = conf.float('orbit', 'timeout', 120)

    def put(self, url, **kwargs):
        resp = self.request(url, method='PUT', **kwargs)
        return resp.getcode() == 200

    def delete(self, url, **kwargs):
        resp = self.request(url, method='DELETE', **kwargs)
        return resp.getcode() == 200

    def get(self, url, **kwargs):
        resp = self.request(url, method='GET', **kwargs)
        if resp.getcode() == 200:
            return loads(resp.read())
        return {}

    def request(self, url, method, **kwargs):
        req = Request(url, urlencode(kwargs))
        req.get_method = lambda *args, **kwargs: method

        try:
            return urlopen(req, timeout=self.timeout)
        except (HTTPError, URLError, HTTPException, socket.timeout), e:
            e = HTTPClientError(req, e)
            log.error(str(e))
            return EmptyResponse(e.code)


class BackupSuspects(Suspects):
    """
        Attempt to restart suspect backups that are taking to long

        The backups are suspect because they are taking a while to
        complete which may indicate they have failed, or they may
        just be taking a while. We don't know nor care, just try to
        restart the backup. If the backup is still running the storage
        node should return a 202, if the backup was not running the
        storage node should return a 201
    """

    def __init__(self, conf, session):
        Suspects.__init__(self, conf)
        self.span = self.parse(conf.string('backup-suspects', 'span',
                                           self.span))
        self.interval = self.parse(conf.string('backup-suspects', 'interval',
                                               self.interval))
        self._sess = session

    def suspects(self, interval, now):
        """
            Finds old backups that are still in a status of SAVING
        """
        delta = now - interval
        log.debug("Looking for suspect backups older than %s"
                  % delta.strftime("%Y-%m-%d %H:%M:%S"))
        return self._sess.query(Backup).filter(Backup.last_modified < delta)\
            .filter(Backup.status == 'SAVING')

    def url(self, backup):
        volume = backup.volume
        node = volume.node
        return 'http://%s:%s/volumes/%s/backups/%s' % (node.hostname,
                                                       node.port,
                                                       volume.id,
                                                       backup.id)

    def run(self, now=None):
        now = now or datetime.now()
        # Find all suspect backups that are older than span
        query = self.suspects(self.span, now=now)

        try:
            # Attempt to restart all the backups that are suspect
            for backup in query.all():
                log.info("Long running backup '%s' on node '%s'"
                         % (backup.id, backup.volume.node.id))
                # Re-create the timestamp used
                timestamp = int(mktime(backup.created_at.timetuple()))
                # Make a create call to the storage server
                if self.put(self.url(backup), timestamp=timestamp,
                            account=backup.account.id):
                    log.info("Restarted backup '%s' on node '%s'"
                             % (backup.id, backup.volume.node.id))
                    # Update last_modified so we don't restart next time
                    backup.last_modified = func.now()
            self._sess.commit()
        except OperationalError, e:
            logger.warning("DB error", exc_info=True)
            self._sess.close()


class RestoreSuspects(Suspects):
    """
        Attempt to restart suspect restore jobs that are taking to long
    """

    def __init__(self, conf, session):
        Suspects.__init__(self, conf)
        self.span = self.parse(conf.string('restore-suspects', 'span',
                                           self.span))
        self.interval = self.parse(conf.string('restore-suspects', 'interval',
                                               self.interval))
        self._sess = session

    def suspects(self, interval, now):
        """
            Finds restores that are still in a status of BUILDING
        """
        delta = now - interval
        log.debug("Looking for suspect restores older than %s"
                  % delta.strftime("%Y-%m-%d %H:%M:%S"))
        return self._sess.query(Volume).\
            filter(Volume.last_modified < delta).\
            filter(Volume.status == 'BUILDING')

    def run(self, now=None):
        now = now or datetime.now()
        try:
            for restore in self.suspects(self.span, now=now).all():
                log.info("Long running restore '%s' on node '%s'"
                         % (restore.id, restore.node.id))
                url = 'http://%s:%s/volumes/%s' % (restore.node.hostname,
                                                   restore.node.port,
                                                   restore.id)
                # Get the Backup the restore is of
                backup = self._sess.query(Backup).get(restore.restore_of)
                # Make a restore create call to the storage server
                if self.put(url, backup_source_volume_id=backup.volume_id,
                            backup_id=backup.id, size=backup.size,
                            account=backup.account.id):
                    log.info("Restarted restore job '%s' on node '%s'"
                             % (restore.id, restore.node.id))
                    # Update last_modified so we don't restart next time
                    restore.last_modified = func.now()
            self._sess.commit()
        except OperationalError, e:
            logger.warning("DB error", exc_info=True)
            self._sess.close()


class ScrubSuspects(Suspects):
    """
        Attempt to restart suspect scrub jobs that are taking to long
    """

    def __init__(self, conf, session):
        Suspects.__init__(self, conf)
        self.span = self.parse(conf.string('scrub-suspects', 'span',
                                           self.span))
        self.interval = self.parse(conf.string('scrub-suspects', 'interval',
                                               self.interval))
        self._sess = session

    def suspects(self, interval, now):
        """
            Finds scrubs that are still in a status of DELETING
        """
        delta = now - interval
        log.debug("Looking for suspect scrubs older than %s"
                  % delta.strftime("%Y-%m-%d %H:%M:%S"))
        return self._sess.query(Volume).\
            filter(Volume.last_modified < delta).\
            filter(Volume.status == 'DELETING')

    def run(self, now=None):
        now = now or datetime.now()
        try:
            for volume in self.suspects(self.span, now=now).all():
                log.info("Long running scrub '%s' on node '%s'"
                         % (volume.id, volume.node.id))
                url = 'http://%s:%s/volumes/%s' % (volume.node.hostname,
                                                   volume.node.port,
                                                   volume.id)
                # Make a volume create call to the storage server
                if self.delete(url):
                    log.info("Restarted scrub job '%s' on node '%s'"
                             % (volume.id, volume.node.id))
                    # Update last_modified so we don't restart next time we run
                    volume.last_modified = func.now()
            self._sess.commit()
        except OperationalError, e:
            logger.warning("DB error", exc_info=True)
            self._sess.close()


class AuditSuspects(Suspects):
    """
        Run audit on volumes that need it.
    """

    def __init__(self, conf, session):
        Suspects.__init__(self, conf)
        self.span = self.parse(conf.string('audit-suspects', 'span',
                                           self.span))
        self.interval = self.parse(conf.string('audit-suspects', 'interval',
                                               self.interval))
        self._sess = session

    def suspects(self, interval, now):
        """
            Finds backups that are still in a status of AUDITING
        """
        delta = now - interval
        log.debug("Looking for suspect audits older than %s"
                  % delta.strftime("%Y-%m-%d %H:%M:%S"))
        return self._sess.query(Backup).filter(Backup.last_modified < delta)\
            .filter(Backup.status == 'AUDITING')

    def url(self, backup, postfix):
        volume = backup.volume
        node = volume.node
        return 'http://%s:%s/volumes/%s%s' \
            % (node.hostname, node.port, volume.id, postfix)

    def locked(self, backup):
        # GET /volumes/{id}/lock
        resp = self.get(self.url(backup, '/lock'))
        if resp.get('in-use', False):
            # Is this a delete of a backup?
            if re.search(
                    '^DELETE /volumes/[A-Za-z0-9-]*/backups/[A-Za-z0-9-]*$',
                    resp['uri']):
                log.info("Purge job for '%s' is still running" % backup.id)
            # Is this a delete of a backup?
            elif re.search('^PUT /volumes/[A-Za-z0-9-]*/audit*$', resp['uri']):
                log.info("Audit job for '%s' is still running" % backup.id)
            else:
                log.info("Conflicting job still running: %s" % resp['uri'])
            return True
        return False

    def run(self, now=None):
        now = now or datetime.now()
        try:
            for backup in self.suspects(self.span, now=now).all():
                log.info("Backup audit suspect: '%s' on node '%s'"
                         % (backup.id, backup.volume.node.id))
                if not self.locked(backup):
                    # PUT /volumes/{id}/audit
                    #     ?backup_id=backup_id&account=account_id
                    if self.put(self.url(backup, '/audit'),
                                account=backup.account.id,
                                backup_id=backup.id):
                        log.info("Running audit for backup '%s' on node '%s'"
                                 % (backup.id, backup.volume.node.id))
                        # Update last_modified so we don't restart next time
                        backup.last_modified = func.now()
            self._sess.commit()
        except OperationalError, e:
            logger.warning("DB error", exc_info=True)
            self._sess.close()


class PruneSuspects(Suspects):
    """
        Rerun prune jobs that have failed.
    """

    def __init__(self, conf, session):
        Suspects.__init__(self, conf)
        self.span = self.parse(conf.string('prune-suspects', 'span',
                                           self.span))
        self.interval = self.parse(conf.string('prune-suspects', 'interval',
                                               self.interval))
        self._sess = session

    def suspects(self, interval, now):
        """
            Finds backups that are still in a status of DELETING
        """
        delta = now - interval
        log.debug("Looking for suspect prunes older than %s"
                  % delta.strftime("%Y-%m-%d %H:%M:%S"))
        return self._sess.query(Backup).\
            filter(Backup.last_modified < delta).\
            filter(Backup.status == 'DELETING')

    def volume_url(self, backup, postfix):
        volume = backup.volume
        node = volume.node
        return 'http://%s:%s/volumes/%s%s' % (node.hostname,
                                              node.port,
                                              volume.id,
                                              postfix)

    def url(self, backup):
        volume = backup.volume
        node = volume.node
        return 'http://%s:%s/volumes/%s/backups/%s' % (node.hostname,
                                                       node.port,
                                                       volume.id,
                                                       backup.id)

    def locked(self, backup):
        # GET /volumes/{id}/lock
        resp = self.get(self.volume_url(backup, '/lock'))
        if resp.get('in-use', False):
            # Is this a delete of a backup?
            if re.search(
                    '^DELETE /volumes/[A-Za-z0-9-]*/backups/[A-Za-z0-9-]*$',
                    resp['uri']):
                log.info("Purge job for '%s' is still running" % backup.id)
            # Is this a delete of a backup?
            elif re.search('^PUT /volumes/[A-Za-z0-9-]*/audit*$', resp['uri']):
                log.info("Audit job for '%s' is still running" % backup.id)
            else:
                log.info("Conflicting job still running: %s" % resp['uri'])
            return True
        return False

    def run(self, now=None):
        now = now or datetime.now()
        try:
            for backup in self.suspects(self.span, now=now).all():
                log.info("Long running prune '%s' on node '%s'"
                         % (backup.id, backup.volume.node.id))
                if not self.locked(backup):
                    # DELETE /volumes/{volume_id}/backups/{backup_id}
                    #        ?account=account.id
                    if self.delete(self.url(backup),
                                   account=backup.account.id):
                        log.info("Restarting prune '%s' on node '%s'"
                                 % (backup.id, backup.volume.node.id))
                        # Update last_modified so we don't restart next time
                        backup.last_modified = func.now()
            self._sess.commit()
        except OperationalError, e:
            logger.warning("DB error", exc_info=True)
            self._sess.close()
