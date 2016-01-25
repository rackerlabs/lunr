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
import json
import re
import socket
from time import mktime, time
from urllib import urlencode
from urllib2 import Request, urlopen, URLError, HTTPError

from sqlalchemy.sql import func
from sqlalchemy.exc import OperationalError

from lunr.common.exc import HTTPClientError
from lunr.cinder import cinderclient
from lunr.db.models import Export
from lunr.orbit import CronJob
from lunr.common import logger

log = logger.get_logger('orbit.detach')


class Detach(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.interval = self.parse(conf.string('detach', 'interval',
                                               'seconds=5'))
        self.span = self.parse(conf.string('detach', 'span', 'hours=1'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self._sess = session
        self.conf = conf

    def get(self, url, **kwargs):
        req = Request(url, urlencode(kwargs))
        req.get_method = lambda *args, **kwargs: 'GET'

        try:
            lines = urlopen(req, timeout=self.timeout).readlines()
            return json.loads(''.join(lines))
        except (HTTPError, URLError, socket.timeout), e:
            raise HTTPClientError(req, e)

    def run(self, now=None):
        now = now or datetime.now()
        # Find all suspect backups that are older than span
        query = self.find(self.span, now=now)

        try:
            # Attempt to clean up detached exports
            for export in query.all():
                log.info("Found stuck detach '%s' on node '%s'"
                         % (export.id, export.volume.node.id))
                # Ask the node if xen initiator is connected to the target
                if not self.connected(export.volume):
                    log.info("Asking cinder to complete the detach for '%s'"
                             % (export.id))
                    # Tell cinder about the detach
                    self.detach(export)
                    # Update last_modified so we don't restart next time
                    export.last_modified = func.now()
            self._sess.commit()
        except OperationalError, e:
            logger.warning("DB error", exc_info=True)
            self._sess.close()

    def find(self, interval, now):
        """
            Find volumes stuck in detaching
        """
        delta = now - interval
        log.debug("Looking for detaching volumes older than %s"
                  % delta.strftime("%Y-%m-%d %H:%M:%S"))
        return self._sess.query(Export).filter(Export.last_modified < delta)\
            .filter(Export.status == 'DETACHING')

    def detach(self, export):
        try:
            client = cinderclient.get_conn(self.conf,
                                           tenant_id=export.volume.account_id)
            client.detach(export.id)
            client.terminate_connection(export.id)
        except cinderclient.CinderError, e:
            log.error("While detaching %s - %s" % (export.id, e))

    def connected(self, volume):
        try:
            # Make a call to the node
            payload = self.get(
                'http://%s:%s/volumes/%s/export'
                % (volume.node.hostname, volume.node.port, volume.id))
        except HTTPClientError, e:
            if e.code != 404:
                log.error("%s" % e)
                return True
            log.info("Export for '%s' does not exist" % volume.id)
            return False

        # If any of the sessions are connected
        for session in payload.get('sessions', []):
            if session.get('connected', 'False') == True:
                return True

        return False
