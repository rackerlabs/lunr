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

from __future__ import print_function

from lunr.orbit import CronJob
from lunr.common import logger
from lunr.cinder import cinderclient
# import sqlalchemy.exc
from lunr.db.models import Event, Audit, Error, Marker
# from lunr.cinder.cinderclient import NotFound, ClientException, BadRequest

import time

log = logger.get_logger('orbit.purgeaccounts')


class PurgeEror(Exception):
    pass


class FailContinue(PurgeEror):
    pass


class PurgeAccounts(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.config = conf
        self.session = session
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.total = 0

    def run(self):
        log.info("purge accounts job is online")

        # accounts = self.fetch_accounts()
        # log.info("Feed returned '%d' tenant_id's to close" % len(accounts))
        throttle = 10

        # Iterate over the list of deletable accounts
        for account in self.fetch_accounts():
            try:
                tenant = account.tenant_id
                self.run_purge(tenant)
                time.sleep(throttle)
                # Mark the account as done
                self.put_done(account)
            except PurgeEror as e:
                # Log the error and continue to attempt purges
                log.error("Purge for '%s' failed - %s" % (tenant, e))

        # Print out the purge totals
        self.print_totals()

    def print_totals(self):
        log.info("Grand Total - %s " % self.total)

    def collect_totals(self, purger):
        self.total['volumes'] += purger.total['volumes']
        self.total['backups'] += purger.total['backups']
        self.total['backup-size'] += purger.total['backup-size']
        for key in purger.total['vtypes'].keys():
            try:
                self.total['vtypes'][key] += purger.total['vtypes'][key]
            except KeyError:
                self.total['vtypes'][key] = purger.total['vtypes'][key]

    def run_purge(self, tenant_id):
        found = False
        purger = None

        try:
            log.debug("Tenant ID: %s" % tenant_id)
            # purger = Purge(tenant_id, self.config, options)
            if purger.purge():
                # If we found something for this tenant
                self.collect_totals(purger)
                found = True

        except FailContinue:
            self.collect_totals(purger)
            raise

        if not found:
            log.info("No Volumes or Backups to purge for '%s'" % tenant_id)
            return True
        if found:
            log.info("Purge of '%s' Completed Successfully" % tenant_id)
        return True

    def fetch_accounts(self):
        events = self.session.query(Event).limit(100)
        return events

        # url = '/'.join([self.url, 'ready', self.app_id])
        # log.info("Fetching Tenant ID's from feed (%s)" % url)
        # resp = requests.get(url, headers={'X-Auth-Token': self.key},
        #                     timeout=10)
        # if resp.status_code == 401:
        #     raise Fail("Feed '%s' returned 401 UnAuthorized, use --register"
        #                "to re-register" % url)
        # resp.raise_for_status()
        # return resp.json()

    def put_done(self, event):
        record = Audit(event_id=event.event_id, tenant_id=event.tenant_id, type='TERMINATED')
        self.session.add(record)
        # Delete the processed event from queue
        self.session.delete(event)
        self.session.commit()
        # url = '/'.join([self.url, 'done', self.app_id, str(account)])
        # log.debug("Marking Tenant ID '%s' as DONE (%s)" % (account, url))
        # resp = requests.get(url, headers={'X-Auth-Token': self.key},
        #                     timeout=10)
        # resp.raise_for_status()
