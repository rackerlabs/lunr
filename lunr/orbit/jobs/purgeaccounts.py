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
from lunr.db.models import Event, Audit, Error
from lunr.common.purge import Purge, PurgeError, FailContinue

import time

log = logger.get_logger('orbit.purgeaccounts')


class PurgeAccounts(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.config = conf
        self.session = session
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.total = 0
        self.options = {'throttle': 1, 'verbose': True}

    def log_error_to_db(self, error, event=None, e_type="processing"):
        """ Log error/exception to db """
        event_id, tenant_id = None, None
        if event is not None:
            event_id = event.event_id
            tenant_id = event.tenant_id

        new_error = Error(event_id=event_id, tenant_id=tenant_id,
                          message=str(error), type=e_type)
        if not self.session.query(Error).filter(Error.message == str(error)).first():
            self.session.add(new_error)
            self.session.commit()

    def remove_errors(self, event=None, e_type=None):
        """ Remove the previous error on successful connection """
        try:
            error = self.session.query(Error).\
                filter(Error.type == e_type).\
                filter(Error.event_id == event.event_id).\
                first()
            if error is None:
                return
            self.session.delete(error)
            self.session.commit()
        except PurgeError as e:
            self.log_error_to_db(e)

    def run(self):
        log.info("purge accounts job is online")

        # accounts = self.fetch_accounts()
        # log.info("Feed returned '%d' tenant_id's to close" % len(accounts))
        account_counter = 0

        # Iterate over the list of deletable accounts
        for event in self.fetch_events():
            try:
                account = event.tenant_id
                self.run_purge(account)
                time.sleep(self.options['throttle'])
                # Mark the account as done
                self.save_to_audit(event)
                account_counter += 1
            except PurgeError as e:
                # Log the error and continue to attempt purges
                log.error("Purge for %s failed on event %s - %s" % (event.tenant_id, event.event_id, e))
                self.log_error_to_db(e, event)

        # Print out the purge totals
        log.info("Processed {0} accounts in this run".format(account_counter))
        self.print_totals()

    def print_totals(self):  # pragma: no cover
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
            purger = Purge(tenant_id, self.config)
            if purger.purge():
                # If we found something for this tenant
                self.collect_totals(purger)
                found = True

        except FailContinue:
            self.collect_totals(purger)
            raise

        if not found and self.options['verbose']:
            log.info("No Volumes or Backups to purge for '%s'" % tenant_id)
            return True
        if found or self.options['verbose']:
            log.info("Purge of '%s' Completed Successfully" % tenant_id)
        return True

    def fetch_events(self):
        events = self.session.query(Event).limit(100)
        return events

    def save_to_audit(self, event):
        record = Audit(event_id=event.event_id, tenant_id=event.tenant_id, type='TERMINATED')
        self.session.add(record)
        # Delete the processed event from queue
        self.session.delete(event)
        # self.session.commit()

