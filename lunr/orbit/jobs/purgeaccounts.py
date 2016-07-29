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
import sqlalchemy.exc
from lunr.db.models import Event, Audit, Error, Marker
from lunr.cinder.cinderclient.exceptions import NotFound, ClientException, BadRequest
import lunrclient
from lunrclient import LunrClient, CinderClient, StorageClient
from requests.exceptions import RequestException
from lunrclient import LunrError, LunrHttpError
from os import path

import requests
import logging
import time
import sys
import os

log = logger.get_logger('orbit.purgeaccounts')


class Purge:

    def __init__(self, tenant_id, creds, options):
        self.lunr = LunrClient(tenant_id, timeout=10, url=creds['lunr_url'],
                               http_agent='cbs-purge-accounts',
                               debug=(options.verbose > 1))
        self.cinder = CinderClient(timeout=10, http_agent='cbs-purge-accounts',
                                   creds=creds, debug=(options.verbose > 1),
                                   logger=log)
        self.throttle = options.throttle
        self.verbose = options.verbose
        self.tenant_id = str(tenant_id)
        self.region = creds['region']
        self.report_only = not options.force
        self.creds = creds
        if not options.cursor:
            self.cursor = None

    def log(self, msg):
        log.info("DDI: %s (%s) - %s" % (self.tenant_id, self.region, msg))

    def debug(self, msg):
        if self.verbose:
            log.debug("DDI: %s (%s) - %s" % (self.tenant_id, self.region, msg))
        else:
            self.spin_cursor()

    def wait_on_status(self, func, status):
        for i in range(20):
            resp = func()
            if resp.status == status:
                return True
            time.sleep(1)
        return False

    def delete_backup(self, backup):
        # Skip backups already in a deleting status
        if backup['status'] in ('DELETING', 'DELETED'):
            self.debug("SKIP - Backup %s in status of %s"
                       % (backup['id'], backup['status']))
            return False

        if self.report_only:
            self.log("Found snapshot '%s' in status '%s'"
                     % (backup['id'], backup['status']))
            return True

        # Catch statuses we may have missed
        if backup['status'] != 'AVAILABLE':
            raise FailContinue("Refusing to delete backup %s in status of %s"
                               % (backup['id'], backup['status']))

        try:
            self.log("Attempting to delete snapshot %s in status %s"
                     % (backup['id'], backup['status']))
            self.cinder.volume_snapshots.delete(str(backup['id']))
        except NotFound:
            self.log("WARNING - Snapshot already deleted - Cinder returned "
                     "404 on delete call %s" % backup['id'])
            return True

        try:
            # Wait until snapshot is deleted
            if self.wait_on_status(lambda: self.cinder.volume_snapshots.get(
                    str(backup['id'])), 'deleted'):
                return True
            raise FailContinue("Snapshot '%s' never changed to status of "
                               "'deleted'" % backup['id'])
        except NotFound:
            self.log("Delete %s Success" % backup['id'])
            return True

    def is_volume_connected(self, volume):
        try:
            # Create a client with 'admin' as the tenant_id
            client = LunrClient('admin', url=self.creds['lunr_url'],
                                debug=(self.verbose > 1))
            # Query the node for this volume
            node = client.nodes.get(volume['node_id'])
            # Build a node url for the storage client to use
            node_url = "http://%s:%s" % (node['hostname'], node['port'])
            # Get the exports for this volume
            payload = StorageClient(node_url, debug=(self.verbose > 1))\
                .exports.get(volume['id'])
            return self._is_connected(payload)
        except LunrHttpError, e:
            if e.code == 404:
                return False
            raise

    def _is_connected(self, payload):
        if 'error' in payload:
            return False
        if payload:
            for session in payload.get('sessions', []):
                if 'ip' in session:
                    return True
        return False

    def clean_up_volume(self, volume):
        # Ask cinder for the volume status
        resp = self.cinder.volumes.get(volume['id'])

        # If the status is 'in-use'
        if resp.status == 'in-use':
            self.log("Cinder reports volumes is 'in-use', "
                     "checking attached status")
            # If the volume is NOT connected
            if not self.is_volume_connected(volume):
                # Force detach the volume
                try:
                    self.log("Volume '%s' stuck in attached state, "
                             "attempting to detach" % volume['id'])
                    return self.cinder.rackspace_python_cinderclient_ext\
                        .force_detach(volume['id'])
                except AttributeError:
                    raise Fail("rackspace_python_cinderclient_ext is not"
                               " installed, and is required to force detach")
            raise FailContinue("Volume '%s' appears to be still connected "
                               "to a hypervisor" % volume['id'])

    def delete_volume(self, volume):
        attempts = 0
        while True:
            try:
                if self._delete_volume(volume):
                    self.incr_volume(volume)
                return
            except BadRequest:
                if attempts > 0:
                    raise
                self.clean_up_volume(volume)
                attempts += 1
                continue

    def _delete_volume(self, volume):
        # Skip volumes in strange status
        if volume['status'] in ('NEW', 'DELETED'):
            self.debug("SKIP - Volume %s in status of %s"
                       % (volume['id'], volume['status']))
            return False

        if volume['status'] in ('ERROR', 'DELETING'):
            self.log("SKIP - Volume %s in status of %s"
                     % (volume['id'], volume['status']))
            return False

        if self.report_only:
            self.log("Found Volume '%s' in status '%s'"
                     % (volume['id'], volume['status']))
            return True

        # Catch statuses we may have missed
        if volume['status'] != 'ACTIVE':
            raise FailContinue("Refusing to delete volume %s in status of %s"
                               % (volume['id'], volume['status']))

        try:
            self.log("Attempting to delete volume %s in status %s"
                     % (volume['id'], volume['status']))
            self.cinder.volumes.delete(str(volume['id']))
        except NotFound:
            self.log("WARNING - Volume already deleted - Cinder returned "
                     "404 on delete call %s" % volume['id'])
            return True

        try:
            # Wait until volume reports deleted
            if self.wait_on_status(lambda: self.cinder.volumes.get(
                    str(volume['id'])), 'deleted'):
                return
            raise FailContinue("Volume '%s' never changed to status of"
                               " 'deleted'" % volume['id'])
        except NotFound:
            self.log("Delete %s Success" % volume['id'])
            return True

    def incr_volume(self, volume):
        self.total['volumes'] += 1
        try:
            self.total['vtypes'][volume['volume_type_name']] += volume['size']
        except KeyError:
            self.total['vtypes'][volume['volume_type_name']] = volume['size']

    def incr_backup(self, backup):
        self.total['backups'] += 1
        self.total['backup-size'] += backup['size']

    def purge(self):
        try:
            # Get a list of all backups
            for backup in self.lunr.backups.list():
                time.sleep(self.throttle)
                # Attempt to delete the backups
                if self.delete_backup(backup):
                    self.incr_backup(backup)

            # Delete all the volumes for this account
            for volume in self.lunr.volumes.list():
                time.sleep(self.throttle)
                if self.delete_volume(volume):
                    self.incr_volume(volume)

            # Delete any quotas for this account
            self.delete_quotas(self.tenant_id)

            # If we found anything to purge, report it here
            if self.total['volumes'] != 0 or self.total['backups'] != 0:
                verb = 'Found' if self.report_only else 'Purged'
                self.log("%s %s" % (verb, self.report(self.total)))
                return True
        except (LunrError, ClientException), e:
            raise FailContinue(str(e))
        return False

    def delete_quotas(self, tenant_id):
        # (Quotas should return to defaults if there were any)
        # self.cinder.quotas.delete(self.tenant_id)

        # NOTE: The following is a temporary fix until we upgrade from havana

        # Get the default quotas
        defaults = self.cinder.quotas.defaults(tenant_id)
        # Get the actual quotas for this tenant
        quotas = self.cinder.quotas.get(tenant_id)
        updates = {}
        for quota_name in quotas.__dict__.keys():
            # Skip hidden attributes on the QuotaSet object
            if quota_name.startswith('_'):
                continue
            # If the quota is different from the default, make it zero
            if getattr(quotas, quota_name) != getattr(defaults, quota_name):
                updates[quota_name] = 0

        if len(updates) > 0:
            self.log("Found non-default quotas, setting quotas [%s] to zero"
                     % ','.join(updates))
            self.cinder.quotas.update(tenant_id, **updates)


class PurgeAccounts(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.config = conf
        self.session = session
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        self.timeout = conf.float('orbit', 'timeout', 120)

    def run(self):
        log.info("purge accounts job is online")

        accounts = self.fetch_accounts()
        log.info("Feed returned '%d' tenant_id's to close" % len(accounts))
        throttle = 10

        # Iterate over the list of deletable accounts
        for account in accounts:
            try:
                self.run_purge(account)
                time.sleep(throttle)
                if options.force:
                    # Mark the account as done
                    self.put_done(account)
            except FailContinue as e:
                # Log the error and continue to attempt purges
                log.error("Purge for '%s' failed - %s" % (account, e))

        # Print out the purge totals
        self.print_totals()

    def print_totals(self):
        log.info("Grand Total - %s " % self.report(self.total))

    def collect_totals(self, purger):
        self.total['volumes'] += purger.total['volumes']
        self.total['backups'] += purger.total['backups']
        self.total['backup-size'] += purger.total['backup-size']
        for key in purger.total['vtypes'].keys():
            try:
                self.total['vtypes'][key] += purger.total['vtypes'][key]
            except KeyError:
                self.total['vtypes'][key] = purger.total['vtypes'][key]

    def run_purge(self, tenant_id, options):
        found = False
        purger = None

        try:
            log.debug("Tenant ID: %s" % tenant_id)
            purger = Purge(tenant_id, self.creds, options)
            if purger.purge():
                # If we found something for this tenant
                self.collect_totals(purger)
                found = True

        except FailContinue:
            self.collect_totals(purger)
            raise

        if not found and options.verbose:
            log.info("No Volumes or Backups to purge for '%s'" % tenant_id)
            return True

        if options.force:
            if options.verbose or found:
                log.info("Purge of '%s' Completed Successfully" % tenant_id)
        return True

    def fetch_accounts(self):
        url = '/'.join([self.url, 'ready', self.app_id])
        log.info("Fetching Tenant ID's from feed (%s)" % url)
        resp = requests.get(url, headers={'X-Auth-Token': self.key},
                            timeout=10)
        if resp.status_code == 401:
            raise Fail("Feed '%s' returned 401 UnAuthorized, use --register"
                       "to re-register" % url)
        resp.raise_for_status()
        return resp.json()

    def put_done(self, account):
        url = '/'.join([self.url, 'done', self.app_id, str(account)])
        log.debug("Marking Tenant ID '%s' as DONE (%s)" % (account, url))
        resp = requests.get(url, headers={'X-Auth-Token': self.key},
                            timeout=10)
        resp.raise_for_status()