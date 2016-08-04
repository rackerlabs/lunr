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

from lunrclient.client import LunrClient, StorageClient
from lunr.cinder import cinderclient
from lunr.common.exc import ClientException
from lunr.cinder.cinderclient import CinderClient
from lunr.common import logger
from lunrclient.base import LunrError, LunrHttpError
import time

log = logger.get_logger('orbit.purge')


class PurgeError(Exception):
    pass


class FailContinue(PurgeError):
    pass


class NotFound(PurgeError):
    pass


class BadRequest(PurgeError):
    pass


class Purge:

    def __init__(self, tenant_id, conf):
        self.lunr_url = conf.string('terminator', 'lunr_url', 'http://localhost:8080')
        self.debug = conf.bool('terminator', 'debug', 'false')
        self.admin_tenant_id = conf.string('cinder', 'admin_tenant_id', None)
        self.config = conf
        self.lunr = LunrClient(tenant_id, timeout=10, url=self.lunr_url,
                               http_agent='cbs-purge-accounts',
                               debug=self.debug)
        self.cinder = CinderClient(tenant_id=self.admin_tenant_id, **cinderclient.get_args(conf))
        self.tenant_id = str(tenant_id)
        self.total = {}
        self.throttle = 1

    def log(self, msg):
        log.info("DDI: %s - %s" % (self.tenant_id, msg))

    @staticmethod
    def wait_on_status(func, status):
        for i in range(20):
            resp = func()
            if resp.status == status:
                return True
            time.sleep(1)
        return False

    def delete_backup(self, backup):
        # Skip backups already in a deleting status
        if backup['status'] in ('DELETING', 'DELETED'):
            self.log("SKIP - Backup %s in status of %s"
                     % (backup['id'], backup['status']))
            return False

        # Catch statuses we may have missed
        if backup['status'] != 'AVAILABLE':
            raise FailContinue("Refusing to delete backup %s in status of %s"
                               % (backup['id'], backup['status']))

        try:
            log.debug("Attempting to delete snapshot %s in status %s"
                      % (backup['id'], backup['status']))
            self.cinder.delete_snapshot(str(backup['id']))
        except NotFound:
            self.log("WARNING - Snapshot already deleted - Cinder returned "
                     "404 on delete call %s" % backup['id'])
            return True

        try:
            # Wait until snapshot is deleted
            if self.wait_on_status(lambda: self.cinder.get_snapshot(
                    str(backup['id'])), 'deleted'):
                return True
            raise FailContinue("Snapshot '%s' never changed to status of "
                               "'deleted'" % backup['id'])
        except NotFound:
            log.debug("Delete %s Success" % backup['id'])
            return True

    def is_volume_connected(self, volume):
        try:
            # Create a client with 'admin' as the tenant_id
            client = LunrClient('admin', url=self.lunr_url,
                                debug=self.debug)
            # Query the node for this volume
            node = client.nodes.get(volume['node_id'])
            # Build a node url for the storage client to use
            node_url = "http://%s:%s" % (node['hostname'], node['port'])
            # Get the exports for this volume
            payload = StorageClient(node_url, debug=self.debug)\
                .exports.get(volume['id'])
            return self._is_connected(payload)
        except LunrHttpError as e:
            if e.code == 404:
                return False
            raise

    @staticmethod
    def _is_connected(payload):
        if 'error' in payload:
            return False
        if payload:
            for session in payload.get('sessions', []):
                if 'ip' in session:
                    return True
        return False

    def clean_up_volume(self, volume):
        # Ask cinder for the volume status
        resp = self.cinder.get_volume(volume['id'])

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
                    return self.cinder.detach(volume['id'])
                except AttributeError:
                    raise FailContinue("rackspace_python_cinderclient_ext is not"
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

        # Catch statuses we may have missed
        if volume['status'] != 'ACTIVE':
            raise FailContinue("Refusing to delete volume %s in status of %s"
                               % (volume['id'], volume['status']))

        try:
            self.log("Attempting to delete volume %s in status %s"
                     % (volume['id'], volume['status']))
            self.cinder.delete_volume(str(volume['id']))
        except NotFound:
            self.log("WARNING - Volume already deleted - Cinder returned "
                     "404 on delete call %s" % volume['id'])
            return True

        try:
            # Wait until volume reports deleted
            if self.wait_on_status(lambda: self.cinder.get_volume(
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
            self.delete_quotas()

            # If we found anything to purge, report it here
            if self.total['volumes'] != 0 or self.total['backups'] != 0:
                verb = 'Found' if self.report_only else 'Purged'
                self.log("%s %s" % (verb, self.report(self.total)))
                return True
        except (LunrError, ClientException) as e:
            raise FailContinue(str(e))
        return False

    def delete_quotas(self):
        # (Quotas should return to defaults if there were any)
        # self.cinder.quotas.delete(self.tenant_id)

        # NOTE: The following is a temporary fix until we upgrade from havana

        # Get the default quotas
        defaults = self.cinder.quota_defaults()
        # Get the actual quotas for this tenant
        quotas = self.cinder.quota_get()
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
            self.cinder.quota_update(**updates)
