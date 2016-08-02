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

import lunrclient
from lunrclient.client import LunrClient, CinderClient, StorageClient
# from lunrclient import LunrError, LunrHttpError
from lunr.cinder.cinderclient import NotFound, ClientException, BadRequest


class Purge:

    def __init__(self, tenant_id, conf, options):
        self.creds = {

            'lunr_url' : self.parse(conf.string('terminator', 'span', 'hours=1'))
        }l
        self.lunr = LunrClient(tenant_id, timeout=10, url=lunr_url,
                               http_agent='cbs-purge-accounts',
                               debug=(options.verbose > 1))
        self.cinder = CinderClient(timeout=10, http_agent='cbs-purge-accounts',
                                   creds=conf, debug=(options.verbose > 1),
                                   logger=log)
        self.tenant_id = str(tenant_id)
        self.region = self.parse(conf.string('terminator', 'region', 'none'))
        # self.report_only = not options.force
        self.config = conf

#     def log(self, msg):
#         log.info("DDI: %s (%s) - %s" % (self.tenant_id, self.region, msg))
#
#     def debug(self, msg):
#         if self.verbose:
#             log.debug("DDI: %s (%s) - %s" % (self.tenant_id, self.region, msg))
#         else:
#             self.spin_cursor()
#
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
