# Copyright (c) 2011-2015 Rackspace US, Inc.
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


from webob import Response
from webob.exc import HTTPNotFound, HTTPPreconditionFailed, HTTPBadRequest, \
    HTTPConflict

from lunr.storage.controller.base import BaseController, lock
from lunr.cinder import cinderclient
from lunr.common import logger
from lunr.storage.helper.utils import NotFound, AlreadyExists


class BackupController(BaseController):

    def index(self, req):
        try:
            volume = self.helper.volumes.get(self.volume_id)
        except NotFound:
            raise HTTPNotFound("No volume named '%s'" % self.volume_id)
        try:
            backups = self.helper.backups.list(volume)
        except NotFound:
            raise HTTPNotFound("No local backups for volume named '%s'" %
                               self.volume_id)
        return Response(backups)

    def show(self, req):
        try:
            volume = self.helper.volumes.get(self.volume_id)
        except NotFound:
            raise HTTPNotFound("No volume named '%s'" % self.volume_id)
        try:
            backup = self.helper.backups.get(volume, self.id)
        except NotFound:
            raise HTTPNotFound("No backup named %s running for "
                               "volume named '%s'" % (self.id, self.volume_id))
        return Response(backup)

    @lock("volumes/%(volume_id)s/resource")
    def create(self, req, lock):
        try:
            timestamp = req.params['timestamp']
        except KeyError:
            raise HTTPBadRequest("Most specify timestamp")

        if '.' in self.id:
            raise HTTPPreconditionFailed("backup id cannot contain '.'")

        try:
            snapshot = self.helper.volumes.create_snapshot(self.volume_id,
                                                           self.id, timestamp)
        except NotFound, e:
            raise HTTPNotFound(str(e))
        except AlreadyExists, e:
            raise HTTPConflict(str(e))

        cinder = None
        account = req.params.get('account')
        if account:
            cinder = self.helper.get_cinder(account)

        def callback():
            self.helper.volumes.delete(snapshot['id'])
            self.helper.make_api_request('backups', self.id,
                                         data={'status': 'AVAILABLE'})
            if cinder:
                try:
                    cinder.snapshot_progress(self.id, "100%")
                except cinderclient.CinderError, e:
                    logger.warning('Error updating snapshot progress: %s' % e)

        def error_callback():
            self.helper.volumes.delete(snapshot['id'])
            self.helper.make_api_request('backups', self.id,
                                         data={'status': 'ERROR'})

        self.helper.backups.create(snapshot, self.id, callback=callback,
                                   error_callback=error_callback,
                                   lock=lock, cinder=cinder)

        snapshot['status'] = 'SAVING'
        return Response(snapshot)

    @lock("volumes/%(volume_id)s/resource")
    def delete(self, req, lock):
        # We don't need a real volume.
        volume = {'id': self.volume_id}

        def callback():
            self.helper.make_api_request('backups', self.id,
                                         data={'status': 'AUDITING'})
            account = req.params.get('account')
            if account:
                cinder = self.helper.get_cinder(account)
                cinder.force_delete('snapshots', self.id)

        self.helper.backups.delete(volume, self.id,
                                   callback=callback, lock=lock)

        backup = {'id': self.id, 'status': 'DELETING'}
        return Response(backup)
