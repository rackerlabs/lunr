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


from time import mktime
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, not_
from webob.exc import HTTPPreconditionFailed, HTTPNotFound, HTTPConflict
from webob import Response

from lunr.api.controller.base import BaseController, NodeError
from lunr.db import NoResultFound
from lunr.db.models import Backup, Volume
from lunr.db.helpers import filter_update_params


class BackupController(BaseController):

    def index(self, request):
        """
        GET /v1.0/{account_id}/backups

        List backups
        """
        q = self.account_query(Backup)
        available_filters = set(['status', 'account_id', 'id', 'volume_id'])
        filters = dict((k, v) for k, v in request.params.items() if k in
                       available_filters)
        if filters:
            q = q.filter_by(**filters)
        return Response([dict(r) for r in q.all()])

    def create(self, request):
        """
        PUT /v1.0/{account_id}/backups/{id}?volume={volume_id}

        Create backup
        """
        # extract volume reference
        try:
            volume_id = request.params['volume']
        except KeyError:
            raise HTTPPreconditionFailed("Must specify a 'volume' parameter")

        try:
            volume = self.account_query(Volume).\
                filter_by(id=volume_id).one()
        except NoResultFound:
            raise HTTPPreconditionFailed("Cannot create backup for "
                                         "non-existent volume '%s'" %
                                         volume_id)

        if volume.status not in ('ACTIVE', 'IMAGING_SCRUB'):
            raise HTTPPreconditionFailed(
                "Status of volume '%s' is '%s', not ACTIVE" % (
                    volume.id, volume.status))

        backup_count = volume.active_backup_count()
        if backup_count >= self.app.backups_per_volume:
            raise HTTPPreconditionFailed(
                "Volume '%s' already has %s out of %s allowed backups" % (
                    volume_id,
                    backup_count,
                    self.app.backups_per_volume))

        params = {
            'id': self.id,
            'account_id': self.account_id,
            'volume': volume,
            'status': 'NEW',
        }
        backup = Backup(**params)
        self.db.add(backup)
        try:
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            # optomistic lock
            count = self.db.query(Backup).filter(
                and_(Backup.id == self.id,
                     Backup.account_id == self.account_id,
                     Backup.status.in_(['ERROR', 'DELETED']))
            ).update(params, synchronize_session=False)
            if not count:
                raise HTTPConflict("Backup '%s' already exists" % self.id)
            # still in uncommited update transaction
            backup = self.db.query(Backup).get(self.id)

        try:
            path = '/volumes/%s/backups/%s' % (backup.volume.id, backup.id)
            params = {
                'account': self.account_id,
                'timestamp': int(mktime(backup.created_at.timetuple())),
            }
            info = self.node_request(backup.volume.node, 'PUT', path, **params)
        except NodeError:
            # Remove the backup record if the node failed to create the backup
            self.db.delete(backup)
            self.db.commit()  # force commit before wsgi rollback
            raise

        backup.status = 'SAVING'
        return Response(dict(backup))

    def delete(self, request):
        """
        DELETE /v1.0/{account_id}/backups/{id}

        Delete backup
        """
        try:
            backup = self.account_query(Backup).filter_by(id=self.id).\
                filter(not_(Backup.status.in_(['DELETED', 'AUDITING']))).one()
        except NoResultFound:
            raise HTTPNotFound(
                "Cannot delete non-existent backup '%s'" % self.id)

        # Any Active restore operations?
        if self.account_query(Volume)\
                .filter_by(restore_of=self.id).count():
            raise HTTPConflict("Cannot delete backup '%s' during restoring"
                               % self.id)

        path = '/volumes/%s/backups/%s' % (backup.volume.id, backup.id)
        # start background job to delete the chunks from swift
        self.node_request(backup.volume.node, 'DELETE', path,
                          account=self.account_id)

        # Mark the backup as deleting
        backup.status = 'DELETING'

        return Response(dict(backup))

    def show(self, request):
        """
        GET /v1.0/{account_id}/backups/{id}

        Show backup info
        """
        try:
            backup = self.account_query(Backup).\
                filter_by(id=self.id).one()
        except NoResultFound:
            raise HTTPNotFound("Cannot show non-existent backup '%s'" %
                               self.id)
        return Response(dict(backup))

    def update(self, request):
        """
        POST /v1.0/{account_id}/backups/{id}

        Update backup info
        """
        update_params, meta_params = filter_update_params(request, Backup)
        num_updated = self.account_query(Backup).\
            filter_by(id=self.id).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot update non-existent backup '%s'" %
                               self.id)
        backup = self.db.query(Backup).filter_by(id=self.id).one()
        return Response(dict(backup))
