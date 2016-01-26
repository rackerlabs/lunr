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


import random
import urllib2

from webob.exc import HTTPPreconditionFailed, HTTPConflict, HTTPNotFound, \
    HTTPServiceUnavailable, HTTPBadRequest
from webob import Response
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_

from lunr.api.controller.base import BaseController, NodeError
from lunr.common import logger
from lunr.db import NoResultFound
from lunr.db.models import Volume, VolumeType, Backup
from lunr.db.helpers import filter_update_params


class VolumeController(BaseController):

    def index(self, request):
        """
        GET /v1.0/{account_id}/volumes

        List volumes
        """
        q = self.account_query(Volume)
        available_filters = set(['status', 'account_id', 'node_id', 'id',
                                 'restore_of'])
        filters = dict((k, v) for k, v in request.params.items() if k in
                       available_filters)
        if filters:
            q = q.filter_by(**filters)
        return Response([dict(r) for r in q.all()])

    def _validate_volume_type(self, params):
        try:
            volume_type_name = params['volume_type_name']
        except KeyError:
            raise HTTPBadRequest("Must specify 'volume_type_name'")
        volume_type = self.db.query(VolumeType).get(volume_type_name)
        if not volume_type or volume_type.status != "ACTIVE":
            raise HTTPPreconditionFailed("Invalid volume type '%s'" %
                                         volume_type_name)
        return volume_type

    def _validate_backup(self, params):
        backup = params.get('backup')
        if not backup:
            return None
        try:
            backup = self.account_query(Backup).filter_by(id=backup).one()
        except NoResultFound:
            raise HTTPPreconditionFailed("No Backup '%s'" % backup)
        if backup.status != 'AVAILABLE':
            raise HTTPPreconditionFailed("Backup '%s' must be AVAILABLE, "
                                         "not '%s'" % (backup.id,
                                                       backup.status))
        return backup

    def _validate_source(self, params):
        source_volume = params.get('source_volume')
        if not source_volume:
            return None
        try:
            source = self.account_query(Volume) \
                .filter_by(id=source_volume) \
                .one()
        except NoResultFound:
            raise HTTPPreconditionFailed("No source '%s'" % source_volume)
        if source.status != 'ACTIVE':
            raise HTTPPreconditionFailed("Source '%s' must be 'ACTIVE', "
                                         "not '%s'" % (source.id,
                                                       source.status))
        if not source.node:
            raise HTTPPreconditionFailed("Source has no node.")
        return source

    def _validate_size(self, params, volume_type, backup=None, source=None):
        try:
            size = int(params['size'])
        except KeyError:
            raise HTTPBadRequest("Must specify 'size' parameter")
        except ValueError:
            raise HTTPPreconditionFailed("'size' parameter must be an "
                                         "integer")
        if size < volume_type.min_size or size > volume_type.max_size:
            raise HTTPPreconditionFailed("'size' parameter must be between "
                                         "%s and %s" % (volume_type.min_size,
                                                        volume_type.max_size))
        if backup:
            if size < backup.size:
                msg = "'size' must be >= backup size: %d" % backup.size
                raise HTTPPreconditionFailed(msg)

        if source:
            if size < source.size:
                msg = "'size' must be >= source volume size: %d" % source.size
                raise HTTPPreconditionFailed(msg)

        return size

    def _validate_affinity(self, params):
        affinity = params.get('affinity')
        if not affinity:
            return ''
        try:
            affinity_type, affinity_rule = affinity.split(':')
        except ValueError:
            msg = "Invalid affinity: %s" % affinity
            raise HTTPPreconditionFailed(msg)
        if affinity_type not in ('different_node', 'different_group'):
            msg = "Invalid affinity type: %s" % affinity_type
            raise HTTPPreconditionFailed(msg)
        return affinity

    def _validate_transfer(self, params):
        if 'account_id' in params:
            account = self.db.get_or_create_account(params['account_id'])
            if account.status != 'ACTIVE':
                raise HTTPNotFound('Account is not ACTIVE')

            try:
                volume = self.account_query(Volume).filter_by(id=self.id).one()
            except NoResultFound:
                raise HTTPNotFound("Cannot transfer non-existent volume '%s'" %
                                   self.id)

            if volume.active_backup_count() > 0:
                msg = "Volume must have no backups to modify account_id"
                raise HTTPPreconditionFailed(msg)

    def _assign_node(self, volume, backup, source, nodes):
        """
        Assigns the new volume to a node.

        :returns: dict, node response on successful placement

        :raises: HTTPError
        """
        request_params = {
            'account': self.account_id,
            'size': volume.size,
            'read_iops': volume.volume_type.read_iops,
            'write_iops': volume.volume_type.write_iops,
        }
        if volume.image_id:
            request_params['image_id'] = volume.image_id
        if backup:
            request_params['backup_source_volume_id'] = backup.volume.id
            request_params['backup_id'] = backup.id
            volume.restore_of = backup.id
        if source:
            request_params['source_volume_id'] = source.id
            request_params['source_host'] = source.node.hostname
            request_params['source_port'] = source.node.port

        last_node_error = None
        for node in nodes:
            volume.node = node
            self.db.commit()  # prevent duplicate/lost volumes
            try:
                path = '/volumes/%s' % self.id
                return self.node_request(node, 'PUT', path, **request_params)
            except NodeError, e:
                last_node_error = e
                # log server errors and continue
                if (e.code // 100) == 5:
                    logger.error(str(e))
                    continue
                # pass client error up to user, immediately
                break
        if not last_node_error:
            raise HTTPServiceUnavailable(
                "No available storage nodes for type '%s'" %
                volume.volume_type.name)
        volume.status = 'DELETED'
        self.db.commit()  # force commit before wsgi rollback
        # pass last error to user
        raise last_node_error

    def create(self, request):
        """
        PUT /v1.0/{account_id}/volumes/{id}?size=X&volume_type_name=Z

        Create volume
        """
        volume_type = self._validate_volume_type(request.params)
        backup = self._validate_backup(request.params)
        source = self._validate_source(request.params)
        size = self._validate_size(request.params, volume_type, backup, source)
        affinity = self._validate_affinity(request.params)
        image_id = request.params.get('image_id')
        status = 'NEW'
        imaging = False
        if image_id:
            # We don't want a huge race condition about scheduling with images
            # running on a node so we'll go ahead and create in that state.
            status = 'IMAGING'
            imaging = True

        nodes = self.get_recommended_nodes(volume_type.name, size,
                                           imaging=imaging,
                                           affinity=affinity)

        volume = Volume(id=self.id, account_id=self.account_id, status=status,
                        volume_type=volume_type, size=size, image_id=image_id)
        self.db.add(volume)

        # issue backend request(s)
        try:
            volume_info = self._assign_node(volume, backup, source, nodes)
        except IntegrityError:
            # duplicate id
            self.db.rollback()
            update_params = {
                'status': status,
                'size': size,
                'volume_type_name': volume_type.name,
                'image_id': image_id,
            }
            # optomistic lock
            count = self.db.query(Volume).\
                filter(and_(Volume.id == self.id,
                            Volume.account_id == self.account_id,
                            Volume.status.in_(['ERROR', 'DELETED']))).\
                update(update_params, synchronize_session=False)
            if not count:
                raise HTTPConflict("Volume '%s' already exists" % self.id)
            # still in uncommited update transaction
            volume = self.db.query(Volume).get(self.id)
            volume_info = self._assign_node(volume, backup, source, nodes)

        volume.status = volume_info['status']
        self.db.commit()
        response = dict(volume)
        response['cinder_host'] = volume.node.cinder_host
        return Response(response)

    def delete(self, request):
        """
        DELETE /v1.0/{account_id}/volumes/{id}

        Delete volume
        """
        update_params = {'status': 'DELETING', 'restore_of': None}
        num_updated = self.account_query(Volume).filter_by(id=self.id).\
            update(update_params)
        if not num_updated:
            raise HTTPNotFound("Cannot delete non-existent volume '%s'" %
                               self.id)
        volume = self.db.query(Volume).filter_by(id=self.id).one()
        if not volume.node:
            volume.status = 'DELETED'
        else:
            try:
                self.node_request(volume.node, 'DELETE', '/volumes/%s' %
                                  volume.id)
            except NodeError, e:
                if e.code == 404:
                    volume.status = 'DELETED'
                    self.db.commit()
                # Raise the exception to the wsgi layer,
                # so it can pass along the error to the user
                raise
        self.db.commit()
        return Response(dict(volume))

    def show(self, request):
        """
        GET /v1.0/{account_id}/volumes/{id}

        Show volume info
        """
        try:
            volume = self.account_query(Volume).filter_by(id=self.id).one()
        except NoResultFound:
            raise HTTPNotFound("Cannot show non-existent volume '%s'" %
                               self.id)
        return Response(dict(volume))

    # TODO I'm not sure that there's anything that can be updated anymore
    def update(self, request):
        """
        POST /v1.0/{account_id}/volumes/{id}

        Update volume info
        """
        self._validate_transfer(request.params)

        update_params, meta_params = filter_update_params(request, Volume)
        num_updated = self.account_query(Volume).\
            filter_by(id=self.id).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot update non-existent volume '%s'" %
                               self.id)
        volume = self.db.query(Volume).filter_by(id=self.id).one()
        return Response(dict(volume))
