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

import netaddr
import string

from webob import Response
from webob.exc import HTTPNotFound, HTTPPreconditionFailed

from lunr.common import logger
from lunr.api.controller.base import BaseController, NodeError
from lunr.db import NoResultFound
from lunr.db.models import Export, Volume
from lunr.db.helpers import filter_update_params


class ExportController(BaseController):

    def _validate_ip(self, ip):
        if not ip:
            # Use '' because None gets urlencoded to "None"...
            return ''
        try:
            netaddr.IPAddress(ip)
            return ip
        except:
            raise HTTPPreconditionFailed("Invalid ip: '%s'" % ip)

    def create(self, request):
        """
        PUT /v1.0/{account_id}/volumes/{id}/export?ip=X&initiator=Y

        Create export
        """
        try:
            volume = self.account_query(Volume).filter_by(id=self.id).one()
        except NoResultFound:
            raise HTTPNotFound(
                "Cannot create export for non-existent volume '%s'" % self.id)

        ip = self._validate_ip(request.params.get('ip'))

        resp = self.node_request(volume.node, 'PUT',
                                 '/volumes/%s/export' % volume.id, ip=ip)

        params = {
            'status': 'ATTACHING',
            'ip': ip,
            'initiator': request.params.get('initiator', ''),
            'target_name': resp['name']
        }
        export, created = self.db.update_or_create(
            Export, params, id=volume.id)
        return Response(dict(export))

    def delete(self, request):
        """
        DELETE /v1.0/{account_id}/volumes/{id}/export

        Delete export
        """
        try:
            volume = self.account_query(Volume).filter_by(id=self.id).one()
        except NoResultFound:
            raise HTTPNotFound(
                "Cannot delete export for non-existent volume '%s'" % self.id)

        if not volume.export:
            raise HTTPNotFound("No export found for volume: '%s'" % self.id)

        try:
            force = request.params.get('force', False)
            initiator = request.params.get('initiator', '')
            self.node_request(
                volume.node,
                'DELETE', '/volumes/%s/export' % volume.id,
                force=force,
                initiator=initiator)
        except NodeError, e:
            if e.code != 404:
                raise

        volume.export.status = 'DELETED'
        return Response(dict(volume.export))

    def show(self, request):
        """
        GET /v1.0/{account_id}/volumes/{id}/export

        Show export info
        """
        try:
            volume = self.account_query(Volume).filter_by(id=self.id).one()
        except NoResultFound:
            raise HTTPNotFound(
                "Cannot show export for non-existent volume '%s'" % self.id)

        if not volume.export:
            raise HTTPNotFound("Export not found for volume '%s'" % self.id)

        return Response(dict(volume.export))

    def update(self, request):
        """
        POST /v1.0/{account_id}/volumes/{id}/export

        Update export info.
        Params: status, instance_id, and mountpoint.
        This also pulls the connected ip and initiator from the storage node.
        """
        try:
            volume = self.account_query(Volume).filter_by(id=self.id).one()
        except NoResultFound:
            raise HTTPNotFound(
                "Cannot update export for non-existent volume '%s'" % self.id)

        update_params, meta_params = filter_update_params(request, Export)

        try:
            node_export = self.node_request(volume.node, 'GET',
                                            '/volumes/%s/export' % volume.id)
        except NodeError, e:
            logger.info('Node error fetching export: %s' % volume.id)
            node_export = {}

        sessions = node_export.get('sessions', [])
        if sessions:
            update_params['session_ip'] = sessions[0].get('ip', '')
            update_params['session_initiator'] = sessions[0].get('initiator',
                                                                 '')

        export, created = self.db.update_or_create(
            Export, update_params, id=volume.id)
        return Response(dict(export))
