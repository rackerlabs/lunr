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


from webob.exc import HTTPPreconditionFailed, HTTPNotFound, HTTPConflict, \
    HTTPBadRequest
from webob import Response

from lunr.api.controller.base import BaseController
from lunr.db import NoResultFound
from lunr.db.models import Node, VolumeType
from lunr.db.helpers import filter_update_params


class NodeController(BaseController):

    def index(self, request):
        """
        GET /v1.0/{account_id}/nodes

        List nodes
        """
        q = self.db.query(Node)
        available_filters = set(['name', 'status', 'volume_type_name'])
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

    def create(self, request):
        """
        POST /v1.0/{account_id}/nodes

        Create volume
        """
        params, meta_params = filter_update_params(request, Node)
        if not params.get('name'):
            raise HTTPPreconditionFailed("Must specify a 'name' parameter")
        self._validate_volume_type(params)
        try:
            params['size'] = int(params.get('size', 0))
        except ValueError:
            raise HTTPPreconditionFailed("'size' parameter must be an integer")
        params['meta'] = meta_params
        node = self.db.query(Node).filter_by(name=params['name']).first()
        if not node or node.status in ('DELETED', 'ERROR'):
            # create or update
            name = params.pop('name')
            params['status'] = params.get('status', 'ACTIVE')
            node, created = self.db.update_or_create(Node, updates=params,
                                                     name=name)
        else:
            raise HTTPConflict("Node '%s' already exists" % params['name'])
        self.db.refresh(node)
        return Response(dict(node))

    def delete(self, request):
        """
        DELETE /v1.0/{account_id}/nodes/{id}

        Delete volume
        """
        update_params = {'status': 'DELETED'}
        num_updated = self.db.query(Node).filter_by(
            id=self.id).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot delete non-existent node '%s'" %
                               self.id)
        n = self.db.query(Node).get(self.id)
        return Response(dict(n))

    def show(self, request):
        """
        GET /v1.0/{account_id}/nodes/{id}

        Show volume info
        """
        node = self.db.query(Node).get(self.id)
        if not node:
            raise HTTPNotFound("Cannot show non-existent node '%s'" %
                               self.id)
        # FIXME: create a helper to do a get with the sumfunc built-in
        node.calc_storage_used()
        return Response(dict(node))

    def update(self, request):
        """
        POST /v1.0/{account_id}/nodes/{id}

        Update volume info
        """
        update_params, meta_params = filter_update_params(request, Node)
        if meta_params:
            node = self.db.query(Node).get(self.id)
            node.meta.update(meta_params)
            update_params['meta'] = node.meta
        num_updated = self.db.query(Node).filter_by(
            id=self.id).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot update non-existent node '%s'" %
                               self.id)
        n = self.db.query(Node).filter_by(id=self.id).one()
        return Response(dict(n))
