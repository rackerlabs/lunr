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


from webob.exc import HTTPNotFound, HTTPPreconditionFailed
from webob import Response

from lunr.api.controller.base import BaseController
from lunr.db.models import VolumeType
from lunr.db.helpers import filter_update_params


class VolumeTypeController(BaseController):

    def index(self, request):
        """
        GET /v1.0/admin/volume_types

        List volume types
        """
        q = self.db.query(VolumeType)
        available_filters = set(['name', 'status'])
        filters = dict((k, v) for k, v in request.params.items() if k in
                       available_filters)
        if filters:
            q = q.filter_by(**filters)
        return Response([dict(r) for r in q.all()])

    def _fetch_valid_int_value(self, params, key):
        val = params.get(key)
        if val:
            try:
                val = int(val)
            except ValueError:
                raise HTTPPreconditionFailed("'%s' parameter must be an "
                                             "integer, not: %s" % (key, val))
        return val

    def _validate_size_range(self, min_size, max_size):
        if min_size is None and max_size is None:
            return
        if min_size is None or max_size is None:
            raise HTTPPreconditionFailed("'min_size' and 'max_size' must be "
                                         "both omitted or specified")
        if min_size > max_size:
            raise HTTPPreconditionFailed("'min_size' parameter must be <= "
                                         "'max_size' parameter")

    def _fetch_iops(self, params, key):
        val = params.get(key, 0)
        if val:
            try:
                val = int(val)
                if val < 0:
                    raise HTTPPreconditionFailed("'%s' must be > 0 " % key)

            except ValueError:
                raise HTTPPreconditionFailed("'%s' parameter must be an "
                                             "integer, not: %s" % (key, val))
        return val

    def create(self, request):
        """
        POST /v1.0/admin/volume_types

        Create volume type
        """
        name = request.params.get('name')
        min_size = self._fetch_valid_int_value(request.params, 'min_size')
        max_size = self._fetch_valid_int_value(request.params, 'max_size')
        self._validate_size_range(min_size, max_size)
        params = {'status': 'ACTIVE'}
        if min_size:
            params['min_size'] = min_size
        if max_size:
            params['max_size'] = max_size
        params['read_iops'] = self._fetch_iops(request.params, 'read_iops')
        params['write_iops'] = self._fetch_iops(request.params, 'write_iops')
        vt, created = self.db.update_or_create(VolumeType,
                                               updates=params, name=name)
        self.db.refresh(vt)
        return Response(dict(vt))

    def delete(self, request):
        """
        DELETE /v1.0/admin/volume_types/{name}

        Delete volume type
        """
        update_params = {'status': 'DELETED'}
        num_updated = self.db.query(VolumeType).filter_by(
            name=self.name).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot delete non-existent volume_type '%s'" %
                               self.name)
        vt = self.db.query(VolumeType).get(self.name)
        return Response(dict(vt))

    def show(self, request):
        """
        GET /v1.0/admin/volume_types/{name}

        Show volume type info
        """
        vt = self.db.query(VolumeType).get(self.name)
        if not vt:
            raise HTTPNotFound("Cannot delete non-existent volume_type '%s'" %
                               self.name)
        return Response(dict(vt))

    def update(self, request):
        """
        POST /v1.0/admin/volume_types/{name}

        Update volume type
        """
        update_params, _meta_params = filter_update_params(request, VolumeType)
        num_updated = self.db.query(VolumeType).filter_by(
            name=self.name).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot update non-existent volume_type '%s'" %
                               self.name)
        vt = self.db.query(VolumeType).get(self.name)
        return Response(dict(vt))
