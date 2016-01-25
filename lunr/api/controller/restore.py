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


from lunr.api.controller.base import BaseController
from webob.exc import HTTPNotFound
from lunr.db import NoResultFound
from lunr.db.models import Volume
from lunr.common import logger
from webob import Response


class RestoreController(BaseController):

    def _get_volume(self):
        """ Query for a volume by restore_of and id """
        try:
            backup_id = self.route.get('backup_id')
            return self.account_query(Volume).\
                filter_by(restore_of=backup_id, id=self.id).\
                one()
        except NoResultFound:
            raise HTTPNotFound("non-existent restore '%s' for backup '%s'"
                               % (self.id, backup_id))

    def index(self, request):
        """
        GET /v1.0/{account_id:admin}/backups/{backup_id}/restores

        List active restore for this backup
        """
        # Return all volumes where this backup is the restore source
        query = self.account_query(Volume).\
            filter_by(restore_of=self.route.get('backup_id'))
        return Response([dict(i) for i in query.all()])

    def show(self, request):
        """
        GET /v1.0/{account_id:admin}/backups/{backup_id}/restore/{id}

        Show detail retore info
        """
        return Response(dict(self._get_volume()))

    def delete(self, request):
        """
        DELETE /v1.0/admin/backups/{backup_id}/restore/{id}

        Delete the volume to backup reference
        """
        volume = self._get_volume()
        volume.restore_of = None
        return Response(dict(volume))
