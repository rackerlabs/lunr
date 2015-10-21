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
from lunr.common import logger
from lunr.storage.helper.utils import NotFound, AlreadyExists


class CloneController(BaseController):

    @lock("volumes/%(volume_id)s/resource")
    def create(self, req, lock):
        try:
            source = self.helper.volumes.get(self.volume_id)
        except NotFound:
            raise HTTPNotFound("No volume named '%s'" % self.volume_id)
        try:
            iqn = req.params['iqn']
        except KeyError:
            raise HTTPBadRequest("Must specify an export iqn")
        try:
            iscsi_ip = req.params['iscsi_ip']
        except KeyError:
            raise HTTPBadRequest("Must specify iscsi ip")
        try:
            iscsi_port = int(req.params.get('iscsi_port', 3260))
        except ValueError:
            raise HTTPPreconditionFailed("Port must be an integer")
        try:
            mgmt_host = req.params['mgmt_host']
        except KeyError:
            raise HTTPBadRequest("Must specify mgmt_host")
        try:
            mgmt_port = req.params['mgmt_port']
        except ValueError:
            raise HTTPBadRequest("Must specify mgmt_port")
        except KeyError:
            raise HTTPPreconditionFailed("Must specify mgmt_port")

        cinder = None
        account = req.params.get('account')
        if account:
            cinder = self.helper.get_cinder(account)

        def callback():
            path = '/volumes/%s/export' % self.id
            self.helper.node_request(mgmt_host, mgmt_port, 'DELETE', path)
            self.helper.make_api_request('volumes', self.id,
                                         data={'status': 'ACTIVE'})
            if cinder:
                cinder.delete_volume_metadata(self.id, 'clone-progress')

        self.helper.volumes.create_clone(self.volume_id, self.id, iqn,
                                         iscsi_ip, iscsi_port, cinder=cinder,
                                         callback=callback, lock=lock)

        return Response(source)
