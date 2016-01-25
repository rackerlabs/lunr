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

import netaddr
import string

from webob import Response
from webob.exc import HTTPNotFound, HTTPPreconditionFailed, \
    HTTPInternalServerError, HTTPConflict, HTTPBadRequest

from lunr.common import logger
from lunr.common.config import Config
from lunr.storage.controller.base import BaseController, lock
from lunr.storage.helper.volume import NotFound, AlreadyExists
from lunr.storage.helper.utils import ServiceUnavailable, ResourceBusy


class ExportController(BaseController):

    def _filter_ip(self, ip):
        if not ip:
            return None
        try:
            return netaddr.IPAddress(ip)
        except:
            raise HTTPPreconditionFailed("Invalid ip: '%s'" % ip)

    def show(self, req):
        try:
            export = self.helper.exports.get(self.volume_id)
        except NotFound:
            raise HTTPNotFound("No export for volume: '%s'" % self.volume_id)
        return Response(export)

    def create(self, req):
        try:
            ip = self._filter_ip(req.params.get('ip'))
            export = self.helper.exports.create(self.volume_id, ip)
        except NotFound:
            raise HTTPNotFound("No volume named: '%s'" % self.volume_id)
        except AlreadyExists:
            export = self.helper.exports.get(self.volume_id)
        return Response(export)

    def delete(self, req):
        try:
            force = Config.to_bool(req.params.get('force', False))
            initiator = req.params.get('initiator')
            export = self.helper.exports.delete(self.volume_id, force=force,
                                                initiator=initiator)
        except NotFound:
            raise HTTPNotFound("No export for volume: '%s'" % self.volume_id)
        except ResourceBusy, e:
            raise HTTPConflict(str(e))
        return Response()
