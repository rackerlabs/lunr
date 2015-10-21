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
from webob.exc import HTTPNotFound

from lunr.storage.controller.base import BaseController


class StatusController(BaseController):

    def index(self, req):
        status = {
            'api': self.helper.api_status(),
            'volumes': self.helper.volumes.status(),
            'exports': self.helper.exports.status(),
            'backups': self.helper.backups.status(),
        }
        return Response(status)

    def api_status(self, req):
        return Response(self.helper.api_status())

    def conf_status(self, req):
        return Response(self.app.conf.values)

    def show(self, req):
        try:
            helper = getattr(self.helper, self.route['helper_type'])
        except (KeyError, AttributeError):
            raise HTTPNotFound("No status for '%s'" % req.path)
        status = helper.status()
        return Response(status)
