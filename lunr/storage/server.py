#!/usr/bin/env python
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


import sys
from urllib2 import HTTPError, URLError

from lunr.common.config import LunrConfig
from webob.exc import HTTPServiceUnavailable
from lunr.common.wsgi import wsgi_main, LunrWsgiApp
from lunr.storage.helper.utils import ServiceUnavailable
from lunr.common import logger
from lunr.storage.helper.base import Helper
from lunr.storage.urlmap import urlmap


class StorageWsgiApp(LunrWsgiApp):
    def __init__(self, conf, urlmap, helper=None):
        super(StorageWsgiApp, self).__init__(conf, urlmap, helper)

    def _get_helper(self, conf):
        return Helper(conf)

    def call(self, request):
        # Match the Request URL to an action
        action = self.match(request)
        try:
            return action(request)
        except ServiceUnavailable, e:
            msg = "Internal storage error: %s" % e
            logger.critical(msg)
            raise HTTPServiceUnavailable(msg)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI API server"""

    # Reload the paster config, since paster only passes us our config
    conf = LunrConfig.from_conf(global_conf['__file__'])

    # ensure global logger is named
    logger.rename(__name__)

    app = StorageWsgiApp(conf, urlmap)

    # Check for a valid volume config
    app.helper.volumes.check_config()

    try:
        app.helper.check_registration()
    except Exception:
        logger.exception('Registration failed')

    volumes = app.helper.volumes.list()
    app.helper.cgroups.load_initial_cgroups(volumes)
    app.helper.exports.init_initiator_allows()
    return app


def main():
    return wsgi_main(__name__, LunrConfig().lunr_storage_config)

if __name__ == "__main__":
    sys.exit(main())
