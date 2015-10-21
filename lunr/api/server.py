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


import os
import sys

from webob.exc import HTTPNotFound, HTTPNotImplemented, HTTPMethodNotAllowed, \
    HTTPInternalServerError, HTTPError
from webob.dec import wsgify
from webob import Response

from lunr import db
from time import sleep
from lunr.common import logger
from lunr.api.urlmap import urlmap
from lunr.common.config import LunrConfig
from lunr.common.wsgi import LunrWsgiApp, wsgi_main
from sqlalchemy.exc import OperationalError


class ApiWsgiApp(LunrWsgiApp):
    def __init__(self, conf, urlmap, helper=None):
        super(ApiWsgiApp, self).__init__(conf, urlmap, helper)
        self.fill_percentage_limit = conf.float('placement',
                                                'fill_percentage_limit', 0.5)
        self.fill_strategy = conf.string('placement',
                                         'fill_strategy', 'broad_fill')
        self.image_convert_limit = conf.int('placement',
                                            'image_convert_limit', 1)
        self.node_timeout = conf.float('storage', 'node_timeout', 120)

    def _get_helper(self, conf):
        return db.configure(conf)

    def call(self, request):
        # Match the Request URL to an action
        action = self.match(request)
        for attempts in range(0, 3):
            try:
                result = action(request)
                self.helper.commit()
                return result
            except OperationalError, e:
                if hasattr(e, 'orig') and e.orig.args[0] == 2006:
                    # MySQL server has gone away
                    logger.warning("DB connection error attempt #%d"
                                   % attempts, exc_info=True)
                    sleep(2 ** attempts)
                    continue
                logger.exception("Database Error: %s" % e)
                raise HTTPInternalServerError("Internal database error")
            finally:
                self.helper.rollback()
                self.helper.close()
        raise HTTPInternalServerError("Unable to re-connect to database")


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI API server"""

    # Reload the paster config, since paster only passes us our config
    conf = LunrConfig.from_conf(global_conf['__file__'])
    # ensure global logger is named
    logger.rename(__name__)

    app = ApiWsgiApp(conf, urlmap)
    return app


def main():
    return wsgi_main(__name__, LunrConfig().lunr_api_config)
