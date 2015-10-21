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


import logging
import time

from webob import Request, Response
from webob.dec import wsgify


def filter_factory(global_conf, **local_conf):
    def stat_filter(app):
        @wsgify
        def log_response(req):
            start = time.time()
            resp = req.get_response(app)
            duration = time.time() - start
            if (hasattr(resp, 'status_int') and
                    hasattr(req, 'environ') and
                    'PATH_INFO' in req.environ):
                logging.info('GR-STAT Path: %s Status: %s Duration: %s',
                             req.environ['PATH_INFO'],
                             resp.status_int, duration)
            return resp
        return log_response

    return stat_filter
