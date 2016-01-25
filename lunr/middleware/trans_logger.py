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

from logging import _levelNames as LEVEL_NAMES
from webob.dec import wsgify
from uuid import uuid4
from urllib import urlencode

from lunr.common.config import LunrConfig
from lunr.common import logger


def log_level(levelname):
    return LEVEL_NAMES[levelname]


def filter_factory(global_conf, **local_conf):
    section = 'filter:trans-logger'
    conf = LunrConfig({section: local_conf})

    echo = conf.bool(section, 'echo', False)
    level = conf.option(section, 'level', 'DEBUG', cast=log_level)
    name = conf.string(section, 'name', '')

    global_logger = logger
    if name:
        local_logger = logger.get_logger(name)
    else:
        local_logger = global_logger

    def trans_logger_filter(app):
        @wsgify
        def log_response(req):
            req.headers['X-Request-Id'] = req.headers.get(
                'x-request-id', 'lunr-%s' % uuid4())
            logger.local.request_id = req.headers['x-request-id']
            if echo:
                local_logger.log(level, 'REQUEST:\n%s', req)
            resp = req.get_response(app)
            resp.headers['X-Request-Id'] = req.headers['x-request-id']
            if req.params:
                request_str = '?'.join((req.path, urlencode(req.params)))
            else:
                request_str = req.path
            global_logger.info(' '.join(str(x) for x in (
                # add more fields here
                req.remote_addr or '-',
                '"%s %s"' % (req.method, request_str),
                resp.status_int,
                resp.content_length,
            )))
            if echo:
                local_logger.log(level, 'RESPONSE:\n%s', resp)
            logger.local.request_id = None
            return resp
        return log_response
    return trans_logger_filter
