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


import sys

from paste.script.serve import ServeCommand
from webob import Response
from webob.dec import wsgify
from webob.exc import HTTPNotFound, HTTPNotImplemented, HTTPError, \
    HTTPInternalServerError, HTTPException

from lunr.common import logger, config
from lunr.common.jsonify import encode
from StringIO import StringIO


class LunrWsgiApp(object):
    def __init__(self, conf, urlmap, helper=None):
        self.conf = conf
        self.urlmap = urlmap
        if helper:
            self.helper = helper
        else:
            self.helper = self._get_helper(conf)

    def _get_helper(conf):
        pass

    def match(self, request):
        route = self.urlmap.match(environ=request.environ)
        if route is None:
            raise HTTPNotFound("No route matched for path '%s'" % request.path)
        logger.debug("Request path: %s matched: %s" % (request.path,
                                                       repr(route)))

        try:
            # Try to call the controller and action
            controller = route['controller'](route, self)
            return controller.__getattribute__(route['action'])
        except AttributeError, e:
            # Couldn't find the action on the controller
            logger.debug(
                'No action (%s) for controller (%s) Error: %s' %
                (route.get('action', ''), route.get('controller', ''), e))
            raise HTTPNotImplemented("No action for path '%s'" % request.path)
        except Exception, e:
            logger.exception('controller/action FAIL: %s %s' % (e, route))
            raise HTTPInternalServerError('Internal route error')

    @wsgify
    def __call__(self, request):
        """Handle WSGI request."""
        try:
            try:
                # Call the implementation to handle the action
                return self.encode_response(self.call(request))
            except HTTPException:
                raise
            except Exception, e:
                logger.exception("Caught unknown exception, traceback dumped")
                raise HTTPInternalServerError("Internal controller error")
        except HTTPException, e:
            # Avoid logging HTTPOk Exceptions
            if isinstance(e, HTTPError):
                logger.error("%s" % e)
            # (thrawn01) returning a HTTPError exception to WSGI
            # container would result in a text/html content type
            # provided by webob, this is not desired
            body = {
                'reason': e.detail,
                'request-id': request.headers.get('x-request-id', '-')
            }
            return self.encode_response(Response(body=body, status=e.status))

    def call(self, request):
        raise HTTPNotImplemented("LunrWsgiApp.call() not implemented")

    def encode_response(self, result):
        # TODO:(thrawn01) Do some content negotiation, did the url end
        # in .json or .xml or include accept headers
        result.body = encode(result.body) + '\n'
        result.content_type = 'application/json; charset=UTF-8'
        return result

    def log_config(self, conf):
        s = StringIO()
        conf.write(s)
        s.seek(0)
        logger.info("LunrConfig:")
        for line in s:
            logger.info(line.rstrip())


class LunrServeCommand(ServeCommand):

    def __init__(self, name):
        self.name = name if name != '__main__' else None
        ServeCommand.__init__(self, '')

    def logging_file_config(self, config_file):
        if getattr(self.options, 'daemon', False):
            logger.configure(config_file, self.name)
        else:
            logger.configure(config_file, self.name, log_to_console=True,
                             lunr_log_level=logger.DEBUG, level=logger.WARNING)


def wsgi_main(name, default_config):
    args = sys.argv[1:]
    runner = LunrServeCommand(name)
    found_config = False
    for arg in args:
        if arg in runner.possible_subcommands:
            # config can't appear after subcommand
            break
        elif not arg.startswith('-'):
            # found config arg
            found_config = True
            break
    if not found_config:
        args.insert(0, default_config)
    return runner.run(args)
