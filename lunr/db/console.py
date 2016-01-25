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


from optparse import OptionParser
import readline
from sqlalchemy import func
from sqlalchemy.orm import Query
import sys

from lunr.common.config import LunrConfig
from lunr.common import logger
from lunr.db import models, helpers
from lunr import db

try:
    from IPython.Shell import IPShellEmbed

    class DBConsole(IPShellEmbed):

        def __init__(self, banner=None, locals=None):
            IPShellEmbed.__init__(self, argv=[], banner=banner,
                                  user_ns=locals)

except ImportError:
    from code import InteractiveConsole

    class DBConsole(InteractiveConsole):

        def __init__(self, banner=None, locals=None):
            self.banner = banner
            InteractiveConsole.__init__(self, locals=locals)

        def __call__(self):
            return self.interact(banner=self.banner)


def load_conf(options, args):
    if options.config:
        try:
            conf = LunrConfig.from_conf(options.config)
        except IOError, e:
            print 'Error: %s' % e
            sys.exit(1)
    else:
        try:
            conf = LunrConfig.from_api_conf()
        except IOError, e:
            conf = LunrConfig()
            print 'Warning: %s' % e

    if options.verbose:
        conf.set('db', 'echo', options.verbose)
    if args:
        conf.set('db', 'url', args[0])
    return conf


def main():
    parser = OptionParser('%prog [options] [DB_URL]')
    parser.add_option('-v', '--verbose', action='store_true',
                      help='make sqlalchemy noisy')
    parser.add_option('-C', '--config', help="override config file")
    parser.add_option('-c', '--command', help="execute command and quit")
    options, args = parser.parse_args()

    logger.configure(log_to_console=True, capture_stdio=False)
    # Attempt to load a config
    conf = load_conf(options, args)
    # Create the session used to connect to the database
    session = db.configure(conf)

    banner = "session object 'db' connected to %s" % session.bind.url
    share = {
        'db': session,
        'func': func,
        'Query': Query,
        'helpers': helpers,
    }
    # add all models
    for model in models.ModelBase.__subclasses__():
        share[model.__name__] = model

    if options.command:
        exec options.command in globals(), share
    else:
        c = DBConsole(banner=banner, locals=share)
        return c()

if __name__ == "__main__":
    sys.exit(main())
