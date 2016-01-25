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
import os
from optparse import OptionParser
from migrate.versioning import shell
from lunr import db
from lunr.common.config import LunrConfig


class LunrMonkeyParser(shell.PassiveOptionParser, object):

    def __init__(self, *args, **kwargs):
        super(LunrMonkeyParser, self).__init__(*args, **kwargs)
        self.add_option('-C', '--config', default=LunrConfig.lunr_api_config,
                        help="override file name of lunr config")

    def parse_args(self, *args, **kwargs):
        options, args = super(LunrMonkeyParser, self).parse_args(*args,
                                                                 **kwargs)
        try:
            conf = LunrConfig.from_conf(options.config)
        except IOError, e:
            raise SystemExit('Error: %s' % e)
        options.url = conf.string('db', 'url',
                                  'sqlite:///' + conf.path('lunr.db'))
        options.repository = os.path.dirname(__file__)
        return options, args


def main():
    shell.PassiveOptionParser = LunrMonkeyParser
    return shell.main()

if __name__ == '__main__':
    sys.exit(main())
