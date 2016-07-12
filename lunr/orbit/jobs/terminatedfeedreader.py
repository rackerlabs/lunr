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

from __future__ import print_function

# import cloudfeedclient
from lunr.orbit import CronJob
# from sqlalchemy import create_engine,
# from sqlalchemy.orm import sessionmaker
from lunr.common import logger
import ConfigParser
import requests
from lunr.cinder import cinderclient
# import CinderClient get_args

# Base = declarative_base()

## TODO
#  - lunr cron job
#  - auth with orbit
#  - read events
#  - write to database
#  - Rename to indicate CRON
#  - Read about yield (Py)

# 1. Use cinderclient auth to get back a token
# 2. Pass token to cloudfeedsclient to start getting events.

log = logger.get_logger('orbit.terminatedfeedreader')


class TerminatedFeedReader(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=15'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self._sess = session
        self.config = conf

    def run(self, now=None):
        cinder = cinderclient.CinderClient(**cinderclient.get_args(self.config))
        print(cinder.token)
        url = self.config.string('terminator', 'dburl', 'none')
        # engine = create_engine(url, echo=True, pool_recycle=3600)
        # headers = {'Content-type': 'application/json'}
        # payload = {"auth": {"passwordCredentials": {"username": "terminator", "password": "PWD"}}}
        # token = requests.post(
        #     "http://localhost:6000/v2.0/tokens",
        #     json=payload, headers=headers)
        # print(token.json())
