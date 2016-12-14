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


from lunr.orbit.jobs.suspects import AuditSuspects, BackupSuspects,\
        RestoreSuspects, ScrubSuspects, PruneSuspects
from lunr.orbit.jobs.terminatedfeedreader import TerminatedFeedReader
from lunr.orbit.jobs.purgeaccounts import PurgeAccounts
from lunr.orbit.daemon import Daemon, DaemonError
from lunr.orbit.jobs.detach import Detach
from lunr.common.config import LunrConfig
from lunr.orbit import Cron, CronError
from argparse import ArgumentParser
from lunr.common import logger
from lunr import db
import signal
import time
import sys
import os
import re


log = logger.get_logger('orbit')


def main():
    parser = ArgumentParser(description="Orbit, lunr's maintiance droid")
    parser.add_argument('-c', '--config', action='store',
                        help="Provide a config file for orbit to use")
    parser.add_argument('-p', '--pid', action='store',
                        help="Specify the file name of the pid to use")
    parser.add_argument('-u', '--user', action='store',
                        help="Specify the user the daemon will run as")
    parser.add_argument(
        'command', nargs='?', default='foreground',
        help="(start|stop|status|foreground) defaults to foreground")
    options = parser.parse_args()

    try:
        file = options.config or LunrConfig.lunr_orbit_config
        conf = LunrConfig.from_conf(file)
    except Exception, e:
        print "-- Config Failure: %s" % e
        parser.print_help()
        return 1

    if options.user:
        conf.set('orbit', 'user', options.user)
    if options.command == 'foreground':
        conf.set('orbit', 'foreground', True)
    if options.command != 'foreground' and not options.pid:
        print "--pid option required if command is (start|stop|status)"
        return 1

    # Init the daemon object
    daemon = Daemon(conf, options.pid)

    if options.command == 'stop':
        pid = daemon.alive()
        if pid:
            os.kill(pid, signal.SIGTERM)
            return 0
        print "-- Orbit not running"
        return 1

    if options.command == 'status':
        pid = daemon.alive()
        if pid:
            print "-- Orbit is running '%s' (%s)" % (daemon.pid_file, pid)
            return 0
        print "-- Orbit NOT running"
        return 1

    if not re.match('(start|foreground)', options.command):
        return parser.print_help()

    if options.command == 'start':
        if daemon.alive():
            print "-- Orbit Already running"
            return 1

    try:
        log.info("Starting Orbit..")
        with daemon:
            # load the logging config and get our log handle
            detach = conf.bool('orbit', 'foreground', False)
            logger.configure(conf.file, log_to_console=detach)
            # Connect to the database
            session = db.configure(conf)
            # TODO(thrawn): make this configurable
            # Pass in a list of jobs cron should run
            cron = Cron([AuditSuspects(conf, session),
                         BackupSuspects(conf, session),
                         RestoreSuspects(conf, session),
                         ScrubSuspects(conf, session),
                         PruneSuspects(conf, session),
                         Detach(conf, session),
                         TerminatedFeedReader(conf, session),
                         PurgeAccounts(conf, session)])
            # Run the cron
            return cron.run()
    except (DaemonError, CronError), e:
        log.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
