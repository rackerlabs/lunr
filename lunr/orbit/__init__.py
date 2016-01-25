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


from datetime import timedelta, datetime
from lunr.common import logger
import time
import sys


log = logger.get_logger('orbit')


class CronError(Exception):
    pass


class CronJob(object):
    def __init__(self):
        # Set by our subclass, the interval job should run
        self.interval = timedelta(seconds=1)
        self.last_run = 0

    def __call__(self):
        # Is it our time to run?
        if time.time() >= self.next():
            # record that we are running the job
            self.last_run = time.time()
            self.run()
        # return the time of our next run
        return self.next()

    def next(self):
        """ Calculates the time of our next run, from epoch """
        delta = datetime.fromtimestamp(self.last_run) + self.interval
        return time.mktime(delta.timetuple())

    def parse(self, string):
        """ parse format 'hours=1, minutes=2, days=2' into a dict """
        result = {}
        try:
            for arg in string.split(','):
                lvalue, rvalue = arg.split('=')
                result[lvalue.strip()] = int(rvalue.strip())
            return timedelta(**result)
        except (ValueError, TypeError):
            raise CronError("'%s' is not a valid time format")

    def run(self):
        raise NotImplementedError()


class Cron(object):
    def __init__(self, jobs):
        self.jobs = jobs

    def run(self):
        try:
            crons = {}
            while True:
                # Run each job sequentially
                for job in self.jobs:
                    # Each job must report when they should run next
                    crons[job] = job()
                # Sleep until a job should run
                self.sleep(crons)
        except KeyboardInterrupt:
            log.warning("Caught CTRL-C; by user request?")
            sys.exit(1)
        except Exception, e:
            # log a traceback
            log.error("Caught %s" % repr(e), exc_info=True)
            return 1

    def sleep(self, crons):
        """ Sleeps until one of the jobs in 'crons' needs to run """
        wakeups = crons.values()
        # Sort by the earliest wake time
        wakeups.sort()
        # Calculate the sleep time from now
        sleep = wakeups[0] - time.time()
        log.debug("Sleeping '%d' seconds" % sleep)
        # return if we are past our wakeup time
        if sleep < 0:
            return
        time.sleep(sleep)
