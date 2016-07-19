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

from lunr.orbit import CronJob

from lunr.common import logger, cloudfeedclient
from lunr.common.cloudfeedclient import FeedError
from lunr.cinder.cinderclient import CinderError
from lunr.cinder import cinderclient
from sqlalchemy.ext.declarative import declarative_base
import datetime
from lunr.db.models import Audit, Event, Error, Marker, setup_tables

Base = declarative_base()

log = logger.get_logger('orbit.terminatedfeedreader')
status_code = 'terminated'
MIN_TIME = datetime.datetime(1970, 1, 1, 0, 0, 0)


class CloudFeedsReadFailed(Exception):
    pass


class TerminatedFeedReader(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        # for dev purpose, interval set to 5 sec, but for prod, 1 min
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        # self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=60'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.config = conf
        self._sess = session

    def log_error_to_db(self, error, event, e_type):
        event_id = None
        tenant_id = None
        if event is not None:
            event_id = event['id']
            tenant_id = event['tenantId']
        new_error = Error(event_id, tenant_id, error=error, type=e_type)
        if not self._sess.query(Error).filter(Error.message == error).first():
            self._sess.add(new_error)
        return

    def run(self, now=None):
        # closedAccounts = []
        # # Get Closed Accounts
        # accounts = GetClosedAccounts()
        # for account in accounts:
        #     # Purge Closed Accounts
        #     closedAccounts.append(purge(account))
        #
        # for closed in closedAccounts:
        #     markClosed(closed)
        #
        # Mark Closed Accounts as done
        setup_tables()

        # Get closed accounts

        try:
            cinder = cinderclient.CinderClient(**cinderclient.get_args(self.config))
            auth_token = cinder.token

            # Clear the previous error on successful connection
            cinder_error = self._sess.query(Error).filter(Error.type == "auth").first()
            if cinder_error is not None:
                self._sess.delete(cinder_error)

            feed_url = self.config.string('terminator', 'feed_url', 'none')
            feed = cloudfeedclient.Feed(self.config, log, None, feed_url, auth_token)

            log_counter = 0
            marker = self._sess.query(Marker).first()

            last_marker_id = ''
            last_marker_time = MIN_TIME
            last_run_marker_id = ''

            if marker is not None:
                last_marker_id = marker.last_marker
                last_marker_time = marker.marker_timestamp
                last_run_marker_id = last_marker_id
                # On marker change, re-fetching the feed
                feed = cloudfeedclient.Feed(self.config, log, last_marker_id, feed_url, auth_token)

            try:
                feed_events = feed.get_events()

                # Clear the previous error on successful connection
                feed_error = self._sess.query(Error).filter(Error.type == "feed").first()
                if feed_error is not None:
                    self._sess.delete(feed_error)

                for event in feed_events:

                    if event['product']['status'].lower() == status_code:
                        new_event = Event(event)

                        if new_event.timestamp >= last_marker_time and new_event.uuid != last_marker_id:
                            # Count of log
                            log_counter += 1
                            # Store event log
                            # new_event = Event(timestamp, event_id)
                            last_marker_id = new_event.uuid
                            last_marker_time = new_event.timestamp

                            self._sess.add(new_event)

                            # Audit the log
                            auditor = Audit(event)
                            self._sess.add(auditor)

                            # Catch exceptions and log into error
            except FeedError as e:
                self.log_error_to_db(e, None, "feed")
                log.error(e)
                # log.debug('Error in retrieving feed from: %s' % feed_url)

            # Store the marker
            if last_run_marker_id != last_marker_id:
                new_marker = Marker(last_marker_id, last_marker_time)
                self._sess.add(new_marker)

            log.debug('Events to be terminated in this run: %s' % log_counter)

        except CinderError as e:
            self.log_error_to_db(e, None, "auth")
            log.error(e)

        self._sess.commit()
        self._sess.close()
