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
from lunr.common import logger, feed_client
from lunr.common.feed_client import FeedError
from lunr.cinder.cinderclient import CinderError
from lunr.cinder import cinderclient
import sqlalchemy.exc
from lunr.db.models import Event, Error, Marker
import datetime

console_logger = logger.get_logger('orbit.terminatedfeedreader')


class CloudFeedsReadFailed(FeedError):  # pragma: no cover
    pass


class EmptyEvent(FeedError):  # pragma: no cover
    pass


class DBError(Exception):  # pragma: no cover
    pass


class FeedReader(CronJob):
    """ Orbit Job to read terminated events from cloud feeds and save them in to the lunr MySQL database. """

    def __init__(self, conf, session):  # pragma: no cover
        CronJob.__init__(self)
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        # for dev purpose, interval set to 5 sec, but for prod, 1 min
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        # self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=60'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.config = conf
        self.url = self.config.string('terminator', 'feed_url', 'none')
        self.auth_token = None
        self.session = session
        self.marker = None

    def log_error_to_db(self, error, event=None, e_type="feed"):
        """ Log error/exception to db """
        event_id, tenant_id = None, None
        if event is not None:
            event_id = event.event_id
            tenant_id = event.tenant_id

        console_logger.error(error)
        new_error = Error(event_id=event_id, tenant_id=tenant_id,
                          message=str(error), type=e_type)
        if not self.session.query(Error).filter(Error.message == str(error)).first():
            self.session.add(new_error)
            self.session.commit()

    def remove_errors(self, e_type=None):
        """ Remove the previous error on successful connection """
        try:
            error = self.session.query(Error).filter(Error.type == e_type).first()
            if error is None:
                return
            self.session.delete(error)
            self.session.commit()
        except FeedError as e:
            self.log_error_to_db(e)

    def fetch_last_marker(self):
        """ Return back the last marker from database (if any) """
        marker = self.session.query(Marker).first()
        if marker is not None:
            return marker.last_marker
        return marker

    def fetch_token(self):  # pragma: no cover
        """ Return auth token from keystone, using cinder client """
        # Use cinder client to authenticate
        cinder = cinderclient.CinderClient(**cinderclient.get_args(self.config))
        # This will fetch our auth token
        return cinder.token

    def fetch_feed(self):  # pragma: no cover
        """ Return feed from cloud feeds, using cloud feed client """
        return cloudfeedclient.Feed(self.config, console_logger, self.url,
				    self.auth_token, last_event=self.marker)

    def fetch_events(self):
        """ Authenticate with Identity and fetch new events from cloud feeds """
        self.auth_token = self.fetch_token()
        # If we had errors on our last run, remove them here
        self.remove_errors("auth")
        # Fetch our last marker from the database
        self.marker = self.fetch_last_marker()
        feed = self.fetch_feed()
        # Fetch all NEW events from the last marker
        return feed.get_events()

    def save_event(self, event):
        """ Save event to database """
        event_time = datetime.datetime.strptime(event['eventTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
        new_event = Event(
            event_id=event['id'],
            tenant_id=event['tenantId'],
            timestamp=event_time,
            processed='No',
            last_purged=None
        )
        self.session.add(new_event)

    def save_marker(self):
        """ Save marker to database """
        marker = self.session.query(Marker).first()
        if marker:
            marker.last_marker = self.marker
        else:
            marker = Marker(last_marker=self.marker)
        self.session.add(marker)

    def run(self):  # pragma: no cover
        """ Implements the CRON run method """
        count = 0
        try:
            # Remove existing errors from db
            self.remove_errors("feed")

            # Read new events from the feed
            for event in self.fetch_events():
                # Save marker with any status for quick parsing later
                self.marker = event['id']
                if event['product']['status'].lower() != 'terminated':
                    continue
                count += 1
                self.save_event(event)
                # Commit the session after processing 25 events
                if (count % 25) == 0:
                    self.save_marker()
                    self.session.commit()

            # Commit the session and marker at end of run
            self.save_marker()
            self.session.commit()

            console_logger.debug("Found {0} events to be saved in DB for this run".format(count))
            self.session.close()

        except CinderError as e:
            self.log_error_to_db(e, e_type="auth")
        except FeedError as e:
            self.log_error_to_db(e)
        except sqlalchemy.exc as e:
            console_logger.error("TerminatedFeedReader.run() - %s" % e)
            self.session.rollback()
        except DBError as e:
            console_logger.error("TerminatedFeedReader.run() - %s" % e)
