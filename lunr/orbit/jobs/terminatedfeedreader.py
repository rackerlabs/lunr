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
from lunr.db.models import Audit, Event, Error, Marker

Base = declarative_base()

log = logger.get_logger('orbit.terminatedfeedreader')
status_code = 'terminated'
MIN_TIME = datetime.datetime(1970, 1, 1, 0, 0, 0)


class CloudFeedsReadFailed(FeedError):
    pass


class EmptyEvent(FeedError):
    pass


class DBError(Exception):
    pass


class TerminatedFeedReader(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        # for dev purpose, interval set to 5 sec, but for prod, 1 min
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        # self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=60'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.url = self.config.string('terminator', 'feed_url', 'none')
        self.config = conf
        self.session = session
        self.marker = None

    def log_error_to_db(self, error, event=None, e_type="feed"):
        event_id, tenant_id = None, None
        if event is not None:
            event_id = event['id']
            tenant_id = event['tenantId']

        log.error(error)
        new_error = Error(event_id=event_id, tenant_id=tenant_id,
                          message=str(error), type=e_type)
        if not self.session.query(Error).filter(Error.message == str(error)).first():
            self.session.add(new_error)

    def remove_errors(self, e_type=None):
        """ Remove the previous error on successful connection """
        try:
            error = self.session.query(Error).filter(Error.type == e_type).first()
            if error is None:
                return
            self.session.delete(error)
            self.session.commit()
        except FeedError as e:
            self.log_error_to_db(e, e_type, "feed")

    def fetch_last_marker(self):
        marker = self.session.query(Marker).first()
        if marker is None:
            self.marker = None
        else:
            self.marker = marker.last_marker

    def fetch_events(self):
        # Use cinder client to authenticate
        cinder = cinderclient.CinderClient(**cinderclient.get_args(self.config))
        # This will fetch our auth token
        auth_token = cinder.token
        # If we had errors on our last run, remove them here
        self.remove_errors("auth")
        # Fetch our last marker from the database
        self.fetch_last_marker()
        feed = cloudfeedclient.Feed(self.config, log, self.marker, self.url, auth_token)
        # Fetch all NEW events from the last marker
        return feed.get_events()

    def save_event(self, event):
        new_event = Event(
            event_id=event['uuid'],
            tenant_id=event['tenantId']
        )
        self.session.add(new_event)

    def save_marker(self):
        self.session.add(Marker(last_marker=self.marker))

    def run(self, now=None):
        count = 0
        try:
            # Remove existing errors from db
            self.remove_errors("feed")

            # Read new events from the feed
            for event in self.fetch_events():
                count += 1
                self.save_event(event)
                if (count % 25) == 0:
                    self.save_marker()
                    self.session.commit()

            log.debug("Found '{0}' events to be terminated on this run".format(count))
            self.session.close()

        except CinderError as e:
            self.log_error_to_db(e, None, "auth")
        except FeedError as e:
            self.log_error_to_db(e)
        except DBError as e:
            log.error("TerminatedFeedReader.run() - %s" % e)


