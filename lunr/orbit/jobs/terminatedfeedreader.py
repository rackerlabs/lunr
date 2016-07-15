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
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import sessionmaker
from lunr.common import logger, cloudfeedclient
from lunr.cinder import cinderclient
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()

log = logger.get_logger('orbit.terminatedfeedreader')
status_code = 'terminated'


class CloudFeedsReadFailed(Exception):
    pass


class Event(Base):
    __tablename__ = 'events'
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })

    id = Column(Integer, primary_key=True, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow())
    uuid = Column(String(45), unique=True, nullable=False)
    timestamp = Column(String(25), nullable=False)

    def __init__(self, timestamp, event_id):
        self.created_at = datetime.datetime.utcnow()
        self.uuid = event_id
        self.timestamp = timestamp

    def __init__(self, raw_event):
        self.created_at = datetime.datetime.utcnow()
        self.timestamp = time_parser(raw_event['eventTime'])
        self.uuid = raw_event['id']

    def get_timestamp(self):
        return self.timestamp

    def get_uuid(self):
        return self.uuid

    def __repr__(self):
        return "<Event %s: %s %s>" % (self.uuid, self.created_at, self.timestamp)


class Audit(Base):
    __tablename__ = 'audit'
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })

    id = Column(Integer, primary_key=True, nullable=False)
    event_id = Column(String(50), index=True, nullable=False)
    tenant_id = Column(String(20), index=True, nullable=False)
    timestamp = Column(String(25), nullable=False)
    type = Column(String(15), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow())

    def __init__(self, event_id, tenant_id, **kwargs):
        self.event_id = event_id
        self.tenant_id = tenant_id
        self.timestamp = kwargs.pop('timestamp')
        self.type = kwargs.pop('type')
        self.created_at = datetime.datetime.utcnow

    def __init__(self, raw_event):
        self.event_id = raw_event['id']
        self.tenant_id = raw_event['']

    def __repr__(self):
        return "<Audit %s: %s %s %s %s>" % (self.event_id, self.tenant_id, self.timestamp, self.type, self.created_at)


class Error(Base):
    __tablename__ = 'error'
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })

    id = Column(Integer, primary_key=True, nullable=False)
    event_id = Column(String(50), nullable=False)
    tenant_id = Column(String(20), nullable=False)
    created_at = Column(String(25), default=datetime.datetime.utcnow(), nullable=False)
    type = Column(String(15), nullable=False)
    message = Column(String(200), nullable=False)

    def __init__(self, event_id, tenant_id, **kwargs):
        self.event_id = event_id
        self.tenant_id = tenant_id
        self.created_at = datetime.datetime.utcnow()
        self.type = kwargs.pop('type')
        self.message = kwargs.pop('message')

    def __repr__(self):
        return "<Error %s: %s %s %s %s>" % (self.event_id, self.tenant_id, self.created_at, self.type, self.message)


class Marker(Base):
    __tablename__ = 'marker'
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow(), nullable=False)
    last_marker = Column(String(45), unique=True, nullable=False)
    marker_timestamp = Column(DateTime, nullable=False)

    def __init__(self, last_marker, timestamp):
        self.last_marker = last_marker
        self.marker_timestamp = timestamp

    def __repr__(self):
        return "<Marker %s: %s >" % (self.last_marker, self.updated_at)


def time_parser(timestamp):
    return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')


class TerminatedFeedReader(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.config = conf
        self._sess = self.database_setup(conf)

    @staticmethod
    def database_setup(conf):
        db_url = conf.string('terminator', 'db_url', 'none')
        db_name = conf.string('terminator', 'db_name', 'terminator')
        url = db_url + db_name
        # engine = create_engine(url, echo=True, pool_recycle=3600)
        engine = create_engine(url, pool_recycle=3600)
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)
        _session = session()
        return _session

    def run(self, now=None):
        cinder = cinderclient.CinderClient(**cinderclient.get_args(self.config))
        auth_token = cinder.token

        feed_url = self.config.string('terminator', 'feed_url', 'none')
        feed = cloudfeedclient.Feed(self.config, log, None, feed_url, auth_token, read_forward=True)

        log_counter = 0
        marker = self._sess.query(Marker).first()

        last_marker_id = ''
        last_marker_time = datetime.datetime(1970, 1, 1, 0, 0, 0)
        last_run_marker_id = ''

        if marker is not None:
            last_marker_id = marker.last_marker
            last_marker_time = marker.marker_timestamp
            last_run_marker_id = last_marker_id
            # On marker change, re-fetching the feed
            feed = cloudfeedclient.Feed(self.config, log, last_marker_id, feed_url, auth_token, read_forward=True)

        try:
            feed_events = feed.get_events()
        except CloudFeedsReadFailed:
            log.debug('Error in retrieving feed from: %s' % feed_url)

        for event in feed_events:
            # Audit the log
            auditor = Audit(event)

            if event['product']['status'].lower() == status_code:
                # timestamp = time_parser(event['eventTime'])
                # event_id = event['id']
                new_event = Event(event)

                # print("1. %s > %s, " %(timestamp, last_marker_time) + str(timestamp > last_marker_time))
                # print("2. %s != %s, " % (event_id, last_marker_id) + str(event_id != last_marker_id))
                if new_event.get_timestamp() >= last_marker_time and new_event.get_uuid() != last_marker_id:
                    # Count of log
                    log_counter += 1
                    # Store event log
                    # new_event = Event(timestamp, event_id)
                    last_marker_id = new_event.get_uuid()
                    last_marker_time = new_event.get_timestamp()



                    self._sess.add(new_event)

                # Catch exceptions and log into error

            self._sess.add(auditor)
        # Store the marker
        if last_run_marker_id != last_marker_id:
            new_marker = Marker(last_marker_id, last_marker_time)
            self._sess.add(new_marker)

        log.debug('Events to be terminated in this run: %s' % log_counter)

        self._sess.commit()
        self._sess.close()
