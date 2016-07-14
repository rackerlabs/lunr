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

## TODO
#  - write to database
#  - create classes for tables

log = logger.get_logger('orbit.terminatedfeedreader')


class Event(Base):
    __tablename__ = 'events'
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })

    id = Column(Integer, primary_key=True, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow())
    event_id = Column(String(45), unique=True, nullable=False)
    timestamp = Column(String(25), nullable=False)

    def __init__(self, timestamp, event_id):
        self.created_at = datetime.datetime.utcnow()
        self.event_id = event_id
        self.timestamp = timestamp

    def __repr__(self):
        return "<Event %s: %s %s>" % (self.event_id, self.created_at, self.timestamp)


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
    last_marker = Column(String(45), nullable=False)

    def __init__(self, last_marker):
        self.last_marker = last_marker

    def __repr__(self):
        return "<Marker %s: %s >" % (self.last_marker, self.updated_at)


class TerminatedFeedReader(CronJob):

    def __init__(self, conf, session):
        CronJob.__init__(self)
        self.span = self.parse(conf.string('terminator', 'span', 'hours=1'))
        self.interval = self.parse(conf.string('terminator', 'interval', 'seconds=5'))
        self.timeout = conf.float('orbit', 'timeout', 120)
        self.config = conf
        self._sess = self.database_setup(conf)

    def database_setup(self, conf):
        db_url = conf.string('terminator', 'db_url', 'none')
        db_name = conf.string('terminator', 'db_name', 'terminator')
        url = db_url + db_name
        engine = create_engine(url, echo=True, pool_recycle=3600)
        log.debug('Database connection established on %s' % url)
        # Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)
        _session = session()
        return _session

    def run(self, now=None):
        cinder = cinderclient.CinderClient(**cinderclient.get_args(self.config))
        auth_token = cinder.token

        feed_url = self.config.string('terminator', 'feed_url', 'none')
        feed = cloudfeedclient.Feed(self.config, log, None, feed_url, auth_token, read_forward=True)
        feed_events = feed.get_events()
        if feed_events is not None:
            log.debug("Events are being retrieved from %s" % feed_url)
        for event in feed_events:
            continue
