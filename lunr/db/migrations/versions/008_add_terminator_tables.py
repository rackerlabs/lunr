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


from sqlalchemy import *

meta = MetaData()

table_kwargs = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8',
}

# Table for logging currently active errors
error = Table(
    'error', meta,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('event_id', String(50), nullable=True),
    Column('tenant_id', String(20), nullable=True),
    Column('type', String(15), nullable=False),
    Column('message', String(200), unique=True, nullable=False),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)

# Table that holds current marker of event fetched from cloud feeds
marker = Table(
    'marker', meta,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('last_marker', String(45)),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)

# Table for saving events from cloud feeds, processed flag, last purge attempt info
events = Table(
    'events', meta,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('event_id', String(45), unique=True, nullable=False),
    Column('tenant_id', String(20), index=True, nullable=False),
    Column('timestamp', DateTime, nullable=False),
    Column('processed', String(5), nullable=False),
    Column('last_purged', DateTime, nullable=True),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)


def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    meta.bind = migrate_engine
    error.create()
    marker.create()
    events.create()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    meta.bind = migrate_engine
    error.drop()
    marker.drop()
    events.drop()
