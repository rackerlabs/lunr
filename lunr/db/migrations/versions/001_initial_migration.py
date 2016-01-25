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
from lunr.db.uuidimpl import UUID

meta = MetaData()

table_kwargs = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8',
}

account = Table(
    'account', meta,
    Column('id', String(36), primary_key=True, nullable=False),
    Column('status', String(32)),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)

backup = Table(
    'backup', meta,
    Column('id', String(36), primary_key=True, nullable=False),
    Column('status', String(32)),
    Column('size', Integer, nullable=False),
    Column('volume_id', String(36), ForeignKey('volume.id'), nullable=True),
    Column('account_id', String(36), ForeignKey('account.id'), nullable=False),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)

export = Table(
    'export', meta,
    Column('id', String(36), primary_key=True, nullable=False),
    Column('status', String(32)),
    Column('instance_id', String(36)),
    Column('mountpoint', String(32)),
    Column('ip', String(255)),
    Column('initiator', String(255)),
    Column('session_ip', String(255)),
    Column('session_initiator', String(255)),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    Column('target_name', String(255)),
    **table_kwargs
)

node = Table(
    'node', meta,
    Column('id', UUID(), primary_key=True, nullable=False),
    Column('name', String(255)),
    Column('status', String(32), default='ACTIVE'),
    Column('size', Integer, nullable=False),
    Column('volume_type_name', String(255), ForeignKey('volume_type.name'),
           nullable=False),
    Column('meta', String(1024)),
    Column('hostname', String(256), nullable=False),
    Column('port', Integer, nullable=False, default=8081),
    Column('storage_hostname', String(256), nullable=False),
    Column('storage_port', Integer, nullable=False, default=3260),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)

volume = Table(
    'volume', meta,
    Column('id', String(36), primary_key=True, nullable=False),
    Column('status', String(32)),
    Column('size', Integer, nullable=False),
    Column('volume_type_name', String(255), ForeignKey('volume_type.name'),
           nullable=False),
    Column('node_id', UUID(), ForeignKey('node.id')),
    Column('account_id', String(36), ForeignKey('account.id'), nullable=False),
    Column('clone_of', String(36), nullable=True),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)

volume_type = Table(
    'volume_type', meta,
    Column('name', String(255), primary_key=True, nullable=False),
    Column('status', String(32)),
    Column('min_size', Integer, nullable=False),
    Column('max_size', Integer, nullable=False),
    Column('read_iops', Integer, nullable=False),
    Column('write_iops', Integer, nullable=False),
    Column('created_at', DateTime),
    Column('last_modified', DateTime),
    **table_kwargs
)


def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    meta.bind = migrate_engine
    volume_type.create()
    node.create()
    account.create()
    volume.create()
    backup.create()
    export.create()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    meta.bind = migrate_engine
    export.drop()
    backup.drop()
    volume.drop()
    account.drop()
    node.drop()
    volume_type.drop()
