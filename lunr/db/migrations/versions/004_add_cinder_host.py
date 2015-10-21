# Copyright (c) 2011-2015 Rackspace US, Inc.
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


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    nodes = Table('node', meta, autoload=True)
    cinder_host = Column('cinder_host', String(255))
    nodes.create_column(cinder_host)


def downgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    nodes = Table('node', meta, autoload=True)
    cinder_host = Column('cinder_host', String(255))
    nodes.drop_column(cinder_host)
