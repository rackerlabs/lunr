#!/usr/bin/env python
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
import operator
import sys
from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey,\
    Boolean, ForeignKeyConstraint
from sqlalchemy.orm import relation, backref, object_mapper
from sqlalchemy.sql import func, desc
from sqlalchemy import and_
from sqlalchemy.orm.interfaces import MapperExtension
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData, UniqueConstraint

from uuidimpl import UUID
from jsonimpl import FrozenDict, JsonEncodedDict

# TODO: this needs to be configurable maybe set globally in the db via api call
# when volume types are created?
DEFAULT_VOLUME_TYPE = 'vtype'


class DictableBase(object):
    def __iter__(self):
        return ((c.name, getattr(self, c.name)) for c in
                object_mapper(self).columns)

    @classmethod
    def get_mutable_columns(cls):
        """
        Returns cls.__table__.columns minus any immutable columns, as defined
        in the __immutable_columns__ attribute.
        """
        immutable_columns = getattr(cls, '__immutable_columns__', [])
        if immutable_columns:
            return [c for c in cls.__table__.columns
                    if c.name not in immutable_columns]
        else:
            return cls.__table__.columns

    @classmethod
    def get_mutable_column_names(cls):
        return map(operator.attrgetter('name'), cls.get_mutable_columns())

metadata = MetaData()
ModelBase = declarative_base(metadata=metadata, cls=DictableBase)


class NodeExtension(MapperExtension):

    def reconstruct_instance(self, mapper, instance):
        instance._storage_used = None


def DateFields(cls):
    cls.created_at = Column(DateTime, default=func.now())
    cls.last_modified = Column(DateTime, default=func.now(),
                               onupdate=func.now())
    cls.__immutable_columns__ += ['last_modified', 'created_at']
    return cls


@DateFields
class Node(ModelBase):
    __tablename__ = "node"
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    }
    __mapper_args__ = {
        'extension': NodeExtension(),
    }
    __immutable_columns__ = ['id', 'meta']

    id = Column(UUID(), primary_key=True, default=uuid4)
    name = Column(String(255), unique=True)
    status = Column(String(32), default='ACTIVE')
    size = Column(Integer, nullable=False)
    volume_type_name = Column(ForeignKey("volume_type.name"), nullable=False)
    volumes = relation("Volume", backref=backref('node', lazy=False))
    meta = Column(JsonEncodedDict(1024), default={})
    hostname = Column(String(256), nullable=False)
    port = Column(Integer, default=8081, nullable=False)
    storage_hostname = Column(String(256), nullable=False)
    # NOTE(clayg): 3260 is default port for iscsi
    storage_port = Column(Integer, default=3260, nullable=False)
    cinder_host = Column(String(255), nullable=False)
    affinity_group = Column(String(255), nullable=False, default='')
    maintenance_zone = Column(String(255), nullable=False, default='')

    @property
    def _meta(self):
        """
        Use this attribute to modify instance meta

        i.e.
        obj._meta[key] = value
        """
        self.meta = dict(self.meta)
        return self.meta

    def __init__(self, name=None, size=0, volume_type_name=DEFAULT_VOLUME_TYPE,
                 **kwargs):
        if name is None:
            kwargs['id'] = uuid4()
            name = 'node-' + str(kwargs['id'])
        if 'hostname' not in kwargs:
            kwargs['hostname'] = 'localhost'
        if 'storage_hostname' not in kwargs:
            kwargs['storage_hostname'] = kwargs['hostname']
        if 'cinder_host' not in kwargs:
            kwargs['cinder_host'] = kwargs['hostname']
        ModelBase.__init__(self, name=name, size=size,
                           volume_type_name=volume_type_name, **kwargs)
        if self.meta is None:
            self.meta = {}

    @property
    def storage_used(self):
        if self._sa_instance_state.expired or \
                not hasattr(self, '_storage_used') or \
                self._storage_used is None:
            self.calc_storage_used()
        return self._storage_used

    def calc_storage_used(self):
        db = Session.object_session(self)
        if db:
            self._storage_used = db.query(func.sum(Volume.size)).\
                filter(and_(Volume.node_id == self.id,
                            Volume.status != 'DELETED')).one()[0] or 0
        else:
            self._storage_used = None
        return self._storage_used

    @property
    def storage_free(self):
        return self.size - (self.storage_used or 0)

    def __iter__(self):
        def _iter():
            for name, attr in super(Node, self).__iter__():
                yield name, attr
            if hasattr(self, '_storage_used') and \
                    self._storage_used is not None:
                yield 'storage_used', self.storage_used
                yield 'storage_free', self.storage_free
        return _iter()

    def __repr__(self):
        return "<Node %s: %s %sGB %s>" % (self.id, repr(self.name),
                                          self.size, self.volume_type_name)


@DateFields
class Account(ModelBase):
    __tablename__ = "account"
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    }
    __immutable_columns__ = ['id']

    id = Column(String(36), primary_key=True, nullable=False)
    status = Column(String(32), default='ACTIVE')
    volumes = relation("Volume", backref='account')
    backups = relation("Backup", backref='account')

    def __init__(self, **kwargs):
        if not kwargs.get('id'):
            kwargs['id'] = str(uuid4())
        ModelBase.__init__(self, **kwargs)

    def __repr__(self):
        return "<Account %s: %s>" % (self.id, self.status)


def account_query(db, model, account):
    q = db.query(model)
    if account != 'admin':
        q = q.filter_by(account_id=account)
    return q


@DateFields
class Volume(ModelBase):
    __tablename__ = "volume"
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })
    __immutable_columns__ = ['id', 'node_id']

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    status = Column(String(32), default='NEW')
    size = Column(Integer, nullable=False)
    volume_type_name = Column(ForeignKey("volume_type.name"), nullable=False)
    backups = relation("Backup", backref=backref('volume', lazy=False))
    node_id = Column(ForeignKey("node.id"), nullable=True)
    account_id = Column(ForeignKey("account.id"), nullable=False)
    restore_of = Column(String(36), nullable=True)
    image_id = Column(String(36), nullable=True)
    name = Column(String(255), nullable=True)

    def __init__(self, size=0, volume_type_name=DEFAULT_VOLUME_TYPE, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = str(uuid4())
        if 'name' not in kwargs:
            kwargs['name'] = kwargs['id']

        ModelBase.__init__(self, size=size,
                           volume_type_name=volume_type_name, **kwargs)

    def __repr__(self):
        return "<%s:%s %sGB %s %s>" % (self.id, self.name, self.size,
                                       self.status, self.volume_type_name)

    def active_backup_count(self):
        def func(x, y):
            return x if y.status in ('AUDITING', 'DELETED') else x + 1
        return reduce(func, self.backups, 0)


@DateFields
class Export(ModelBase):
    __tablename__ = "export"
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })
    __immutable_columns__ = ['id']

    id = Column(ForeignKey("volume.id"), primary_key=True)
    status = Column(String(32), default='ATTACHING')
    instance_id = Column(String(36))
    mountpoint = Column(String(32))
    ip = Column(String(255))
    initiator = Column(String(255))
    session_ip = Column(String(255))
    session_initiator = Column(String(255))
    target_name = Column(String(255), nullable=True)  # iqn

    volume = relation("Volume", backref=backref('export', uselist=False))

    @property
    def target_portal(self):
        if self.volume.node:
            return '%s:%s' % (self.volume.node.storage_hostname,
                              self.volume.node.storage_port)
        else:
            return None

    def __iter__(self):

        def _iter():
            for name, attr in super(Export, self).__iter__():
                yield name, attr
            yield 'target_portal', self.target_portal
        return _iter()

    def __repr__(self):
        return "<%s: %s instance:%s>" % (self.id, self.status,
                                         self.instance_id)


@DateFields
class Backup(ModelBase):
    __tablename__ = "backup"
    __table_args__ = ({
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    })
    __immutable_columns__ = ['id']

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    status = Column(String(32), default='NEW')
    size = Column(Integer, nullable=False)
    volume_id = Column(ForeignKey("volume.id"), nullable=True)
    account_id = Column(ForeignKey("account.id"), nullable=False)

    def __init__(self, volume, **kwargs):
        if not filter(lambda x: x in ('account', 'account_id'), kwargs):
            # no account or account_id in kwargs, try and steal it from volume
            if volume.account_id:
                kwargs['account_id'] = volume.account_id
            else:
                kwargs['account'] = volume.account
        size = kwargs.pop('size', volume.size)
        ModelBase.__init__(self, volume=volume, size=size, **kwargs)

    def __repr__(self):
        return "<%s: %sGB %s>" % (self.id, self.size, self.status)


@DateFields
class VolumeType(ModelBase):
    __tablename__ = "volume_type"
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    }
    __immutable_columns__ = []

    name = Column(String(255), nullable=False, primary_key=True)
    status = Column(String(32), default='ACTIVE')
    min_size = Column(Integer, nullable=False, default=1)
    max_size = Column(Integer, nullable=False, default=1024)
    read_iops = Column(Integer, nullable=False, default=1000)
    write_iops = Column(Integer, nullable=False, default=1000)

    volumes = relation("Volume", backref='volume_type')
    nodes = relation("Node", backref='volume_type')

    def __init__(self, name, **kwargs):
        ModelBase.__init__(self, name=name, **kwargs)

    def __ref__(self):
        return "<VolumeType %s: %s %s?>" % (
            self.ed, repr(self.name), self.status)


if __name__ == "__main__":
    from lunr.db.console import main
    sys.exit(main())
