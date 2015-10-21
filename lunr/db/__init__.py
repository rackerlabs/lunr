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


import os
from functools import partial
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import pool
from sqlalchemy.interfaces import PoolListener
from migrate.versioning import api as migrate_api
from migrate.exceptions import DatabaseNotControlledError
from lunr.common.config import LunrConfig
from lunr.common import logger

from models import metadata, ModelBase
from lunr.db.session import LunrSession

try:
    # sqlalchemy 0.7+
    from sqlalchemy import event

    def _fk_pragma_on_connect(dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON')

    def get_engine(url, **kwargs):
        engine = create_engine(url, **kwargs)
        if 'sqlite' in url:
            event.listen(engine, 'connect', _fk_pragma_on_connect)
        return engine

except ImportError:
    # sqlalchemy 0.6

    class ForeignKeysListener(PoolListener):
        def connect(self, dbapi_con, con_record):
            dbapi_con.execute('pragma foreign_keys=ON')

    def get_engine(url, **kwargs):
        if 'sqlite' in url:
            kwargs['listeners'] = [ForeignKeysListener()]
        if url.startswith('sqlite:///') and 'poolclass' not in kwargs:
            # pool doesn't default to the right thing for files in 0.6
            kwargs['poolclass'] = pool.NullPool
        engine = create_engine(url, **kwargs)
        return engine


Session = None
cast_poolclass = partial(LunrConfig.to_class, module=pool)


class DBError(Exception):
    pass


def _remove(_dict):
    """ Remove items from _dict that have None """
    for key, value in _dict.items():
        if value is None:
            del _dict[key]
    return _dict


def get_repo():
    return os.path.join(os.path.dirname(__file__), 'migrations')


def configure(conf):
    global Session

    # We set the defaults to 'None' so only kwargs that are specified
    # in the config are passed to create_engine()
    kwargs = _remove({
        'poolclass': conf.option('db', 'poolclass', None, cast_poolclass),
        'echo': conf.bool('db', 'echo', None),
        'echo_pool': conf.bool('db', 'echo_pool', None),
        'pool_size': conf.int('db', 'pool_size', None),
        'pool_recycle': conf.int('db', 'pool_recycle', None)
    })

    # Grab the url from our config
    url = conf.string('db', 'url', 'sqlite:///' + conf.path('lunr.db'))

    engine = get_engine(url, **kwargs)
    try:
        engine.execute('select 1')
    except OperationalError, e:
        raise DBError("(%s) Unable to connect to database %s" % (e.orig, url))

    auto_create = conf.bool('db', 'auto_create', False)
    if auto_create:
        models.metadata.create_all(engine)
    else:
        repo = get_repo()
        try:
            db_version = migrate_api.db_version(url, repo)
            version = migrate_api.version(repo)
            if db_version != version:
                logger.warn("DB: '%s' version: %s, repo version: %s. "
                            "Try running lunr-manage upgrade." %
                            (url, db_version, version))
        except DatabaseNotControlledError:
            logger.warn("DB: '%s' not version controlled. "
                        "Try running lunr-manage version_control." % url)

    Session = LunrSession(sessionmaker())
    Session.configure(bind=engine)
    return Session
