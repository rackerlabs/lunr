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

import disk
import swift
import memory
from lunr.common.exc import ClientException


def conn_cast(value):
    # If the value is callable, ignore the mapping
    if callable(value):
        return value

    try:
        CONNECTION_MAP = {
            'disk': disk.connect,
            'swift': swift.connect,
            'memory': memory.connect,
        }
        return CONNECTION_MAP[value]
    except KeyError:
        raise Exception('unknown backup connection type %s' % value)


def get_conn(conf):
    # Get the selected backup client
    connect = conf.option('backup', 'client', 'disk', conn_cast)
    # connect to the backup client
    conn = connect(conf)
    if not hasattr(conn, 'ClientException'):
        conn.ClientException = ClientException

    return conn
