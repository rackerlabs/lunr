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

from urllib2 import Request, urlopen
from urllib import urlencode
from webob import exc
import urllib2
from uuid import uuid4
from webob.exc import HTTPServiceUnavailable, HTTPInsufficientStorage, \
        HTTPNotFound
from httplib import HTTPException
import socket
from random import shuffle
from functools import partial

from lunr.db.models import Node, Volume, Account
from lunr.common.jsonify import loads
from lunr.common import logger
from lunr.common.exc import NodeError
from sqlalchemy import Float, and_
from sqlalchemy.sql import func, desc
from sqlalchemy.sql.expression import cast, case, null


class BaseController(object):
    def __init__(self, route, app):
        self.db = app.helper
        self.route = route
        self.app = app
        self._account_id = None

        # Add a couple of shortcuts
        self.id = route.get('id', None)
        self.name = route.get('name', None)
        self.req_account_id = route.get('account_id', None)
        self.deep_fill = partial(self.fill_strategy, 'deep_fill')
        self.broad_fill = partial(self.fill_strategy, 'broad_fill')

    @property
    def account_id(self):
        if not self._account_id:
            if self.req_account_id and self.req_account_id != 'admin':
                account = self.db.get_or_create_account(self.req_account_id)
                if account.status != 'ACTIVE':
                    raise HTTPNotFound('Account is not ACTIVE')
                self._account_id = account.id
        return self._account_id

    def account_query(self, model):
        q = self.db.query(model)
        if self.req_account_id != 'admin':
            q = q.filter_by(account_id=self.account_id)
        return q

    def node_request(self, node, method, path, **kwargs):
        url = 'http://%s:%s%s' % (node.hostname, node.port, path)
        headers = {'X-Request-Id': getattr(logger.local, 'request_id',
                                           'lunr-%s' % uuid4())}

        if method in ('GET', 'HEAD', 'DELETE'):
            url += '?' + urlencode(kwargs)

        req = Request(url, urlencode(kwargs), headers=headers)
        req.get_method = lambda *args, **kwargs: method
        try:
            resp = urlopen(req, timeout=self.app.node_timeout)
            logger.debug(
                "%s on %s succeeded with %s" %
                (req.get_method(), req.get_full_url(), resp.getcode()))
            return loads(resp.read())
        except (socket.timeout, urllib2.HTTPError,
                urllib2.URLError, HTTPException), e:
            raise NodeError(req, e)

    def fill_strategy(self, type, volume_type_name, size, count,
                      imaging=False, affinity=''):
        volumes_used = func.coalesce(func.count(Volume.size), 0)
        storage_used = func.coalesce(func.sum(Volume.size), 0)
        fill_percent = ((cast(storage_used, Float) + size) / Node.size)

        # This gives the number of volumes per node for this account_id
        # because NULL isn't counted in count()
        # count(case volume.account_id when 1234-1234 then 1 else null end)
        acct_vol_used =\
            func.count(
                case([(Volume.account_id == self.account_id, 1)],
                     else_=null()))

        q = self.db.query(Node, storage_used).\
            filter_by(volume_type_name=volume_type_name, status='ACTIVE').\
            having(fill_percent <= self.app.fill_percentage_limit).\
            outerjoin((Volume, and_(Volume.node_id == Node.id,
                                    Volume.status != 'DELETED'))).\
            group_by(Node)

        # Don't include nodes that have any volumes in the IMAGING state
        if imaging:
            imaging_vol_used =\
                func.count(
                    case([(Volume.status == 'IMAGING', 1),
                          (Volume.status == 'IMAGING_SCRUB', 1),
                          (Volume.status == 'IMAGING_ERROR', 1)],
                         else_=null()))
            q = q.having(imaging_vol_used < self.app.image_convert_limit)

        if affinity:
            affinity_type, affinity_rule = affinity.split(':')
            affinity_rules = affinity_rule.split(',')

            if affinity_type == 'different_node':
                q1 = self.db.query(Volume.node_id).\
                        filter(Volume.id.in_(affinity_rules))
                q = q.having(~Node.id.in_(q1))

            if affinity_type == 'different_group':
                q1 = self.db.query(Node.affinity_group).\
                        join('volumes').\
                        filter(Volume.id.in_(affinity_rules))
                q = q.having(~Node.affinity_group.in_(q1))


        def sort(q):
            if type == 'deep_fill':
                q = q.order_by(acct_vol_used)
                if imaging:
                    q = q.order_by(imaging_vol_used)
                q = q.order_by(desc(fill_percent))
            else:
                q = q.order_by(acct_vol_used)
                if imaging:
                    q = q.order_by(imaging_vol_used)
                q = q.order_by(volumes_used)
                q = q.order_by(fill_percent)
            return q
        return sort(q).limit(count)

    def get_fill_strategy(self, volume_type_name, size, count, imaging=False,
                          affinity=''):
        if self.app.fill_strategy == 'deep_fill':
            return self.deep_fill(volume_type_name, size, count, imaging,
                                  affinity)
        return self.broad_fill(volume_type_name, size, count, imaging,
                               affinity)

    def get_recommended_nodes(self, volume_type_name, size, count=3,
                              imaging=False, affinity=''):
        q = self.get_fill_strategy(volume_type_name, size, count, imaging,
                                   affinity)
        nodes = []
        for node, storage_used in q:
            node._storage_used = storage_used
            nodes.append(node)
        if not nodes:
            if not self.db.query(Node)\
                   .filter_by(volume_type_name=volume_type_name,
                              status='ACTIVE').count():
                raise HTTPServiceUnavailable(
                    "No nodes for type '%s' are ACTIVE" % volume_type_name)
            raise HTTPInsufficientStorage(
                "No suitable node to place volume of size %s" % size)
        if self.app.fill_strategy != 'deep_fill':
            shuffle(nodes)
        return nodes
