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


from webob.exc import HTTPNotFound
from webob import Response

from lunr.api.controller.base import BaseController
from lunr.db.models import Account
from lunr.db.helpers import filter_update_params


class AccountController(BaseController):

    def index(self, request):
        """
        GET /v1.0/admin/accounts

        List accounts
        """
        q = self.db.query(Account)
        available_filters = set(['status', 'id'])
        filters = dict((k, v) for k, v in request.params.items() if k in
                       available_filters)
        if filters:
            q = q.filter_by(**filters)
        return Response([dict(r) for r in q.all()])

    def create(self, request):
        """
        POST /v1.0/admin/accounts

        Create account
        """
        id_ = request.params.get('id')
        a = Account(id=id_)
        self.db.add(a)
        self.db.commit()
        self.db.refresh(a)
        return Response(dict(a))

    def delete(self, request):
        """
        DELETE /v1.0/admin/accounts/{id}

        Delete account
        """
        update_params = {'status': 'DELETED'}
        num_updated = self.db.query(Account).filter_by(
            id=self.id).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot delete non-existent account '%s'" %
                               self.id)
        a = self.db.query(Account).get(self.id)
        return Response(dict(a))

    def show(self, request):
        """
        GET /v1.0/admin/accounts/{id}

        Show account info
        """
        a = self.db.query(Account).get(self.id)
        if not a:
            raise HTTPNotFound("Cannot show non-existent account '%s'" %
                               self.id)
        return Response(dict(a))

    def update(self, request):
        """
        POST /v1.0/admin/accounts/{id}

        Update account
        """
        update_params, meta_params = filter_update_params(request, Account)
        num_updated = self.db.query(Account).filter_by(
            id=self.id).update(update_params)
        self.db.commit()
        if not num_updated:
            raise HTTPNotFound("Cannot update non-existent account '%s'" %
                               self.id)
        a = self.db.query(Account).get(self.id)
        return Response(dict(a))
