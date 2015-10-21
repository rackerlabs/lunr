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

from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy.exc import IntegrityError

from lunr.db.models import Account
from lunr.db import NoResultFound


class LunrSession(ScopedSession):

    def update_or_create(self, model, updates=None, **kwargs):
        updates = updates or {}
        instance = self.query(model).filter_by(**kwargs).first()
        if instance:
            # update
            for attr, value in updates.items():
                setattr(instance, attr, value)
            created = False
        else:
            # create
            updates.update(kwargs)
            instance = model(**updates)
            created = True
        self.add(instance)
        self.commit()
        return instance, created

    def get_or_create_account(self, account_id):
        try:
            account = self.query(Account).filter_by(id=account_id).one()
        except NoResultFound:
            account = Account(id=account_id)
            self.add(account)
            try:
                self.commit()
            except IntegrityError:
                self.rollback()
                account = self.query(Account).filter_by(id=account_id).one()
        return account
