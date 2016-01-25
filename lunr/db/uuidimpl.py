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


from sqlalchemy.types import TypeDecorator, TypeEngine, String
import uuid

try:
    from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
except ImportError:

    class PostgresUUID(TypeEngine):

        def get_col_spec(self):
            return 'UUID'


class UUID(TypeDecorator):
    impl = String(32)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if dialect.name.startswith('postgres'):
            return value
        return str(uuid.UUID(value))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name.startswith('postgres'):
            return value
        return str(value).replace('-', '')

    def load_dialect_impl(self, dialect):
        if dialect.name.startswith('postgres'):
            self.impl = PostgresUUID(as_uuid=True)
        return dialect.type_descriptor(self.impl)
