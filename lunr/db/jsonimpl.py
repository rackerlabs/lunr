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


from collections import Mapping
from sqlalchemy.types import TypeDecorator, VARCHAR, TEXT
try:
    import simplejson as json
except ImportError:
    import json

from lunr.common import jsonify


class FrozenDict(Mapping):

    def __init__(self, *args, **kwargs):
        self._d = dict(*args, **kwargs)

    def __getitem__(self, key):
        return self._d[key]

    def __len__(self):
        return len(self._d)

    def __iter__(self):
        return iter(self._d)

    def __repr__(self):
        return repr(self._d)


class JsonEncodedDict(TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage::

        JsonEncodedDict(255)

    """

    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        return jsonify.encode(dict(value or {}))

    def process_result_value(self, value, dialect):
        if value is None:
            return FrozenDict()
        return FrozenDict(json.loads(value))

    def load_dialect_impl(self, dialect, **kwargs):
        if dialect.name.startswith('postgres'):
            return dialect.type_descriptor(TEXT)
        return TypeDecorator.load_dialect_impl(self, dialect, **kwargs)
