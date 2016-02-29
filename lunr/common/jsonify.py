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


"""JSON encoding functions."""

import datetime
import decimal
import uuid

try:
    from simplejson import JSONEncoder, loads
except ImportError:
    from json import JSONEncoder, loads

from webob.multidict import MultiDict
from collections import Mapping


class LunrJSONEncoder(JSONEncoder):
    """Lunr JSON Encoder class"""

    def default(self, obj):
        if hasattr(obj, '__json__') and callable(obj.__json__):
            return obj.__json__()
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            # Return ISO8601 format
            return obj.strftime('%Y-%m-%dT%H:%M:%S')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, MultiDict):
            return obj.mixed()
        elif isinstance(obj, Mapping):
            return dict(obj)
        elif isinstance(obj, uuid.UUID):
                        return str(obj)
        else:
            return JSONEncoder.default(self, obj)


_encoder = LunrJSONEncoder()


def encode(obj):
    """Return a JSON string representation of a Python object."""
    return _encoder.encode(obj)
