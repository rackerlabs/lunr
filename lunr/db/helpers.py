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

from webob.exc import HTTPBadRequest


def filter_update_params(req, model):
    allowed_params = set(c.name for c in model.get_mutable_columns())
    update_params = {}
    meta_params = {}
    for k, v in req.params.items():
        if k in allowed_params:
            update_params[k] = v
        elif k.lower().startswith('x-meta-'):
            key = k[len('x-meta-'):]
            meta_params[key] = v
        else:
            raise HTTPBadRequest("Invalid request parameter '%s'" % k)
    return update_params, meta_params
