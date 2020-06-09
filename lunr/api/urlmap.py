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


from routes import Mapper
from webob.exc import HTTPMethodNotAllowed

from lunr.api.controller.base import BaseController
from lunr.api.controller.volume import VolumeController
from lunr.api.controller.export import ExportController
from lunr.api.controller.backup import BackupController
from lunr.api.controller.node import NodeController
from lunr.api.controller.account import AccountController
from lunr.api.controller.volume_type import VolumeTypeController
from lunr.api.controller.restore import RestoreController
from lunr.common.routing import lunr_connect


urlmap = Mapper()


# Volumes

lunr_connect(urlmap, '/v1.0/{account_id:admin}/volumes', VolumeController,
             {'GET': 'index'})
lunr_connect(urlmap, '/v1.0/{account_id}/volumes', VolumeController,
             {'GET': 'index'})

lunr_connect(urlmap,
             '/v1.0/{account_id:admin}/volumes/{id}', VolumeController,
             {'GET': 'show', 'POST': 'update', 'DELETE': 'delete', 'PUT': 'update_node_id'})

lunr_connect(urlmap, '/v1.0/{account_id}/volumes/{id}', VolumeController,
             {'PUT': 'create', 'POST': 'update', 'GET': 'show',
              'DELETE': 'delete'})

# Exports

lunr_connect(urlmap, '/v1.0/{account_id:admin}/volumes/{id}/export',
             ExportController,
             {'PUT': 'create', 'POST': 'update', 'GET': 'show',
              'DELETE': 'delete'})
lunr_connect(urlmap, '/v1.0/{account_id}/volumes/{id}/export',
             ExportController,
             {'PUT': 'create', 'POST': 'update', 'GET': 'show',
              'DELETE': 'delete'})

# Backups

lunr_connect(urlmap, '/v1.0/{account_id:admin}/backups', BackupController,
             {'GET': 'index'})
lunr_connect(urlmap, '/v1.0/{account_id}/backups', BackupController,
             {'GET': 'index'})
lunr_connect(urlmap,
             '/v1.0/{account_id:admin}/backups/{id}', BackupController,
             {'GET': 'show', 'POST': 'update', 'DELETE': 'delete'})
lunr_connect(urlmap, '/v1.0/{account_id}/backups/{id}', BackupController,
             {'PUT': 'create', 'POST': 'update', 'GET': 'show',
              'DELETE': 'delete'})

# Restores

lunr_connect(urlmap,
             '/v1.0/{account_id:admin}/backups/{backup_id}/restores',
             RestoreController,
             {'GET': 'index'})
lunr_connect(urlmap,
             '/v1.0/{account_id:admin}/backups/{backup_id}/restores/{id}',
             RestoreController,
             {'GET': 'show', 'DELETE': 'delete'})

# Nodes

lunr_connect(urlmap, '/v1.0/admin/nodes', NodeController,
             {'POST': 'create', 'GET': 'index'})
lunr_connect(urlmap, '/v1.0/admin/nodes/{id}', NodeController,
             {'POST': 'update', 'GET': 'show', 'DELETE': 'delete'})

# Accounts

lunr_connect(urlmap, '/v1.0/admin/accounts', AccountController,
             {'POST': 'create', 'GET': 'index'})
lunr_connect(urlmap, '/v1.0/admin/accounts/{id}', AccountController,
             {'POST': 'update', 'GET': 'show', 'DELETE': 'delete'})

# Volume Types

lunr_connect(urlmap, '/v1.0/admin/volume_types', VolumeTypeController,
             {'POST': 'create', 'GET': 'index'})
lunr_connect(urlmap, '/v1.0/admin/volume_types/{name}', VolumeTypeController,
             {'POST': 'update', 'GET': 'show', 'DELETE': 'delete'})
