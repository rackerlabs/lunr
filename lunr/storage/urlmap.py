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

from lunr.common.routing import lunr_connect
from lunr.storage.controller.backup import BackupController
from lunr.storage.controller.clone import CloneController
from lunr.storage.controller.volume import VolumeController
from lunr.storage.controller.export import ExportController
from lunr.storage.controller.status import StatusController


urlmap = Mapper()

# Volumes

lunr_connect(urlmap, '/volumes', VolumeController,
             {'GET': 'index'})
lunr_connect(urlmap, '/volumes/{id}', VolumeController,
             {'PUT': 'create', 'GET': 'show', 'DELETE': 'delete'})

# Volume audit
lunr_connect(urlmap, '/volumes/{id}/audit', VolumeController,
             {'PUT': 'audit'})

# Volume lock
lunr_connect(urlmap, '/volumes/{id}/lock', VolumeController,
             {'GET': 'lock'})

# Exports
lunr_connect(urlmap, '/volumes/{volume_id}/export', ExportController,
             {'PUT': 'create', 'GET': 'show', 'DELETE': 'delete'})

# Backups

lunr_connect(urlmap, '/volumes/{volume_id}/backups', BackupController,
             {'GET': 'index'})
lunr_connect(urlmap, '/volumes/{volume_id}/backups/{id}', BackupController,
             {'PUT': 'create', 'GET': 'show', 'DELETE': 'delete'})

# Clones

# lunr_connect(urlmap, 'volumes/{source_volume_id}/clones')
lunr_connect(urlmap, '/volumes/{volume_id}/clones/{id}',
             CloneController, {'PUT': 'create'})

# Status

lunr_connect(urlmap, '/status', StatusController, {'GET': 'index'})
lunr_connect(urlmap, '/status/api', StatusController, {'GET': 'api_status'})
lunr_connect(urlmap, '/status/conf', StatusController, {'GET': 'conf_status'})
lunr_connect(urlmap, '/status/{helper_type:(volumes|exports|backups)}',
             StatusController, {'GET': 'show'})
