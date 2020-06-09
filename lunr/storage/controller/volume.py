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


from webob import Response
from webob.exc import HTTPNotFound, HTTPPreconditionFailed, \
    HTTPInternalServerError, HTTPConflict, HTTPBadRequest

from lunr.cinder.cinderclient import CinderError
from lunr.common import logger
from lunr.common.exc import NodeError
from lunr.storage.controller.base import BaseController, lock, inspect
from lunr.common.lock import ResourceFile
from lunr.storage.helper.volume import NotFound, AlreadyExists, InvalidImage
from lunr.storage.helper.utils import ServiceUnavailable, ResourceBusy


class VolumeController(BaseController):

    def index(self, req):
        volumes = self.helper.volumes.list()
        return Response(volumes)

    def show(self, req):
        try:
            volume = self.helper.volumes.get(self.id)
        except NotFound:
            raise HTTPNotFound("No volume named '%s'" % self.id)
        return Response(volume)

    def _validate_iops(self, req):
        try:
            read_iops = int(req.params.get('read_iops', 0))
        except ValueError:
            raise HTTPPreconditionFailed("'read_iops' parameter must be an "
                                         "integer")
        if read_iops < 0:
            raise HTTPPreconditionFailed("'read_iops' parameter can not be "
                                         "negative")
        try:
            write_iops = int(req.params.get('write_iops', 0))
        except ValueError:
            raise HTTPPreconditionFailed("'write_iops' parameter must be an "
                                         "integer")
        if write_iops < 0:
            raise HTTPPreconditionFailed("'write_iops' parameter can not be "
                                         "negative")
        return {
            'read_iops': read_iops,
            'write_iops': write_iops
        }

    def _validate_size(self, req):
        try:
            size = int(req.params['size'])
        except KeyError:
            raise HTTPBadRequest("Must specify size")
        except ValueError:
            raise HTTPPreconditionFailed("'size' parameter must be an integer")
        if size < 0:
            raise HTTPPreconditionFailed("'size' parameter can not be "
                                         "negative")
        return size

    def _validate_backup_params(self, req):
        backup_id = req.params['backup_id']
        if len(backup_id) > 60:
            raise HTTPPreconditionFailed(
                "length of 'backup_id' parameter cannot exceed 60")
        try:
            backup_source_volume_id = req.params['backup_source_volume_id']
            if len(backup_source_volume_id) > 60:
                raise HTTPPreconditionFailed(
                    "length of 'backup_source_volume_id' parameter cannot"
                    " exceed 60")
        except KeyError:
            raise HTTPBadRequest("Must specify backup_source_volume_id")

        return {
            'backup_source_volume_id': backup_source_volume_id,
            'backup_id': backup_id,
        }

    def _create_from_image_cb(self, req, iops):
        def callback():
            lunr_state = 'IMAGING_SCRUB'
            try:
                volume = self.helper.volumes.get(self.id)
                self.helper.cgroups.set_read_iops(volume, iops['read_iops'])
                self.helper.cgroups.set_write_iops(volume, iops['write_iops'])
            except NotFound:
                lunr_state = 'IMAGING_ERROR'

            self.helper.make_api_request('volumes', self.id, data={
                'status': lunr_state})
        return callback

    def _post_scrub_cb(self):
        def callback():
            lunr_state = 'ACTIVE'
            try:
                volume = self.helper.volumes.get(self.id)
            except NotFound:
                lunr_state = 'DELETED'

            self.helper.make_api_request('volumes', self.id, data={
                'status': lunr_state})
        return callback

    def _create_from_backup_cb(self, req, iops):
        def callback():
            volume = self.helper.volumes.get(self.id)
            self.helper.cgroups.set_read_iops(volume, iops['read_iops'])
            self.helper.cgroups.set_write_iops(volume, iops['write_iops'])
            self.helper.make_api_request('volumes', self.id, data={
                'status': 'ACTIVE'})
            self.helper.make_api_request(
                'backups/%s/restores' % req.params['backup_id'],
                self.id, method='DELETE')
            account = req.params.get('account')
            if account:
                cinder = self.helper.get_cinder(account)
                cinder.delete_volume_metadata(self.id, 'restore-progress')
        return callback

    def _validate_source_params(self, req):
        source_volume_id = req.params['source_volume_id']
        if len(source_volume_id) > 60:
            raise HTTPPreconditionFailed(
                "length of 'source_volume_id' parameter "
                "cannot exceed 60")
        try:
            source_host = req.params['source_host']
        except KeyError:
            raise HTTPBadRequest("Must specify source_host")
        try:
            source_port = req.params['source_port']
        except KeyError:
            raise HTTPBadRequest("Must specify source_port")
        return {
            'id': source_volume_id,
            'host': source_host,
            'port': source_port,
        }

    @lock("volumes/%(id)s/resource")
    def create(self, req, lock):
        if len(self.id) > 94:
            raise HTTPPreconditionFailed(
                "length of volume id cannot exceed 94")
        if '.' in self.id:
            raise HTTPPreconditionFailed("volume id cannot contain '.'")
        params = {'lock': lock}
        params['size'] = self._validate_size(req)
        iops = self._validate_iops(req)

        # Create from backup.
        if req.params.get('backup_id'):
            params.update(self._validate_backup_params(req))
            params['callback'] = self._create_from_backup_cb(req, iops)
            account = req.params.get('account')
            if account:
                params['cinder'] = self.helper.get_cinder(account)
            try:
                self.helper.volumes.create(self.id, **params)
            except AlreadyExists, e:
                raise HTTPConflict(str(e))

            volume = self.helper.volumes.get(self.id)
            volume['status'] = 'BUILDING'
        # Create a clone
        elif req.params.get('source_volume_id'):
            source = self._validate_source_params(req)

            # FIXME.  Setting cgroups here would be silly, since we
            # want a fast clone. How do we set them later?
            # def callback():
            #     pass
            # params['callback'] = callback

            try:
                self.helper.volumes.create(self.id, **params)
            except AlreadyExists, e:
                raise HTTPConflict(str(e))

            volume = self.helper.volumes.get(self.id)
            logger.debug('Created new volume %s to be clone of %s'
                         % (volume['id'], source['id']))
            logger.debug('Creating export of new volume %s' % volume['id'])
            try:
                export = self.helper.exports.create(volume['id'])
            except ServiceUnavailable:
                self.helper.volumes.delete(volume['id'], lock=lock)
                raise

            # Tell other node to clone!
            path = '/volumes/%s/clones/%s' % (source['id'], volume['id'])
            node_params = {
                'account': req.params.get('account', ''),
                'iqn': export['name'],
                'iscsi_ip': self.helper.storage_host,
                'iscsi_port': self.helper.storage_port,
                # First dirty method to close the export.
                'mgmt_host': self.helper.management_host,
                'mgmt_port': self.helper.management_port,
                'cinder_host': self.helper.cinder_host,
            }
            try:
                self.helper.node_request(source['host'], source['port'],
                                         'PUT', path, **node_params)
            except NodeError, e:
                logger.error('Clone node request failed: %s' % e)
                self.helper.exports.delete(volume['id'])
                self.helper.volumes.delete(volume['id'], lock=lock)
                raise

            volume['status'] = 'CLONING'
        # Create from image
        elif req.params.get('image_id'):
            image_id = params['image_id'] = req.params['image_id']
            account = params['account'] = req.params.get('account')

            params['callback'] = self._create_from_image_cb(req, iops)
            params['scrub_callback'] = self._post_scrub_cb()
            try:
                self.helper.volumes.create(self.id, **params)
            except InvalidImage, e:
                logger.error("InvalidImage: %s" % e)
                raise HTTPPreconditionFailed("Invalid image: %s" % image_id)
            except AlreadyExists:
                raise HTTPConflict("Volume named '%s' already exists" %
                                   self.id)
            volume = self.helper.volumes.get(self.id)
            volume['status'] = 'IMAGING'
        else:
            # create raw volume
            try:
                self.helper.volumes.create(self.id, **params)
            except AlreadyExists:
                raise HTTPConflict("Volume named '%s' already exists" %
                                   self.id)

            volume = self.helper.volumes.get(self.id)
            self.helper.cgroups.set_read_iops(volume, iops['read_iops'])
            self.helper.cgroups.set_write_iops(volume, iops['write_iops'])
            volume['status'] = 'ACTIVE'
        return Response(volume)

    @lock("volumes/%(id)s/resource")
    def delete(self, req, lock):
        try:
            volume = self.helper.volumes.get(self.id)
        except NotFound:
            raise HTTPNotFound("Cannot delete non-existant volume '%s'" %
                               self.id)
        try:
            # delete export in try block to avoid race
            out = self.helper.exports.delete(self.id)
        except NotFound:
            # Might have recieved a duplicate delete request,
            # but are still in the process of deletion
            logger.debug("Requested deletion of '%s' but no export was "
                         "found" % self.id)
        except ResourceBusy:
            raise HTTPConflict("Cannot delete '%s' while export in "
                               "use" % self.id)

        def callback():
            self.helper.make_api_request('volumes', self.id,
                                         data={'status': 'DELETED'})
        # delete volume
        try:
            self.helper.cgroups.set_read_iops(volume, 0)
            self.helper.cgroups.set_write_iops(volume, 0)
            out = self.helper.volumes.delete(self.id, callback, lock)
            volume['status'] = 'DELETING'
            return Response(volume)
        except NotFound:
            raise HTTPNotFound("No volume named '%s'" % self.id)

    @lock("volumes/%(id)s/resource")
    def audit(self, req, lock):
        # We don't need a real volume to do this, it might be deleted.
        volume = {'id': self.id}
        callback = None
        backup_id = req.params.get('backup_id')
        if backup_id:
            def cb():
                self.helper.make_api_request('backups', backup_id,
                                             data={'status': 'DELETED'})
                # Wee bit of pessimism here. This happens in backup.delete,
                # but if it fails, we'll retry after audit.
                account = req.params.get('account')
                if account:
                    cinder = self.helper.get_cinder(account)
                    try:
                        cinder.force_delete('snapshots', backup_id)
                    except CinderError, e:
                        if e.code != 404:
                            raise
            callback = cb
        self.helper.backups.run_audit(volume, lock=lock, callback=callback)
        return Response(volume)

    @lock("volumes/%(id)s/resource")
    def rename(self, req, lock):
        try:
            logger.info("Renaming logical volume inprogress .")
            volume = self.helper.volumes.get(self.id)
        except NotFound:
            raise HTTPNotFound("Cannot rename non-existant volume '%s'" %
                               self.id)

        callback = None
        new_name = req.params.get('new_name')
        self.helper.volumes.rename(self.id, new_name,
                                  lock=lock, callback=callback)
        logger.info("Renaming logical volume done.")
        return Response(volume)

    def lock(self, req):
        info = inspect(self, req, "volumes/%(id)s/resource")
        with ResourceFile(info['lock_file']) as lock:
            used = lock.used()
            if used:
                resp = {'in-use': True}
                resp.update(used)
                return Response(resp)
            pass
        return Response({'in-use': False})
