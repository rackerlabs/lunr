#! /usr/bin/env python
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


from contextlib import contextmanager
import json
from socket import gethostbyname
import os
import re
import time
from time import sleep
import unittest
from urllib2 import Request, urlopen, HTTPError
from urllib import urlencode
from urlparse import urlparse
from uuid import uuid4

from lunr.storage.helper.utils import execute, ProcessError
from lunr.storage.helper.base import Helper
from lunr.storage.urlmap import urlmap

from testlunr.functional import SkipTest, LunrTestCase, \
        TemporaryDirectory, LunrStorageService
from testlunr.functional.test_api import LunrApiService


def find_device(portal, target):
    if ':' not in portal:
        portal += ':3260'
    device_template = "/dev/disk/by-path/ip-%(portal)s-iscsi-%(target)s-lun-0"
    device = device_template % locals()
    for attempts in range(3):
        sleep(attempts ** 2)
        if os.path.exists(device):
            return device
    raise Exception("Unable to find iscsi export at '%s'" % device)


def find_initiatorname():
    with open('/etc/iscsi/initiatorname.iscsi') as f:
        for line in f:
            m = re.match('InitiatorName=(.+)', line)
            if m:
                return m.group(1)


@contextmanager
def temporary_mount(device):
    with TemporaryDirectory() as mount_path:
        execute('mount', device, mount_path)
        yield mount_path
        # leave a message in the bottle
        execute('umount', mount_path)


class StorageServerTestCase(LunrTestCase):

    def setUp(self):
        # Start the Lunr Storage Service if needed
        self.storage = LunrStorageService()

        # setup our timeouts
        super(StorageServerTestCase, self).setUp()

    def request(self, uri, *args, **kwargs):
        url = "http://%s:%s/%s" % (self.storage.host, self.storage.port, uri)
        return self.urlopen(url, *args, **kwargs)

    # FIXME. This doesn't really work for the backup list. The backup job is
    # still running the callback after it has removed the entry from the local
    # manifest.
    def wait_until_gone(self, key, uri):
        for i in range(10):
            resp = self.request(uri)
            if resp.code != 200:
                return
            if key in resp.body:
                sleep(i)
                continue
            return
        self.fail("'%s' never went away for GET '%s'" % (key, uri))

    def wait_on_code(self, uri, code, *args, **kwargs):
        for i in range(10):
            resp = self.request(uri, *args, **kwargs)
            if resp.code == code:
                return
            sleep(i)
        self.fail("%s never returned a code of '%s'" % (uri, code))


class TestVolumeController(StorageServerTestCase):

    def assertShow(self, resp, volname):
        self.assertEqual(resp.body['id'], volname)
        self.assertIn(volname, resp.body['path'])
        self.assertIn('size', resp.body)

    def test_create_and_delete_volume(self):
        volname = str(uuid4())
        # show not found
        resp = self.get('volumes/%s' % volname)
        self.assertEquals(resp.code, 404)
        # create
        resp = self.put('volumes/%s?size=0' % volname)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volname)
        # show
        resp = self.get('volumes/%s' % volname)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volname)
        # delete
        resp = self.delete('volumes/%s' % volname)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volname)
        self.assertEquals(resp.body['status'], 'DELETING')

    def test_create_already_exists(self):
        volname = str(uuid4())
        # create
        resp = self.put('volumes/%s?size=0' % volname)
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['id'], volname)
        # show
        resp = self.get('volumes/%s' % volname)
        self.assertCode(resp, 200)
        self.assertShow(resp, volname)
        # create again
        resp = self.put('volumes/%s?size=0' % volname)
        self.assertCode(resp, 409)
        self.assertIn('already exists', resp.body['reason'])
        # Show
        resp = self.get('volumes/%s' % volname)
        self.assertCode(resp, 200)
        self.assertShow(resp, volname)
        # delete
        resp = self.delete('volumes/%s' % volname)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volname)
        self.assertEquals(resp.body['status'], 'DELETING')

    def test_export(self):
        # create a volume
        volume = str(uuid4())
        resp = self.put('volumes/%s?size=0' % volume)
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['id'], volume)

        # iscsiadm does not know how to resolve 'localhost'
        # we work around that here
        host_ip = gethostbyname(self.storage.host)
        initiator = find_initiatorname()

        # create the export for some other ip
        resp = self.put('volumes/%s/export' % volume, params={
            'ip': '10.10.10.10',
            'initiator': initiator
        })

        self.assertCode(resp, 200)
        target = resp.body['name']
        self.assert_(volume in target)

        # iscsi initator discovery should not find our volume
        try:
            out = execute('iscsiadm', mode='discovery', type='sendtargets',
                          portal=host_ip)
            self.assertNotIn(volume, out)
        except ProcessError, e:
            # Newer iscsiadm (than precise) throws an error on no targets
            self.assertEquals(e.errcode, 21)

        # clean up
        self.delete('volumes/%s/export' % volume)

        # create the export for our ip
        resp = self.put('volumes/%s/export' % volume, params={
            'ip': host_ip,
            'initiator': initiator
        })
        self.assertCode(resp, 200)
        target = resp.body['name']
        self.assert_(volume in target)

        # iscsi initator login
        out = execute('iscsiadm', mode='discovery', type='sendtargets',
                      portal=host_ip)

        # Now discovery certainly *should* show our volume.
        self.assertIn(volume, out)

        execute('iscsiadm', mode='node', targetname=target,
                portal=host_ip, login=None)

        # Export should show attached
        resp = self.get('volumes/%s/export' % volume)
        self.assertEquals(resp.body['sessions'][0]['connected'], True)

        # find device
        device = find_device(host_ip, target)

        # Block device should work as expected
        data = "IT JUST GOT REAL!"
        with open(device, 'wb') as file:
            file.write(data)

        with open(device, 'rb') as file:
            input = file.read(len(data))
            self.assertEquals(data, input)

        # Shouldn't be able to delete export while attached with no intitator
        resp = self.delete('volumes/%s/export' % volume)
        self.assertEquals(resp.code, 409)

        # Shouldn't be able to delete export while attached with our initiator
        initiator = find_initiatorname()
        resp = self.delete('volumes/%s/export?initiator=%s' %
                           (volume, initiator))
        self.assertEquals(resp.code, 409)

        # Should be able to "delete" export for a nonattached initiator
        resp = self.delete('volumes/%s/export?initiator=%s' %
                           (volume, 'someotheriniator'))
        self.assertEquals(resp.code // 100, 2)

        # iscsi initator logout
        execute('iscsiadm', mode='node', targetname=target, portal=host_ip,
                logout=None)

        # *Should* be able to delete export after detaching
        resp = self.delete('volumes/%s/export?initiator=%s' %
                           (volume, 'someotheriniator'))
        self.assertEquals(resp.code // 100, 2)

        # Should now 404
        resp = self.delete('volumes/%s/export' % volume)
        self.assertEquals(resp.code, 404)

        # delete the volume
        resp = self.delete('volumes/%s' % volume)
        self.assertEquals(resp.code // 100, 2)

    def test_export_force_delete(self):
        # create a volume
        volume = str(uuid4())
        resp = self.put('volumes/%s?size=0' % volume)
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['id'], volume)

        # iscsiadm does not know how to resolve 'localhost'
        # we work around that here
        host_ip = gethostbyname(self.storage.host)

        # create the export
        resp = self.put('volumes/%s/export' % volume, params={
            'ip': host_ip,
            'initiator': 'fake_initiator'
        })
        self.assertCode(resp, 200)
        target = resp.body['name']
        self.assert_(volume in target)

        # iscsi initator login
        execute('iscsiadm', mode='discovery', type='sendtargets',
                portal=host_ip)
        execute('iscsiadm', mode='node', targetname=target,
                portal=host_ip, login=None)

        resp = self.get('volumes/%s/export' % volume)
        self.assertEquals(resp.body['sessions'][0]['connected'], True)

        # force delete the export
        resp = self.delete('volumes/%s/export' % volume,
                           params={'force': True})
        self.assertCode(resp, 200)

        resp = self.get('volumes/%s/export' % volume)
        self.assertEquals(resp.body.get('sessions', []), [])

        # iscsi initator logout
        execute('iscsiadm', mode='node', targetname=target,
                portal=host_ip, logout=None)

        # delete the volume
        resp = self.delete('volumes/%s' % volume)
        self.assertCode(resp, 200)

    def test_max_name_length(self):
        # Volume Name
        resp = self.put('volumes/%s?size=0' % ('A' * 95))
        self.assertCode(resp, 412)
        self.assertIn("length of volume id cannot exceed 94",
                      resp.body['reason'])

        # Backup id
        resp = self.put('volumes/somename', params={
            'size': 0,
            'backup_id': 'A' * 61,
        })
        self.assertCode(resp, 412)
        self.assertIn("length of 'backup_id' parameter", resp.body['reason'])

        # Source Volume ID
        resp = self.put('volumes/somename', params={
            'size': 0,
            'backup_id': 'somebackup',
            'backup_source_volume_id': 'A' * 61,
        })
        self.assertCode(resp, 412)
        self.assertIn("length of 'backup_source_volume_id' parameter",
                      resp.body['reason'])

    def test_volume_name_invalid_chars(self):
        resp = self.put('volumes/some.name?size=0')
        self.assertCode(resp, 412)
        self.assertEqual("volume id cannot contain '.'", resp.body['reason'])


class TestCloneController(StorageServerTestCase):

    def test_clone(self):
        source_id = str(uuid4())
        target = None
        target_id = str(uuid4())

        resp = self.put('volumes/%s?size=0' % source_id)
        self.assertCode(resp, 200)

        with open(resp.body['path'], 'w') as f:
            for i in range(20):
                f.write('asdf' * 1024)
            f.flush()
            os.fsync(f.fileno())

        source_md5 = self.md5sum(resp.body['path'])

        resp = self.put('volumes/%s?size=0&source_host=%s'
                        '&source_port=%s&source_volume_id=%s'
                        % (target_id, self.storage.host,
                           self.storage.port, source_id))
        self.assertCode(resp, 200)
        target = resp.body['path']

        # Find the clone and wait for it to be done.
        resp = self.get('volumes')
        clone = None
        for volume in resp.body:
            if target_id == volume.get('clone_id', 0):
                clone = volume['id']
                break
        if not clone:
            self.fail("Could not find clone for volume: %s" % source_id)

        self.wait_on_code('volumes/%s' % clone, 404)

        target_md5 = self.md5sum(target)
        self.assertEquals(source_md5, target_md5)

        self.delete('volumes/%s' % source_id)
        self.wait_on_code('volumes/%s' % source_id, 404)
        self.delete('volumes/%s' % target_id)
        self.wait_on_code('volumes/%s' % target_id, 404)


class TestBackupController(StorageServerTestCase):

    def test_create_duplicate_request_accepted(self):
        volume_id = str(uuid4())
        backup_id = str(uuid4())
        backup2_id = str(uuid4())

        # Create a volume
        resp = self.put('volumes/%s?size=0' % volume_id)
        self.assertCode(resp, 200)

        # Start a backup of the volume
        resp = self.put('volumes/%s/backups/%s?timestamp=1' % (
                volume_id, backup_id))
        self.assertEquals(resp.code, 200)

        # Attempt to start another with the same name
        resp = self.put('volumes/%s/backups/%s?timestamp=2' %
                        (volume_id, backup_id))
        # Should respond with a 202 Accepted
        self.assertEquals(resp.code, 202)

        # Attempt to start another with the different name
        resp = self.put('volumes/%s/backups/blah?timestamp=3' % volume_id)
        # Should respond with a 409 Conflict
        self.assertEquals(resp.code, 409)

        # Wait on the backup to complete
        self.wait_on_code('volumes/%s/backups/%s' %
                          (volume_id, backup_id), 404)

        # Start a SECOND backup of the volume
        resp = self.put('volumes/%s/backups/%s?timestamp=4' %
                        (volume_id, backup2_id))
        self.assertEquals(resp.code, 200)

        # Wait on the backup to complete
        self.wait_on_code('volumes/%s/backups/%s' %
                          (volume_id, backup2_id), 404)

        # Delete the backup
        resp = self.request('volumes/%s/backups/%s' %
                            (volume_id, backup_id), 'DELETE')
        self.assertCode(resp, 200)

        # Attempt to delete another backup with the same name
        # With instant deletes, this is a whole lot harder to catch this
        # race, since the previous call has probably already completed.
        # resp = self.request('volumes/%s/backups/%s'
        # % (volume_id, backup_id), 'DELETE')
        # Should respond with a 202 Accepted
        # self.assertCode(resp, 202)
        # resp = self.request('volumes/%s/backups/%s'
        # % (volume_id, backup2_id), 'DELETE')
        # Should respond with a 409 Conflict
        # self.assertCode(resp, 409)

        # Wait until the FIRST backup is deleted
        self.wait_on_code('volumes/%s/backups/%s' % (volume_id, backup_id),
                          404)

        # Now delete the SECOND backup
        resp = self.request('volumes/%s/backups/%s' %
                            (volume_id, backup2_id), 'DELETE')
        self.assertCode(resp, 200)
        # Wait until the SECOND backup is deleted
        self.wait_on_code('volumes/%s/backups/%s' % (volume_id, backup2_id),
                          404)
        # Delete the volume
        resp = self.request('volumes/%s' % volume_id, 'DELETE')
        self.assertCode(resp, 200)
        # Wait until volume is deleted
        self.wait_on_code('volumes/%s' % volume_id, 404)

    def test_restore_from_backup(self):
        volume_id = str(uuid4())
        backup_id = str(uuid4())
        restore_id = str(uuid4())

        # Create a volume
        resp = self.put('volumes/%s?size=0' % volume_id)
        self.assertCode(resp, 200)

        # Start a backup of the volume
        resp = self.put('volumes/%s/backups/%s?timestamp=1' %
                        (volume_id, backup_id))
        self.assertEquals(resp.code, 200)

        # Wait on the backup to complete
        self.wait_on_code('volumes/%s/backups/%s' %
                          (volume_id, backup_id), 404)

        # Restore from the backup
        resp = self.put(
            'volumes/%s?size=0' % restore_id,
            params={'backup_id': backup_id,
                    'backup_source_volume_id': volume_id})
        self.assertEquals(resp.code, 200)

        # Attempt the same operation again
        resp = self.put('volumes/%s?size=0' % restore_id, params={
            'backup_id': backup_id,
            'backup_source_volume_id': volume_id
        })
        # Should return 202 Accepted
        self.assertCode(resp, 202)

        # Wait until the restore is complete
        self.wait_until_gone('status', 'volumes/%s' % restore_id)

        # Now delete the backup
        resp = self.request('volumes/%s/backups/%s' %
                            (volume_id, backup_id), 'DELETE')
        self.assertCode(resp, 200)

        # Wait until the backup is deleted
        self.wait_on_code('volumes/%s/backups/%s' % (volume_id, backup_id),
                          404)

        # Delete the volume
        resp = self.request('volumes/%s' % volume_id, 'DELETE')
        self.assertCode(resp, 200)
        # Delete the restore
        resp = self.request('volumes/%s' % restore_id, 'DELETE')
        self.assertCode(resp, 200)

    def test_create_duplicate_backup_id(self):
        volume1_id = str(uuid4())
        volume2_id = str(uuid4())
        backup_id = str(uuid4())

        resp = self.put('volumes/%s?size=0' % volume1_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volume1_id)

        resp = self.put('volumes/%s?size=0' % volume2_id)
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['id'], volume2_id)

        url = 'volumes/%s/backups/%s?timestamp=1' % (
                volume1_id, backup_id)
        resp = self.put(url)
        self.assertEquals(resp.code, 200)
        self.assertEquals(resp.body['status'], 'SAVING')

        url = 'volumes/%s/backups/%s?timestamp=1' % (
                volume2_id, backup_id)
        resp = self.put(url)
        # No duplicate backup ids on different volumes!
        self.assertEquals(resp.code, 409)

        # Backup dissappears when finished.
        self.wait_on_code('volumes/%s/backups/%s' %
                          (volume1_id, backup_id), 404)

        resp = self.request('volumes/%s/backups/%s' %
                            (volume1_id, backup_id), 'DELETE')
        self.assertCode(resp, 200)
        self.assertEquals(resp.body['status'], 'DELETING')

        self.wait_on_code('volumes/%s/backups/%s' %
                          (volume1_id, backup_id), 404)

        resp = self.request('volumes/%s' % volume1_id, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'DELETING')
        self.wait_on_code('volumes/%s' % volume1_id, 404)

        resp = self.request('volumes/%s' % volume2_id, 'DELETE')
        self.assertEquals(resp.code // 100, 2)
        self.assertEquals(resp.body['status'], 'DELETING')
        self.wait_on_code('volumes/%s' % volume2_id, 404)


class TestStatusController(StorageServerTestCase):

    def test_status(self):
        resp = self.get('status')
        self.assertEquals(resp.code // 100, 2)
        expected = ['api', 'backups', 'exports', 'volumes']
        self.assertEquals(sorted(resp.body.keys()), expected)

""" Commenting this out for now until we get started on the backup story
class TestBackupController(StorageServerTestCase):

    def setUp(self):
        # do something to assert root-ness
        try:
            execute('iscsiadm')
        except ProcessError, e:
            raise SkipTest(str(e))

    def test_backup_and_restore_volume(self):
        volname = 'vol-%s' % uuid4()
        # create volume
        resp = self.put('volumes/%s' % volname)
        # create export
        resp = self.put('exports/%s' % volname)
        self.assertEquals(resp.code // 100, 2)
        export = resp.body
        self.assert_(volname in export['name'])

        # iscsi initator login
        execute('iscsiadm', mode='discovery', type='sendtargets',
                portal=self.host)
        execute('iscsiadm', mode='node', targetname=export['name'],
                portal=self.host, login=None)

        # find device
        device = find_device(export['name'])
        self.assert_(os.path.exists(device))

        # format device
        execute('mkfs.ext4', device, '-F')
        # mount device
        with temporary_mount(device) as mount_path:
            execute('chmod', '777', mount_path)
            # leave a message in the bottle
            file_path = os.path.join(mount_path, 'bottle')
            with open(file_path, 'w') as f:
                f.write('test message')

        # take backup
        resp = self.put('backups/%s' % volname)
        self.assertEquals(resp.code // 100, 2)
        backup = resp.body
        self.assert_(volname in backup['name'])

        # wait for backup to finish
        while True:
            resp = self.get('backups/%s' % volname)
            self.assertEquals(resp.code // 100, 2)
            backup = resp.body
            status = backup['status']
            try:
                self.assert_(status.endswith('ING'), status)
            except AssertionError:
                if status != 'FINISHED':
                    raise
                break
            sleep(2)

        # delete bottle
        with temporary_mount(device) as mount_path:
            file_path = os.path.join(mount_path, 'bottle')
            os.unlink(file_path)

        # make restore
        resp = self.put('restores/%s' % volname)
        self.assertEquals(resp.code // 100, 2)
        restore = resp.body
        self.assert_(volname in restore['name'])

        # wait for restore to finish
        while True:
            resp = self.get('restores/%s' % volname)
            self.assertEquals(resp.code // 100, 2)
            restore = resp.body
            status = restore['status']
            try:
                self.assert_(status.endswith('ING'), status)
            except AssertionError:
                if status != 'FINISHED':
                    raise
                break
            sleep(2)

        # veriy message in the bottle
        with temporary_mount(device) as mount_path:
            file_path = os.path.join(mount_path, 'bottle')
            with open(file_path) as f:
                self.assertEquals(f.read(), 'test message')

        # iscsi initator logout
        execute('iscsiadm', mode='node', targetname=export['name'],
                portal=self.host, logout=None)

        # delete export
        resp = self.delete('exports/%s' % volname)
        self.assertEquals(resp.code // 100, 2)

        # delete volume
        resp = self.delete('volumes/%s' % volname)
"""

if __name__ == "__main__":
    unittest.main()
