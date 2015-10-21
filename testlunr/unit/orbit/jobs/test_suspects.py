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


from datetime import datetime, timedelta
from json import dumps
import socket
import unittest
from urlparse import parse_qs
import uuid

from testlunr.unit import patch

from lunr import db
from lunr.db.models import Backup, VolumeType, Volume, Node, Account
from lunr.common.config import LunrConfig
from lunr.orbit.jobs.suspects import BackupSuspects, RestoreSuspects, \
    ScrubSuspects, PruneSuspects
from lunr.orbit.jobs import suspects


class MockLog(object):
    count = 0

    def info(self, msg):
        self.count += 1


class MockResponse(object):
    def __init__(self, body, code):
        self.body = body
        self.code = code

    def getcode(self):
        return self.code

    def read(self):
        return self.body


class TestBackupSuspects(unittest.TestCase):

    def setUp(self):
        self.conf = LunrConfig({'db': {'auto_create': True,
                                       'url': 'sqlite://'}})
        self.sess = db.configure(self.conf)

        vtype = VolumeType('vtype')
        node = Node('node1', volume_type=vtype,
                    hostname='10.127.0.1', port=8080)
        account_id = self.sess.get_or_create_account('test_account').id
        self.volume = Volume(1, 'vtype', node=node, account_id=account_id)
        self.sess.add_all([vtype, node, self.volume])
        self.sess.commit()

    def test_suspects(self):
        backup = BackupSuspects(self.conf, self.sess)

        # Insert a backup suspect
        expected1 = Backup(self.volume, id='1', status='SAVING', size=1,
                           last_modified=datetime(2000, 01, 01, 1, 1, 1))
        self.sess.add(expected1)

        # Not expected, because the status is AVAIL
        notexpected_avail = Backup(
            self.volume, id='2', status='AVAILABLE', size=1,
            last_modified=datetime(2000, 01, 01, 1, 1, 1))
        self.sess.add(notexpected_avail)

        # Not expected, because the time is within our 10 second window
        notexpected_new = Backup(
            self.volume, id='3', status='NEW', size=1,
            last_modified=datetime(2000, 01, 01, 1, 1, 25))
        self.sess.add(notexpected_new)

        # Query the backup suspects for backups older than 10 seconds ago
        results = backup.suspects(
            timedelta(seconds=10), datetime(2000, 01, 01, 1, 1, 30)).all()

        # Assert the correct backups are in the results
        self.assertIn(expected1, results)
        self.assertNotIn(notexpected_avail, results)
        self.assertNotIn(notexpected_new, results)


class TestRestoreSuspects(unittest.TestCase):

    def create(self, status, last_modified):
        volume = Volume(0, 'vtype', status=status,
                        id=str(uuid.uuid4()), node=self.node,
                        account=self.account)
        backup = Backup(volume, status='AVAILABLE',
                        volume_id=str(uuid.uuid4()))
        self.db.add_all([volume, backup])
        self.db.commit()
        # Assign the backup as the restore of the volume
        volume.restore_of = backup.id
        volume.last_modified = last_modified
        self.db.commit()
        return volume

    def setUp(self):
        self.timeout = 42
        self.conf = LunrConfig({
            'db': {'auto_create': True, 'url': 'sqlite://'},
            'restore-suspects': {'span': 'seconds=10'},
            'orbit': {'timeout': self.timeout},
        })
        self.db = db.configure(self.conf)

        self.account = Account()
        vtype = VolumeType('vtype')
        self.node = Node('node', 10, volume_type=vtype,
                         hostname='10.127.0.1', port=8080)
        self.db.add_all([vtype, self.node])

    def test_suspects(self):

        expected = self.create('BUILDING', datetime(2000, 01, 01, 1, 1, 1))
        available = self.create('AVAILABLE', datetime(2000, 01, 01, 1, 1, 1))
        not_expected = self.create('BUILDING',
                                   datetime(2000, 01, 01, 1, 1, 25))

        restore = RestoreSuspects(self.conf, self.db)
        # Query the backup suspects for backups older than 10 seconds ago
        results = restore.suspects(
            timedelta(seconds=10), datetime(2000, 01, 01, 1, 1, 30)).all()

        # Assert the correct backups are in the results
        self.assertIn(expected, results)
        self.assertNotIn(available, results)
        self.assertNotIn(not_expected, results)

    def test_run(self):
        expected = self.create('BUILDING', datetime(2000, 01, 01, 1, 1, 1))
        available = self.create('AVAILABLE', datetime(2000, 01, 01, 1, 1, 1))
        not_expected = self.create('BUILDING',
                                   datetime(2000, 01, 01, 1, 1, 25))
        self.count = 0

        def urlopen(request, **kwargs):
            self.assert_(kwargs.get('timeout'))
            self.assertEquals(kwargs['timeout'], self.timeout)
            self.count += 1
            data = parse_qs(request.get_data())
            self.assertIn('backup_source_volume_id', data)
            self.assertEquals(data['backup_id'][0], expected.restore_of)
            self.assertEquals(data['size'][0], '0')
            return MockResponse('{}', 200)

        restore = RestoreSuspects(self.conf, self.db)
        with patch(suspects, 'urlopen', urlopen):
            restore.run(datetime(2000, 01, 01, 1, 1, 30))
        self.assertEquals(self.count, 1)

    def test_timeout(self):
        expected = self.create('BUILDING', datetime(2000, 01, 01, 1, 1, 1))
        self.called = False

        def urlopen_timeout(request, **kwargs):
            self.called = True
            raise socket.timeout('TIMEOUT')

        restore = RestoreSuspects(self.conf, self.db)
        with patch(suspects, 'urlopen', urlopen_timeout):
            restore.run(datetime(2000, 01, 01, 1, 1, 30))
        self.assert_(self.called)


class TestScubSuspects(unittest.TestCase):

    def create(self, status, last_modified):
        volume = Volume(0, 'vtype', status=status,
                        id=str(uuid.uuid4()), node=self.node,
                        account=self.account)
        backup = Backup(volume, status='AVAILABLE',
                        volume_id=str(uuid.uuid4()))
        self.db.add_all([volume, backup])
        self.db.commit()
        # Assign the backup as the restore of the volume
        volume.restore_of = backup.id
        volume.last_modified = last_modified
        self.db.commit()
        return volume

    def setUp(self):
        self.conf = LunrConfig({
            'db': {'auto_create': True, 'url': 'sqlite://'},
            'scrub-suspects': {'span': 'seconds=10'},
        })
        self.db = db.configure(self.conf)

        self.account = Account()
        vtype = VolumeType('vtype')
        self.node = Node('node', 10, volume_type=vtype,
                         hostname='10.127.0.1', port=8080)
        self.db.add_all([vtype, self.node])
        volume = Volume(0, 'vtype', status='AVAILABLE', id=str(uuid.uuid4()),
                        node=self.node, account=self.account)
        volume = Volume(0, 'vtype', status='DELETED', id=str(uuid.uuid4()),
                        node=self.node, account=self.account)
        volume = Volume(0, 'vtype', status='DELETING', id=str(uuid.uuid4()),
                        node=self.node, account=self.account)
        self.db.add(volume)
        self.db.commit()

    def test_suspects(self):
        backup = ScrubSuspects(self.conf, self.db)

        not_expected1 = Volume(0, 'vtype', status='AVAILABLE', id='n1',
                               last_modified=datetime(2000, 01, 01, 1, 1, 1),
                               node=self.node, account=self.account)
        self.db.add(not_expected1)

        not_expected2 = Volume(0, 'vtype', status='DELETING', id='n2',
                               last_modified=datetime(2000, 01, 01, 1, 1, 25),
                               node=self.node, account=self.account)

        self.db.add(not_expected1)
        expected = Volume(0, 'vtype', status='DELETING', id='v1',
                          last_modified=datetime(2000, 01, 01, 1, 1, 1),
                          node=self.node, account=self.account)
        self.db.add(expected)

        # Query the suspects for scrubs that are older than 10 seconds ago
        results = backup.suspects(
            timedelta(seconds=10), datetime(2000, 01, 01, 1, 1, 30)).all()

        # Assert the correct scrubs are in the results
        self.assertIn(expected, results)
        self.assertNotIn(not_expected1, results)
        self.assertNotIn(not_expected2, results)


class TestPruneSuspects(unittest.TestCase):

    def setUp(self):
        self.conf = LunrConfig({'db': {'auto_create': True,
                                       'url': 'sqlite://'}})
        self.sess = db.configure(self.conf)

        vtype = VolumeType('vtype')
        node = Node('node1', volume_type=vtype,
                    hostname='10.127.0.1', port=8080)
        account_id = self.sess.get_or_create_account('test_account').id
        self.volume = Volume(1, 'vtype', node=node, account_id=account_id)
        self.sess.add_all([vtype, node, self.volume])
        self.sess.commit()

    def test_used(self):
        backup = Backup(self.volume, id='1', status='SAVING', size=1,
                        last_modified=datetime(2000, 01, 01, 1, 1, 1))

        def urlopen(request, **kwargs):
            return MockResponse(dumps({
                'in-use': True,
                'uri': 'DELETE /volumes/ed209cdd-1317-41e8-8474-b0c0f6c3369c/'
                       'backups/a30a6e5b-2a96-489c-bde1-56f9c615ea1f',
            }), 200)

        prune = PruneSuspects(self.conf, self.sess)
        with patch(suspects, 'urlopen', urlopen):
            with patch(suspects, 'log', MockLog()):
                prune.locked(backup)
                self.assertEquals(suspects.log.count, 1)


if __name__ == "__main__":
    unittest.main()
