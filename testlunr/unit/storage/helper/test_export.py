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

import unittest
import netaddr
import os
from textwrap import dedent
from tempfile import mkdtemp
from shutil import rmtree

from lunr.common.config import LunrConfig
from lunr.storage.helper.utils import ProcessError, ServiceUnavailable, \
    NotFound
from lunr.storage.helper import export

# from lunr.common import logger
# logger.configure(log_to_console=True, capture_stdio=False)


class TestExportHelper(unittest.TestCase):

    def setUp(self):
        self.scratch = mkdtemp()

    def tearDown(self):
        rmtree(self.scratch)

    def test_status(self):
        VOLUME_DATA = dedent(
            """
            tid:1 name:iqn.2010-11.com.rackspace:vol1
            \tlun:0 path:/dev/lunr1/vol1
            tid:2 name:iqn.2010-11.com.rackspace:vol2
            \tlun:0 path:/dev/lunr1/vol2
            tid:3 name:iqn.2010-11.com.rackspace:vol3
            """
        )
        SESSION_DATA = dedent(
            """
            tid:1 name:iqn.2010-11.com.rackspace:vol1
            \tsid:281474997486080 initiator:iqn.2010-11.org:baaa6e50093
            \t\tcid:0 ip:127.0.0.1 state:active hd:none dd:none
            tid:2 name:iqn.2010-11.com.rackspace:vol2
            tid:3 name:iqn.2010-11.com.rackspace:vol3
            """
        )
        proc_iet_volume = os.path.join(self.scratch, 'volume')
        proc_iet_session = os.path.join(self.scratch, 'session')
        ietd_config = os.path.join(self.scratch, 'ietd.conf')
        with open(proc_iet_volume, 'w') as f:
            f.write(VOLUME_DATA)
        with open(proc_iet_session, 'w') as f:
            f.write(SESSION_DATA)
        conf = LunrConfig({
            'export': {
                'proc_iet_volume': proc_iet_volume,
                'proc_iet_session': proc_iet_session,
                'ietd_config': ietd_config,
            }
        })
        h = export.ExportHelper(conf)
        expected = {'exports': 3, 'connected': 1, 'sessions': 3}
        self.assertEquals(h.status(), expected)

    def test_status_unavailable(self):
        proc_iet_volume = os.path.join(self.scratch, 'volume')
        proc_iet_session = os.path.join(self.scratch, 'session')
        ietd_config = os.path.join(self.scratch, 'ietd.conf')
        ietd_config = os.path.join(self.scratch, 'ietd.conf')
        conf = LunrConfig({
            'export': {
                'proc_iet_volume': proc_iet_volume,
                'proc_iet_session': proc_iet_session,
                'ietd_config': ietd_config,
            }
        })
        h = export.ExportHelper(conf)
        self.assertRaises(ServiceUnavailable, h.status)
        try:
            h.status()
        except ServiceUnavailable, e:
            self.assert_('not running' in str(e))

    def test_delete(self):
        vol_id = 'somevolumeid'
        VOLUME_DATA = dedent(
            """
            tid:1 name:iqn.2010-11.com.rackspace:%(vol_id)s
            \tlun:0 path:/dev/lunr1/%(vol_id)s
            tid:2 name:iqn.2010-11.com.rackspace:vol2
            \tlun:0 path:/dev/lunr1/vol2
            tid:3 name:iqn.2010-11.com.rackspace:vol3
            """ % {'vol_id': vol_id}
        )
        SESSION_DATA = dedent(
            """
            tid:1 name:iqn.2010-11.com.rackspace:%(vol_id)s
            \tsid:281474997486080 initiator:iqn.2010-11.org:baaa6e50093
            \t\tcid:0 ip:127.0.0.1 state:active hd:none dd:none
            tid:2 name:iqn.2010-11.com.rackspace:vol2
            tid:3 name:iqn.2010-11.com.rackspace:vol3
            """ % {'vol_id': vol_id}
        )
        proc_iet_volume = os.path.join(self.scratch, 'volume')
        proc_iet_session = os.path.join(self.scratch, 'session')
        ietd_config = os.path.join(self.scratch, 'ietd.conf')
        with open(proc_iet_volume, 'w') as f:
            f.write(VOLUME_DATA)
        with open(proc_iet_session, 'w') as f:
            f.write(SESSION_DATA)
        conf = LunrConfig({
            'export': {
                'proc_iet_volume': proc_iet_volume,
                'proc_iet_session': proc_iet_session,
                'ietd_config': ietd_config,
            }
        })
        h = export.ExportHelper(conf)

        def fake_ietadm(op=None, tid=None):
            fake_ietadm.op = op
            fake_ietadm.tid = tid
            return True
        fake_ietadm.tid = None
        fake_ietadm.op = None
        h.ietadm = fake_ietadm

        h.delete(vol_id)
        self.assertEquals(fake_ietadm.tid, '1')
        self.assertEquals(fake_ietadm.op, 'delete')

    def test_delete_not_found(self):
        proc_iet_volume = os.path.join(self.scratch, 'volume')
        proc_iet_session = os.path.join(self.scratch, 'session')
        ietd_config = os.path.join(self.scratch, 'ietd.conf')
        with open(proc_iet_volume, 'w') as f:
            f.write('')
        with open(proc_iet_session, 'w') as f:
            f.write('')
        conf = LunrConfig({
            'export': {
                'proc_iet_volume': proc_iet_volume,
                'proc_iet_session': proc_iet_session,
                'ietd_config': ietd_config,
            }
        })
        h = export.ExportHelper(conf)
        self.assertRaises(NotFound, h.delete, 'unexported')


class TestInitiatorsAllow(unittest.TestCase):

    def setUp(self):
        self.scratch = mkdtemp()
        self.initiators_allow = os.path.join(self.scratch, 'initiators')
        self.proc_iet_volume = os.path.join(self.scratch, 'volume')
        self.proc_iet_session = os.path.join(self.scratch, 'session')
        self.iqn_prefix = 'iqn.monkey.corp'
        self.default_allows = '10.127.0.0/24'
        self.conf = LunrConfig({
            'export': {
                'iqn_prefix': self.iqn_prefix,
                'initiators_allow': self.initiators_allow,
                'default_allows': self.default_allows,
                'proc_iet_volume': self.proc_iet_volume,
                'proc_iet_session': self.proc_iet_session,
            }
        })
        self.allow_data = dedent(
            """
            # Some random comments we might use in a header
            # just to assert our dominance over this file!
            iqn.monkey.corp:volume-00000001 10.0.0.1
            iqn.monkey.corp:volume-00000002 10.0.0.1, 10.0.0.2
            ALL 10.127.0.0/24
            """
        )
        with open(self.initiators_allow, 'w') as f:
            f.write(self.allow_data)

    def tearDown(self):
        rmtree(self.scratch)

    def test_scan_initiators(self):
        h = export.ExportHelper(self.conf)
        initiators = h._scan_initiators()
        self.assertEqual(len(initiators), 2)
        self.assertItemsEqual(initiators.keys(),
                              ['iqn.monkey.corp:volume-00000001',
                               'iqn.monkey.corp:volume-00000002'])
        first = initiators['iqn.monkey.corp:volume-00000001']
        self.assertItemsEqual(first, ['10.0.0.1'])
        second = initiators['iqn.monkey.corp:volume-00000002']
        self.assertItemsEqual(second, ['10.0.0.1', '10.0.0.2'])

    def test_rewrite_initiators(self):
        h = export.ExportHelper(self.conf)
        initiators = {
            'iqn1': ['allow1'],
            'iqn2': ['allow2'],
            'iqn3': ['allow1', 'allow2'],
        }
        h.initiators_allow_warning = "#placeholder\n"
        h._rewrite_initiators(initiators)
        with open(self.initiators_allow) as allows_file:
            allow_lines = allows_file.readlines()

        last_line = 'ALL %s\n' % self.default_allows
        self.assertItemsEqual(allow_lines, ['#placeholder\n',
                                            'iqn1 allow1\n',
                                            'iqn2 allow2\n',
                                            'iqn3 allow1, allow2\n',
                                            last_line])
        self.assertEqual(allow_lines[-1], last_line)

    def test_blank_rewrite_initiators(self):
        self.conf.values['export']['default_allows'] = ''
        h = export.ExportHelper(self.conf)
        initiators = {
            'iqn1': [''],
            'iqn2': ['allow2'],
            'iqn3': ['allow1', 'allow2'],
        }
        h.initiators_allow_warning = "#placeholder\n"
        h._rewrite_initiators(initiators)
        with open(self.initiators_allow) as allows_file:
            allow_lines = allows_file.readlines()

        last_line = 'ALL \n'
        self.assertItemsEqual(allow_lines, ['#placeholder\n',
                                            'iqn1 \n',
                                            'iqn2 allow2\n',
                                            'iqn3 allow1, allow2\n',
                                            last_line])
        self.assertEqual(allow_lines[-1], last_line)

    def test_add_initiator_allow(self):
        h = export.ExportHelper(self.conf)
        initiators = h._scan_initiators()
        new_ip = netaddr.IPAddress('10.1.1.1')
        new_volid = 'newvolid'
        h.add_initiator_allow(new_volid, new_ip)
        initiators2 = h._scan_initiators()
        self.assertEqual(len(initiators) + 1, len(initiators2))
        target = h._generate_target_name(new_volid)
        self.assertFalse(target in initiators)
        self.assertTrue(target in initiators2)
        self.assertEqual(initiators2[target], [str(new_ip)])

    def test_add_initiator_allow_in_range(self):
        ranges = '10.0.0.0/8,11.0.0.0/8'
        self.conf.values['export']['allow_subnets'] = ranges
        h = export.ExportHelper(self.conf)
        ip = netaddr.IPAddress('10.1.1.1')
        volid = 'v1'
        h.add_initiator_allow(volid, ip)
        initiators = h._scan_initiators()
        target = h._generate_target_name(volid)
        self.assertEqual(initiators[target], [str(ip)])
        ip = netaddr.IPAddress('11.1.1.1')
        volid = 'v2'
        h.add_initiator_allow(volid, ip)
        initiators = h._scan_initiators()
        target = h._generate_target_name(volid)
        self.assertEqual(initiators[target], [str(ip)])
        ip = netaddr.IPAddress('12.1.1.1')
        volid = 'v3'
        h.add_initiator_allow(volid, ip)
        initiators = h._scan_initiators()
        target = h._generate_target_name(volid)
        self.assertEqual(initiators[target], [self.default_allows])

    def test_add_initiator_allow_blank(self):
        h = export.ExportHelper(self.conf)
        new_volid = 'newvolid'
        h.add_initiator_allow(new_volid, None)
        initiators = h._scan_initiators()
        target = h._generate_target_name(new_volid)
        self.assertTrue(target in initiators)
        self.assertEqual(initiators[target], [self.default_allows])

    def test_append_initiator_allow(self):
        h = export.ExportHelper(self.conf)
        initiators = h._scan_initiators()
        new_ip = netaddr.IPAddress('10.0.0.2')
        old_volid = 'volume-00000001'
        h.add_initiator_allow(old_volid, new_ip)
        initiators2 = h._scan_initiators()
        self.assertEqual(len(initiators), len(initiators2))
        target = h._generate_target_name(old_volid)
        self.assertTrue(target in initiators2)
        self.assertEqual(initiators2[target], ['10.0.0.1', str(new_ip)])

    def test_remove_bad_initiator(self):
        # Do we care about this case?
        h = export.ExportHelper(self.conf)
        h.remove_initiator_allow('JUNKVOL')
        self.assert_(True)

    def test_remove_initiator_allow(self):
        h = export.ExportHelper(self.conf)
        old_volid = 'volume-00000002'
        # Remove all allows for this volume
        h.remove_initiator_allow(old_volid)
        initiators = h._scan_initiators()
        self.assertEqual(len(initiators), 1)
        target = h._generate_target_name(old_volid)
        self.assertFalse(target in initiators)

    def check_initiator_all_default_rule(self):
        found_default = False
        with open(self.initiators_allow) as f:
            for line in f:
                line = line.strip()
                if line == 'ALL %s' % self.default_allows:
                    found_default = True
        return found_default

    def test_init_initiator_allows(self):
        f = open(self.initiators_allow, 'w')
        f.close()
        self.assertFalse(self.check_initiator_all_default_rule())

        h = export.ExportHelper(self.conf)
        h.init_initiator_allows()
        initiators = h._scan_initiators()
        self.assertEqual(len(initiators), 0)
        self.assertTrue(self.check_initiator_all_default_rule())

        with open(self.initiators_allow, 'w') as f:
            f.write(self.allow_data)

        h = export.ExportHelper(self.conf)
        h.init_initiator_allows()
        initiators = h._scan_initiators()
        self.assertEqual(len(initiators), 2)
        self.assertTrue(self.check_initiator_all_default_rule())


if __name__ == "__main__":
    unittest.main()
