#!/usr/bin/env python
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


import errno
import itertools
from json import dumps
import netaddr
from optparse import OptionParser
import os
from shutil import rmtree
from StringIO import StringIO
import subprocess
import sys
import socket
from tempfile import mkdtemp
from time import sleep
from urllib2 import addinfourl, URLError, HTTPError
import unittest
from uuid import uuid4
import urlparse

from lunr.storage.helper import base
from lunr.storage.helper import volume, utils, export
from lunr.storage.helper.utils import APIError
from lunr.common.config import LunrConfig
from lunr.common.lock import ResourceFile

from testlunr.unit import patch

vgs_parser = OptionParser('vgs')
vgs_parser.add_option('--units')
vgs_parser.add_option('--noheadings', action='store_true')
vgs_parser.add_option('--separator')
vgs_parser.add_option('--options')

lvs_parser = OptionParser('lvs')
lvs_parser.add_option('--units')
lvs_parser.add_option('--noheadings', action='store_true')
lvs_parser.add_option('--separator')
lvs_parser.add_option('--options')

lvcreate_parser = OptionParser('lvcreate')
lvcreate_parser.add_option('--name')
lvcreate_parser.add_option('--size')
lvcreate_parser.add_option('--addtag')
lvcreate_parser.add_option('--snapshot', action='store_true')

lvremove_parser = OptionParser('lvremove')
lvremove_parser.add_option('--force', action='store_true')

lvrename_parser = OptionParser('lvrename')

lvchange_parser = OptionParser('lvchange')
lvchange_parser.add_option('--addtag')
lvchange_parser.add_option('--deltag')

ietadm_parser = OptionParser('ietadm')
ietadm_parser.add_option('--tid')
ietadm_parser.add_option('--params')
ietadm_parser.add_option('--op')
ietadm_parser.add_option('--lun')

dmsetup_parser = OptionParser('dmsetup')
dmsetup_parser.add_option('--force', action='store_true')

iscsiadm_parser = OptionParser('iscsiadm')
iscsiadm_parser.add_option('--portal')
iscsiadm_parser.add_option('--type')
iscsiadm_parser.add_option('--mode')
iscsiadm_parser.add_option('--login')
iscsiadm_parser.add_option('--logout')

dd_parser = OptionParser('dd')

qemu_img_parser = OptionParser('qemu_img')
qemu_img_parser.add_option('-O')

tar_parser = OptionParser('tar')
tar_parser.add_option('-C')
tar_parser.add_option('-z')
tar_parser.add_option('-x')
tar_parser.add_option('-f')

vhd_util_parser = OptionParser('vhd-util')
vhd_util_parser.add_option('-n')
vhd_util_parser.add_option('-s')
vhd_util_parser.add_option('-j')
vhd_util_parser.add_option('-p')
vhd_util_parser.add_option('-v', action='store_true')

mkfs_ext4_parser = OptionParser('mkfs.ext4')
mkfs_ext4_parser.add_option('-E')

mount_parser = OptionParser('mount')
mount_parser.add_option('-t')
mount_parser.add_option('-o')

umount_parser = OptionParser('umount')


UNITS = {
    'B': 1,
    'M': 1048576,
    'G': 1073741824,
}


def unit_size_to_bytes(size):
    unit = size[-1]
    value = int(size[:-1])
    return value * UNITS[unit]


class MockStorageNode(object):

    def __init__(self, scratch='/tmp'):
        self.volumes = []
        self.exports = []
        self.mounts = {}
        self.scratch = scratch
        self.run_dir = os.path.join(self.scratch, 'run')
        self.device_prefix = os.path.join(self.scratch, 'dev')
        self.proc_iet_volume = os.path.join(self.scratch, 'proc_iet_volume')
        self.proc_iet_session = os.path.join(self.scratch, 'proc_iet_session')
        for f in (self.proc_iet_volume, self.proc_iet_session):
            with open(f, 'w'):
                pass
        os.mkdir(self.device_prefix)
        os.mkdir(self.run_dir)

    def tearDown(self):
        pass


class MockProcess(object):

    def __init__(self, argv, storage=None):
        self.storage = storage
        if not self.storage:
            self.storage = MockStorageNode()
        self.cmdline = ' '.join(argv)
        self.cmd = argv.pop(0).replace('-', '_')
        self.cmd = self.cmd.replace('.', '_')
        self.args = list(argv)

    def vgs(self, options, args):
        volume_group = args[0]
        lines = []
        headers = options.options.split(',')
        if not options.noheadings:
            line = options.separator.join(headers)
            lines.append(line)
        volumes = [v for v in self.storage.volumes
                   if v['vg_id'] == volume_group]
        if not volumes:
            errmsg = "Volume group '%s' not found." % volume_group
            raise utils.ProcessError(self.cmdline, '', errmsg, 5)
        bytes_used = sum([int(v['lv_size']) for v in volumes])
        vg_size = 10000000000
        info = {
            'vg_size': '%sB' % vg_size,
            'lv_count': str(len(volumes))
        }
        info['vg_free'] = "%sB" % (vg_size - bytes_used)
        values = [info[key] for key in headers]
        line = options.separator.join(values)
        lines.append(line)
        return '\n'.join(lines)

    def lvs(self, options, args):
        volume_group = args[0]
        lines = []
        headers = options.options.split(',')
        if not options.noheadings:
            line = options.separator.join(headers)
            lines.append(line)

        if '/' in volume_group:
            group, name = volume_group.split('/')
            volumes = [v for v in self.storage.volumes if (
                v['lv_name'] == name and v['vg_id'] == group)]
            if not volumes:
                errmsg = "One or more specified logical volume(s) not found."
                raise utils.ProcessError(self.cmdline, '', errmsg, 5)
        else:
            volumes = [v for v in self.storage.volumes
                       if v['vg_id'] == volume_group]
        for v in volumes:
            values = []
            for key in headers:
                if 'size' in key:
                    value = '%sB' % v[key]
                else:
                    value = v[key]
                values.append(value)
            line = options.separator.join(values)
            lines.append(line)
        return '\n'.join(lines)

    def lvcreate(self, options, args):
        if options.snapshot:
            path = args.pop(0)
            volume_group, origin = path.rsplit('/', 2)[-2:]
        else:
            origin = ''
            volume_group = args.pop(0)
        conflicts = [v for v in self.storage.volumes if (
            v['lv_name'] == options.name and v['vg_id'] == volume_group)]
        if conflicts:
            errmsg = 'Logical volume "%s" already exists in ' \
                    'volume group "%s"' % (options.name, volume_group)
            raise utils.ProcessError(self.cmdline, '', errmsg, 5)
        v = {'vg_id': volume_group}
        v['lv_name'] = options.name
        v['lv_size'] = str(unit_size_to_bytes(options.size))
        v['origin'] = origin
        v['origin_size'] = str(unit_size_to_bytes(options.size))
        v['lv_tags'] = options.addtag or ''
        v['lv_kernel_major'] = '253'
        self.storage.volumes.append(v)
        v['lv_kernel_minor'] = str(len(self.storage.volumes))
        lun_path = os.path.join(
            self.storage.device_prefix, volume_group, options.name)
        try:
            os.makedirs(os.path.dirname(lun_path))
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        with open(lun_path, 'w'):
            pass
        return 'Logical volume "%s" created' % options.name

    def lvremove(self, options, args):
        volume_group, name = args.pop(0).rsplit('/', 2)[-2:]
        if not options.force:
            choice = raw_input("Do you really want to remove active "
                               "logical volume vol1? [y/n]: ")
            if not choice.lower.startswith('y'):
                errmsg = "Logical volume %s not removed" % name
                raise utils.ProcessError(self.cmdline, errmsg, '', 5)
        v = [v for v in self.storage.volumes if v['lv_name'] == name and
             v['vg_id'] == volume_group][0]
        self.storage.volumes.remove(v)
        lun_path = os.path.join(self.storage.device_prefix, volume_group, name)
        os.unlink(lun_path)
        return 'Logical volume "%s" successfully removed' % name

    def lvrename(self, options, args):
        try:
            if len(args) < 3:
                old_path, new_id = args
                volume_group, old_id = old_path.rsplit('/', 1)
            else:
                volume_group, old_id, new_id = args
                old_path = os.path.join(self.storage.device_prefix,
                                        volume_group, old_id)
        except ValueError:
            errmsg = 'Old and new logical volume names required'
            raise utils.ProcessError(self.cmdline, '', errmsg, 3)
        if not os.path.isabs(old_path):
            errmsg = 'Path required for Logical Volume "%s"' % old_id
            raise utils.ProcessError(self.cmdline, '', errmsg, 3)
        if not os.path.exists(old_path):
            errmsg = 'Existing logical volume "%s" not found in volume ' \
                    'group "%s"' % (old_id, volume_group)
            raise utils.ProcessError(self.cmdline, '', errmsg, 5)
        # find & remove old
        old_v = [v for v in self.storage.volumes if v['lv_name'] == old_id and
                 v['vg_id'] == volume_group][0]
        self.storage.volumes.remove(old_v)
        os.unlink(old_path)
        # create new
        v = dict(old_v)
        v['lv_name'] = new_id
        self.storage.volumes.append(v)
        lun_path = os.path.join(self.storage.device_prefix, volume_group,
                                new_id)
        try:
            os.makedirs(os.path.dirname(lun_path))
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        with open(lun_path, 'w'):
            pass
        return 'Renamed "%s" to "%s" in volume group "%s"' % (
            old_id, new_id, volume_group)

    def lvchange(self, options, args):
        volume_group, name = args.pop(0).rsplit('/', 2)[-2:]
        old_v = [v for v in self.storage.volumes if v['lv_name'] == name and
                 v['vg_id'] == volume_group][0]
        if options.deltag:
            v['lv_tags'] = v['lv_tags'].replace(options.deltag, '')
        if options.addtag:
            v['lv_tags'] = options.addtag or ''
        return "Why does it care about a return value?"

    def write_proc_iet_volume(self):
        lines = []
        session_lines = []
        for export in self.storage.exports:
            if not export:
                continue
            params = []
            for k in ('tid', 'name'):
                params.append('%s:%s' % (k, export[k]))
            line = ' '.join(params)
            session_lines.append(line)
            lines.append(line)
            params = []
            for lun in export['luns']:
                for k in ('lun', 'state', 'iotype', 'iomode', 'path'):
                    params.append('%s:%s' % (k, lun[k]))
                line = '\t' + ' '.join(params)
                lines.append(line)
        with open(self.storage.proc_iet_volume, 'w') as f:
            f.writelines('\n'.join(lines))
        with open(self.storage.proc_iet_session, 'w') as f:
            f.writelines('\n'.join(session_lines))
        """
        with open(self.storage.proc_iet_volume) as f:
            print 'proc_iet_volume:\n' + f.read()
        """

    def ietadm(self, options, args):
        # print 'options: %s' % options
        params = {}
        if options.params:
            params = dict(p.split('=') for p in options.params.split(','))
        # print 'params: %s' % params
        # print 'args: %s' % args

        def new():
            if options.tid.isdigit():
                tid = int(options.tid)
            else:
                tid = len(self.storage.exports) + 1
            try:
                export = self.storage.exports[tid - 1]
            except IndexError:
                if 'Name' not in params:
                    raise utils.ProcessError(self.cmdline, 'Invalid argument.',
                                             '', 234)
                for export in self.storage.exports:
                    if export['name'] == params['Name']:
                        # duplicate target name
                        raise utils.ProcessError(self.cmdline, '',
                                                 'Invalid argument.', 234)
                export = {
                    'tid': tid,
                    'name': params['Name'],
                    'luns': [],
                }
            if options.lun:
                for volume in self.storage.volumes:
                    if params['Path'].endswith(volume['lv_name']):
                        break
                else:
                    # did not find path, and break out - path does not exist!
                    raise utils.ProcessError(self.cmdline, '',
                                             'No such file.', 254)
                # create new lun
                lun = {
                    'lun': options.lun,
                    'path': params['Path'],
                    'state': 0,
                    'iotype': 'fileio',
                    'iomode': 'wt',
                }
                i = int(options.lun)
                try:
                    export['luns'][i]
                except IndexError:
                    pass
                else:
                    if export['luns'][i]['lun'] == options.lun:
                        # TODO(clayg): confirm error code
                        raise utils.ProcessError(self.cmdline,
                                                 'Invalid argument.',
                                                 '', 234)
                export['luns'].insert(i, lun)
            while len(self.storage.exports) < tid:
                self.storage.exports.append(None)
            self.storage.exports[tid - 1] = export
            return ''

        def delete():
            if options.tid.isdigit():
                tid = int(options.tid)
            else:
                tid = len(self.storage.exports) + 1
            try:
                export = self.storage.exports[tid - 1]
            except IndexError:
                # TODO(clayg): confirm error code
                raise utils.ProcessError(self.cmdline, 'Invalid argument', '',
                                         234)
            if options.lun:
                # delete lun first
                for lun in export['luns']:
                    if lun['lun'] == options.lun:
                        break
                else:
                    # TODO(clayg): confirm error code
                    raise utils.ProcessError(self.cmdline, 'Invalid argument',
                                             '', 234)
                export['luns'].remove(lun)
            else:
                # delete target
                self.storage.exports.remove(export)
            return ''

        op_map = {
            'new': new,
            'delete': delete,
        }
        out = op_map[options.op]()
        self.write_proc_iet_volume()
        return out

    def iscsiadm(self, options, args):
        return 'fake'

    def dd(self, options, args):
        return 'later'

    def dmsetup(self, options, args):
        return "why?"

    def qemu_img(self, options, args):
        args_copy = args[:]
        args_copy.insert(0, 'qemu-img')
        args_copy.insert(2, '-O')
        args_copy.insert(3, options.O)
        stuff = ' '.join(args_copy)
        file_ = args.pop()
        # This is a little silly, but it lets us know we did something
        with open(file_, "w+") as f:
            f.write(stuff)
        sleep(0.001)
        return 'qemu-img'

    def tar(self, options, args):
        sleep(0.001)
        return 'lets just ignore this.'

    def vhd_util(self, options, args):
        return 'ignoring for now.'

    def mkfs_ext4(self, options, args):
        return 'mkfs'

    def mount(self, options, args):
        dev, mnt = args
        self.storage.mounts[mnt] = dev
        return 'mounted'

    def umount(self, options, args):
        mnt = args[0]
        if mnt not in self.storage.mounts:
            raise utils.ProcessError(self.cmdline, '', 'not mounted', 1)
        del self.storage.mounts[mnt]
        return 'unmounted'

    def communicate(self):
        self.cmd = os.path.basename(self.cmd)
        try:
            f = getattr(self, self.cmd)
        except AttributeError:
            out = '%s: command not found' % self.cmd
            raise utils.ProcessError(self.cmdline, out, '', 127)
        try:
            parser = globals()['%s_parser' % self.cmd]
        except KeyError:
            raise Exception('global %s_parser is not defined!' % self.cmd)

        try:
            options, args = parser.parse_args(self.args)
        except SystemExit:
            orig_stdout, orig_stderr = sys.stdout, sys.stderr
            stdout, stderr = StringIO(), StringIO()
            sys.stdout, sys.stderr = stdout, stderr
            try:
                options, args = parser.parse_args(self.args)
            except SystemExit, e:
                errcode = e.args[0]
            sys.stdout, sys.stderr = orig_stdout, orig_stderr
            raise utils.ProcessError(self.cmdline, stdout.getvalue(),
                                     stderr.getvalue(), errcode)

        out = f(options, args)
        return out, None

    @property
    def returncode(self):
        return 0


class MockSubprocess(object):

    PIPE = subprocess.PIPE
    STDOUT = subprocess.STDOUT

    def __init__(self, storage=None):
        self.history = []
        self.storage = storage
        if not self.storage:
            self.storage = MockStorageNode()

    def Popen(self, args, **kwargs):
        cmd = ' '.join(args)
        self.history.append(cmd)
        if args[0] == 'sudo':
            argv = args[1:]
        else:
            argv = list(args)
        return MockProcess(argv, storage=self.storage)


class BaseHelper(unittest.TestCase):

    def setUp(self):
        self.scratch = mkdtemp()
        self.storage = MockStorageNode(self.scratch)
        self._orig_subprocess = utils.subprocess
        utils.subprocess = MockSubprocess(storage=self.storage)
        self.conf = LunrConfig({
            'export': {
                'ietd_config': os.path.join(self.scratch, 'ietd.conf'),
                'proc_iet_volume': self.storage.proc_iet_volume,
                'proc_iet_session': self.storage.proc_iet_session,
                'device_prefix': self.storage.device_prefix,
                'initiators_allow': os.path.join(self.scratch,
                                                 'initiators.allow'),
            },
            'storage': {'skip_fork': True, 'run_dir':  self.storage.run_dir},
            'volume': {'device_prefix': self.storage.device_prefix},
            'disk': {'path': os.path.join(self.scratch, 'backups')},
            'glance': {
                'glance_urls': 'snet1,snet2',
                'glance_mgmt_urls': 'mgmt1, mgmt2',
            }})
        self.lockfile = os.path.join(self.scratch, 'lock')
        self.lock = ResourceFile(self.lockfile)

    def tearDown(self):
        self.storage.tearDown()
        rmtree(self.scratch)
        utils.subprocess = self._orig_subprocess


class TestHelper(BaseHelper):

    def setUp(self):
        BaseHelper.setUp(self)
        self.validator_gen = itertools.cycle([lambda req: (200, {})])
        self.orig_urlopen = utils.urlopen
        utils.urlopen = self._mock_urlopen

    def tearDown(self):
        BaseHelper.tearDown(self)
        utils.urlopen = self.orig_urlopen

    def test_create_helper(self):
        h = base.Helper(self.conf)
        self.assert_(hasattr(h, 'volumes'))
        self.assert_(hasattr(h, 'exports'))
        self.assert_(hasattr(h, 'backups'))

    def test_override_cinder_host(self):
        cinder_host = 'therealcinderhostwewant'
        self.conf.set('storage', 'cinder_host', cinder_host)
        h = base.Helper(self.conf)
        self.assertEquals(cinder_host, h.cinder_host)

    def _mock_urlopen(self, req):
        validator = self.validator_gen.next()
        code, info = validator(req)
        body = StringIO(dumps(info))
        return addinfourl(body, {}, req.get_full_url(), code)

    def test_check_registration(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        out = h.volumes.create(name)

        def validate_update(req):
            self.assert_(req.get_full_url().endswith('nodes'))
            self.assertEquals(req.get_method(), 'POST')
            data = dict(urlparse.parse_qsl(req.data, keep_blank_values=True))
            expected = {
                'status': 'PENDING',
                'volume_type_name': 'vtype',
                'name': socket.gethostname(),
                'hostname': '127.0.0.1',
                'storage_hostname': '127.0.0.1',
                'storage_port': '3260',
                'port': '8081',
                'size': '9',
                'cinder_host': '127.0.0.1',
                'affinity_group': '',
            }
            self.assertEquals(data, expected)
            return 200, expected
        validators = [
            # listing
            lambda req: (200, []),
            # post update
            validate_update,
        ]
        self.validator_gen = iter(validators)
        h.check_registration()

    def test_check_reg_vg_not_configured(self):
        h = base.Helper(self.conf)
        self.assertRaises(utils.ServiceUnavailable, h.check_registration)

    def test_check_reg_unable_to_contact_api(self):
        h = base.Helper(self.conf)

        def exploding_validator(req):
            raise URLError('connection refused')
        self.validator_gen = itertools.cycle([exploding_validator])
        with patch(base, 'sleep', lambda t: None):
            self.assertRaises(APIError, h.check_registration)

    def test_check_reg_api_server_error(self):
        h = base.Helper(self.conf)

        def error_validator(req):
            body = StringIO(dumps({'reason': 'Internal Error'}))
            raise HTTPError(req.get_full_url(), 500, 'Server Error', {}, body)
        self.validator_gen = itertools.cycle([error_validator])
        with patch(base, 'sleep', lambda t: None):
            self.assertRaises(APIError, h.check_registration)

    def test_check_reg_api_server_error_retry_success(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        out = h.volumes.create(name)

        def error_validator(req):
            error_validator.called = True
            body = StringIO(dumps({'reason': 'Internal Error'}))
            raise HTTPError(req.get_full_url(), 500, 'Server Error', {}, body)
        error_validator.called = False

        def success_validator(req):
            success_validator.called = True
            return 200, {}
        success_validator.called = False
        validators = [
            # listing
            lambda *args: (200, []),
            error_validator,
            success_validator,
        ]
        self.validator_gen = iter(validators)
        with patch(base, 'sleep', lambda t: None):
            h.check_registration()
            self.assert_(error_validator.called)
            self.assert_(success_validator.called)

    def test_check_reg_api_client_error(self):
        h = base.Helper(self.conf)

        def error_validator(req):
            body = StringIO(dumps({'reason': 'Bad Request'}))
            raise HTTPError(req.get_full_url(), 400, 'Bad Request', {}, body)
        self.validator_gen = itertools.cycle([error_validator])

        def should_not_be_called(t):
            should_not_be_called.called = True
        should_not_be_called.called = False
        with patch(base, 'sleep', should_not_be_called):
            self.assertRaises(APIError, h.check_registration)
        self.assertFalse(should_not_be_called.called)

    def test_check_reg_duplicate_entry(self):
        h = base.Helper(self.conf)
        validators = [
            # duplicate listing
            lambda req: (200, [{'id': 'node1'}, {'id': 'node2'}]),
        ]
        self.validator_gen = itertools.cycle(validators)
        self.assertRaises(utils.ServiceUnavailable, h.check_registration)
        try:
            h.check_registration()
        except utils.ServiceUnavailable, e:
            self.assert_('duplicate' in str(e).lower())

    def test_check_reg_unable_to_register(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        out = h.volumes.create(name)

        def error_update(req):
            body = StringIO(dumps({'reason': 'invalid param'}))
            raise HTTPError(req.get_full_url(), 400, 'Bad Request', {}, body)
        validators = [
            # listing
            lambda req: (200, []),
            # post update
            error_update,
        ]
        self.validator_gen = iter(validators)
        self.assertRaises(APIError, h.check_registration)

    def test_check_reg_update_node(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        out = h.volumes.create(name)
        node_info = {
            'id': 'node1',
            'status': 'ACTIVE',
            'volume_type_name': 'vtype',
            'name': socket.gethostname(),
            'hostname': '127.0.0.1',
            'storage_hostname': '127.0.0.1',
            'storage_port': 3260,
            'port': 8081,
            'size': '1',
            'cinder_host': '127.0.0.1',
            'affinity_group': 'ONE',
        }

        def validate_update(req):
            data = dict(urlparse.parse_qsl(req.data))

            expected = {
                'status': 'PENDING',
                'size': '9'
            }
            self.assertEquals(data, expected)
            return 200, expected
        validators = [
            # listing
            lambda req: (200, [node_info]),
            # get
            lambda req: (200, node_info),
            # post update
            validate_update,
        ]
        self.validator_gen = iter(validators)
        h.check_registration()


class TestVolumeHelper(BaseHelper):

    def test_list_empty_volumes(self):
        h = base.Helper(self.conf)
        volumes = h.volumes.list()
        self.assertEquals(volumes, [])

    def test_create_volume(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        out = h.volumes.create(name)
        v = h.volumes.get(name)
        self.assertTrue(os.path.exists(v['path']))

    def test_get_volume(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        h.volumes.create(name)
        v = h.volumes.get(name)
        expected = {
            'id': name,
            'path': os.path.join(self.storage.device_prefix,
                                 h.volumes.volume_group, name),
            'realpath': os.path.join(self.storage.device_prefix,
                                     h.volumes.volume_group, name),
            'size': 12582912,
            'origin': '',
            'volume': True,
            'device_number': '253:1',
        }
        self.assertEquals(v, expected)

    def test_delete_volume(self):

        def mock_spawn(_lock_junk, method, vol, callback=None, skip_fork=None):

            def run():
                method(vol)
            mock_spawn.run = run

        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        h.volumes.create(name)

        _orig_spawn = volume.spawn
        try:
            volume.spawn = mock_spawn
            h.volumes.delete(name)
        finally:
            volume.spawn = _orig_spawn

        v = h.volumes.get(name)
        self.assert_(v['zero'])

        mock_spawn.run()

        self.assertRaises(volume.NotFound, h.volumes.get, name)


class TestExportHelper(BaseHelper):

    def test_create_export(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        h.volumes.create(name)
        ip = netaddr.IPAddress('1.2.3.4')
        h.exports.create(name, ip=ip)
        e = h.exports.get(name)
        self.assertEquals(e['volume'], name)
        self.assertEquals(e['state'], '0')
        self.assertEquals(e['iomode'], 'wt')
        self.assertEquals(e['tid'], '1')
        self.assertEquals(e['iotype'], 'fileio')
        self.assertEquals(e['lun'], '0')
        self.assertEquals(e['path'], os.path.join(self.scratch,
                          self.storage.device_prefix, 'lunr-volume', name))
        self.assertEquals(e['sessions'], [{
                             'name': e['name'],
                             'tid': '1',
                             'volume': name,
                             'connected': False}])
        prefix, name = e['name'].split(':')
        # TODO: FIX ME
        # self.assertNotEquals(name, e['volume'])
        self.assertEquals(name, e['volume'])
        self.assertEquals(name.split('.')[0], e['volume'])

    def test_delete_export(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        h.volumes.create(name)
        h.exports.create(name)
        e = h.exports.get(name)
        self.assertEquals(e['volume'], name)
        h.exports.delete(name)
        self.assertRaises(utils.NotFound, h.exports.get, name)

    def test_list_export(self):
        h = base.Helper(self.conf)
        for i in range(3):
            name = 'volume-%s' % uuid4()
            h.volumes.create(name)
            h.exports.create(name)
        exports = h.exports.list()
        self.assertEquals(len(exports), 3)

    def test_get_export(self):
        h = base.Helper(self.conf)
        first_id = 'volume-%s' % uuid4()
        h.volumes.create(first_id)
        h.exports.create(first_id)
        for i in range(3):
            name = 'volume-%s' % uuid4()
            h.volumes.create(name)
            h.exports.create(name)
        last_id = 'volume-%s' % uuid4()
        h.volumes.create(last_id)
        h.exports.create(last_id)
        first_export = h.exports.get(first_id)
        self.assertEquals(first_export['volume'], first_id)
        last_export = h.exports.get(last_id)
        self.assertEquals(last_export['volume'], last_id)

    def test_create_duplicate_id(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        h.volumes.create(name)
        h.exports.create(name)
        self.assertRaises(utils.AlreadyExists, h.exports.create, name)

    def test_create_export_for_non_existant_path(self):
        h = base.Helper(self.conf)
        name = 'volume-%s' % uuid4()
        self.assertRaises(utils.NotFound, h.exports.create, name)


if __name__ == "__main__":
    unittest.main()
