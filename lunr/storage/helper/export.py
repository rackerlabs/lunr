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


import fcntl
import errno
import netaddr
import os
from collections import defaultdict
from uuid import uuid4
from string import Template
from time import sleep

from lunr.common import logger, lock
from lunr.storage.helper.utils import execute, NotFound, ProcessError, \
    ServiceUnavailable, ResourceBusy, AlreadyExists


class IscsitargetError(ProcessError):
    """
    Wrapper class for iscsitarget ProcessErrors

    :param e: instance of ProcessError
    """
    err = None
    errcode = None

    def __init__(self, e):
        self.cmd = e.cmd
        self.out = e.out
        self.err = self.err or e.err
        self.errcode = self.errcode or e.errcode
        Exception.__init__(self, self.err, self.errcode)

    def __str__(self):
        return '[Errno %s] %s' % (self.errcode, self.err)

    @classmethod
    def get_exc(cls, e):
        for subcls in cls.__subclasses__():
            if e.errcode == subcls.errcode and subcls.err in e.err:
                return subcls(e)
        return e


class ConnectionRefused(IscsitargetError):
    """
    Raised when a call to ietadm fails with "Connection refused"

    e.g.
        making a call to ietadm while iscsitarget is stopped
    """
    err = "Connection refused"
    errcode = 145


class InvalidArgument(IscsitargetError):
    """
    Raised when a call to ietadm fails with "Invalid argument"

    e.g.
        recreating a target name that already exists
    """
    err = "Invalid argument"
    errcode = 234


class DeviceBusy(IscsitargetError):
    """
    Raised when a call to ietadm fails with "Device or resource busy"

    e.g.
        trying to delete a target while it's attached to an initiator
    """
    err = "Device or resource busy"
    errcode = 240


class NoSuchFile(IscsitargetError):
    """
    Raised when a call to ietadm fails with "No such file"

    e.g.
        trying to create a lun for a path that does not exist
    """
    err = "No such file"
    errcode = 254


class NotPermitted(IscsitargetError):
    """
    Raised when a call to ietadm fails with "Operation not permitted"

    e.g.
        trying to create a lun for a path that does not exist
    """
    err = "Operation not permitted"
    errcode = 255


def ietadm(*args, **kwargs):
    try:
        return execute('ietadm', *args, **kwargs)
    except ProcessError, e:
        raise IscsitargetError.get_exc(e)


def format_config_line(export):

    config_template = Template(
        "# ${volume}\n"
        "Target ${name}\n"
        "    Lun ${lun} Path=${path},Type=${iotype},IOMode=${iomode}\n")

    try:
        return config_template.substitute(export)
    except KeyError:
        # this seems to happen if the volume of behind an export was deleted
        logger.exception('invalid export %s' % repr(export))
        return ''


class ExportHelper(object):

    iet_config_warning = "WARNING: This file is automatically rewritten by "\
        "LunR to persist exports.\n Editing it is futile - your changes will "\
        "be destroyed by the next update.\n\n"

    initiators_allow_warning = "# WARNING: This file is automatically "\
        "by LunR to persist allowed initiators\n# per export.\n# Editing it "\
        "is futile - your changes will be destroyed by the next update.\n\n"

    def __init__(self, conf):
        self.ietd_config = conf.string('export', 'ietd_config',
                                       '/etc/iet/ietd.conf')
        self.volume_group = conf.string('volume', 'volume_group',
                                        'lunr-volume')
        self.iqn_prefix = conf.string('export', 'iqn_prefix',
                                      'iqn.2010-11.com.rackspace')
        self.device_prefix = conf.string('export', 'device_prefix', '/dev')
        self.proc_iet_volume = conf.string('export', 'proc_iet_volume',
                                           '/proc/net/iet/volume')
        self.proc_iet_session = conf.string('export', 'proc_iet_session',
                                            '/proc/net/iet/session')
        self.initiators_allow = conf.string('export', 'initiators_allow',
                                            '/etc/iet/initiators.allow')
        self.default_allows = conf.string('export', 'default_allows', 'ALL')
        logger.info("Setting export default_allows: %s" % self.default_allows)
        subnets = conf.list('export', 'allow_subnets', '0.0.0.0/0')
        logger.debug("Setting export allow_subnets: %s" % subnets)
        self.allow_subnets = []
        for subnet in subnets:
            if subnet:
                self.allow_subnets.append(netaddr.IPNetwork(subnet))
        self.run_dir = conf.string('storage', 'run_dir', conf.path('run'))

    def _build_lock_path(self, id):
        return os.path.join(self.run_dir, 'volumes', str(id), 'export')

    def _generate_target_name(self, id):
        # TODO: FIX ME
        # return self.iqn_prefix + ':' + id + '.' + str(uuid4())
        return self.iqn_prefix + ':' + id

    def _lun_path(self, id):
        return os.path.join(self.device_prefix, self.volume_group, id)

    def _get_exports(self, volume):
        return [v for v in self._scan_exports() if v['volume'] == volume]

    def _get_tid(self, volume):
        try:
            export = self._get_exports(volume)[0]
        except IndexError:
            raise NotFound("No export for volume '%s'" % volume)
        return export['tid']

    def _scan_exports(self):
        return self._scan_file(self.proc_iet_volume)

    def _scan_sessions(self):
        return self._scan_file(self.proc_iet_session)

    def _scan_file(self, file):
        """ Scans IET files in the form

        tid:1 name:iqn.2010-11.com.rackspace:volume-00000001.uuid
        \tsid:562950527844864 initiator:iqn.1993-08.org.debian:01:47441681ba44
        \t\tcid:0 ip:127.0.0.1 state:active hd:none dd:none
        \tsid:281474997486080 initiator:iqn.1993-08.org.debian:01:669c9c15f124
        \t\tcid:0 ip:192.168.56.1 state:active hd:none dd:none

        and returns an array of key:value dicts for each entry it finds
        in the file
        """

        # TODO(clayg): this needs a refactor, it's really hard to follow, sry
        records = []
        try:
            with open(file) as f:
                record = {}
                for line in f:
                    if not line.strip():
                        continue
                    # new record, get it in the list and update via ref
                    if not line.startswith('\t'):
                        record = {}
                        records.append(record)
                    subrecord = self.parse_export_line(line)
                    if not record:
                        base_record = dict(subrecord)
                    if any(k in record for k in subrecord):
                        # this is a second entry
                        record = dict(base_record)
                        records.append(record)
                    record.update(subrecord)
        except IOError, e:
            if e.errno == errno.ENOENT:
                raise ServiceUnavailable("'%s' does not exist, iscsitarget"
                                         "is not running." % file)
            else:
                msg = "Unexpected error trying to read '%s'", file
                logger.exception(msg)
                raise ServiceUnavailable(msg)
        return records

    def parse_export_line(self, line):
        info = {}
        for pair in line.split():
            key, value = pair.split(':', 1)
            info[key] = value
            if key == 'name':
                prefix, name = value.split(':')
                info['volume'] = name.split('.', 1)[0]
        return info

    def list(self):
        return self._scan_exports()

    def get(self, id, exports=None):
        if exports is None:
            exports = self._scan_exports()
        for v in exports:
            if v.get('volume') == id:
                v['sessions'] = self._sessions(id)
                return v
        raise NotFound("No exports named '%s'" % id)

    def rewrite_config(self):
        """
        Rewrite iet config to persit updates to exports list.
        """
        with open(self.ietd_config, 'w') as f:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX)
            f.write(self.iet_config_warning)
            exports = self._scan_exports()
            for export in exports:
                f.write(format_config_line(export))
        return exports

    def _sessions(self, id):
        all_sessions = self._scan_sessions()
        sessions = [item for item in all_sessions if item['volume'] == id]
        for session in sessions:
            session['connected'] = 'ip' in session
        return sessions

    def ietadm(self, *args, **kwargs):
        try:
            return ietadm(*args, **kwargs)
        except ConnectionRefused, e:
            if not os.path.exists(self.proc_iet_volume):
                raise ServiceUnavailable("'%s' does not exist, ietd is not "
                                         "running." % self.proc_iet_volume)
            msg = 'Unexpected ConnectionRefused: %s' % e
            logger.exception(msg)
            raise ServiceUnavailable(msg)

    def create(self, id, ip=None):
        with lock.ResourceFile(self._build_lock_path(id)):
            try:
                # see if an export was created while we were locking
                self.get(id)
            except NotFound:
                pass
            else:
                raise AlreadyExists("An export already exists for "
                                    "volume '%s'" % id)
            # create target
            params = {'Name': self._generate_target_name(id)}
            try:
                out = self.ietadm(op='new', tid='auto', params=params)
            except InvalidArgument:
                logger.exception("Unable to create target for '%s'" % id)
                raise ServiceUnavailable("Invalid argument while trying to "
                                         "create export for '%s'" % id)
            # lookup tid
            tid = self._get_tid(id)
            # add lun
            path = self._lun_path(id)
            params = {'Path': path, 'Type': 'blockio'}
            try:
                out += self.ietadm(op='new', tid=tid, lun=0, params=params)
            except (NoSuchFile, NotPermitted):
                # clean up the target
                self.delete(id)
                if not os.path.exists(path):
                    raise NotFound("No volume named '%s'" % id)
                logger.exception('Unable to create export for %s' % id)
                raise ServiceUnavailable("Invalid param trying to create "
                                         "export for '%s'" % id)
        # Write the new exports to the iet config
        exports = self.rewrite_config()

        if ip:
            self.add_initiator_allow(id, ip)

        return self.get(id, exports=exports)

    def force_delete(self, id):
        sessions = self._sessions(id)
        if not sessions:
            return
        # delete sessions
        for session in sessions:
            out = self.ietadm(op='delete', tid=session['tid'],
                              sid=session['sid'], cid=session['cid'])
        # delete target (to prevent reconnect)
        out = self.ietadm(op='delete', tid=sessions[0]['tid'])
        # FIXME(cory) This doesn't rewrite the iet config file??
        self.remove_initiator_allow(id)

    def delete(self, id, force=False, initiator=None):
        exports = self._get_exports(id)
        if not exports:
            raise NotFound("No export for volume '%s'" % id)
        # we hope there will only ever be one export, and try to ensure with
        # syncronization in create
        for export in exports:
            tid = export['tid']
            try:
                out = self.ietadm(op='delete', tid=tid)
            except DeviceBusy, e:
                if force:
                    for i in range(3):
                        try:
                            return self.force_delete(id)
                        except IscsitargetError:
                            logger.exception(
                                'force delete attempt %s failed' % (i + 1))
                            sleep(1 + i ** 2)
                    # try one more time and let it die
                    return self.force_delete(id)
                sessions = self._sessions(id)
                if initiator:
                    for session in sessions:
                        if (initiator == session['initiator'] and
                                session['connected']):
                            raise ResourceBusy(
                                "Volume '%s' is currently attached "
                                "to '%s' for initiator: %s" %
                                (id, session['ip'], initiator))
                    # Fall through, our initiator didn't match, someone else
                    # is attached. Delay "failure" until delete.
                    return
                else:
                    for session in sessions:
                        if session['connected']:
                            raise ResourceBusy(
                                "Volume '%s' is currently attached "
                                "to '%s'" % (id, session['ip']))
                logger.exception("Unable to remove target for '%s' "
                                 "because it was busy" % id)
                raise ResourceBusy("Volume '%s' is currently attached" % id)
            self.rewrite_config()
        self.remove_initiator_allow(id)

    def status(self):
        sessions = self._scan_sessions()
        connected_sessions = [s for s in sessions if 'ip' in s]
        status = {
            'exports': len(self._scan_exports()),
            'sessions': len(sessions),
            'connected': len(connected_sessions),
        }
        return status

    def _scan_initiators(self):
        """
        This fetchs the entries in initators.allow.

        Note, this is racy versus rewrite_initiators.
        """
        records = defaultdict(list)
        with open(self.initiators_allow) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('#'):
                    continue
                if line.startswith('ALL'):
                    continue
                (iqn, allow_str) = line.split(' ', 1)
                for allow in allow_str.split(','):
                    records[iqn].append(allow.strip())
        return records

    def _rewrite_initiators(self, allows):
        """
        Rewrite initiators.allow to persist export ACLs.
        """
        with open(self.initiators_allow, 'w') as f:
            f.write(self.initiators_allow_warning)
            for iqn, rules in allows.iteritems():
                f.write("%s " % iqn)
                f.write(', '.join(map(str, rules)))
                f.write("\n")
            f.write("ALL %s\n" % self.default_allows)

    def init_initiator_allows(self):
        allows = self._scan_initiators()
        self._rewrite_initiators(allows)

    def add_initiator_allow(self, id, ip):
        allow = self.default_allows
        if ip:
            for subnet in self.allow_subnets:
                if ip in subnet:
                    allow = ip
                    break
        target = self._generate_target_name(id)
        with lock.ResourceFile(self.initiators_allow):
            initiators = self._scan_initiators()
            initiators[target].append(allow)
            self._rewrite_initiators(initiators)

    def remove_initiator_allow(self, id):
        target = self._generate_target_name(id)
        with lock.ResourceFile(self.initiators_allow):
            initiators = self._scan_initiators()
            if target not in initiators:
                return
            initiators.pop(target, None)
            self._rewrite_initiators(initiators)
