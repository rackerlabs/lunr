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


from contextlib import contextmanager
from StringIO import StringIO
import random
import sys
import time
import unittest

from lunr.api.console import Console, TypeConsole, NodeConsole, AccountConsole
from testlunr.integration import IetTest


def _parse_long(value):
    """
    Attempts to parse the long-handed output console commands output, usually
    in a form similar to::

               status: ACTIVE
                 name: zzzd
           created_at: 2013-08-23 17:04:46
             min_size: 1
        last_modified: 2013-08-23 17:04:46
            read_iops: 0
           write_iops: 0
             max_size: 1024

    Returns a dictionary of a value similar to that example, parsed.

    Lines without a colon will be ignored.
    """
    dict_value = {}

    for line in value.split('\n'):
        if ':' in line:
            k, v = line.split(':', 1)
            dict_value[k.strip()] = v.strip()

    return dict_value


def _parse_table(value):
    """
    Parses a textual table that the console commands generate.
    Each row is converted into a dictionary.
    """
    lines = value.split('\n')
    header = None
    rows = []

    for l in lines:
        if l.startswith('+-'):
            pass
        elif l.startswith('|'):
            columns = [c.strip() for c in l.split('|')[1:-1]]
            if header is None:
                header = columns
            else:
                row = {}
                for i, c in enumerate(columns):
                    if len(header)-1 <= i:
                        row[i] = c
                    else:
                        row[header[i]] = c
                rows.append(row)
    return rows


def _generate_name(name):
    """
    Generate names for tested objects where you can probably tell at a glance
    which objects were created by which executions of the unittests.
    """
    return 'test-%s-%s-%s' % (time.strftime('%Y%m%d%H%M%S'),
                              random.randint(0, 999), name)


@contextmanager
def captured(stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr):
    """
    Used to capture standard input/output for testing I/O on console commands.
    Intended to be used with the "with" statement:

    >>> from StringIO import StringIO
    >>> with captured(stdout=StringIO()) as (stdin, stdout, stderr):
    ...     print 'foo'
    ...
    >>>
    >>> stdout.getvalue()
    'foo\n'
    >>>
    """
    original_streams = {}

    try:
        for stream in ('stdin', 'stdout', 'stderr'):
            original_streams[stream] = getattr(sys, stream)
            setattr(sys, stream, locals()[stream])
        yield (stdin, stdout, stderr)
    finally:
        for stream_name, original_stream in original_streams.items():
            setattr(sys, stream_name, original_stream)


class TestConsoleType(IetTest):
    """
    Tests the TypeConsole class.
    """
    def setUp(self):
        c = TypeConsole()
        c.verbose = c.config = c.url = None
        self.c = c

    def tearDown(self):
        pass

    def test_type_create(self):
        name = _generate_name('type_create')

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.create({'name': name, 'url': None, 'min_size': None,
                           'read_iops': None, 'config': None,
                           'write_iops': None, 'max_size': None,
                           'verbose': None})

        self.failIf(stderr.getvalue())
        output_values = _parse_long(stdout.getvalue())

        self.failUnlessEqual(output_values['name'], name)
        # TODO: Will this always be active?
        self.failUnlessEqual(output_values['status'], 'ACTIVE')

        self.c.delete(name)

    def test_type_list(self):
        name = _generate_name('type_list')

        self.c.create({'name': name, 'url': None, 'min_size': None,
                       'read_iops': None, 'config': None,
                       'write_iops': None, 'max_size': None, 'verbose': None})

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.list()

        self.failIf(stderr.getvalue())

        data = _parse_table(stdout.getvalue())
        for row in data:
            if row['name'] == name and row['status'] == 'ACTIVE':
                break
        else:
            self.fail('Failed to find created row in list.')
        self.c.delete(name)

    def test_type_get(self):
        name = _generate_name('type_get')

        self.c.create({'name': name, 'url': None, 'min_size': None,
                       'read_iops': None, 'config': None,
                       'write_iops': None, 'max_size': None, 'verbose': None})

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(name=name)

        self.failIf(stderr.getvalue())
        value = _parse_long(stdout.getvalue())
        self.failUnlessEqual(value['name'], name)

        self.c.delete(name)

    def test_type_delete(self):
        name = _generate_name('type_delete')

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.create({'name': name, 'url': None, 'min_size': None,
                           'read_iops': None, 'config': None,
                           'write_iops': None, 'max_size': None,
                           'verbose': None})
        self.failIf(stderr.getvalue())

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.delete(name=name)
        self.failIf(stderr.getvalue())

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(name=name)
        self.failIf(stderr.getvalue())
        value = _parse_long(stdout.getvalue())
        self.failUnlessEqual(value['status'], 'DELETED')

        self.c.delete(name)


class TestConsoleNode(IetTest):
    """
    Tests the NodeConsole class.
    """
    def setUp(self):
        c = NodeConsole()
        c.verbose = c.config = c.url = None
        self.vtc = TypeConsole()
        self.vtc.config = self.vtc.url = self.vtc.verbose = None
        self.c = c

    def tearDown(self):
        pass

    def _create_node(self, node_name, volume_name):
        self.vtc.create({'name': volume_name, 'url': None, 'min_size': None,
                         'read_iops': None, 'config': None, 'write_iops': None,
                         'max_size': None, 'verbose': None})
        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.create({
                'volume_type_name': volume_name, 'verbose': None, 'url': None,
                'hostname': 'localhost', 'storage_hostname': 'localhost',
                'size': '1', 'config': None, 'port': '1234',
                'name': node_name})

        self.failIf(stderr.getvalue())
        return _parse_long(stdout.getvalue())

    def test_node_create(self):
        volume_name = _generate_name('node_create_volume')
        node_name = _generate_name('node_create_node')

        node_info = self._create_node(node_name, volume_name)
        self.failUnlessEqual(node_info['name'], node_name)

        self.c.delete(node_info['id'])
        self.vtc.delete(volume_name)

    def test_node_list(self):
        volume_name = _generate_name('node_list_volume')
        node_name = _generate_name('node_list_node')

        node_info = self._create_node(node_name, volume_name)

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.list()

        self.failIf(stderr.getvalue())

        data = _parse_table(stdout.getvalue())

        for row in data:
            if (row['name'] == node_name and
                    row['volume_type_name'] == volume_name):
                break
        else:
            self.fail('Did not find created node in node list')

        self.c.delete(node_info['id'])
        self.vtc.delete(volume_name)

    def test_node_get(self):
        volume_name = _generate_name('node_get_volume')
        node_name = _generate_name('node_get_node')

        node_info = self._create_node(node_name, volume_name)

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(id=node_info['id'])
        self.failIf(stderr.getvalue())

        node_info2 = _parse_long(stdout.getvalue())
        self.failUnlessEqual(node_info2['id'], node_info['id'])
        self.failUnlessEqual(node_info2['name'], node_info['name'])

        self.c.delete(node_info['id'])
        self.vtc.delete(volume_name)

    def test_node_update(self):
        volume_name = _generate_name('node_get_volume')
        node_name = _generate_name('node_get_node')
        node_name2 = _generate_name('node_get_node2')

        node_info = self._create_node(node_name, volume_name)

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(id=node_info['id'])
        self.failIf(stderr.getvalue())

        self.c.update(node_info['id'], {'name': node_name2})

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(id=node_info['id'])
        self.failIf(stderr.getvalue())

        data = _parse_long(stdout.getvalue())

        self.failUnlessEqual(data['name'], node_name2)

        self.c.delete(node_info['id'])
        self.vtc.delete(volume_name)

    def test_node_delete(self):
        volume_name = _generate_name('node_get_volume')
        node_name = _generate_name('node_get_node')
        node_info = self._create_node(node_name, volume_name)

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(id=node_info['id'])
        self.failIf(stderr.getvalue())

        self.c.delete(node_info['id'])

        with captured(stdout=StringIO(), stderr=StringIO()) as \
                (stdin, stdout, stderr):
            self.c.get(id=node_info['id'])
        self.failIf(stderr.getvalue())
        data = _parse_long(stdout.getvalue())
        self.failUnlessEqual(data['status'], 'DELETED')

        self.vtc.delete(volume_name)


if __name__ == "__main__":
    unittest.main()
