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

from textwrap import dedent
from testlunr.unit import temp_disk_file, patch
from lunr.common.config import Config, BadSectionNameError, LunrConfig, \
        InvalidConfigError
import unittest
import os
from StringIO import StringIO
from tempfile import mkdtemp
from shutil import rmtree


class TestConfig(unittest.TestCase):

    def test_string_opts(self):
        conf = Config()
        value = conf.string('section1', 'string', 'default')
        self.assertEquals(value, 'default')

        conf = Config({'section1': {'string': 'value'}})
        value = conf.string('section1', 'string', 'default')
        self.assertEquals(value, 'value')

    def test_int_opts(self):
        conf = Config()
        value = conf.int('section1', 'int', 1)
        self.assertEquals(value, 1)

        conf = Config({'section1': {'int': 2}})
        value = conf.int('section1', 'int', 1)
        self.assertEquals(value, 2)

    def test_list_opts(self):
        conf = Config()
        value = conf.list('section1', 'list', [])
        self.assertEquals(value, [])

        value = conf.list('section1', 'list', 'monkey')
        self.assertEquals(value, ['monkey'])

        value = conf.list('section1', 'list', ['monkey'])
        self.assertEquals(value, ['monkey'])

        # Not a list or string, just bail?
        self.assertRaises(ValueError, conf.list, 'a', 'b', 42)

        conf = Config({'section1': {'list': 'monkey'}})
        value = conf.list('section1', 'list', [])
        self.assertEquals(value, ['monkey'])

        conf = Config({'section1': {'list': 'monkey, walrus,dog,apple  '}})
        value = conf.list('section1', 'list', [])
        self.assertEquals(value, ['monkey', 'walrus', 'dog', 'apple'])

    def test_bool_opts(self):
        conf = Config()
        value = conf.bool('section1', 'bool', True)
        self.assertEquals(value, True)

        conf = Config({'section1': {'bool': False}})
        value = conf.bool('section1', 'bool', True)
        self.assertEquals(value, False)

    def test_string_opt_casts(self):
        conf = Config({'section1': {'int': 1, 'bool': True}})
        value = conf.string('section1', 'bool', 'False')
        self.assertEquals(value, 'True')
        value = conf.string('section1', 'int', '2')
        self.assertEquals(value, '1')

    def test_int_opt_casts(self):
        conf = Config({'section1': {'int': 'non-numeric', 'bool': True}})
        self.assertRaises(ValueError, conf.int, 'section1', 'int', 1)
        value = conf.int('section1', 'bool', True)
        self.assertEquals(value, 1)

    def test_bool_opt_casts(self):
        conf = Config({'section1': {'help': 'no', 'bool': True}})
        value = conf.bool('section1', 'help', True)
        self.assertEquals(value, False)
        value = conf.bool('section1', 'bool', True)
        self.assertEquals(value, True)

        def truthy_test(value, expected):
            conf = Config({'section1': {'verbose': value}})
            value = conf.bool('section1', 'verbose', True)
            self.assertEquals(value, expected)

        truthy_test('TRUE', True)
        truthy_test('True', True)
        truthy_test('true', True)
        truthy_test('YES', True)
        truthy_test('yes', True)
        truthy_test('Yes', True)
        truthy_test('T', True)
        truthy_test('t', True)
        truthy_test('Y', True)
        truthy_test('y', True)
        truthy_test('1', True)
        truthy_test('ON', True)
        truthy_test('On', True)
        truthy_test('on', True)
        truthy_test('ENABLE', True)
        truthy_test('Enable', True)
        truthy_test('enable', True)
        truthy_test(True, True)

    def test_environment_variables(self):
        conf = Config()

        os.environ['DEFAULT_TEST_LUNR_DIR'] = '/home/foo'
        value = conf.string('default', 'test_lunr_dir', '/home/lunr')
        self.assertEquals(value, '/home/foo')

    def test_set(self):
        conf = Config()
        # Explicity set a value for a section
        conf.set('section1', 'int', 2)
        value = conf.int('section1', 'int', 1)
        self.assertEquals(value, 2)

    def test_section(self):
        conf = Config({'section1': {'value1': 'value2'}})
        self.assertEquals(conf.section('section1'), {'value1': 'value2'})

    def test_write(self):
        s = StringIO()
        conf = Config()
        conf.write(s)
        self.assertEquals(s.getvalue(), "[default]\n")
        s.seek(0)
        conf = Config({'aaa': {'a': 'AAA', 'b': 'BBB'},
                       'bbb': {'c': 'CCC', 'd': 'DDD'},
                       'default': {}})
        conf.write(s)
        expected = "[aaa]\na = AAA\nb = BBB\n[bbb]\n" \
                   "c = CCC\nd = DDD\n[default]\n"
        self.assertEquals(s.getvalue(), expected)


class TestLunrConfig(unittest.TestCase):

    def test_path(self):
        conf = LunrConfig({'default': {'lunr_dir': '/tmp'}})
        self.assertEquals(conf.path('me'), '/tmp/me')

    def test_parse_config_file(self):
        conf = None

        conf_str = "[DEFAULT]\n" \
                   "foo = bar\n" \
                   "[foo]\n" \
                   "foo = baz\n" \
                   "[foo]\n" \
                   "fog = buz\n" \
                   "[fiz]\n" \
                   "bang = bazt\n" \
                   "[casts]\n" \
                   "size = 25\n" \
                   "flush = True\n"

        with temp_disk_file(conf_str) as file:
            conf = LunrConfig.from_conf(file)
            self.assertEquals(conf.string('default', '__file__', ''), file)

        # All parsed options default to string
        value = conf.string('default', 'foo', '')
        self.assertEquals(value, 'bar')
        value = conf.string('foo', 'foo', '')
        self.assertEquals(value, 'baz')
        value = conf.string('foo', 'fog', '')
        self.assertEquals(value, 'buz')
        value = conf.string('fiz', 'bang', '')
        self.assertEquals(value, 'bazt')

        # Non-string casts
        value = conf.int('casts', 'size', 1)
        self.assertEquals(value, 25)
        value = conf.bool('casts', 'flush', False)
        self.assertEquals(value, True)

    def test_parse_config_bad_section(self):
        conf_str = "[DEFAULT]\n" \
                   "foo = bar\n" \
                   "[__file__]\n" \
                   "foo = baz\n"
        with temp_disk_file(conf_str) as file:
            self.assertRaises(BadSectionNameError, LunrConfig.from_conf, file)

    def test_parse_syntax_error(self):
        conf_str = dedent(
            """
            DEFAULT]
            foo = bar
            """
        )
        with temp_disk_file(conf_str) as file:
            self.assertRaises(InvalidConfigError, LunrConfig.from_conf, file)

    def test_from_api_conf(self):
        conf_str = dedent(
            """
            [DEFAULT]
            foo = bar
            """
        )
        with temp_disk_file(conf_str) as file:
            with patch(LunrConfig, 'lunr_api_config', file):
                conf = LunrConfig.from_api_conf()
                self.assertEquals(conf.lunr_api_config, file)
                self.assertEquals(conf.string('default', '__file__', ''),
                                  conf.lunr_api_config)
                self.assertEquals(conf.string('default', 'foo', ''), 'bar')

    def test_from_storage_conf(self):
        conf_str = dedent(
            """
            [DEFAULT]
            foo = bar
            """
        )
        with temp_disk_file(conf_str) as file:
            with patch(LunrConfig, 'lunr_storage_config', file):
                conf = LunrConfig.from_storage_conf()
                self.assertEquals(conf.lunr_storage_config, file)
                self.assertEquals(conf.string('default', '__file__', ''),
                                  conf.lunr_storage_config)
                self.assertEquals(conf.string('default', 'foo', ''), 'bar')

    def test_multi_config(self):
        base_conf_body = dedent(
            """
            [db]
            # url = sqlite://
            echo = False
            pool_size = 5
            """
        )
        db_conf_body = dedent(
            """
            [db]
            url = mysql://root:@localhost/lunr
            echo = True
            """
        )
        scratch = mkdtemp()
        try:
            os.mkdir(os.path.join(scratch, 'api-server.conf.d'))
            base_conf_filename = os.path.join(scratch, 'api-server.conf')
            db_conf_filename = os.path.join(scratch,
                                            'api-server.conf.d/db.conf')
            with open(base_conf_filename, 'w') as f:
                f.write(base_conf_body)
            with open(db_conf_filename, 'w') as f:
                f.write(db_conf_body)
            conf = LunrConfig.from_conf(base_conf_filename)
            # override commented value
            self.assertEquals(conf.string('db', 'url', ''),
                              'mysql://root:@localhost/lunr')
            # override base value
            self.assertEquals(conf.bool('db', 'echo', None), True)
            # inherit base value
            self.assertEquals(conf.int('db', 'pool_size', 0), 5)
        finally:
            rmtree(scratch)

    def test_case_sensitive_parser(self):
        conf_str = dedent(
            """
            [vtype-mapping]
            SATA = STANDARD
            SSD = HIGH
            """
        )
        with temp_disk_file(conf_str) as f:
            conf = LunrConfig.from_conf(f)
            section = conf.section('vtype-mapping')
            expected = {
                'SATA': 'STANDARD',
                'SSD': 'HIGH',
            }
            self.assertEquals(section, expected)


if __name__ == "__main__":
    unittest.main()
