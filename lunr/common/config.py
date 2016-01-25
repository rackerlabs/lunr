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


from ConfigParser import RawConfigParser, Error
from string import upper, lower
import errno
import os
import urlparse


class NoConfigError(IOError):

    def __init__(self, conf_file):
        IOError.__init__(self, errno.ENOENT, 'No such config: %s' % conf_file)


class InvalidConfigError(Error):
    pass


class BadSectionNameError(InvalidConfigError):

    def __init__(self, section):
        InvalidConfigError.__init__(self, 'Invalid section key: %s' % section)


class Config(object):
    """
        conf = Config()
        conf.set('section', 'verbose', False)
        my = MyObject(conf)

        class MyObject(object):
            def __init__(self, conf=Config()):
                self.key1 = conf.string('default', 'key1', 'value1')
                self.verbose = conf.bool('section', 'verbose', True)
                self.level = conf.int('section', 'level', 1)
                self.class = conf.option('section', 'exception',
                        Exception, to_exc)

    """

    def __init__(self, values=None):
        self.values = values or {}
        if 'default' not in self.values:
            self.values['default'] = dict(self.values)

    def option(self, section, key, default, cast):
        # Check the environment for a var called '(section)_(key)'
        if upper(section + '_' + key) in os.environ:
            return cast(os.environ[upper(section + '_' + key)])

        # if the section doesn't exist return the default
        if section not in self.values:
            return self._cast(cast, default)

        # Get the key from the section
        return self._cast(cast, self.values[section].get(key, default))

    def string(self, section, key, default):
        return self.option(section, key, default, str)

    def int(self, section, key, default):
        return self.option(section, key, default, int)

    def float(self, section, key, default):
        return self.option(section, key, default, float)

    def bool(self, section, key, default):
        return self.option(section, key, default, self.to_bool)

    def list(self, section, key, default):
        return self.option(section, key, default, self.to_list)

    def set(self, section, key, value):
        self.values.setdefault(section, {})[key] = value

    def section(self, name):
        return self.values.get(name, {})

    def write(self, fp):
        for section in sorted(self.values.keys()):
            fp.write("[%s]\n" % section)
            for (k, v) in self.values[section].items():
                if k == "__file__":
                    continue
                fp.write("%s = %s\n" % (k, v))

    @staticmethod
    def _cast(_cast, value):
        """ The _cast() method allows users to specify a default of None,
        incase some options do not have reasonable defaults """
        if value is None:
            return None
        return _cast(value)

    @staticmethod
    def to_bool(value):
        true_values = ('TRUE', 'True', 'true', 'YES', 'Yes', 'yes',
                       'T', 't', 'Y', 'y', '1', 'ON', 'On', 'on',
                       'ENABLE', 'Enable', 'enable', True)
        return value in true_values

    @staticmethod
    def to_list(value):
        if isinstance(value, basestring):
            return [v.strip() for v in value.split(',')]
        if isinstance(value, list):
            return value
        raise ValueError("Cannot convert '%r' to list" % value)

    @staticmethod
    def to_class(_class, module):
        # Allow _class = None to pass through
        if not _class:
            return None
        # _class must be a string
        if not isinstance(_class, str):
            raise ValueError("Unable to find class '%s'; "
                             "class name is not a string" % _class)

        try:
            return getattr(module, _class)
        except AttributeError:
            raise ValueError("Unable to find class '%s' in module '%s'"
                             % (_class, module))


class LunrConfig(Config):

    class LunrConfigError(Exception):
        pass

    lunr_api_config = '/etc/lunr/api-server.conf'
    lunr_storage_config = '/etc/lunr/storage-server.conf'
    lunr_orbit_config = '/etc/lunr/orbit.conf'

    def __init__(self, values=None):
        values = values or {}

        Config.__init__(self, values)
        self.lunr_dir = self.string('default', 'lunr_dir', '/etc/lunr')

    def path(self, file):
        """Relative paths in configs are assumed to be relative to LUNR_DIR """
        return os.path.normpath(os.path.join(self.lunr_dir, file))

    @property
    def file(self):
        try:
            return self.values['default']['__file__']
        except KeyError:
            return None

    @classmethod
    def from_conf(cls, file):
        conf_files = []
        file = os.path.abspath(file)
        for conf_file in (file, file + '.d'):
            if not os.path.exists(conf_file):
                continue
            if os.path.isdir(conf_file):
                for root, _dirs, basenames in os.walk(conf_file):
                    for basename in basenames:
                        conf_files.append(os.path.join(root, basename))
            else:
                conf_files.append(conf_file)

        conf_files.sort()

        # Read the config file with standard INI parser
        parser = RawConfigParser()
        parser.optionxform = str
        try:
            success = parser.read(conf_files)
        except Error, e:
            raise InvalidConfigError(e.message)
        if not success:
            raise NoConfigError(file)

        # All values without a section, go into the 'DEFAULT' section
        values = {'default': parser.defaults()}

        for item in parser.sections():
            # Sections named __file__ not allowed
            if item in ('__file__', '__files__'):
                raise BadSectionNameError(item)

            # load all the items into our config
            values[lower(item)] = dict(parser.items(item))

        # Let the config know what file(s) were parsed
        values['default']['__file__'] = file
        values['default']['__files__'] = conf_files
        return cls(values)

    @classmethod
    def from_api_conf(cls):
        return cls.from_conf(cls.lunr_api_config)

    @classmethod
    def from_storage_conf(cls):
        return cls.from_conf(cls.lunr_storage_config)

    @classmethod
    def from_orbit_conf(cls):
        return cls.from_conf(cls.lunr_orbit_config)
