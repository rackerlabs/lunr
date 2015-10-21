#!/usr/bin/python
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

from setuptools import setup, find_packages

from lunr import __canonical_version__ as version

name = 'lunr'

setup(
    name=name,
    version=version,
    description='Lunr',
    license='Apache License (2.0)',
    author='Rackspace US, Inc.',
    packages=find_packages(exclude=['test', 'bin']),
    package_data={'lunr': ['db/migrations/migrate.cfg']},
    test_suite='nose.collector',
    scripts=['bin/lunr-setup-storage'],
    entry_points={
        'paste.app_factory': [
            'storage_server=lunr.storage.server:app_factory',
            'api_server=lunr.api.server:app_factory',
            ],
        'paste.filter_factory': [
            'healthcheck=lunr.middleware.healthcheck:filter_factory',
            'error_catcher=lunr.middleware.error_catcher:filter_factory',
            'trans_logger=lunr.middleware.trans_logger:filter_factory',
            'statlogger=lunr.middleware.statlogger:filter_factory',
            ],
        'console_scripts': [
            'lunr-storage = lunr.storage.server:main',
            'lunr-storage-admin = lunr.storage.helper.console:main',
            'lunr-api = lunr.api.server:main',
            'lunr-admin = lunr.api.console:main',
            'lunr-manage = lunr.db.migrations.manage:main',
            'lunr-orbit = lunr.orbit.console:main',
            ],
        }, requires=['sqlalchemy'],
    )
