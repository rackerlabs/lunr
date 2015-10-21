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

from lunr.storage.helper.volume import VolumeHelper
from lunr.storage.helper.utils import ServiceUnavailable
from testlunr.integration import IetTest
from lunr.common import logger
from tempfile import mkdtemp
from uuid import uuid4
import shutil
import sys

# configure logging to log to console if nose was called with -s
logger.configure(log_to_console=('-s' in sys.argv), capture_stdio=False)


class TestVolumeHelper(IetTest):

    def setUp(self):
        IetTest.setUp(self)
        self.tempdir = mkdtemp()
        self.conf = self.config(self.tempdir)
        self.volume = VolumeHelper(self.conf)

    def tearDown(self):
        # Remove the temp dir where backups are created
        shutil.rmtree(self.tempdir)
        IetTest.tearDown(self)

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_create_out_of_space(self):
        # Request a volume that is too large for our disk
        self.assertRaises(ServiceUnavailable, self.volume.create, str(uuid4()),
                          size=1)
