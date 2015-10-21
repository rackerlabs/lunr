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


import logging
from StringIO import StringIO
import sys
import unittest

from lunr.common import logger


class TestLunrFormatter(unittest.TestCase):
    def setUp(self):
        self.stream = StringIO()
        self.logger = logger.get_logger()
        self.handler = logging.StreamHandler(self.stream)
        self.formatter = logger.LunrFormatter('%(message)s')
        self.handler.setFormatter(self.formatter)
        self.logger.setLevel(logging.DEBUG)
        self.logger.logger.addHandler(self.handler)
        super(TestLunrFormatter, self).setUp()

    def tearDown(self):
        self.logger.logger.removeHandler(self.handler)
        self.handler.close()
        super(TestLunrFormatter, self).tearDown()

    def test_req_id(self):
        self.logger.debug("foo")
        self.handler.flush()
        self.assertEquals("[-] foo\n", self.stream.getvalue())
        self.stream.truncate(0)
        self.stream.seek(0)
        logger.local.request_id = "monkey"
        self.logger.debug("foo")
        self.handler.flush()
        self.assertEquals("[monkey] foo\n", self.stream.getvalue())

    def test_newline_substitution(self):
        self.logger.debug("This\nhas\nnewlines!")
        self.handler.flush()
        self.assertEquals("[-] This#012has#012newlines!\n",
                          self.stream.getvalue())

    def test_newline_substitution_exc(self):
        try:
            raise RuntimeError("Foo!")
        except Exception:
            self.logger.exception("error!")
        self.handler.flush()
        print "stream: ", self.stream.getvalue()

        self.stream.truncate(0)
        self.stream.seek(0)
        try:
            raise RuntimeError("Foo!")
        except Exception:
            self.logger.error("There was an exception!",
                              exc_info=sys.exc_info())
        self.handler.flush()
        print "stream: ", self.stream.getvalue()

        self.assert_(True)


if __name__ == "__main__":
    unittest.main()
