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

import os
import time
import signal
import unittest
from tempfile import mkdtemp
from shutil import rmtree
from webob.exc import HTTPConflict

from lunr.common.lock import ResourceFile
from lunr.storage.controller.base import claim, lock


class LockTest(unittest.TestCase):

    def setUp(self):
        self.path = mkdtemp()
        self.lockfile = os.path.join(self.path, 'lock')

    def tearDown(self):
        rmtree(self.path)

    def test_lock_used(self):
        resource = ResourceFile(self.lockfile)
        child = os.fork()
        if not child:
            # Child grabs resource and sleeps until killed.
            with resource:
                resource.acquire({'pid': os.getpid()})
            while True:
                time.sleep(5)

        time.sleep(0.2)
        with resource:
            used = resource.used()
            self.assert_(used)
            self.assertEquals(used['pid'], child)

        os.kill(child, signal.SIGTERM)
        os.waitpid(child, 0)


class Blank(object):
    pass


class FakeController(object):
    def __init__(self, *args, **kwargs):
        self.id = kwargs.get('id', 'fakeid')
        self.volume_id = kwargs.get('volume_id', 'fake_vol_id')
        self.app = Blank()
        self.app.helper = Blank()
        self.app.helper.volumes = Blank()
        self.app.helper.volumes.run_dir = kwargs.get('run_dir', 'run_dir')


class FakeRequest(object):
    def __init__(self, method='GET', path="/somewhere", **kwargs):
        self.method = method
        self.path = path


class StorageLockTest(unittest.TestCase):

    def setUp(self):
        self.path = mkdtemp()
        self.lockfile = os.path.join(self.path, 'lock')
        self.children = []

    def tearDown(self):
        rmtree(self.path)
        for child in self.children:
            try:
                os.kill(child, signal.SIGTERM)
                os.waitpid(child, 0)
            except OSError:
                pass

    def test_interruptible_claim(self):
        resource = ResourceFile(self.lockfile)
        child = os.fork()
        if not child:
            # Child grabs resource and sleeps until killed.
            with resource:
                resource.acquire({'pid': os.getpid(),
                                  'uri': 'child'})
            time.sleep(2)

            with resource:
                resource.acquire({'interruptible': True})

            while(True):
                time.sleep(0.5)
        else:
            self.children.append(child)

        # This is racy. Child is originally uninterruptible, but after a short
        # sleep, he marks himself interruptible
        time.sleep(1)
        info = {'uri': 'parent', 'pid': os.getpid()}
        self.assertRaises(HTTPConflict, claim, resource, info)
        time.sleep(2)
        claim(resource, info)
        time.sleep(1)
        # should be killed by now.
        pid, status = os.waitpid(child, os.WNOHANG)
        self.assertEquals(pid, child)
        with resource:
            used = resource.used()
            self.assert_(used)
            self.assertEquals(used['pid'], os.getpid())
            self.assertEquals(used['uri'], 'parent')
            # Bug: lock.acquire only updates the keys you give it.
            # So I'm marked interruptible unknowingly.
            # lunr.storage.controller.base.inspect was updated to always
            # set interruptible to False because of this.
            self.assertEquals(used['interruptible'], True)

    def test_interruptible_lock(self):
        resource = ResourceFile(self.lockfile)
        fake_controller = FakeController(volume_id='foo', id='bar',
                                         run_dir='somewhere')
        req_1 = FakeRequest(method='PUT', path="something")
        req_2 = FakeRequest(method='PUT', path="something/else")

        @lock(self.lockfile)
        def killable_func(obj, req, resource):
            with resource:
                resource.update({'interruptible': True})
            while True:
                time.sleep(1)

        @lock(self.lockfile)
        def killing_func(obj, req, resource):
            while True:
                time.sleep(1)

        # Go killable func child!
        child1 = os.fork()
        if not child1:
            killable_func(fake_controller, req_1)
        else:
            self.children.append(child1)

        time.sleep(1)
        with resource:
            used = resource.used()

        self.assertEquals(used['interruptible'], True)
        self.assertEquals(used['uri'], 'PUT something')
        self.assertEquals(used['pid'], child1)

        # Go killing func child!
        child2 = os.fork()
        if not child2:
            killing_func(fake_controller, req_2)
        else:
            self.children.append(child2)

        time.sleep(1)
        with resource:
            used = resource.used()

        self.assertEquals(used['interruptible'], False)
        self.assertEquals(used['uri'], 'PUT something/else')
        self.assertEquals(used['pid'], child2)


if __name__ == "__main__":
    unittest.main()
