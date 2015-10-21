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


from lunr.common.lock import LockFile
from signal import signal, SIGTERM
import resource
import atexit
import fcntl
import pwd
import sys
import os


class DaemonError(Exception):
    pass


class Daemon(object):

    def __init__(self, conf, pid_file):
        user = conf.string('orbit', 'user', None)
        self.pid_file = pid_file

        self.uid = os.getuid()
        if user is not None:
            self.uid = pwd.getpwnam(user).pw_uid

        # This ensures atexit function gets called
        signal(SIGTERM, lambda signum, frame: sys.exit(1))

    def register(self, method):
        # Clean up atexit registers from any previous forks
        for i in range(len(atexit._exithandlers) -1, -1, -1):
            if atexit._exithandlers[i][0].__name__ == method.__name__:
                del atexit._exithandlers[i]
        # register the clean up method
        atexit.register(method)

    def __enter__(self):
        # Change process owner if needed
        os.setuid(self.uid)
        # Ensure we clean up when terminated
        self.register(self._on_exit)

        # Detach not requested if no pid file
        if not self.pid_file:
            return self

        # Change to the root directory
        os.chdir('/')
        # Create a new detached parent process
        self.detach()
        try:
            with LockFile(self.pid_file) as lock:
                lock.write(str(os.getpid()))
        except OSError, e:
            raise DaemonError(
                "Error creating pidfile %s - %s" % (self.pid_file, e))
        return self

    def alive(self):
        """ Returns true if the pid file exists pid is used """
        try:
            fd = os.open(self.pid_file, os.O_RDONLY)
            # Pass in a fd here so LockFile doesn't
            # create the pid file when opening
            with LockFile(fd) as lock:
                    pid = int(lock.read())
                    os.kill(pid, 0)
                    return pid
        except OSError, e:
            return False

    def __exit__(self, exc_type, exc_value, traceback):
        """ Context manager exit point. """
        self._on_exit()

    def _on_exit(self):
        try:
            if self.pid_file:
                os.unlink(self.pid_file)
        except OSError, e:
            pass

    def detach(self):
        def fork(msg=None):
            try:
                pid = os.fork()
                if pid > 0:
                    os._exit(0)
            except OSError, e:
                raise RuntimeError("%s:" % (msg, e))

        fork("Failed first fork")
        os.setsid()
        fork("Failed second fork")
