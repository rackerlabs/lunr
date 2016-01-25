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


import gc
import logging
import os
import resource as unix
import threading

from lunr.common import logger
from lunr.storage.helper.utils.worker import SaveFailedInvalidCow


log = logger.get_logger()


def spawn(resource, job, *args, **kwargs):
    """
    Attempt to start job_name if not already running for those args.

    param job: job to run
    param args: args for job's run method
    keyword callback: callback function to pass to job
    keyword error_callback: error_callback function to pass to job
    """
    callback = kwargs.pop('callback', None)
    error_callback = kwargs.pop('error_callback', None)
    interruptible = kwargs.pop('interruptible', False)

    # If we asked to skip fork for testing
    if kwargs.pop('skip_fork', False):
        return run(resource, job, callback, error_callback, args)

    # Fork Once to create a child
    pid = os.fork()
    if pid:
        # wait on the child to fork and exit to prevent zombie
        os.waitpid(pid, 0)
        # Our child now owns the resource, this avoids resource
        # file clean up when we the controller returns 200
        resource.owned = False
        return

    # Fork Twice to orphan the child
    pid = os.fork()
    if pid:
        # exit to orphan child, and release waiting parent
        os._exit(0)

    # Lock resource prior to read/write
    with resource:
        if interruptible:
            # Add the interruptible flag if process can be interrupted
            resource.acquire({'pid': os.getpid(), 'interruptible': True})
        else:
            # Re-assign the owner of the resource to us
            resource.acquire({'pid': os.getpid()})

    # NOTE: explict close of syslog handler to force reconnect and suppress
    # traceback when the next log message goes and finds it's sockets fd is
    # inexplictly no longer valid, this is obviously jank
    # Manually nuking the logging global lock is the best thing ever.
    logging._lock = threading.RLock()
    log = logger.get_logger()
    root = getattr(log, 'logger', log).root
    for handler in root.handlers:
        try:
            # Re-create log handlers RLocks incase we forked during a locked
            # write operation; Not doing this may result in a deadlock the
            # next time we write to a log handler
            handler.createLock()
            handler.close()
        except AttributeError:
            pass

    # Become Session leader
    os.setsid()
    # chdir root
    os.chdir('/')
    # Prevent GC close() race condition
    gc.collect()
    # close fd for api server's socket
    os.closerange(3, unix.getrlimit(unix.RLIMIT_NOFILE)[1])
    # Run the job and exit
    os._exit(run(resource, job, callback, error_callback, args))


def run(lock, job, callback, error_callback, args):
    # Start the Job
    try:
        log.info("starting job '%s'" % job.__name__)
        try:
            job(*args)
        except SaveFailedInvalidCow:
            log.exception("Save job failed!")
            if error_callback:
                try:
                    error_callback()
                except Exception, e:
                    log.exception("unknown exception '%s' while executing "
                                  "error_callback for '%s'" %
                                  (e, job.__name__))
            return 1
        except Exception, e:
            log.exception("unknown exception '%s' while "
                          "executing job '%s'" % (e, job.__name__))
            return 1

        # If callback defined, execute the callback
        if callback:
            log.info("executing callback for '%s'" % job.__name__)
            try:
                callback()
            except Exception, e:
                log.exception("unknown exception '%s' while executing "
                              "callback for '%s'" % (e, job.__name__))
                return 1
        return 0
    finally:
        log.info('finished %s' % job.__name__)
        lock.remove()
