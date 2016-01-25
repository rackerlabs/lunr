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


import ConfigParser
import logging
from logging import NOTSET, DEBUG, WARNING, INFO, ERROR, CRITICAL
import logging.config
from logging import handlers
import os
import sys
import threading

from lunr.common.config import NoConfigError

local = threading.local()

LOG_FORMAT = '%(asctime)s %(name)s:%(levelname)-8s %(message)s'

LOGGER = None
_loggers = {}


class NoLoggerError(Exception):
    pass


def log(level, msg, *args, **kwargs):
    if not LOGGER:
        get_logger()
    LOGGER.log(level, msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    log(DEBUG, msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    log(INFO, msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    log(WARNING, msg, *args, **kwargs)

warn = warning


def error(msg, *args, **kwargs):
    log(ERROR, msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    kwargs.update({'exc_info': True})
    log(ERROR, msg, *args, **kwargs)


def critical(msg, *args, **kwargs):
    log(CRITICAL, msg, *args, **kwargs)


class LoggerFileObject(object):

    def __init__(self, level=INFO, prefix=None):
        self.level = level
        self.prefix = prefix

    def log(self, msg):
        if self.prefix:
            msg = '%s: %s' % (self.prefix, msg)
        log(self.level, msg)

    def write(self, value):
        value = value.strip()
        if value:
            self.log(value)

    def writelines(self, values):
        self.log('#012'.join(values))

    def flush(self):
        pass


class LunrLoggerAdapter(logging.LoggerAdapter, object):

    def __init__(self, logger, extra=None):
        super(LunrLoggerAdapter, self).__init__(logger, extra)

    def process(self, msg, kwargs):
        # with capture_stdio print in this method will cause recursion
        extra = kwargs.setdefault('extra', {})
        extra.update({'request_id': getattr(local, 'request_id', '-')})
        return msg, kwargs

    def setLevel(self, level):
        self.logger.setLevel(level)


class LunrFormatter(logging.Formatter):

    def __init__(self, *args, **kwargs):
        super(LunrFormatter, self).__init__(*args, **kwargs)
        self._fmt = self._fmt.replace(
            '%(message)s', '[%(request_id)s] %(message)s')

    def format(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = '-'
        # We want the whole message in syslog, not just up to the first \n.
        return super(LunrFormatter, self).format(record).replace('\n', '#012')


def already_logging_to_console():
    return [h for h in logging.getLogger().handlers if hasattr(h, 'stream') and
            hasattr(h.stream, 'fileno') and h.stream.fileno() in (1, 2)]


def configure_default_logging(**kwargs):
    if not kwargs:
        logger = logging.getLogger()
        formatter = LunrFormatter(LOG_FORMAT)
        handler = handlers.SysLogHandler('/dev/log',
                                         handlers.SysLogHandler.LOG_USER)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        logging.basicConfig(**kwargs)


def get_logger(name=None):
    global LOGGER
    name = name or 'lunr'
    if name not in _loggers:
        logger = logging.getLogger(name)
        _loggers[name] = LunrLoggerAdapter(logger)
    if not LOGGER:
        LOGGER = _loggers[name]
    return _loggers[name]


def rename(name):
    global LOGGER
    LOGGER = get_logger(name)


def close_and_redirect_stdio(close_stderr=True):
    nullf = open(os.devnull, 'r+')
    for f in (sys.stdin, sys.stdout, sys.stderr):
        f.flush()
        if f is sys.stderr and not close_stderr:
            continue
        os.dup2(nullf.fileno(), f.fileno())
    sys.stdout = LoggerFileObject(level=INFO, prefix='STDOUT')
    sys.stderr = LoggerFileObject(level=ERROR, prefix='STDERR')


def configure(logging_conf_file=None, name=None, capture_stdio=True,
              log_to_console=False, lunr_log_level=NOTSET, **kwargs):
    if logging_conf_file:
        try:
            logging.config.fileConfig(logging_conf_file,
                                      disable_existing_loggers=False)
        except ConfigParser.Error:
            conf = ConfigParser.ConfigParser()
            if not conf.read(logging_conf_file):
                raise NoConfigError(logging_conf_file)
            # conf_file exists, but could not parse logging conf
            for section in ('loggers', 'handlers', 'formatters'):
                if conf.has_section(section):
                    # appears to have misconfigured log section
                    raise
            # this doesn't appear to be a logging conf_file
            configure_default_logging(**kwargs)
    else:
        configure_default_logging(**kwargs)
    logger = get_logger(name)
    logging.getLogger('lunr').setLevel(lunr_log_level)
    logging.getLogger('migrate').setLevel(ERROR)
    if capture_stdio:
        close_and_redirect_stdio(close_stderr=not log_to_console)
    if log_to_console and not already_logging_to_console():
        handler = logging.StreamHandler(sys.__stderr__)
        handler.setFormatter(LunrFormatter(LOG_FORMAT))
        logging.getLogger().addHandler(handler)
    return logger
