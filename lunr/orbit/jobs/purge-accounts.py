# Copyright (c) 2011-2016 Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
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

from __future__ import print_function

from cinderclient.exceptions import NotFound, ClientException, BadRequest
import lunrclient
from lunrclient import LunrClient, CinderClient, StorageClient
from requests.exceptions import RequestException
from lunrclient import LunrError, LunrHttpError
from logging.handlers import SysLogHandler
from argparse import ArgumentParser
from os import path

import requests
import logging
import time
import sys
import os

lof = logging.getLogger("purge-accounts")

"""
class KeyValueParser(dict):

    def __init__(self):
        self.name = None

    def get_or_env(self, key):
        return self.__dict__.get(key) or os.environ.get(key, '')

    def __getitem__(self, index):
        return self.get(index)

    def missing(self, keys):
        # Given a list of keys that should exist in the config, return a list
        # of keys that are missing
        return [key for key in keys if self.get(key) is None]

    def readfp(self, fd):
        # Save the name of the file
        self.name = fd.name
        for line in fd:
            try:
                # Ignore comment lines
                if line.startswith('#'):
                    continue
                # Split key=value
                (key, value) = line.rstrip().split('=')
                # Remove inline comments
                index = value.find('#')
                if index != -1:
                    value = value[0:index]
                # trim whitespace
                value = value.strip()
                # Remove single and double quotes
                value = re.sub(r'^("|\')|("|\')$', '', value)
                self[key.strip()] = value
            except ValueError:
                # Ignore lines with no '='
                continue
"""
"""
def setup_logging(verbose):
    # Setup logger to log to syslog and optionally be more verbose to stdout if
    # asked by user

    syslog = SysLogHandler('/dev/log', SysLogHandler.LOG_LOCAL5)
    streamformat = "%(module)s:%(levelname)s %(message)s"
    syslog.setFormatter(logging.Formatter(streamformat))
    LOG.addHandler(syslog)
    LOG.setLevel(logging.DEBUG)

    # Setup log to stdout for pruge-accounts.py
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("-- %(message)s"))
    LOG.addHandler(stream)
    LOG.setLevel(logging.ERROR)
    if verbose == 1:
        LOG.setLevel(logging.INFO)
    if verbose > 1:
        LOG.setLevel(logging.DEBUG)

    # Add syslog handlers for cinderclient and keystone
    cinder = logging.getLogger("cinderclient.client")
    cinder.addHandler(syslog)
    keystone = logging.getLogger("keystoneclient")
    keystone.addHandler(syslog)
"""

class Fail(Exception):
    pass


class FailContinue(Exception):
    pass

"""
class Base:

    def __init__(self):
        self.total = {
            'backups': 0,
            'backup-size': 0,
            'volumes': 0,
            'vtypes': {}
        }
        self.cursor = '/-\|'
        self.cursor_pos = 0

    def report(self, totals):
        return "%s Volumes ( %s ) and %s Backups ( %sGB ) " % (
            totals['volumes'],
            ', '.join(["%s: %sGB" % (key, totals['vtypes'][key])
                       for key in totals['vtypes'].keys()]),
            totals['backups'],
            totals['backup-size']
        )

    def spin_cursor(self):
        if self.cursor is None:
            return
        self.cursor_pos = (self.cursor_pos + 1) % len(self.cursor)
        sys.stdout.write(self.cursor[self.cursor_pos])
        sys.stdout.flush()
        time.sleep(0.1)
        sys.stdout.write('\b')
"""


class Purge:

    def __init__(self, tenant_id, creds, options):
        # Base.__init__(self)
        self.lunr = LunrClient(tenant_id, timeout=10, url=creds['lunr_url'],
                               http_agent='cbs-purge-accounts',
                               debug=(options.verbose > 1))
        self.cinder = CinderClient(timeout=10, http_agent='cbs-purge-accounts',
                                   creds=creds, debug=(options.verbose > 1),
                                   logger=LOG)
        self.throttle = options.throttle
        self.verbose = options.verbose
        self.tenant_id = str(tenant_id)
        self.region = creds['region']
        self.report_only = not options.force
        self.creds = creds
        if not options.cursor:
            self.cursor = None

    def log(self, msg):
        LOG.info("DDI: %s (%s) - %s" % (self.tenant_id, self.region, msg))

    def debug(self, msg):
        if self.verbose:
            LOG.debug("DDI: %s (%s) - %s" % (self.tenant_id, self.region, msg))
        else:
            self.spin_cursor()

    def wait_on_status(self, func, status):
        for i in range(20):
            resp = func()
            if resp.status == status:
                return True
            time.sleep(1)
        return False

    def delete_backup(self, backup):
        # Skip backups already in a deleting status
        if backup['status'] in ('DELETING', 'DELETED'):
            self.debug("SKIP - Backup %s in status of %s"
                       % (backup['id'], backup['status']))
            return False

        if self.report_only:
            self.log("Found snapshot '%s' in status '%s'"
                     % (backup['id'], backup['status']))
            return True

        # Catch statuses we may have missed
        if backup['status'] != 'AVAILABLE':
            raise FailContinue("Refusing to delete backup %s in status of %s"
                               % (backup['id'], backup['status']))

        try:
            self.log("Attempting to delete snapshot %s in status %s"
                     % (backup['id'], backup['status']))
            self.cinder.volume_snapshots.delete(str(backup['id']))
        except NotFound:
            self.log("WARNING - Snapshot already deleted - Cinder returned "
                     "404 on delete call %s" % backup['id'])
            return True

        try:
            # Wait until snapshot is deleted
            if self.wait_on_status(lambda: self.cinder.volume_snapshots.get(
                    str(backup['id'])), 'deleted'):
                return True
            raise FailContinue("Snapshot '%s' never changed to status of "
                               "'deleted'" % backup['id'])
        except NotFound:
            self.log("Delete %s Success" % backup['id'])
            return True

    def is_volume_connected(self, volume):
        try:
            # Create a client with 'admin' as the tenant_id
            client = LunrClient('admin', url=self.creds['lunr_url'],
                                debug=(self.verbose > 1))
            # Query the node for this volume
            node = client.nodes.get(volume['node_id'])
            # Build a node url for the storage client to use
            node_url = "http://%s:%s" % (node['hostname'], node['port'])
            # Get the exports for this volume
            payload = StorageClient(node_url, debug=(self.verbose > 1))\
                .exports.get(volume['id'])
            return self._is_connected(payload)
        except LunrHttpError, e:
            if e.code == 404:
                return False
            raise

    def _is_connected(self, payload):
        if 'error' in payload:
            return False
        if payload:
            for session in payload.get('sessions', []):
                if 'ip' in session:
                    return True
        return False

    def clean_up_volume(self, volume):
        # Ask cinder for the volume status
        resp = self.cinder.volumes.get(volume['id'])

        # If the status is 'in-use'
        if resp.status == 'in-use':
            self.log("Cinder reports volumes is 'in-use', "
                     "checking attached status")
            # If the volume is NOT connected
            if not self.is_volume_connected(volume):
                # Force detach the volume
                try:
                    self.log("Volume '%s' stuck in attached state, "
                             "attempting to detach" % volume['id'])
                    return self.cinder.rackspace_python_cinderclient_ext\
                        .force_detach(volume['id'])
                except AttributeError:
                    raise Fail("rackspace_python_cinderclient_ext is not"
                               " installed, and is required to force detach")
            raise FailContinue("Volume '%s' appears to be still connected "
                               "to a hypervisor" % volume['id'])

    def delete_volume(self, volume):
        attempts = 0
        while True:
            try:
                if self._delete_volume(volume):
                    self.incr_volume(volume)
                return
            except BadRequest:
                if attempts > 0:
                    raise
                self.clean_up_volume(volume)
                attempts += 1
                continue

    def _delete_volume(self, volume):
        # Skip volumes in strange status
        if volume['status'] in ('NEW', 'DELETED'):
            self.debug("SKIP - Volume %s in status of %s"
                       % (volume['id'], volume['status']))
            return False

        if volume['status'] in ('ERROR', 'DELETING'):
            self.log("SKIP - Volume %s in status of %s"
                     % (volume['id'], volume['status']))
            return False

        if self.report_only:
            self.log("Found Volume '%s' in status '%s'"
                     % (volume['id'], volume['status']))
            return True

        # Catch statuses we may have missed
        if volume['status'] != 'ACTIVE':
            raise FailContinue("Refusing to delete volume %s in status of %s"
                               % (volume['id'], volume['status']))

        try:
            self.log("Attempting to delete volume %s in status %s"
                     % (volume['id'], volume['status']))
            self.cinder.volumes.delete(str(volume['id']))
        except NotFound:
            self.log("WARNING - Volume already deleted - Cinder returned "
                     "404 on delete call %s" % volume['id'])
            return True

        try:
            # Wait until volume reports deleted
            if self.wait_on_status(lambda: self.cinder.volumes.get(
                    str(volume['id'])), 'deleted'):
                return
            raise FailContinue("Volume '%s' never changed to status of"
                               " 'deleted'" % volume['id'])
        except NotFound:
            self.log("Delete %s Success" % volume['id'])
            return True

    def incr_volume(self, volume):
        self.total['volumes'] += 1
        try:
            self.total['vtypes'][volume['volume_type_name']] += volume['size']
        except KeyError:
            self.total['vtypes'][volume['volume_type_name']] = volume['size']

    def incr_backup(self, backup):
        self.total['backups'] += 1
        self.total['backup-size'] += backup['size']

    def purge(self):
        try:
            # Get a list of all backups
            for backup in self.lunr.backups.list():
                time.sleep(self.throttle)
                # Attempt to delete the backups
                if self.delete_backup(backup):
                    self.incr_backup(backup)

            # Delete all the volumes for this account
            for volume in self.lunr.volumes.list():
                time.sleep(self.throttle)
                if self.delete_volume(volume):
                    self.incr_volume(volume)

            # Delete any quotas for this account
            self.delete_quotas(self.tenant_id)

            # If we found anything to purge, report it here
            if self.total['volumes'] != 0 or self.total['backups'] != 0:
                verb = 'Found' if self.report_only else 'Purged'
                self.log("%s %s" % (verb, self.report(self.total)))
                return True
        except (LunrError, ClientException), e:
            raise FailContinue(str(e))
        return False

    def delete_quotas(self, tenant_id):
        # (Quotas should return to defaults if there were any)
        # self.cinder.quotas.delete(self.tenant_id)

        # NOTE: The following is a temporary fix until we upgrade from havana

        # Get the default quotas
        defaults = self.cinder.quotas.defaults(tenant_id)
        # Get the actual quotas for this tenant
        quotas = self.cinder.quotas.get(tenant_id)
        updates = {}
        for quota_name in quotas.__dict__.keys():
            # Skip hidden attributes on the QuotaSet object
            if quota_name.startswith('_'):
                continue
            # If the quota is different from the default, make it zero
            if getattr(quotas, quota_name) != getattr(defaults, quota_name):
                updates[quota_name] = 0

        if len(updates) > 0:
            self.log("Found non-default quotas, setting quotas [%s] to zero"
                     % ','.join(updates))
            self.cinder.quotas.update(tenant_id, **updates)


# noinspection PyPackageRequirements
class Application:

    def __init__(self):
        # Base.__init__(self)
        self.verbose = False

    # the way to register the application has changed to cloud feeds
    # def register(self, url, region):
    #     app_id = 'cbs-purge-accounts-%s' % region
    #     resp = requests.get('/'.join([url, 'register', app_id]), timeout=10)
    #     print("PURGE_ACCOUNTS_URL=%s" % url)
    #     print("PURGE_ACCOUNTS_KEY=%s" % resp.text)
    #     print("PURGE_ACCOUNTS_APP_ID=%s" % app_id)
    #     return 0

    def read_config(self, files):
        """ Given a list of config files, attempt to load each file.
        Return the config for the first valid file we find """
        def open_fd(file):
            try:
                return open(file)
            except IOError:
                return None

        if not any([os.path.exists(rc) for rc in files]):
            raise Fail("Couldn't find any of these config files to load [%s]" %
                       ",".join(files))

        # Read the first file we find
        conf = KeyValueParser()
        for fd in [open_fd(file) for file in files]:
            if fd is None:
                continue
            conf.readfp(fd)
            return conf

    def load_config(self):
        config = self.read_config([path.expanduser('~/.environment'),
                                  '/home/lunr/.environment'])

        # Report if any of these config options are missing
        missing = config.missing(['PURGE_ACCOUNTS_URL', 'PURGE_ACCOUNTS_KEY',
                                  'PURGE_ACCOUNTS_APP_ID', 'OS_AUTH_URL',
                                  'LUNR_API_URL', 'OS_TENANT_NAME',
                                  'OS_PASSWORD', 'OS_USERNAME', 'OS_REGION'])
        if missing:
            print("-- The variable(s) [%s] are missing from config"
                  " file '%s'" % (','.join(missing), config.name))
            # needs to be revamped with cloud feeds
            if config.missing(['PURGE_ACCOUNTS_URL', 'PURGE_ACCOUNTS_KEY',
                               'PURGE_ACCOUNTS_APP_ID']):
                print("-- You must register the purge-accounts.py with "
                      "the account feed")
                print("-- ./purge-accounts.py <REGION> --register "
                      "http://api.actioneer.ohthree.com\n")
            return False

        # Closed Accounts Config
        self.url = config.get('PURGE_ACCOUNTS_URL')
        self.key = config.get('PURGE_ACCOUNTS_KEY')
        self.app_id = config.get('PURGE_ACCOUNTS_APP_ID')

        # Cinder and Lunr Client Credentials
        self.creds = {
            'auth_url': config.get('OS_AUTH_URL'),
            'lunr_url': config.get('LUNR_API_URL'),
            'tenant_name': config.get('OS_TENANT_NAME'),
            'username': config.get('OS_USERNAME'),
            'password': config.get('OS_PASSWORD'),
            'region': config.get('OS_REGION')
        }

        return True

    def purge_quotas(self, options):
        tenants = options.quotas.split(',')
        if not len(tenants):
            print("-- quotas option must provide a comma delimited list of"
                  "tenant_ids (see --help)")
            return 1

        for tenant_id in tenants:
            try:
                LOG.info("Checking quotas for '%s'" % tenant_id)
                purger = Purge(tenant_id, self.creds, options)
                purger.delete_quotas(tenant_id)
            except ClientException, e:
                LOG.error(e)

        return 0

    def run(self, argv):
        # p = ArgumentParser(description="Purges closed accounts from cinder")
        # p.add_argument('--register', metavar='URL',
        #                help="Register the purge script with the account feed")
        # p.add_argument('--force', '-f', action='store_true',
        #                help="Do the actual purge, instead of just reporting")
        # p.add_argument('--region', metavar='REGION',
        #                help="Specify a region to register for")
        p.add_argument('--throttle', '-t', default=0, type=float,
                       metavar='INT',
                       help="Seconds to wait between actions taken")
        p.add_argument('--verbose', '-v', action='count', default=0,
                       help="-v logs info() to stdout, -vv logs "
                       "debugs to stdout")
        # p.add_argument('--cursor', '-c', action='store_true',
        #                help="Enable the spinning cursor")
        # p.add_argument('--quotas', '-q',
        #                help="Purge quotas from the list of provided "
        #                "tenant_id's example: [ --quotas 23423,52356,56232 ]")
        # p.add_argument('--account', '-a', help="Purge only this one account")
        # options = p.parse_args(argv)
        # self.verbose = options.verbose
        #
        # # Setup Logging
        # setup_logging(options.verbose)

        # If option is set to register, the method is called
        # if options.register:
        #     if not options.region:
        #         print("-- You must include --region when using --register")
        #         return 1
        #     return self.register(options.register, options.region)
        #
        # # Attempt to load our config
        # if not self.load_config():
        #     return 1
        #
        # if options.quotas:
        #     return self.purge_quotas(options)
        #
        # if not options.cursor:
        #     self.cursor = None
        #
        # if options.account:
        #     # Only purge this one account
        #     self.run_purge(options.account, options)
        #     self.print_totals()
        #     return

        # Make a call to the closed account feed
        accounts = self.fetch_accounts()
        LOG.info("Feed returned '%d' tenant_id's to close" % len(accounts))

        # Iterate over the list of deletable accounts
        for account in accounts:
            try:
                self.run_purge(account, options)
                time.sleep(options.throttle)
                if options.force:
                    # Mark the account as done
                    self.put_done(account)
            except FailContinue as e:
                # Log the error and continue to attempt purges
                LOG.error("Purge for '%s' failed - %s" % (account, e))

        # Print out the purge totals
        self.print_totals()

    def print_totals(self):
        LOG.info("Grand Total - %s " % self.report(self.total))

    def collect_totals(self, purger):
        self.total['volumes'] += purger.total['volumes']
        self.total['backups'] += purger.total['backups']
        self.total['backup-size'] += purger.total['backup-size']
        for key in purger.total['vtypes'].keys():
            try:
                self.total['vtypes'][key] += purger.total['vtypes'][key]
            except KeyError:
                self.total['vtypes'][key] = purger.total['vtypes'][key]

    def run_purge(self, tenant_id, options):
        found = False
        purger = None

        try:
            LOG.debug("Tenant ID: %s" % tenant_id)
            purger = Purge(tenant_id, self.creds, options)
            if purger.purge():
                # If we found something for this tenant
                self.collect_totals(purger)
                found = True

        except FailContinue:
            self.collect_totals(purger)
            raise

        if not found and options.verbose:
            LOG.info("No Volumes or Backups to purge for '%s'" % tenant_id)
            return True

        if options.force:
            if options.verbose or found:
                LOG.info("Purge of '%s' Completed Successfully" % tenant_id)
        return True

    def fetch_accounts(self):
        url = '/'.join([self.url, 'ready', self.app_id])
        LOG.info("Fetching Tenant ID's from feed (%s)" % url)
        resp = requests.get(url, headers={'X-Auth-Token': self.key},
                            timeout=10)
        if resp.status_code == 401:
            raise Fail("Feed '%s' returned 401 UnAuthorized, use --register"
                       "to re-register" % url)
        resp.raise_for_status()
        return resp.json()

    def put_done(self, account):
        url = '/'.join([self.url, 'done', self.app_id, str(account)])
        LOG.debug("Marking Tenant ID '%s' as DONE (%s)" % (account, url))
        resp = requests.get(url, headers={'X-Auth-Token': self.key},
                            timeout=10)
        resp.raise_for_status()


if __name__ == "__main__":
    """
        In order for purge-accounts.py to track which accounts have already
        been purged it must first be registered with the closed accounts
        feed, like so

        ./purge-accounts.py --region <REGION> --register

        This will print the registration id, feed url and the application id
        fetched from the closed accounts api. Since this script is run
        independently in each DC, the application id includes the <REGION>
        passed, which makes the registration id unique for each REGION.

        You must write the output from the --register command into either
        /home/lunr/.environment or ~/.environment file.

        Once the registered values have been written to the .environment file
        we can run an account purge report. This will report what volumes and
        backups are still available for the accounts in the feed, use
        --verbose for a very detailed explanation of what purge-accounts.py
        is doing.

        ./purge-accounts.py --verbose

        Once satisfied with the report, you can purge the accounts with
        --force argument

        ./purge-accounts.py --force

    """

    try:
        app = Application()
        sys.exit(app.run(sys.argv[1:]))
    except (RequestException, Fail) as e:
        LOG.error("%s" % str(e))
        LOG.error("Stopped")
        app.print_totals()
        sys.exit(1)
