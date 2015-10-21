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


from lunr.storage.helper.base import bytes_to_gibibytes
from lunr.storage.helper.utils import execute
from operator import itemgetter
import math
import json
import re


def request(helper, url, **kwargs):
    resp = helper.make_api_request(url, **kwargs)
    return json.loads(resp.read())


def to_bytes(_dict, keys):
    """
    Given a dict, convert the keys from Gigs to Bytes
    """
    for key in keys:
        _dict[key] = int(_dict[key]) * int(math.pow(1024, 3))
    return _dict


def lvm_capacity():
    """
    Return the total LVM capacity in bytes
    """
    lines = execute('vgdisplay', 'lunr-volume', units='b')
    for line in lines.split('\n'):
        # Find the Volume Group Size in the output
        if re.search('VG Size', line):
            # First convert the bytes to GiB, then round down (trunc)
            # Then convert back to bytes. Do this because lunr/cinder
            # stores the size as an integer not a float
            return bytes_to_gibibytes(int(line.split()[2]))


def find(key, haystack):
    """
    Given a list of dicts, return a dict where key == id
    """
    for item in haystack:
        if item['id'] == key:
            return item
    return None


def compare_lists(first, second, key='id'):
    """
    Given 2 lists of dicts, Given each dict has a key of 'id'
    compare both lists, if they are identical return True,
    if not return False
    """

    # If both lists are empty
    if len(second) == 0 and len(first) == 0:
        return True

    # If both lists do not have the same number of items
    if len(first) != len(second):
        return False

    # Sort both lists by their 'id'
    first, second = [sorted(i, key=itemgetter(key))
                     for i in (first, second)]
    # flatten the two lists into list of pairs and compare
    pairs = zip(first, second)
    if any(rvalue != lvalue for rvalue, lvalue in pairs):
        return False
    return True


def snapshots(helper):
    """
    Report any lingering snapshots that are not still in use by an
    active clone or backup operation
    """
    # Get a list of active LVM snapshots
    snapshots = [lv for lv in helper.volumes._scan_volumes()
                 if lv['origin'] != '']
    results = []

    def msg(id, _msg):
        results.append({'snapshot': id, 'msg': _msg})

    # For each of the snapshots
    for snapshot in snapshots:
        # Snapshot could be for a backup
        if 'backup_id' in snapshot:
            # Match them up with a current backup
            volume = request(helper, 'backups/%s'
                             % snapshot['backup_id'])
            if volume['status'] != 'SAVING':
                msg(volume['id'], "Lingering snapshot from Backup, API "
                    "reports status is '%s'" % volume['status'])
            continue

        # Snapshot could be for a clone
        if 'clone_id' in snapshot:
            # Match them up with a current clone
            volume = request(helper, 'volumes/%s'
                             % snapshot['clone_id'])
            if volume['status'] != 'CLONING':
                msg(volume['id'], "Lingering snapshot from Clone, API "
                    "reports status is '%s'" % volume['status'])
            continue
        msg(snapshots['id'],
            "Lingering snapshot that is neither Clone or Backup")
    return results


def volumes(helper):
    """
    Report any volume inconsistencies between the storage node and the API
    """
    results = []
    # Get a list of active LVM Volumes
    lvs = [lv for lv in helper.volumes._scan_volumes()
           if lv['origin'] == '']

    # Get our node id
    node = request(helper, 'nodes?name=%s' % helper.name)
    # Get a list of all volumes for this node
    volumes = request(helper, 'volumes?node_id=%s' % node[0]['id'])

    # Convert all the volume sizes from gigs to bytes
    for volume in volumes:
        volume = to_bytes(volume, ['size'])

    def msg(id, _msg):
        results.append({'volume': id, 'msg': _msg})

    # Compare the local list of lvm volumes with lunr api
    for volume in volumes:
        # If any of the volumes are deleted
        if volume['status'] == 'DELETED':
            # But they still exist on the storage node
            if any(lv['id'] == volume['id'] for lv in lvs):
                msg(volume['id'], "Volume is deleted on API, but still "
                    "exists on node '%s'" % (lv['path']))
            continue

        # Find volumes that are in the API results but not on the node
        vol = find(volume['id'], lvs)
        if not vol:
            msg(volume['id'], "Volume exists on API, but missing from Node")
            continue

    # Find volumes that are on the node but not in the API results
    for lv in lvs:
        # Find the lv volume in the list of API volumes
        volume = find(lv['id'], volumes)
        if not volume:
            msg(lv['id'], "Volume exists on node, but missing from API")
            continue

        # Compare the size of the volumes
        if int(lv['size']) != volume['size']:
            msg(lv['id'], "Volume sizes do not agree (%d != %d)" %
                (lv['size'], volume['size']))
            continue
    return results


def node(helper):
    """
    Report any storage capacity and useage inconsistencies between the
    storage node and the API
    """
    # Get a list of active LVM Volumes
    lvs = [lv for lv in helper.volumes._scan_volumes()
           if lv['origin'] == '']

    # Get our node id
    node_id = request(helper, 'nodes?name=%s' % helper.name)
    # Fetch all information about our node
    node = request(helper, 'nodes/%s' % node_id[0]['id'])

    def msg(_msg):
        return {'node': node['id'], 'msg': _msg}

    # Calculate total space used by volumes
    space_used = 0
    for lv in lvs:
        space_used += bytes_to_gibibytes(lv['size'])

    # API should report the same used and available space
    if node['storage_used'] != space_used:
        return msg("API storage_used is inconsistent with node (%s != %s)"
                   % (node['storage_used'], space_used))

    # Ask the hardware how much capacity it has
    total_capacity = lvm_capacity()
    # Compare total capacity with lunr api capacity numbers
    if total_capacity != node['size']:
        return msg("API 'size' is inconsistent with storage node (%s != %s)"
                   % (node['size'], total_capacity))

    # Available storage should match also
    storage_free = total_capacity - space_used
    if storage_free != node['storage_free']:
        return msg("API 'storage_free' is inconsistent with storage node "
                   " (%s != %s)" % (node['storage_free'], storage_free))
