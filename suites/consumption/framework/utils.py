#!/usr/bin/env python3
#
# Copyright 2017-2020 GridGain Systems.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Utility class
"""
import math
import re
from functools import reduce

from tiden import time, log_print


def avg(array):
    """
    Average value between array
    :param array:
    :return: avg
    """
    return 0 if len(array) == 0 else (float(reduce(lambda x, y: x + y, array)) / len(array))


def percentage_diff(base_version, version):
    """
    Diff in percentage by base version

    :param base_version: base version results
    :param version: under test version results
    :return:
    """
    return version if base_version == 0 else (version - base_version) / base_version


def get_current_time():
    """
    :return: current time milliseconds
    """
    return int(round(time() * 1000))


def convert_size(size_bytes, start_byte=False):
    if size_bytes == 0:
        return "0B"

    if start_byte:
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    else:
        size_name = ("KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def get_nodes_directory_size(ignite, ssh, directory, nodes=None, commands=None):
    """
    Total directory size from all nodes

    :return: collected result
    """
    node_ids_to_ignite_home = {}

    if not nodes:
        nodes = ignite.get_all_default_nodes() + ignite.get_all_additional_nodes()

    for node_ids in nodes:
        node_ids_to_ignite_home[node_ids] = ignite.nodes[node_ids]['ignite_home']

    # commands may be overridden if specific commands should be sent to node
    if not commands:
        commands = {}

        for node_idx in nodes:
            host = ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'du -s {}/{}'.format(ignite.nodes[node_idx]['ignite_home'], directory)
                ]
            else:
                commands[host].append('du -s {}/{}'.format(ignite.nodes[node_idx]['ignite_home'], directory))

    results = ssh.exec(commands)
    results_parsed = 0

    for host in results.keys():
        # print(results[host][0])
        search = re.search('(\d+)\\t', results[host][0])
        if not search:
            log_print('Unable to get directory size for host %s. Set directory size as 0.' % host)
            return 0

        results_parsed += int(search.group(1))

    return results_parsed

