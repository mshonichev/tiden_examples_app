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

import re

from pt import log_print
from suites.consumption.framework.probes.abstract import AbstractProbe
from suites.consumption.framework.utils import convert_size, percentage_diff


class DbSizeProbe(AbstractProbe):
    """
    Two point probe (start and end) by directory in IGNITE_HOME/work/db
    """

    def validate_config(self):
        """
        diff is mandatory for this probe
        """
        assert 'diff' in self.probe_config, 'There is no "diff" value in config'

    def start(self):
        """
        Store start time work/db directory size
        """
        super().start()

        self.results[self.version] = {'start': self.get_db_size_per_node_id()}

    def stop(self, **kwargs):
        """
        Store stop time work/db directory size
        """
        super().stop()

        self.results[self.version]['end'] = self.get_db_size_per_node_id()

    def print_results(self):
        log_print('Results for db size probe: {}'.format(self.results))

    def is_passed(self, **kwargs):
        """
        Formula:
        base_version.end_size - observable_version.end_size / base_version.end_size

        I.e. difference between  base and observable version in percentage

        :return is verification passed
        """
        if self.check_single_artifact():
            return True

        is_passed = True

        for version in self.results.keys():
            if version == self.base_version:
                continue

            db_total_size = percentage_diff(self.results[self.base_version]['end'], self.results[version]['end'])

            min_value = float(self.probe_config.get('diff')[0])
            max_value = float(self.probe_config.get('diff')[1])

            is_passed = min_value < db_total_size < max_value

            self.result_message = \
                'Sub of /db folder size difference: {:.2f}. {} (base) - {}, {} - {}' \
                    .format(db_total_size,
                            self.base_version,
                            convert_size(self.results[self.base_version]['end']),
                            version,
                            convert_size(self.results[version]['end']))

            log_print(self.result_message,
                      color=self.get_out_color(is_passed, **kwargs))

        self.result_passed = is_passed

        return is_passed

    def get_db_size_per_node_id(self):
        """
        Total DB directory size from all nodes

        :return: collected result
        """
        commands = {}
        node_ids_to_ignite_home = {}
        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()
        for node_ids in server_nodes:
            node_ids_to_ignite_home[node_ids] = self.ignite.nodes[node_ids]['ignite_home']
        for node_idx in server_nodes:
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'du -s %s/work/db/%s' % (self.ignite.nodes[node_idx]['ignite_home'],
                                             self.get_folder_name_from_consistent_id(node_idx))
                ]
            else:
                commands[host].append('du -s %s/work/db/%s' % (self.ignite.nodes[node_idx]['ignite_home'],
                                                               self.get_folder_name_from_consistent_id(node_idx)))
        results = self.test_class.ssh.exec(commands)
        results_parsed = 0
        # nice! n^2 algorithm
        for host in results.keys():
            search = re.search('(\d+)\\t', results[host][0])
            assert search, 'Unable to get DB size for host {}: {}'.format(host, results[host][0])
            results_parsed += int(search.group(1))

        return results_parsed

    def get_folder_name_from_consistent_id(self, node_idx):
        from re import sub
        return sub('[.,-]', '_', self.ignite.get_node_consistent_id(node_idx))

