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

from pt import log_print
from suites.consumption.framework.probes.abstract import AbstractProbe
from suites.consumption.framework.utils import get_nodes_directory_size


class WalSizeProbe(AbstractProbe):
    """
    Two point probe (start and end) by directory in IGNITE_HOME/work/db/wal

    Analyze results by actual files number, not total size
    """

    def validate_config(self):
        """
        diff is mandatory for this probe (number of WAL files used, not their size)
        """
        assert 'diff' in self.probe_config, 'There is no "diff" value in config'

    def start(self):
        """
        Store start time WAL directory size
        """
        super().start()

        self.results[self.version] = {
            'start': get_nodes_directory_size(self.ignite, self.test_class.ssh, 'work/db/wal')}

    def stop(self, **kwargs):
        """
        Store stop time WAL directory size
        """
        super().stop()

        self.results[self.version]['end'] = get_nodes_directory_size(self.ignite, self.test_class.ssh, 'work/db/wal')

    def is_passed(self, **kwargs):
        """
        Formula:
        abs(base_version.total_wal_files_size - observable.total_wal_files_size) / wal_file_size

        I.e. difference between  base and observable version in percentage

        :return is verification passed
        """
        if self.check_single_artifact():
            return True

        is_passed = False

        for version in self.results.keys():
            if version == self.base_version:
                continue

            wal_segment_size = self.test_class.consumption_config.get('wal_segment_size', 64 * 1024 * 1024) / 1024

            wal_segments_diff = int((self.results[version]['end'] - self.results[self.base_version]['end']) /
                                    wal_segment_size)

            number_of_nodes = len(self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes())

            min_value = int(self.probe_config.get('diff')[0]) * number_of_nodes
            max_value = int(self.probe_config.get('diff')[1]) * number_of_nodes

            is_passed = min_value < wal_segments_diff < max_value

            self.result_message = 'Wal segments difference: {} (over all {} nodes), ' \
                                  '{} (base) - {} segments, ' \
                                  '{} - {} segments'.format(wal_segments_diff, number_of_nodes, self.base_version,
                                                            int(self.results[self.base_version]['end'] /
                                                                wal_segment_size),
                                                            version,
                                                            int(self.results[version]['end'] /
                                                                wal_segment_size))
            log_print(self.result_message,
                      color=self.get_out_color(is_passed, **kwargs))

        self.result_passed = is_passed

        return is_passed

