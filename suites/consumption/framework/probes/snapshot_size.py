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
from suites.consumption.framework.utils import convert_size, percentage_diff, avg


class SnapshotSizeProbe(AbstractProbe):
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

        self.results[self.version] = list()

    def stop(self, **kwargs):
        """
        Store stop time work/db directory size
        """
        if 'avg_snapshot_dir_size' not in kwargs:
            log_print('Unable to get time specific results from test: no such property "avg_snapshot_dir_size"',
                      color='red')
            return

        super().stop()

        self.results[self.version] += kwargs.get('avg_snapshot_dir_size')

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

        is_passed = False

        for version in self.results.keys():
            if version == self.base_version:
                continue

            base_result = avg(self.results[self.base_version])
            comparable_result = avg(self.results[self.base_version])

            avg_snapshot_size_diff = percentage_diff(base_result,
                                                     comparable_result)

            min_value = float(self.probe_config.get('diff')[0])
            max_value = float(self.probe_config.get('diff')[1])

            is_passed = min_value < avg_snapshot_size_diff < max_value

            self.result_message = \
                'Average snapshot directory size difference: {:.2f}. {} (base) - {}, {} - {}'.format(
                    avg_snapshot_size_diff,
                    self.base_version,
                    convert_size(base_result),
                    version,
                    convert_size(comparable_result)
                )

            log_print(self.result_message,
                      color=self.get_out_color(is_passed, **kwargs))

        self.result_passed = is_passed

        return is_passed

