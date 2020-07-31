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

from tiden import log_print
from suites.consumption.framework.probes.abstract import AbstractProbe
from suites.consumption.framework.utils import percentage_diff, avg


class ExchangeTimeProbe(AbstractProbe):
    """
    Two point result probe for validate operation time execution
    """

    def validate_config(self):
        """
        diff is mandatory for this probe
        """
        assert 'diff' in self.probe_config, 'There is no "diff" value in probe config'

    def start(self, **kwargs):
        self.results[self.version] = []
        return

    def stop(self, **kwargs):
        """
        Store stop time work/db directory size
        """
        if 'x1_time' not in kwargs:
            log_print('Unable to get specific results from test: no such property "x1_time"', color='red')
            return

        if 'name' in self.probe_config:
            name = self.probe_config['name']

            if type(kwargs['x1_time']) != type({}):
                log_print('Expected "x1_time" to be a dictionary', color='red')
                return

            if name not in kwargs['x1_time']:
                log_print('Unable to get specific results from test: "x1_time" has no data for "%s" probe' % name,
                          color='red')
                return

            self.results[self.version] += kwargs['x1_time'][name]
        else:
            self.results[self.version] += kwargs['x1_time']

        super().stop()

    def is_passed(self, **kwargs):
        """
        Formula:
        base_version.total_time - observable_version.total_time / base_version.total_time

        I.e. difference between  base and observable version in percentage

        :return is verification passed
        """
        if self.check_single_artifact():
            return True

        is_passed = True

        for version in self.results:
            if version == self.base_version:
                continue

            base_version_result = avg(self.results[self.base_version])
            comparable_version_result = avg(self.results[version])

            time_diff = percentage_diff(base_version_result, comparable_version_result)
            min_value = float(self.probe_config.get('diff')[0])
            max_value = float(self.probe_config.get('diff')[1])

            is_passed = min_value < time_diff < max_value

            self.result_message = '{} time difference: {:.2f}%, {} (base) - {} sec, {} - {} sec'.format(
                self.name,
                time_diff * 100,
                self.base_version,
                round(base_version_result / 1000.0, 4),
                version,
                round(comparable_version_result / 1000.0, 4)
            )

            log_print(self.result_message,
                      color=self.get_out_color(is_passed, **kwargs))

        self.result_passed = is_passed

        return is_passed

