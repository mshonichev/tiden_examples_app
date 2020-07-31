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

import numpy

from pt import log_print
from suites.consumption.framework.probes.abstract import AbstractProbe
from suites.consumption.framework.utils import percentage_diff


class CheckpointProbe(AbstractProbe):
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

    def stop(self, **kwargs):
        """
        Store stop time work/db directory size
        """
        super().stop()

        collected_cps = []

        non_prod_cps = 0
        prod_cps = 0

        for cp_result in self.collect_cp_info().values():
            prod_run = False
            cps = cp_result.split('\n')

            for cp in cps:
                if prod_run:
                    prod_cps += 1
                else:
                    non_prod_cps += 1

                if 'PRODUCTION RUN STARTED' in cp:
                    prod_run = True
                    continue

                if prod_run:
                    m = re.search('.*Checkpoint started \[.*checkpointLockHoldTime=(\d+)ms.*reason=\'timeout\'', cp)

                    if m:
                        for group in m.groups():
                            collected_cps.append(int(group))

        size = len(collected_cps)
        log_print('Total counted CPs (all nodes): production - {}, skipped - {}. Collected by "timeout" reason cps: {}'
                  .format(prod_cps, non_prod_cps, size),
                  color='yellow')

        self.results[self.version] = numpy.percentile(collected_cps, 95) if size > 0 else 0

    def print_results(self):
        log_print('Results for checkpointLockHoldTime probe: {}'.format(self.results))

    def is_passed(self, **kwargs):
        """
        Formula:
        base_version - observable_version / base_version

        I.e. difference between  base and observable version in percentage

        :return is verification passed
        """
        if self.check_single_artifact():
            return True

        is_passed = True

        for version in self.results.keys():
            if version == self.base_version:
                continue

            cp_speed = percentage_diff(self.results[self.base_version], self.results[version])

            min_value = float(self.probe_config.get('diff')[0])
            max_value = float(self.probe_config.get('diff')[1])

            is_passed = min_value < cp_speed < max_value

            self.result_message = \
                'checkpointLockHoldTime 95 percentile difference: {:.2f}. {} (base) - {:.2f} ms, {} - {:.2f} ms' \
                    .format(cp_speed,
                            self.base_version,
                            self.results[self.base_version],
                            version,
                            self.results[version])

            log_print(self.result_message,
                      color=self.get_out_color(is_passed, **kwargs))

        self.result_passed = is_passed

        return is_passed

    def collect_cp_info(self):
        collected_cps = {}

        self.ignite.get_data_from_log(
            'server',
            'Checkpoint started',
            '((.|\n)*)',
            'cp_info',
            default_value=''
        )

        for node_id, content in self.ignite.nodes.items():
            if 'cp_info' in content:
                collected_cps[node_id] = content['cp_info']

        return collected_cps

