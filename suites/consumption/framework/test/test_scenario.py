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
from suites.consumption.framework.scenarios.abstract import AbstractScenario


class TestScenario(AbstractScenario):

    def _validate_config(self):
        super()._validate_config()

        assert self.config.get('test_scenario_param'), 'There is no "test_scenario_param" variable in config'

    def run(self, artifact_name):
        """
        Run scenario for defined artifact

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running test() benchmark with config: %s" % self.config, color='green')

        self.start_probes(artifact_name)

        warmup_runs, prod_runs = self._get_number_of_runs()

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        for i in range(0, warmup_runs + prod_runs):
            warmup_iteration = False if warmup_runs == 0 else i < warmup_runs

            self.write_time_event('iteration_%s start' % i)

            log_print("Running iteration %s, (%s)" % (i, 'warmup' if warmup_iteration else 'prod'))
            log_print("Imagine that some job happens here for version %s" % version)

        self.stop_probes(test_results=self.config.get('should_pass'))

        self.results['evaluated'] = True

