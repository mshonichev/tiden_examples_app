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

import traceback
from abc import ABC

from tiden import log_print, version_num
from tiden.util import get_jvm_options
from suites.consumption.framework.probes.factory import ProbeFactory


class AbstractScenario(ABC):
    """
    Abstract scenario class
    """

    def __init__(self, test_class):
        self.probes = {}
        self.test_class = test_class
        self.config = None
        self.artifact_config_variables = dict()
        self.artifact_jvm_properties = list()
        self.artifact_name = None
        self.current_runs = 0

        self.results = None
        self.reset()

        self.probes = {}
        self.scenario_passed = True

    def run(self, artifact_name):
        """
        Reset passed boolean for possible scenario rerun
        """
        self.artifact_name = artifact_name

        artifact_attribute = self.test_class.tiden.config['artifacts'][artifact_name].get('attribute')

        if artifact_attribute:
            self.artifact_config_variables = \
                artifact_attribute.get('config_variables', dict())
            self.artifact_jvm_properties = get_jvm_options(artifact_attribute, 'jvm_properties')

            log_print('\nDefined special artifact attributes\nConfig variables: %s\nJVM properties: %s' %
                      (self.artifact_config_variables, self.artifact_jvm_properties), color='green')

        self.scenario_passed = True

    def reset(self):
        """
        Reset results (to cleanup failed runs)
        """
        self.results = {'time': {}, 'probes': {}, 'evaluated': False}

    def initialize(self, scenario_config):
        """
        Abstract initializer + scenario config validation
        """
        self.config = scenario_config
        self._validate_config()
        ProbeFactory.initialize(self, scenario_config['probes'])

    def start_probes(self, artifact_name, app_name=None):
        """
        Start all probes that defined in config
        """
        for probe in self.probes.values():
            probe.initialize(self.test_class, artifact_name, app_name=app_name)

            probe.start()

    def stop_probes(self, **kwargs):
        """
        Stop all probes that defined in config
        """
        for probe in self.probes.values():
            probe.stop(**kwargs)

    def write_time_event(self, name):
        for probe in self.probes.values():
            probe.write_event(name)

    def write_results(self):
        """
        Write scenario probes into result
        """
        #
        self.define_passed()

        for probe in self.probes.values():
            self.results['probes'][probe.name] = {
                'main': probe.results,
                'events': probe.events,
                'result_passed': probe.result_passed,
                'result_message': probe.result_message
            }

    def define_passed(self):
        """
        Validate scenario execution by analyzing each probe result.
        If any probe analyze() call failed - scenario will fail.

        :return: boolean that defines is scenario passed
        """
        for probe in self.probes.values():
            # we do need to all analyze() methods to be executed so it's placed first arg in boolean expression
            try:
                passed = probe.is_passed(colorless=True)
            except Exception:
                # in case of exception fail probe - there must be a bug in code
                log_print(traceback.format_exc(), color='red')
                passed = False

            self.scenario_passed = passed and self.scenario_passed

        return self.scenario_passed

    def _get_number_of_runs(self):
        # default times to run
        # plus warmup times
        # plus rerun times
        rerun_runs = self.current_runs * int(self.config.get('times_to_run_increment', 0))

        warmup_runs = int(self.config.get('warmup_run', 0))
        prod_runs = int(self.config.get('times_to_run', 0)) + rerun_runs

        return warmup_runs, prod_runs

    @staticmethod
    def read_lock_property_value(version):
        ignite27_version = version_num("2.7")

        return True if version_num(version) > ignite27_version else False

    def _validate_config(self):
        """
        Assert that probe config is correct: it's contains all necessary arguments
        E.g. data size, time to run etc.
        """
        assert 'probes' in self.config, "There is no probes scenario config for this scenario"

