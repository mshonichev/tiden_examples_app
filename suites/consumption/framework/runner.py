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

from tiden import log_print, TidenException
from copy import deepcopy
from .scenarios.abstract import AbstractScenario
from ..abstract import AbstractConsumptionTest


class ScenarioRunner:
    """
    Main class of consumption framework

    Run scenarios, decide if it need to be reexecuted and return results

    Config variables:

    print_background: True/False - print background probe result (plots feature will be available later)
    fail_every_run: just fail test on every run but verify probes on each run
    debug_background_probe: launch Thread instead Process PoolExecutor - pydev debug will be available
    """

    def __init__(self, test_class: AbstractConsumptionTest, scenario_config, scenario: AbstractScenario):
        self.artifacts = []

        if not scenario_config:
            raise TidenException("There is no config for scenario: %s" % scenario_config)

        base_found = False
        for name in test_class.tiden.config['artifacts'].keys():
            if test_class.tiden.config['artifacts'][name].get('type', '') == 'ignite':
                # base artifact will be first element of the versions array

                if test_class.tiden.config['artifacts'][name].get('base', False):
                    if not base_found:
                        base_found = True
                    else:
                        assert False, \
                            "Ignite artifacts should contain one and only only base artifact (base: True)!"

                    self.artifacts.insert(0, name)
                else:
                    self.artifacts.append(name)

        # run in single artifact mode if only one artifact presented
        # in this case no analyze() method will be called
        self.single_run = True if len(self.artifacts) == 1 else False
        if self.single_run:
            log_print(
                "Consumption suite '{}' launched in single artifact mode.".format(test_class.__class__.__name__),
                color='red'
            )

        self.global_config = test_class.tiden.config

        self.config = scenario_config
        self.scenario = scenario

        self._validate_config()

        self.scenario.initialize(scenario_config)

    def _validate_config(self):
        assert self.config.get('max_tries'), 'There is no "max_tries" variable in config'

    def run(self):
        """
        Run defined scenario for all artifacts and validate that all probes passed if it's not previously executed
        If any probe failed - rerun with increased tries

        Approximate logic:

        if not previously executed:
            while (max tries limit not reached):
                for each artifact in config:
                    run scenario
                if any run failed with Exception (connection drop, python error) - mark test as failed and reset results

                validate that every probes in scenario is_passed()

                if passed:
                    exit run and mark as successful
                else:
                    rerun with increased times_to_run

            if there is no tries - fail run and return existing (failed) results
        else:
            return previous result

        :return: run result
        """

        log_print('Evaluating benchmark for following artifacts: %s' % self.artifacts, color='blue')

        if not self.scenario.results['evaluated']:
            log_print('There is no previous execution. Running scenario', color='blue')

            while True:
                if self.scenario.current_runs >= int(self.config['max_tries']):
                    log_print("Maximum number of tries has been reached. Using results as is.", color='red')
                    break

                try:
                    for artifact in self.artifacts:
                        self.scenario.run(artifact_name=artifact)
                except Exception as e:
                    # exit from benchmark evaluating on any exception
                    self.scenario.reset()
                    log_print("Evaluating scenario failed with exception: %s" % e, color='red')
                    raise e

                self.scenario.write_results()

                # print results of background probes if defined
                if self.single_run or self.global_config.get('print_background', False):
                    log_print("Results: {}".format(self.scenario.results), color='blue')

                # re-run scenario for all artifacts if scenario is not passed
                #
                # don't re-run if we have only one artifact (in that case scenario should't never be passed, but
                # we just print results and that's ok)
                #
                # also, re-run scenario even if it is passed, but 'fail_every_run' is set (e.g. we want to re-run
                # valid scenario 'max_tries' times just for the sake of it.
                #
                if self.single_run or \
                        self.scenario.scenario_passed and not self.global_config.get('fail_every_run', False):
                    break

                log_print("Unable to get appropriate results: rerun with increased number of tries.",
                          color='yellow')

                self.scenario.current_runs += 1
        else:
            log_print('Using previous execution result.', color='blue')

        return self.scenario.results

    @staticmethod
    def initialize(test_class, tiden_config, suite_path):
        def _get_suite_configuration():
            def _get_raw_configuration():
                cur_config = tiden_config
                cur_path = []

                for config_key in (suite_path + '.').split('.'):
                    if config_key == '':
                        return cur_config
                    cur_path.append(config_key)
                    assert config_key in cur_config, "Configuration key '{}' not found, pass --tc=suite-<name>.yaml".format('.'.join(cur_path))
                    cur_config = cur_config[config_key]

            raw_config = _get_raw_configuration()

            defaults = {}

            # non-dictionary top-level keys are considered suite defaults
            for config_key, config_value in raw_config.items():
                if type(config_value) != type({}):
                    defaults[config_key] = config_value

            suite_config = deepcopy(defaults)

            # dictionary top-level keys are scenarios, copy defaults to them and update with scenario configuration
            for config_key, config_value in raw_config.items():
                if type(config_value) == type({}):
                    suite_config[config_key] = deepcopy(defaults)
                    suite_config[config_key].update(config_value)
            return suite_config

        def _get_artifacts_and_add_apps():
            artifacts = {}
            for name, artf in tiden_config['artifacts'].items():
                if artf.get('type', '') == 'ignite':
                    test_class.add_app(
                        name,
                        app_class_name='ignite',
                    )
                    artifacts[name] = artf
            return artifacts

        return _get_suite_configuration(), _get_artifacts_and_add_apps()

    @staticmethod
    def validate_probe(res, probe_name):
        assert res['probes'][probe_name]['result_passed'], res['probes'][probe_name]['result_message']

        # if we fall through here - assertion succeed, so just dump result once again for the report
        log_print(res['probes'][probe_name]['result_message'], color='green', report=True)

