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

from tiden.case.apptestcase import AppTestCase
from tiden.util import with_setup

from suites.consumption.framework.runner import ScenarioRunner
from suites.consumption.framework.test.test_scenario import TestScenario


class TestFramework(AppTestCase):
    """
    Self test for consumption framework:

    Run:

    --tc=config/suite-consumption.yaml
    --tc=config/env_*.yaml
    --tc=config/artifacts-*.yaml
    --ts="consumption.test_framework"
    --clean=tests

    """
    main_utils = None

    client_config = None
    server_config = None

    def __init__(self, *args):
        super().__init__(*args)

        for name in args[0]['artifacts'].keys():
            if args[0]['artifacts'][name].get('type', '') == 'ignite':
                self.add_app(
                    name,
                    app_class_name='ignite',
                )

        self.consumption_config = None

    def setup(self):
        self.consumption_config = self.tiden.config['consumption']['test_consumption']

        super().setup()

    def setup_testcase(self):
        pass

    def teardown_testcase(self):
        pass

    @with_setup(setup_testcase, teardown_testcase)
    def test_test_passed(self):
        config = {
            'warmup_run': 2,  # times to warmup
            'times_to_run': 5,  # times to run (warmup + times to run = total runs in one try)
            'max_tries': 1,  # maximum number of full reruns (after 1 runs - fail the test)
            'times_to_run_increment': 3,  # increment time to run on each rerun
            'data_size': 5000,  # data load size
            'test_scenario_param': True,  # data load size
            'should_pass': True,
            'probes': {
                'test': {
                    'test_probe_param': True,
                }
            }
        }

        res = ScenarioRunner(self, config, TestScenario(self)).run()

        assert res['probes']['TestProbe']['result_passed'], \
            res['probes']['TestProbe']['result_message']

    @with_setup(setup_testcase, teardown_testcase)
    def test_test_failed(self):
        config = {
            'warmup_run': 2,  # times to warmup
            'times_to_run': 5,  # times to run (warmup + times to run = total runs in one try)
            'max_tries': 1,  # maximum number of full reruns (after 1 runs - fail the test)
            'times_to_run_increment': 3,  # increment time to run on each rerun
            'data_size': 5000,  # data load size
            'should_pass': False,
            'test_scenario_param': True,  # data load size
            'probes': {
                'test': {
                    'test_probe_param': True,
                }
            }
        }

        res = ScenarioRunner(self, config, TestScenario(self)).run()

        assert not res['probes']['TestProbe']['result_passed'], \
            res['probes']['TestProbe']['result_message']

    @with_setup(setup_testcase, teardown_testcase)
    def test_test_failed_scenario_config_check(self):
        config = {
            'warmup_run': 2,  # times to warmup
            'times_to_run': 5,  # times to run (warmup + times to run = total runs in one try)
            'max_tries': 1,  # maximum number of full reruns (after 1 runs - fail the test)
            'times_to_run_increment': 3,  # increment time to run on each rerun
            'data_size': 5000,  # data load size
            'should_pass': False,
            'probes': {
                'test': {
                    'test_probe_param': True,
                }
            }
        }

        try:
            ScenarioRunner(self, config, TestScenario(self)).run()
        except AssertionError as e:
            assert "There is no \"test_scenario_param\" variable in config" in str(e)

    @with_setup(setup_testcase, teardown_testcase)
    def test_test_failed_probe_config_check(self):
        config = {
            'warmup_run': 2,  # times to warmup
            'times_to_run': 5,  # times to run (warmup + times to run = total runs in one try)
            'max_tries': 1,  # maximum number of full reruns (after 1 runs - fail the test)
            'times_to_run_increment': 3,  # increment time to run on each rerun
            'data_size': 5000,  # data load size
            'should_pass': False,
            'test_scenario_param': True,  # data load size
            'probes': {
                'test': {
                }
            }
        }

        try:
            ScenarioRunner(self, config, TestScenario(self)).run()
        except AssertionError as e:
            assert "There is no \"test_probe_param\" value in config" in str(e)

