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

import copy

from tiden import attr, test_case_id
from suites.consumption.abstract import AbstractConsumptionTest
from suites.consumption.framework.runner import ScenarioRunner
from suites.consumption.framework.scenarios.rebalance import FullRebalanceScenario, HistoricalRebalanceScenario


class TestBenchmarkRebalance(AbstractConsumptionTest):
    """
    Run (newly added config file):

    --tc=config/suite-consumption.yaml
    --tc=config/env_*.yaml
    --tc=config/artifacts-*.yaml
    --ts="consumption.test_benchmark_rebalance"
    --clean=tests

    """
    main_utils = None

    client_config = None
    server_config = None

    def __init__(self, *args):
        super().__init__(*args)

    @attr('full', 'load', 'in_memory', 'common', 'single_partition')
    def test_in_memory_backup_3_load_no_index_one_partition(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_3_load_no_index_one_partition'),
                      FullRebalanceScenario(self))

    @attr('full', 'load', 'pds', 'common', 'single_partition')
    def test_pds_backup_3_load_index_10_one_partition(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_3_load_index_10_one_partition'),
                      FullRebalanceScenario(self))

    @attr('full', 'historic', 'pds', 'common', 'single_partition')
    def test_historic_pds_backup_3_no_load_index_10_one_partition(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('historic_pds_backup_3_no_load_index_10_one_partition'),
                      HistoricalRebalanceScenario(self))

    @attr('full', 'load', 'in_memory')
    def test_in_memory_backup_1_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_1_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'load', 'in_memory')
    def test_in_memory_backup_3_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_3_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'load', 'in_memory')
    def test_in_memory_backup_1_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_1_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'load', 'in_memory')
    def test_in_memory_backup_3_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_3_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'load', 'pds')
    def test_pds_backup_1_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_1_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'load', 'pds')
    def test_pds_backup_3_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_3_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'load', 'pds')
    def test_pds_backup_1_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_1_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'load', 'pds', 'common')
    @test_case_id('149738')
    def test_pds_backup_3_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_3_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'in_memory')
    def test_in_memory_backup_1_no_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_1_no_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'in_memory')
    def test_in_memory_backup_3_no_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_3_no_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'in_memory')
    def test_in_memory_backup_1_no_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_1_no_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'in_memory')
    def test_in_memory_backup_3_no_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_3_no_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'pds')
    def test_in_memory_backup_3_load_index_30(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('in_memory_backup_3_load_index_30'), FullRebalanceScenario(self))

    @attr('full', 'pds')
    def test_pds_backup_1_no_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_1_no_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'pds', 'common')
    @test_case_id('257201')
    def test_pds_backup_3_no_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_3_no_load_no_index'), FullRebalanceScenario(self))

    @attr('full', 'pds')
    def test_pds_backup_1_no_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_1_no_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'pds', 'common')
    @test_case_id('149737')
    def test_pds_backup_3_no_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_3_no_load_index_10'), FullRebalanceScenario(self))

    @attr('full', 'pds')
    def test_pds_backup_3_load_index_30(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('pds_backup_3_load_index_30'), FullRebalanceScenario(self))

    @attr('historic', 'pds')
    def test_historic_pds_backup_1_no_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('historic_pds_backup_1_no_load_no_index'),
                      HistoricalRebalanceScenario(self))

    @attr('historic', 'pds')
    def test_historic_pds_backup_3_no_load_no_index(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('historic_pds_backup_3_no_load_no_index'),
                      HistoricalRebalanceScenario(self))

    @attr('historic', 'pds')
    def test_historic_pds_backup_1_no_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('historic_pds_backup_1_no_load_index_10'),
                      HistoricalRebalanceScenario(self))

    @attr('historic', 'pds', 'common')
    @test_case_id('180775')
    def test_historic_pds_backup_3_no_load_index_10(self):
        """
        Rebalance with loading time benchmark
        """
        self.run_test(self.get_rebalance_config('historic_pds_backup_3_no_load_index_10'),
                      HistoricalRebalanceScenario(self))

    def run_test(self, rebalance_config, scenario):
        """
        Run test

        :param rebalance_config: rebalance config from yaml
        :param scenario: scenario object
        :return:
        """
        res = ScenarioRunner(self,
                             rebalance_config,
                             scenario).run()

        self.assert_result(res, 'RebalanceSpeedProbe')

    def get_rebalance_config(self, name):
        """

        rebalance_qa_2402:
            warmup_run: 1  # times to warmup
            times_to_run: 2  # times to run (warmup + times to run = total runs in one try)
            max_tries: 3  # maximum number of full reruns (after 3 runs - fail the test)
            times_to_run_increment: 1  # increment time to run on each rerun
            data_size_kb: 100000 # data load size kB
            idle_verify: True
            configurations: {
            in_memory_backup_1_no_load_no_index: {
              in_memory: True,
              backups: 1,
              load: False,
              index: 0
            },
            in_memory_backup_3_no_load_no_index: {
              in_memory: True,
              backups: 3,
              load: False,
              index: 0
            }
          }

        :param name:
        :return:
        """
        rebalance_config = self.get_consumption_test_config('rebalance_various')
        conf = copy.deepcopy(rebalance_config)

        del conf['configurations']

        for key, value in rebalance_config['configurations'][name].items():
            conf[key] = value

        return conf

