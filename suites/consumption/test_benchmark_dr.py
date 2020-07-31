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

from tiden import attr
from suites.consumption.abstract import AbstractConsumptionTest
from suites.consumption.framework.runner import ScenarioRunner
from suites.consumption.framework.scenarios.data_replication import DataReplicationScenario, DataReplicationFSTScenario


class TestBenchmarkDr(AbstractConsumptionTest):
    """
    Run (newly added config file):

    --tc=config/suite-consumption.yaml
    --tc=config/env_*.yaml
    --tc=config/artifacts-*.yaml
    --ts="consumption.test_benchmark_dr"
    --clean=tests

    """
    main_utils = None

    client_config = None
    server_config = None

    dc1_hosts = None
    dc2_hosts = None

    def __init__(self, *args):
        super().__init__(*args)

    def on_init_applications(self, tiden_config):
        """
        called during test __init__ to add required applications
        :param tiden_config:
        :return:
        """
        self._init_dc_hosts(tiden_config)

        for name, artf in tiden_config['artifacts'].items():
            if artf.get('type', '') == 'ignite':
                dc1_name = name + '_dc1'
                self.add_app(
                    dc1_name,
                    name=dc1_name,
                    grid_name='dc1',
                    artifact_name=name,
                    app_class_name='ignite',
                )
                tiden_config['environment'][dc1_name + '_hosts'] = self.dc1_hosts
                dc2_name = name + '_dc2'
                self.add_app(
                    dc2_name,
                    name=dc2_name,
                    grid_name='dc2',
                    artifact_name=name,
                    app_class_name='ignite',
                )
                tiden_config['environment'][dc2_name + '_hosts'] = self.dc2_hosts

    def _init_dc_hosts(self, tiden_config):
        server_hosts = tiden_config['environment']['server_hosts']
        assert len(server_hosts) > 0, "Not enough server hosts for Ignite!"
        if len(server_hosts) > 1:
            num_hosts_per_dc = len(server_hosts) // 2
            assert num_hosts_per_dc * 2 == len(server_hosts), "Must have even number of server hosts for Ignite!"
            self.dc1_hosts = server_hosts[:num_hosts_per_dc]
            self.dc2_hosts = server_hosts[num_hosts_per_dc:]
        else:
            self.dc1_hosts = server_hosts.copy()
            self.dc2_hosts = server_hosts.copy()

    def on_setup_application_config_sets(self):
        """
        called upon base application setup
        :return:
        """
        # config sets are created later on within DrScenario
        pass

    @attr('dr')
    def test_dr_speed(self):
        """
        Replication time benchmark
        """
        self.run_test(self.get_dr_config('dr_senders_on_client'), DataReplicationScenario(self))

    @attr('dr')
    def test_dr_speed_fst(self):
        """
        Replication time during FST benchmark
        """
        self.run_test(self.get_dr_config('dr_senders_on_client'), DataReplicationFSTScenario(self))

    def run_test(self, dr_config, scenario):
        """
        Run test

        :param dr_config: dr config from yaml
        :param scenario: scenario object
        :return:
        """
        res = ScenarioRunner(self,
                             dr_config,
                             scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    def get_dr_config(self, name):
        """
        :param name:
        :return:
        """
        return self.get_consumption_test_config(name)

