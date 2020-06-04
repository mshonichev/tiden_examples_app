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

from tiden.apps.ignite import Ignite
from tiden.util import attr, test_case_id, with_setup
from suites.consumption.abstract import AbstractConsumptionTest

from suites.consumption.framework.runner import ScenarioRunner
from suites.consumption.framework.scenarios.put_all import PutAllLoadingScenario
from suites.consumption.framework.scenarios.snapshot import IncrementalSnapshotScenario, \
    FullSnapshotScenario, PointInTimeRecoveryScenario, RestoreFullSnapshot
from suites.consumption.framework.scenarios.start_time import StartTimeScenario
from suites.consumption.framework.scenarios.streamer import StreamerLoadingScenario


class TestConsumption(AbstractConsumptionTest):
    """
    Run (newly added config file):

    --tc=config/suite-consumption.yaml
    --tc=config/env_*.yaml
    --tc=config/artifacts-*.yaml
    --ts="consumption.test_consumption"
    --clean=tests

    """
    client_config = None
    server_config = None

    def __init__(self, *args):
        super().__init__(*args)

    def setup(self):
        super().setup()

        self.client_config = Ignite.config_builder.get_config('client', config_set_name='base')

        # test objects (need to be global to avoid reruns)
        # snapshots tests
        self.snapshot_config = self.consumption_config.get('snapshot', None)
        self.inc_snapshot_config = self.consumption_config.get('inc-snapshot', None)
        self.snapshot_scenario = FullSnapshotScenario(self)
        self.inc_snapshot_scenario = IncrementalSnapshotScenario(self)
        self.full_restore_scenario = RestoreFullSnapshot(self)
        self.pitr_scenario = PointInTimeRecoveryScenario(self)

        # putall tests
        self.put_all_config = self.consumption_config.get('putall', None)
        self.put_all_scenario = PutAllLoadingScenario(self)

        # streamer tests
        self.streamer_config = self.consumption_config.get('streamer', None)
        self.streamer_scenario = StreamerLoadingScenario(self)

        # volatile tests (putall based)
        self.volatile_config = self.consumption_config.get('volatile', None)
        self.volatile_scenario = PutAllLoadingScenario(self)

        # start node tests
        self.start_node_config = self.consumption_config.get('start_time', None)
        self.start_node_scenario = StartTimeScenario(self)

    def setup_testcase(self):
        super().setup_testcase()

    def teardown_testcase(self):
        super().teardown_testcase()

    @attr('snapshot', 'ult')
    @test_case_id('143080')
    @with_setup(setup_testcase, teardown_testcase)
    def test_full_snapshot_time(self):
        """
        Snapshot FULL execution time benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.snapshot_config, self.snapshot_scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    @attr('snapshot', 'ult')
    @test_case_id('143081')
    @with_setup(setup_testcase, teardown_testcase)
    def test_inc_snapshot_time(self):
        """
        Snapshot INC execution time benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.inc_snapshot_config, self.inc_snapshot_scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    @attr('snapshot', 'ult')
    @test_case_id('143081')
    @with_setup(setup_testcase, teardown_testcase)
    def test_inc_snapshot_wal(self):
        """
        Snapshot INC execution time benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.inc_snapshot_config, self.inc_snapshot_scenario).run()

        self.assert_result(res, 'SnapshotSizeProbe')

    @attr('restore', 'ult')
    @test_case_id('257203')
    @with_setup(setup_testcase, teardown_testcase)
    def test_restore_time(self):
        """
        Snapshot restore time benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.snapshot_config, self.full_restore_scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    @attr('pitr', 'ult')
    @test_case_id('257202')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pitr_time(self):
        """
        PITR execution time benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.snapshot_config, self.pitr_scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    @attr('snapshot', 'ult')
    @test_case_id('143079')
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_wal(self):
        """
        Snapshot WALs benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.snapshot_config, self.snapshot_scenario).run()

        self.assert_result(res, 'WalSizeProbe')

    @attr('snapshot', 'ult')
    @test_case_id('143088')
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_db(self):
        """
        Snapshot DB folder size benchmark

        Load data, take few snapshots
        """
        res = ScenarioRunner(self, self.snapshot_config, self.snapshot_scenario).run()

        self.assert_result(res, 'SnapshotSizeProbe')

    @attr('streamer')
    @test_case_id('143087')
    @with_setup(setup_testcase, teardown_testcase)
    def test_streamer_time(self):
        """
        Streamer time benchmark
        """
        res = ScenarioRunner(self, self.streamer_config, self.streamer_scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    @attr('streamer')
    @test_case_id('143086')
    @with_setup(setup_testcase, teardown_testcase)
    def test_streamer_wal(self):
        """
        Streamer WALs benchmark
        """
        res = ScenarioRunner(self, self.streamer_config, self.streamer_scenario).run()

        self.assert_result(res, 'WalSizeProbe')

    @attr('streamer')
    @test_case_id('143085')
    @with_setup(setup_testcase, teardown_testcase)
    def test_streamer_db(self):
        """
        Streamer DB size benchmark
        """
        res = ScenarioRunner(self, self.streamer_config, self.streamer_scenario).run()

        self.assert_result(res, 'DbSizeProbe')

    @attr('putall')
    @test_case_id('143084')
    @with_setup(setup_testcase, teardown_testcase)
    def test_put_all_time(self):
        """
        putAll() time benchmark
        """
        res = ScenarioRunner(self, self.put_all_config, self.put_all_scenario).run()

        self.assert_result(res, 'ExecutionTimeProbe')

    @attr('putall')
    @test_case_id('143083')
    @with_setup(setup_testcase, teardown_testcase)
    def test_put_all_wal(self):
        """
        putAll() WALs benchmark
        """
        res = ScenarioRunner(self, self.put_all_config, self.put_all_scenario).run()

        self.assert_result(res, 'WalSizeProbe')

    @attr('putall')
    @test_case_id('143082')
    @with_setup(setup_testcase, teardown_testcase)
    def test_put_all_db(self):
        """
        putAll() DB folder size benchmark
        """
        res = ScenarioRunner(self, self.put_all_config, self.put_all_scenario).run()

        self.assert_result(res, 'DbSizeProbe')

    @attr('volatile')
    @with_setup(setup_testcase, teardown_testcase)
    def test_cp_time(self):
        """
        CP time benchmark
        """
        res = ScenarioRunner(self, self.volatile_config, self.volatile_scenario).run()

        self.assert_result(res, 'CheckpointProbe')

    @attr('start_time')
    @with_setup(setup_testcase, teardown_testcase)
    def test_start_node_time(self):
        """
        Start node time benchmark
        """
        res = ScenarioRunner(self, self.start_node_config, self.start_node_scenario).run()

        self.assert_result(res, 'StartTimeProbe')

