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

from tiden import *
from tiden_gridgain.piclient.helper.operation_utils import create_cpu_load_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden.configuration_decorator import test_configuration
from suites.snapshots.ult_utils import UltimateUtils
from tiden.stress import StressT

STABILIZATION_TIMEOUT = 30


@test_configuration([
    'zookeeper_enabled',
    'pitr_enabled',
], [
    # Generic Release
    [False, False],
    # Sber release
    [True, True],
    [False, True],
])
class TestBaselinePerformance(UltimateUtils):
    rebalance_timeout = 30
    logger = get_logger('tiden')
    sbt_model_enabled = False

    teardown = UltimateUtils.teardown

    def setup(self):
        super().setup()
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestBaseline]')

    def setup_testcase(self):
        super().setup_testcase()
        if self.get_context_variable('pitr_enabled'):
            self.su.wait_no_snapshots_activity_in_cluster()

    def teardown_testcase(self):
        log_print('TestTeardown is called')
        super().teardown_testcase()

    @attr('baseline', 'sbt')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_sbt_model_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            import json

            with open("%s/json_model.json" % self.config['rt']['test_resource_dir'], 'r') as f:
                model_descriptor_file = f.read()

            model_descriptor = json.loads(json.loads(model_descriptor_file))
            caches_to_run = [item for item in model_descriptor.values()]

            with TransactionalLoading(self,
                                      caches_to_run=caches_to_run,
                                      kill_transactions_on_exit=True,
                                      cross_cache_batch=50,
                                      skip_atomic=True,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     start_key=1,
                                                                     end_key=99,
                                                                     transaction_timeout=1000),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self._sleep_and_custom_event(tx_loading, 'start nodes')
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.create_loading_metrics_graph('test_baseline_sbt_model_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)
        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id('60274')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_one_node_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     transaction_timeout=50),
                                      skip_consistency_check=True,
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self._sleep_and_custom_event(tx_loading, 'start nodes')
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.create_loading_metrics_graph('test_baseline_adding_one_node_with_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id('60286')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_two_nodes_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     transaction_timeout=50),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                util_sleep_for_a_while(20)
                self.ignite.add_additional_nodes(self.get_server_config(), 2)
                self._sleep_and_custom_event(tx_loading, 'start nodes')
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.create_loading_metrics_graph('test_baseline_adding_two_nodes_with_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_two_nodes_with_metrics_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     transaction_timeout=50),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                util_sleep_for_a_while(20)
                self.ignite.add_additional_nodes(self.get_server_config(), 2)
                self._sleep_and_custom_event(tx_loading, 'start nodes')
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.create_loading_metrics_graph('test_baseline_adding_two_nodes_with_metrics_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id('60276')
    @require(min_ignite_version='2.5.1')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_one_node_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     transaction_timeout=50),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 1
                self._sleep_and_custom_event(tx_loading, 'kill node')
                self.ignite.kill_node(2)
                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.create_loading_metrics_graph('test_baseline_removing_one_node_with_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id('60277')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_two_nodes_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     transaction_timeout=50),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 2
                self._sleep_and_custom_event(tx_loading, 'kill nodes')
                self.ignite.kill_node(2)
                self.ignite.kill_node(3)

                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.create_loading_metrics_graph('test_baseline_removing_two_nodes_with_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id('60275')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_two_nodes_removed_one_added_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     commit_possibility=0.2,
                                                                     transaction_timeout=50),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 2
                self._sleep_and_custom_event(tx_loading, 'kill nodes')
                self.ignite.kill_node(2)
                self.ignite.kill_node(3)

                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self._sleep_and_custom_event(tx_loading, 'add new nodes')
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')
                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        log_print(inspect.stack()[0].function)
        self.create_loading_metrics_graph('test_baseline_two_nodes_removed_one_added_with_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id('60275')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_two_nodes_removed_one_added_with_cpu_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     commit_possibility=0.2,
                                                                     transaction_timeout=1000),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 2
                self._sleep_and_custom_event(tx_loading, 'kill nodes')
                self.ignite.kill_node(2)
                self.ignite.kill_node(3)

                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self._sleep_and_custom_event(tx_loading, 'add new nodes')
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._sleep_and_custom_event(tx_loading, 'set blt')
                self._set_baseline_few_times()

                self._sleep_and_custom_event(tx_loading, 'sleep')

                self._sleep_and_custom_event(tx_loading, 'cpu_load')

                cpu_load_operation = create_cpu_load_operation(1.0, 1.0, 2)
                cpu_load_operation.evaluate()
                self._sleep_and_custom_event(tx_loading, 'cpu_load_sleep_end')
                cpu_load_operation.interrupt()

                self._sleep_and_custom_event(tx_loading, 'end loading')

                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)

        log_print(inspect.stack()[0].function)
        self.create_loading_metrics_graph('test_baseline_two_nodes_removed_one_added_with_loading',
                                          metrics)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('REST')
    @with_setup('setup_testcase', 'teardown_testcase')
    def test_loading_rest(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     transaction_timeout=5000),
                                      tx_metrics=True) as tx_loading:
                self._sleep_and_custom_event(tx_loading, 'start')
                util_sleep_for_a_while(100)
                self._sleep_and_custom_event(tx_loading, 'end')
        self.ignite.wait_for_topology_snapshot(client_num=0)
        self.create_loading_metrics_graph('test_baseline_adding_two_nodes_with_loading', tx_loading)
        self.cu.list_transactions()
        self.cu.control_utility('--cache', 'idle_verify')
        
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('stress-tolerance', 'HW')
    @with_setup('setup_testcase', 'teardown_testcase')
    def test_loading_server(self):
        node_first = self.ignite.nodes[self.ignite.get_alive_default_nodes()[-1]]
        node_last = self.ignite.nodes[self.ignite.get_alive_default_nodes()[-2]]
        fault_command_run = {
            "disc load": False,
            "network load": True,
            "cpu load": True,
            "ram load": True,
            "sigstop_server": True,
            "sigstop_client": False,
            "packets loss": True,
            "packets duplicate": True,
            "packets corrupt": True
        }
        self.test_cluster_stress_tolerance(node_first, node_last, fault_command_run)

    def test_cluster_stress_tolerance(self, node_under_test, other_node, fault_combination):
        timeout = 15
        thread_timeout = 10
        take_a_rest_timeout = 10
        host_under_test = node_under_test.get('host')
        other_host = other_node.get('host')

        stress = StressT(self.ssh)

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     commit_possibility=1.0,
                                                                     transaction_timeout=5000),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                util_sleep_for_a_while(take_a_rest_timeout, msg='Loading warm up for')
                self._custom_event(tx_loading, 'start')
                util_sleep_for_a_while(take_a_rest_timeout)
                for key, value in fault_combination.items():
                    if value:
                        self._sleep_and_custom_event(tx_loading, '%s start' % key)
                        print_red('%s start' % key)
                        if key == 'disc load' and value:
                            stress.load_disk(node_under_test['ignite_home'], host_under_test, timeout=timeout)
                        if key == 'network load' and value:
                            stress.load_network(host_under_test, other_host, timeout=timeout)
                        if key == 'cpu load' and value:
                            stress.load_cpu(host_under_test, timeout=timeout)
                        if key == 'ram load' and value:
                            stress.load_ram(host_under_test, timeout=timeout)
                        if key in ['sigstop_server', 'sigstop_client'] and value:
                            if key == 'sigstop_server':
                                pid = stress.get_random_server_pid(host_under_test)
                            else:
                                pid = stress.get_random_client_pid(host_under_test)

                            stress.sigstop_process(host_under_test, pid, timeout=thread_timeout)
                        if key in ['packets loss', ' packets duplicate', 'packets corrupt'] and value:
                            stress.network_emulate_packet(host_under_test, other_host, lost_rate='5.0%',
                                                          timeout=timeout, type=key.split()[-1])
                        self._custom_event(tx_loading, ' ')
                        print_red('%s stop' % key)
                        # util_sleep_for_a_while(take_a_rest_timeout, msg='Rest between tests for')
                util_sleep_for_a_while(take_a_rest_timeout)
                self._custom_event(tx_loading, 'end')
                metrics = tx_loading.metrics

        self.ignite.wait_for_topology_snapshot(client_num=0)
        self.create_loading_metrics_graph('test_baseline_adding_two_nodes_with_loading', metrics)
        self.cu.list_transactions()
        self.cu.control_utility('--cache', 'idle_verify')
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @staticmethod
    def _sleep_and_custom_event(tx_loading, event_name=''):
        print_green("Custom event added on plot: %s" % event_name)

        util_sleep_for_a_while(STABILIZATION_TIMEOUT)
        tx_loading.metrics_thread.add_custom_event(event_name)

    @staticmethod
    def _custom_event(tx_loading, event_name=''):
        tx_loading.metrics_thread.add_custom_event(event_name)

