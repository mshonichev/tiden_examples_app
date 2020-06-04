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
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_cross_cache_account_runner_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden.configuration_decorator import test_configuration
from suites.snapshots.ult_utils import UltimateUtils


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
class TestBaseline(UltimateUtils):
    rebalance_timeout = 30
    snapshot_storage = None

    teardown = UltimateUtils.teardown
    setup_shared_storage_test = UltimateUtils.setup_shared_storage_test
    teardown_shared_storage_test = UltimateUtils.teardown_shared_storage_test

    def setup(self):
        super().setup()
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestBaseline]')

    def setup_testcase(self):
        super().setup_testcase()
        if self.get_context_variable('pitr_enabled'):
            self.su.wait_no_snapshots_activity_in_cluster()

        if not is_enabled(self.config.get('blt_auto_adjust_enabled')) and self.cu.is_baseline_autoajustment_supported():
            self.cu.disable_baseline_autoajustment()
            log_print("Baseline autoadjustment disabled", color='green')

    def teardown_testcase(self):
        log_print('TestTeardown is called')
        super().teardown_testcase()

    def set_baseline_wrapper(self, include={}, exclude={}):

        if is_enabled(self.config.get('blt_auto_adjust_enabled')):
            timeout = int(self.blt_auto_adjust_timeout) // 1000 + 5

            blt_node, non_blt_node = self.get_baseline_nodes()
            util_sleep_for_a_while(timeout, msg="Wait auto-adjust event happen")
            actual_blt_node, actual_non_blt_node = self.get_baseline_nodes()
            tiden_assert_equal(sorted(actual_blt_node), sorted(set(blt_node | set(include)).difference(set(exclude))),
                               'Assert expected BLT')
        else:
            self._set_baseline_few_times()

    @test_case_id(70887)
    @with_setup(setup_testcase, teardown_testcase)
    def test_switch_consistent_id(self):
        self.run_snapshot_utility('snapshot', '-type=full')

        self.ignite.stop_nodes(True)

        nodes_to_remove = []
        for node_id in self.ignite.get_all_default_nodes():
            content = self.ignite.nodes[node_id]
            self.ignite.nodes[node_id + 10] = content
            nodes_to_remove.append(node_id)

        for node_id in nodes_to_remove:
            print_green('Removing old node from nodes')
            del self.ignite.nodes[node_id]

            print_green('Start clone with changed consistent id')
            self.ignite.start_node(node_id + 10)

        self.cu.activate()

        util_sleep_for_a_while(15)

        self.run_snapshot_utility('snapshot', '-type=full')

        util_sleep_for_a_while(15)

        self.run_snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(1))

        util_sleep_for_a_while(15)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60269)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_one_node_remove_two(self):
        caches_after = self.calc_checksums_on_client()

        new_server_num = len(self.ignite.get_alive_default_nodes()) - 1
        self.ignite.kill_node(2)
        self.ignite.kill_node(3)
        additional_node_id = self.ignite.add_additional_nodes(self.get_server_config(), 1)
        self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())
        self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

        self.set_baseline_wrapper(exclude={self.ignite.get_node_consistent_id(2),
                                           self.ignite.get_node_consistent_id(3)},
                                  include=set(map(self.ignite.get_node_consistent_id, additional_node_id)))

        caches_finish = self.calc_checksums_on_client()
        self.util_verify(save_lfs_on_exception=True)
        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60279)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_manual_activation(self):
        log_print('Killing all nodes in cluster')
        alive_server_nodes = self.ignite.get_alive_default_nodes()
        for node in alive_server_nodes:
            self.ignite.kill_node(node)

        log_print('Starting cluster w/o one node')
        self.ignite.start_nodes()

        new_server_num = len(self.ignite.get_alive_default_nodes()) - 1
        self.ignite.kill_node(2)

        self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

        self.cu.activate()

        log_print('Calculating new baseline')
        self.cu.control_utility('--baseline')

        assert 'Cluster state: active' in self.cu.latest_utility_output, \
            'Cluster is inactive'

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60268)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_one_node(self):
        self.ignite.snapshot_timeout = 460

        caches_before = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        additional_node_id = self.ignite.add_additional_nodes(self.get_server_config(), 1)

        self.ignite.start_additional_nodes(additional_node_id)

        self.set_baseline_wrapper(include=set(map(self.ignite.get_node_consistent_id, additional_node_id)))

        caches_after = self.calc_checksums_on_client()

        util_sleep_for_a_while(self.rebalance_timeout)

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60285)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_two_nodes(self):
        self.ignite.snapshot_timeout = 460

        caches_before = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        additional_node_id = self.ignite.add_additional_nodes(self.get_server_config(), 2)
        self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

        self.set_baseline_wrapper(include=set(map(self.ignite.get_node_consistent_id, additional_node_id)))

        util_sleep_for_a_while(self.rebalance_timeout)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60270)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_one_node(self):
        caches_before = self.calc_checksums_on_client()

        self.ignite.kill_node(2)

        self.set_baseline_wrapper(exclude={self.ignite.get_node_consistent_id(2)})

        util_sleep_for_a_while(self.rebalance_timeout)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'coordinator')
    @test_case_id(70883)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_one_coordinator(self):
        caches_before = self.calc_checksums_on_client()

        self.ignite.kill_node(1)

        # wait for new coordinator resolved
        util_sleep_for_a_while(10, msg='wait for new coordinator resolved')

        self.set_baseline_wrapper(exclude={self.ignite.get_node_consistent_id(1)})

        util_sleep_for_a_while(self.rebalance_timeout)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'coordinator')
    @test_case_id(70884)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_one_coordinator_and_second_coordinator(self):
        caches_before = self.calc_checksums_on_client()

        self.ignite.kill_node(1)
        self.ignite.kill_node(2)  # waaat?

        # wait for new coordinator resolved
        util_sleep_for_a_while(10, msg='wait for new coordinator resolved')

        self.set_baseline_wrapper(
            exclude={self.ignite.get_node_consistent_id(1), self.ignite.get_node_consistent_id(2)})

        util_sleep_for_a_while(self.rebalance_timeout)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(70880)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_restart_coordinator_in_baseline(self):
        caches_before = self.calc_checksums_on_client()

        current_server_nodes = self.ignite.get_nodes_num('server')

        log_print('Kill coordinator nodes')
        self.ignite.kill_node(1)

        log_print('Start ex-coordinator again')
        self.ignite.start_node(1)

        self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(70881)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_restart_one_node_in_baseline(self):
        caches_before = self.calc_checksums_on_client()

        current_server_nodes = self.ignite.get_nodes_num('server')

        log_print('Kill one nodes')
        self.ignite.kill_node(2)

        log_print('Start node again')
        self.ignite.start_node(2)

        self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(70885)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_remove_and_back_one_node_in_baseline(self):
        current_server_nodes = self.ignite.get_nodes_num('server')

        log_print('Kill one nodes')
        self.ignite.kill_node(2)

        log_print('Remove node from baseline')
        self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes - 1)

        self._set_baseline_few_times()

        self.load_data_with_streamer(1001, 1501)

        log_print('Start node again')
        self.ignite.start_node(2)

        self._set_baseline_few_times()

        self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(70886)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_remove_and_back_one_node_with_additional_in_baseline(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(run_for_seconds=30, delay=10)
                                      ):
                current_server_nodes = self.ignite.get_nodes_num('server')

                self.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config(), 2))

                self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes + 2)

                log_print('Kill one nodes: baseline and additional')
                self.ignite.kill_node(2)
                self.ignite.kill_node(self.ignite.get_alive_additional_nodes()[0])

                log_print('Remove node from baseline')
                self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes)

                # self._set_baseline_few_times()
                self.cu.control_utility('--baseline')
                self.cu.remove_node_from_baseline(self.ignite.get_node_consistent_id(2))

                self.load_data_with_streamer(1001, 1501)

                log_print('Start node again')
                self.ignite.start_node(2)

                self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes + 1)

                # self._set_baseline_few_times()
                self.cu.add_node_to_baseline(self.ignite.get_node_consistent_id(2))

                self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes + 1)

        print_red("AssertExceptions: %s" % str(self.ignite.find_exception_in_logs("java.lang.AssertionError")))

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(70882)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_restart_two_nodes_in_baseline(self):
        caches_before = self.calc_checksums_on_client()

        current_server_nodes = self.ignite.get_nodes_num('server')

        log_print('Kill two nodes')
        self.ignite.kill_node(2)
        self.ignite.kill_node(3)

        log_print('Start nodes again')
        self.ignite.start_node(2)
        self.ignite.start_node(3)

        self.ignite.wait_for_topology_snapshot(server_num=current_server_nodes)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60271)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_two_nodes(self):
        caches_before = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        new_server_num = len(self.ignite.get_alive_default_nodes()) - 2
        self.ignite.kill_node(2)
        self.ignite.kill_node(3)

        self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

        self.set_baseline_wrapper(
            exclude={self.ignite.get_node_consistent_id(2), self.ignite.get_node_consistent_id(3)})

        util_sleep_for_a_while(self.rebalance_timeout)

        caches_after = self.calc_checksums_on_client()

        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline')
    @test_case_id(60278)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_auto_activation(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache_names = piclient.get_ignite().cacheNames()

            async_operations = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_cross_cache_account_runner_operation,
                                                         cache_name, 1, 1000, 0.5, delay=1, run_for_seconds=20)
                async_operations.append(async_operation)
                async_operation.evaluate()

            # wait operations to complete
            for async_op in async_operations:
                print(async_op.getResult())

        alive_default_nodes = self.ignite.get_alive_default_nodes()
        for node_id in alive_default_nodes:
            self.ignite.kill_node(node_id)

        util_sleep_for_a_while(5)

        self.ignite.start_nodes()

        self.ignite.add_additional_nodes(self.get_server_config(), 1)
        self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

        print_red("AssertExceptions: %s" % str(self.ignite.find_exception_in_logs("java.lang.AssertionError")))

        self.cu.control_utility('--baseline')

        assert 'Cluster state: active' in self.cu.latest_utility_output, \
            'Cluster is inactive'

        util_sleep_for_a_while(self.rebalance_timeout)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading', 'smoke')
    @test_case_id(60274)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_one_node_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self):
                self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._set_baseline_few_times()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id(60286)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_adding_two_nodes_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self):
                self.ignite.add_additional_nodes(self.get_server_config(), 2)
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._set_baseline_few_times()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading', 'smoke')
    @test_case_id(60276)
    @require(min_ignite_version='2.5.1')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_one_node_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self):
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 1
                self.ignite.kill_node(2)
                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self._set_baseline_few_times()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading')
    @test_case_id(60277)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_removing_two_nodes_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self):
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 2
                self.ignite.kill_node(2)
                self.ignite.kill_node(3)

                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self._set_baseline_few_times()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('baseline', 'loading', 'smoke')
    @test_case_id(60275)
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_two_nodes_removed_one_added_with_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self):
                new_server_num = len(self.ignite.get_alive_default_nodes()) - 2
                self.ignite.kill_node(2)
                self.ignite.kill_node(3)

                self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

                self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

                self._set_baseline_few_times()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        util_sleep_for_a_while(self.rebalance_timeout)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline')
    @test_case_id(60272)
    @require(min_ignite_version='2.5.1')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_baseline_restore_full_one_additional_node_not_in_blt(self):
        self.ignite.wait_for_topology_snapshot(client_num=0)
        self.run_snapshot_utility('snapshot', '-type=full')

        self.load_data_with_streamer(1001, 2001)

        self.ignite.wait_for_topology_snapshot(client_num=0)

        caches_after = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.run_snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')

        self.run_snapshot_utility('move', '-id=%s -dest=%s -force' % (self.get_snapshot_id(2), self.snapshot_storage))

        self._change_grid_topology_and_set_baseline()

        new_nodes = self.ignite.add_additional_nodes(self.get_server_config(), 1)
        self.ignite.start_additional_nodes(new_nodes)

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), self.snapshot_storage))

        caches_finish = self.calc_checksums_on_client()

        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline')
    @test_case_id(70888)
    @require(min_ignite_version='2.5.1-p6')
    @with_setup(setup_testcase, teardown_testcase)
    def test_baseline_restart_node_add_one_additional_node(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self):
                self.ignite.kill_node(2)

                self.delete_lfs(node_ids=[2, ])

                self.ignite.start_node(2)

                util_sleep_for_a_while(5)

                new_nodes = self.ignite.add_additional_nodes(self.get_server_config(), 1)
                self.ignite.start_additional_nodes(new_nodes)

                # self.cu.kill_transactions()
                self._set_baseline_few_times(5)

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline', 'current')
    @test_case_id(60273)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_baseline_restore_wo_node_in_blt(self):
        caches_after = self.calc_checksums_on_client()

        self.run_snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')

        new_server_num = len(self.ignite.get_alive_default_nodes()) - 1
        self.ignite.kill_node(2)

        self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        caches_finish = self.calc_checksums_on_client()

        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline')
    @test_case_id(60280)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_baseline_restore_inc_one_additional_node_not_in_blt(self):
        self.ignite.wait_for_topology_snapshot(client_num=0)
        self.run_snapshot_utility('snapshot', '-type=full')

        self.load_data_with_streamer(1001, 2001)

        self.ignite.wait_for_topology_snapshot(client_num=0)

        caches_after = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.run_snapshot_utility('snapshot', '-type=inc')

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')

        # move previous FULL snapshot (INC snapshot will be forcefully moved)
        self.run_snapshot_utility('move', '-id=%s -dest=%s -force' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._change_grid_topology_and_set_baseline()

        new_nodes = self.ignite.add_additional_nodes(self.get_server_config(), 1)
        self.ignite.start_additional_nodes(new_nodes)

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), self.snapshot_storage))

        caches_finish = self.calc_checksums_on_client()

        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline', 'current')
    @test_case_id(60281)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_baseline_restore_inc_wo_node_in_blt(self):
        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.run_snapshot_utility('snapshot', '-type=full')

        self.load_data_with_streamer(1001, 2001)

        self.ignite.wait_for_topology_snapshot(client_num=0)

        caches_after = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.run_snapshot_utility('snapshot', '-type=inc')

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')

        new_server_num = len(self.ignite.get_alive_default_nodes()) - 1
        self.ignite.kill_node(2)

        self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), self.snapshot_storage))

        caches_finish = self.calc_checksums_on_client()

        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline')
    @test_case_id(60283)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_baseline_change_blt_add_one_node_restore(self):
        caches_after = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.run_snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')

        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._change_grid_topology_and_set_baseline()

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        caches_finish = self.calc_checksums_on_client()

        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

    @attr('snapshot', 'baseline', 'current')
    @test_case_id(60284)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_baseline_change_blt_remove_one_node_restore(self):
        caches_after = self.calc_checksums_on_client()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        self.run_snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')

        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._change_grid_topology_and_set_baseline()

        # self.restart_empty_grid()

        new_server_num = len(self.ignite.get_alive_default_nodes() + self.ignite.get_alive_additional_nodes()) - 1
        self.ignite.kill_node(2)

        self.ignite.wait_for_topology_snapshot(server_num=new_server_num)

        self._set_baseline_few_times()

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        caches_finish = self.calc_checksums_on_client()

        tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')

        self.util_verify(save_lfs_on_exception=True)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'),
                           "No AssertionError in logs"
                           )

