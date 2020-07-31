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
from tiden_gridgain.piclient.piclient import PiClient
from suites.snapshots.test_baseline import TestBaseline


class TestAutoBaseline(TestBaseline):

    def setup(self):
        super().setup()

    def __dir__(self):
        # override method for exlude parent tests
        attr_filter = lambda s: any(
            [pattern for pattern in ['test_baseline', 'test_snapshot', 'test_blink', 'test_switch'] if
             s.startswith(pattern)])
        new_attr_list = [attr for attr in super(TestAutoBaseline, self).__dir__() if not attr_filter(attr)]
        return new_attr_list

    def setup_testcase(self):
        self.config['blt_auto_adjust_enabled'] = True
        super().setup_testcase()
        log_print('Enable auto_adjust blt feature')
        self.cu.control_utility('--baseline auto_adjust enable timeout {} --yes'.format(self.blt_auto_adjust_timeout))

    def teardown_testcase(self):
        super().teardown_testcase()

    def setup_in_memory_cluster(self):
        self.create_context('in-memory', persistence_enabled=False, set_context=True)
        self.setup_testcase()

    @attr('auto-blt')
    @test_case_id(167644)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_switch_consistent_id(self):
        super().test_switch_consistent_id()

    @attr('auto-blt')
    @test_case_id(167631)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_adding_one_node_remove_two(self):
        super().test_baseline_adding_one_node_remove_two()

    @attr('auto-blt')
    @test_case_id(167629)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_adding_one_node(self):
        super().test_baseline_adding_one_node()

    @attr('auto-blt')
    @test_case_id(167630)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_adding_two_nodes(self):
        super().test_baseline_adding_two_nodes()

    @attr('auto-blt')
    @test_case_id(167632)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_removing_one_node(self):
        super().test_baseline_removing_one_node()

    @attr('auto-blt')
    @test_case_id(167638)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_removing_one_coordinator(self):
        super().test_baseline_removing_one_coordinator()

    @attr('auto-blt')
    @test_case_id(167639)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_removing_one_coordinator_and_second_coordinator(self):
        super().test_baseline_removing_one_coordinator_and_second_coordinator()

    @attr('auto-blt')
    @test_case_id(167635)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_restart_coordinator_in_baseline(self):
        super().test_baseline_restart_coordinator_in_baseline()

    @attr('auto-blt')
    @test_case_id(167636)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_restart_one_node_in_baseline(self):
        super().test_baseline_restart_one_node_in_baseline()

    @attr('auto-blt')
    @test_case_id(167637)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_restart_two_nodes_in_baseline(self):
        super().test_baseline_restart_two_nodes_in_baseline()

    @attr('auto-blt')
    @test_case_id(167633)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_removing_two_nodes(self):
        super().test_baseline_removing_two_nodes()

    @attr('auto-blt')
    @test_case_id(167645)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_baseline_auto_activation(self):
        super().test_baseline_auto_activation()

    @attr('auto-blt')
    @test_case_id(167642)
    @with_setup(setup_testcase, teardown_testcase)
    def test_autoadjust_blink_client_in_the_end_of_soft_adjust_timeout(self):
        """
        Check soft adjust timeout shift if topology change with server nodes. Client nodes do not change this timeout.
        Test:
        1. Start cluster.
        2. Start additional node, check it's out of baseline.
        3. Wait for some time, and start one more additional node.
        4. Check first node is not in blt cause soft timeout changed by second node come in.
        5. Kill second node and check first is still not in blt.
        6. Wait for soft timeout and check first node in blt.
        """
        self.ignite.snapshot_timeout = 460
        other_nodes = []

        caches_before = self.calc_checksums_on_client()
        self.ignite.wait_for_topology_snapshot(client_num=0)

        def _assert_nodes_in_baseline(expected_blt_nodes, expected_other_nodes):
            actual_blt_nodes, actual_non_blt_nodes = self.get_baseline_nodes()
            tiden_assert_equal(sorted(actual_blt_nodes), sorted(expected_blt_nodes), 'Assert expected BLT nodes')
            tiden_assert_equal(sorted(actual_non_blt_nodes), sorted(expected_other_nodes),
                               'Assert expected non-BLT nodes')

        blt_nodes, non_blt_node = self.get_baseline_nodes()

        tmp_node_id = self.ignite.add_additional_nodes(self.get_server_config(), 1)
        other_nodes.append(self.ignite.get_node_consistent_id(tmp_node_id[-1]))
        self.ignite.start_additional_nodes(tmp_node_id)

        _assert_nodes_in_baseline(blt_nodes, other_nodes)

        util_sleep_for_a_while(int(self.blt_auto_adjust_timeout) // 1000 - 30)

        self.cu.control_utility('--baseline')
        test_node_id = self.ignite.add_additional_nodes(self.get_server_config(), 1)
        other_nodes.append(self.ignite.get_node_consistent_id(test_node_id[-1]))
        self.ignite.start_additional_nodes(test_node_id)

        _assert_nodes_in_baseline(blt_nodes, other_nodes)

        util_sleep_for_a_while(10)
        self.cu.control_utility('--baseline')

        self.ignite.kill_node(test_node_id[-1])
        other_nodes.remove(self.ignite.get_node_consistent_id(test_node_id[-1]))

        _assert_nodes_in_baseline(blt_nodes, other_nodes)

        util_sleep_for_a_while(int(self.blt_auto_adjust_timeout) // 1000)
        _assert_nodes_in_baseline(blt_nodes + other_nodes, [])

        caches_after = self.calc_checksums_on_client()
        util_sleep_for_a_while(self.rebalance_timeout)
        tiden_assert_equal(caches_before, caches_after, 'Caches checksum')
        self.util_verify(save_lfs_on_exception=True)

    @attr('distributed_metastore')
    # @known_issue('GG-20830')
    @test_case_id(1111)
    @with_setup(setup_in_memory_cluster, teardown_testcase)
    def test_distributed_metastorage_in_memory_cluster(self):
        """
        Check auto-baseline for in-memory cluster.
        1. Started nodes added into baselien.
        2. Stopped nodes removed from cluster.
        3. The same could be done if auto-baseline disabled.
        """

        self.cu.control_utility('--baseline')
        self.cu.control_utility('--baseline auto_adjust enable timeout {} --yes'.format(self.blt_auto_adjust_timeout))

        all_nodes = [self.ignite.get_node_consistent_id(node_id) for node_id in self.ignite.get_alive_default_nodes()]
        for i in range(1, 3):
            node_id = self.ignite.add_additional_nodes(self.get_server_config(), 1)
            all_nodes.append(self.ignite.get_node_consistent_id(node_id[-1]))
            self.ignite.start_additional_nodes(node_id)

        for i in range(1, 6):
            self.cu.control_utility('--baseline')
            util_sleep_for_a_while(20)

        actual_blt_node, actual_non_blt_node = self.get_baseline_nodes()
        tiden_assert_equal(sorted(all_nodes), sorted(actual_blt_node), 'Expecting all nodes in BLT')

        self.cu.control_utility('--baseline')

        for node_id in range(1, 3):
            self.ignite.kill_node(node_id)
            all_nodes.remove(self.ignite.get_node_consistent_id(node_id))

        for i in range(1, 6):
            self.cu.control_utility('--baseline')
            util_sleep_for_a_while(20)

        actual_blt_node, actual_non_blt_node = self.get_baseline_nodes()
        tiden_assert_equal(sorted(all_nodes), sorted(actual_blt_node), 'Expecting not all nodes in BLT')

        self.cu.disable_baseline_autoajustment()
        self.cu.control_utility('--baseline')

        self.ignite.start_node(1)
        self.cu.control_utility('--baseline')

        self.cu.set_current_topology_as_baseline()

    @attr('distributed_metastore')
    @test_case_id(1111)
    @with_setup(setup_testcase, teardown_testcase)
    def test_distributed_metastorage_tcp_to_zk_discovery(self):
        """
        Check getting correct values from distributed metastorage after restart with another discovery (TCP -> ZK).
        """

        def check_utility_output(expecting_status):
            status, soft_timeout = self.cu.get_auto_baseline_params()
            tiden_assert_equal(status, expecting_status, 'Auto baseline status is enabled')
            tiden_assert_equal(soft_timeout, self.blt_auto_adjust_timeout, 'Soft timeout is correct')

        self.cu.control_utility('--baseline')
        self.cu.control_utility('--baseline auto_adjust enable timeout {} --yes'.format(self.blt_auto_adjust_timeout))
        check_utility_output('enabled')

        self.cu.disable_baseline_autoajustment()
        self.cu.control_utility('--baseline')
        check_utility_output('disabled')

        self.ignite.start_node(1)
        self.cu.control_utility('--baseline')

        self.zoo.deploy_zookeeper()
        self.start_zookeeper()

        self.create_context('zk_discovery', zookeeper_enabled=True, zoo_connection=self.zoo._get_zkConnectionString(),
                            set_context=True)

        self.restart_grid(with_activation=False)
        self.cu.control_utility('--baseline')
        check_utility_output('disabled')

        self.cu.enable_baseline_autoajustment()
        self.cu.control_utility('--baseline')
        check_utility_output('enabled')

        additional_node = self.ignite.add_additional_nodes(self.get_server_config(), 1)
        all_nodes = [self.ignite.get_node_consistent_id(node_id) for node_id in self.ignite.get_alive_default_nodes()]
        all_nodes.append(self.ignite.get_node_consistent_id(additional_node[-1]))
        all_nodes.remove(self.ignite.get_node_consistent_id(1))

        self.ignite.start_additional_nodes(additional_node)
        self.ignite.kill_node(1)

        for i in range(1, 6):
            self.cu.control_utility('--baseline')
            util_sleep_for_a_while(20)

        actual_blt_node, non_blt_node = self.get_baseline_nodes()
        tiden_assert_equal(sorted(all_nodes), sorted(actual_blt_node), 'Expecting all nodes in BLT')

