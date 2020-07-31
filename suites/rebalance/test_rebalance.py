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

from contextlib import ExitStack

from tiden.apps.zookeeper import ZkNodesRestart
from tiden import test_case_id
from tiden.assertions import *
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.loading import TransactionalLoading
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import create_distributed_checksum_operation, PiClientIgniteUtils
from tiden.tidenexception import TidenException
from tiden.util import is_enabled, log_print, attr, with_setup, known_issue, util_sleep
from suites.rebalance.abstract import AbstractRebalanceTest


class TestRebalance(AbstractRebalanceTest):

    def setup_fitness_testcase(self):
        super().setup_fitness_testcase()

    def setup_testcase(self, **kwargs):
        super().setup_testcase()

    def teardown_testcase(self):
        super().teardown_testcase()

    @attr('extra', 'loading')
    @with_setup(setup_testcase, teardown_testcase)
    def test_round_restart(self):
        with PiClient(self.ignite, self.get_client_config()):

            for node_id in range(1, 5):
                util_sleep(10)
                log_print(f"Restarting node {node_id}", color='blue')

                self.ignite.kill_node(node_id)

                self.load_data_with_streamer(end_key=1000,
                                             value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                             allow_overwrite=True)

                util_sleep(10)
                self.start_node(node_id)
                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('config')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201920)
    def test_different_rebalance_pool_size(self):
        current_server_num = self.ignite.get_alive_default_nodes()

        add_nodes = self.ignite.add_additional_nodes(config=self.get_server_config('diff_pool'))
        try:
            self.ignite.start_additional_nodes(add_nodes)
        except TidenException:
            pass

        # node should not be in cluster
        self.ignite.wait_for_topology_snapshot(server_num=len(current_server_num) + len(add_nodes))

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201935)
    def test_loading_blinking_node_clean_lfs(self):
        self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                  self.group_names)

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            # what the fix?
            self.wait_for_running_clients_num(piclient.nodes_num, 90)

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.cleanup_lfs(2)
                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201940)
    def test_loading_blinking_node_with_extra_node_in_blt(self):
        with PiClient(self.ignite, self.get_client_config()):
            self.ignite.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config()))

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)

                    util_sleep(10)

                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201939)
    def test_loading_blinking_extra_node_in_blt(self):
        with PiClient(self.ignite, self.get_client_config()):
            additional_node = self.ignite.add_additional_nodes(self.get_server_config())[0]
            self.ignite.start_additional_nodes(additional_node)

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(additional_node)

                    self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))

                    # self.ignite.start_additional_nodes(additional_node)
                    self.start_node(additional_node)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201941)
    def test_loading_blinking_two_nodes_blt_and_extra_node(self):
        with PiClient(self.ignite, self.get_client_config()):
            additional_node = self.ignite.add_additional_nodes(self.get_server_config())[0]
            self.ignite.start_additional_nodes(additional_node)

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.ignite.kill_node(additional_node)

                    # self.ignite.start_node(2)
                    # self.ignite.start_additional_nodes(additional_node)
                    self.start_node(2)
                    self.start_node(additional_node)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201934)
    def test_loading_blinking_node(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            self.wait_for_running_clients_num(piclient.nodes_num, 90)

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)

                    util_sleep(10)

                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201937)
    def test_loading_two_blinking_nodes_clean_lfs(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            self.wait_for_running_clients_num(piclient.nodes_num, 90)

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.cleanup_lfs(2)
                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.kill_node(3)

                    util_sleep(10)

                    # self.ignite.start_node(3)
                    self.start_node(3)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201936)
    def test_loading_two_blinking_nodes(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            self.wait_for_running_clients_num(piclient.nodes_num, 90)
            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.kill_node(3)
                    # self.ignite.start_node(3)
                    self.start_node(3)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('loading')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201938)
    def test_loading_blinking_node_baseline(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            self.wait_for_running_clients_num(piclient.nodes_num, 90)

            with ExitStack() as stack:
                stack.enter_context(TransactionalLoading(self,
                                                         cross_cache_batch=2,
                                                         skip_atomic=True,
                                                         post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))

                    self.cu.set_current_topology_as_baseline()

                    util_sleep(5)

                    self.start_node(2)
                    self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))
                    self.cu.set_current_topology_as_baseline()

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'common')
    # @known_issue('IGNITE-8879')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201921)
    def test_blinking_node_with_extra_node_in_blt(self):
        # IGNITE-8893
        with PiClient(self.ignite, self.get_client_config()):
            self.ignite.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config()))

            for iteration in range(0, self.iterations):
                log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                self.assert_nodes_alive()

                self.ignite.kill_node(2)

                util_sleep(10)

                self.ignite.start_node(2)

                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)
                util_sleep(60)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'common')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201922)
    def test_blinking_extra_node_in_blt(self):
        with PiClient(self.ignite, self.get_client_config()):
            additional_node = self.ignite.add_additional_nodes(self.get_server_config())[0]
            self.ignite.start_additional_nodes(additional_node)

            for iteration in range(0, self.iterations):
                log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                self.assert_nodes_alive()

                self.ignite.kill_node(additional_node)
                self.start_node(additional_node)

                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'common')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201924)
    def test_blinking_two_nodes_blt_and_extra_node(self):
        with PiClient(self.ignite, self.get_client_config()):
            additional_node = self.ignite.add_additional_nodes(self.get_server_config())[0]
            self.ignite.start_additional_nodes(additional_node)

            with ExitStack() as stack:
                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.ignite.kill_node(additional_node)

                    # self.ignite.start_node(2)
                    self.start_node(2)
                    self.ignite.start_additional_nodes(additional_node)
                    # self.start_node(additional_node)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('extra', 'common')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201927)
    def test_blinking_two_nodes_blt_and_extra_node_clean_lfs(self):
        with PiClient(self.ignite, self.get_client_config()):
            additional_node = self.ignite.add_additional_nodes(self.get_server_config())[0]
            self.ignite.start_additional_nodes(additional_node)

            with ExitStack() as stack:
                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.cleanup_lfs(2)
                    self.ignite.kill_node(additional_node)

                    # self.ignite.start_node(2)
                    self.start_node(2)
                    # self.ignite.start_additional_nodes(additional_node)
                    self.ignite.start_additional_nodes(additional_node)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('common')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201929)
    def test_blinking_nodes_clean_lfs(self):
        with PiClient(self.ignite, self.get_client_config()):

            with ExitStack() as stack:
                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.cleanup_lfs(2)
                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('common')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201928)
    def test_blinking_node(self):
        with PiClient(self.ignite, self.get_client_config()):
            for iteration in range(0, self.iterations):
                log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                self.assert_nodes_alive()

                self.ignite.kill_node(2)

                util_sleep(10)

                self.ignite.start_node(2)

                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('common')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201931)
    def test_two_blinking_nodes_clean_lfs(self):
        with PiClient(self.ignite, self.get_client_config()):

            with ExitStack() as stack:
                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    self.ignite.kill_node(2)
                    self.cleanup_lfs(2)
                    # self.ignite.start_node(2)
                    self.start_node(2)

                    self.ignite.kill_node(3)

                    util_sleep(10)

                    # self.ignite.start_node(3)
                    self.start_node(3)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('common', 'smoke')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201930)
    def test_two_blinking_nodes(self):
        with PiClient(self.ignite, self.get_client_config()):
            for iteration in range(0, self.iterations):
                log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                self.assert_nodes_alive()

                self.ignite.kill_node(2)
                self.ignite.start_node(2)

                self.ignite.kill_node(3)
                self.ignite.start_node(3)

                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('common', 'smoke')
    # @known_issue('IGNITE-8879')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201932)
    def test_blinking_node_baseline(self):
        # IGNITE-8879
        with PiClient(self.ignite, self.get_client_config()):
            for iteration in range(0, self.iterations):
                log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                self.assert_nodes_alive()

                self.ignite.kill_node(2)
                self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))

                self.cu.set_current_topology_as_baseline()

                util_sleep(5)

                self.ignite.start_node(2)
                self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))
                self.cu.set_current_topology_as_baseline()

                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('regress')
    @with_setup(setup_testcase, teardown_testcase)
    @test_case_id(201933)
    def test_blinking_clients_clean_lfs(self):
        """
        IGN-9159 (IGNITE-7165)

        Re-balancing is canceled if client node joins.
        Re-balancing can take hours and each time when client node joins it starts again
        :return:
        """
        self.wait_for_running_clients_num(client_num=0, timeout=120)

        for iteration in range(0, self.iterations):
            log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

            self.assert_nodes_alive()

            self.ignite.kill_node(2)
            self.cleanup_lfs(2)
            self.start_node(2)

            # restart clients 4 times
            for restart_time in range(0, 4):
                with PiClient(self.ignite, self.get_client_config()):
                    pass

            self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                      self.group_names)
            tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('regress', '24hourfitness')
    # @known_issue('IGNITE-11400')
    @with_setup(setup_fitness_testcase, teardown_testcase)
    @test_case_id(201919)
    def test_rebalancing_with_ttl_caches(self):
        """
        IGN-13549 (IGNITE-11400)

        Rebalancing caches with TTL enabled can cause data corruption.
        :return:
        """

        with PiClient(self.ignite, self.get_client_config()):
            checksums = create_distributed_checksum_operation().evaluate()

        self.wait_for_running_clients_num(client_num=0, timeout=120)
        self.ignite.cu.control_utility('--cache idle_verify --dump --skip-zeros')
        log_print('Calculating checksums done: %s' % checksums)

        self.assert_nodes_alive()

        self.ignite.kill_node(2)

        PiClientIgniteUtils.load_data_with_putall(
            self.ignite, self.get_client_config(),
            start_key=self.preloading_size, end_key=self.preloading_size + 100000)

        util_sleep(5 * 60)

        self.ignite.start_node(2)

        self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout * 2,
                                                  self.group_names)

        with PiClient(self.ignite, self.get_client_config()):  # , jvm_options=jvm_options):
            checksums = create_distributed_checksum_operation().evaluate()

        self.wait_for_running_clients_num(client_num=0, timeout=120)
        log_print('Calculating checksums done: %s' % checksums)
        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    @attr('loading')
    @with_setup(setup_testcase, teardown_testcase)
    def test_indexes_rebuilded(self):
        """
        https://ggsystems.atlassian.net/browse/GG-17428

        1. Start cluster.
        2. Start transactional loading.
        3. Stop one node and remove index.bin files for the caches.
        4. Start node and let it finish rebalance.
        5. Check indexes are not broken after rebalance.
        :return:
        """
        self.need_delete_lfs_on_teardown = True
        debug = False

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            self.wait_for_running_clients_num(piclient.nodes_num, 90)

            with ExitStack() as stack:
                # todo unreachable code
                if False:
                    stack.enter_context(TransactionalLoading(self,
                                                             cross_cache_batch=2,
                                                             skip_atomic=True,
                                                             post_checksum_action=self.idle_verify_action))

                if is_enabled(self.config.get('zookeeper_enabled')) and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 2))

                for iteration in range(0, self.iterations):
                    log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

                    self.assert_nodes_alive()

                    with TransactionalLoading(self,
                                              cross_cache_batch=2,
                                              skip_atomic=True):

                        util_sleep(20)
                        self.ignite.kill_node(2)

                    if debug:
                        self.cu.control_utility('--cache idle_verify --dump --skip-zeros')

                    self.remove_index_bin_files(2)
                    util_sleep(10)

                    if debug:
                        self.cu.control_utility('--cache idle_verify --dump --skip-zeros')

                    self.start_node(2)

                    self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                              self.group_names)
                    util_sleep(30)
                    log_print("Check indexes")
                    try:
                        if debug:
                            self.cu.control_utility('--cache idle_verify --dump --skip-zeros')
                        self.idle_verify_action(None)
                    except TidenException:
                        if debug:
                            self.cu.control_utility('--cache idle_verify --dump --skip-zeros')
                        raise TidenException('validate_index failed')

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.AssertionError'), "# of AssertionError")

    def idle_verify_action(self, sum_after):
        idle_verify_pass = ['idle_verify check has finished, no conflicts have been found.']
        self.cu.control_utility('--cache', 'idle_verify', all_required=idle_verify_pass)
        self.verify_no_assertion_errors()

        validate_indexes_pass = ['no issues found.']
        try:
            self.cu.control_utility('--cache', 'validate_indexes', all_required=validate_indexes_pass)
        except TidenException as e:
            log_print('Got exception during index validation: {}'.format(e))
            if 'Cluster not idle' in self.cu.latest_utility_output:
                util_sleep(5)
                self.cu.control_utility('--cache', 'validate_indexes', all_required=validate_indexes_pass)
            else:
                raise e

