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

from random import randint

from tiden import known_issue, require, with_setup, test_case_id, attr, util_sleep, log_print
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, \
    create_put_with_optional_remove_operation, TxDescriptor
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from suites.rebalance.abstract import AbstractRebalanceTest


class TestBackups(AbstractRebalanceTest):

    transactions_timeout = 60
    transaction_check_sleep = 5

    def setup_backup_testcase(self, **kwargs):
        self.preloading_size = 0
        self.preloading_with_streamer = False
        self.prepare_backup_test_config(kwargs)
        self._setup_with_context('backup', **kwargs)

    def teardown_testcase(self):
        super().teardown_testcase()

    # @known_issue('IGNITE-10078')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='historic', atomic_enabled=False)
    @test_case_id(201914)
    @attr('ignite-10078')
    def test_blink_backups_under_load_no_remove_historical_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.0)

    # @known_issue('IGNITE-10078')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='historic', atomic_enabled=False)
    @test_case_id(201918)
    @attr('ignite-10078')
    def test_blink_backups_under_load_more_delete_before_create(self):
        self.do_blink_backups_under_load(initial_remove_probability=1.0)

    # @known_issue('IGNITE-10078')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='full', atomic_enabled=False)
    @test_case_id(201916)
    @attr('ignite-10078')
    def test_blink_backups_under_load_no_remove_full_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.0)

    # @known_issue('IGNITE-10078')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='historic', atomic_enabled=False)
    @test_case_id(201915)
    @attr('ignite-10078')
    def test_blink_backups_under_load_with_remove_historical_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    # @known_issue('IGNITE-10078')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='full', atomic_enabled=False)
    @test_case_id(201917)
    @attr('ignite-10078')
    def test_blink_backups_under_load_with_remove_full_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    @known_issue('GG-27964')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='historic', atomic_enabled=True)
    @test_case_id(201917)
    @attr('ignite-11797', 'more-ignite-10078')
    def test_blink_backups_under_load_with_remove_mixed_cache_group_hist_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    # @known_issue('IGNITE-11797')
    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='full', atomic_enabled=True)
    @test_case_id(201917)
    @attr('ignite-11797', 'more-ignite-10078')
    def test_blink_backups_under_load_with_remove_mixed_cache_group_full_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='historic', onheap_caches_enabled=True)
    @test_case_id(201917)
    @attr('more-ignite-10078')
    def test_blink_backups_under_load_with_remove_onheap_caches_hist_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='full', onheap_caches_enabled=True)
    @test_case_id(201917)
    @attr('more-ignite-10078')
    def test_blink_backups_under_load_with_remove_onheap_caches_full_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='historic', evictions_enabled=True,
                iteration_size=120000)
    @test_case_id(201917)
    @attr('more-ignite-10078')
    def test_blink_backups_under_load_with_remove_and_evict_policy_hist_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    @require(min_ignite_version='2.5.5', min_server_nodes=3, min_client_nodes=3)
    @with_setup(setup_backup_testcase, teardown_testcase, rebalance_type='full', evictions_enabled=True,
                iteration_size=120000)
    @test_case_id(201917)
    @attr('more-ignite-10078')
    def test_blink_backups_under_load_with_remove_and_evict_policy_full_rebalance(self):
        self.do_blink_backups_under_load(initial_remove_probability=0.1)

    def wait_transactions_finish(self):
        log_print("Waiting current transactions to finish for up to {} seconds".format(self.transactions_timeout))
        for tries in range(0, int(self.transactions_timeout / self.transaction_check_sleep)):
            self.ignite.cu.control_utility('--tx')
            if 'Nothing found' in self.ignite.cu.latest_utility_output:
                break
            util_sleep(self.transaction_check_sleep)

    def do_blink_backups_under_load(self, initial_remove_probability):

        iteration_size = self.config.get('iteration_size', 80000)
        start = 0
        keep_coordinator_busy = True
        start_value = 0

        # temporary save LFS even on test pass
        self.need_delete_lfs_on_teardown = False

        first_node = self.ignite.get_node_consistent_id(1)
        second_node = self.ignite.get_node_consistent_id(2)

        if keep_coordinator_busy:
            other_nodes = list(set(self.ignite.get_all_default_nodes()) - set([1]))
        else:
            other_nodes = list(set(self.ignite.get_all_default_nodes()) - set([1, 2]))

        current_server_num = self.ignite.get_nodes_num('server')

        tx_caches = []
        atomic_caches = []

        self.ignite.set_snapshot_timeout(600)

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            gateway = piclient.get_gateway()
            ignite = piclient.get_ignite()

            cache_names = ignite.cacheNames().toArray()
            for cache_name in cache_names:
                # run cross cache transfer task only for transactional caches
                if ignite.getOrCreateCache(cache_name).getConfiguration(
                        gateway.jvm.org.apache.ignite.configuration.CacheConfiguration().getClass()
                ).getAtomicityMode().toString() == 'TRANSACTIONAL':
                    tx_caches.append(cache_name)
                else:
                    atomic_caches.append(cache_name)

        PiClientIgniteUtils.wait_for_running_clients_num(self.ignite, 0, 120)

        for iteration in range(0, self.iterations):
            log_print("Iteration {}/{}".format(str(iteration + 1), str(self.iterations)), color='blue')

            start_key = start + iteration * iteration_size
            end_key = start_key + iteration_size
            if initial_remove_probability > 0.0:
                remove_probability = initial_remove_probability + iteration / self.iterations / 2.0
            else:
                remove_probability = 0.0

            current_client_num = self.ignite.get_nodes_num('client')

            for i in range(0, 3):
                with PiClient(self.ignite, self.get_client_config()) as piclient:
                    log_print(
                        "Loading (remove {probability}%) {load} values per cache into {n_caches} caches".format(
                            probability=remove_probability,
                            load=iteration_size,
                            n_caches=len(tx_caches),
                        ))

                    async_operations = []
                    for cache_name in tx_caches:
                        node_id = piclient.get_node_id()
                        gateway = piclient.get_gateway(node_id)
                        tx_size = randint(1, 10)
                        log_print(
                            "Client {node_id} -> {cache_name}, tx size {tx_size}".format(
                                node_id=node_id,
                                cache_name=cache_name,
                                tx_size=tx_size,
                                removeProbability=remove_probability,
                            ))
                        async_operation = create_async_operation(
                            create_put_with_optional_remove_operation,
                            cache_name, start_key, end_key, remove_probability,
                            gateway=gateway,
                            node_consistent_id=
                            first_node if keep_coordinator_busy
                            else second_node,
                            tx_description=TxDescriptor(concurrency='PESSIMISTIC',
                                                        isolation='REPEATABLE_READ',
                                                        size=tx_size),
                            use_monotonic_value=True,
                            monotonic_value_seed=start_value,
                        )
                        start_value = start_value + iteration_size

                        async_operations.append(async_operation)
                        async_operation.evaluate()

                    # little warm up
                    util_sleep(5)

                    node_id = self.ignite.get_random_server_nodes(1, node_ids=other_nodes)[0]
                    self.ignite.kill_node(node_id)
                    self.ignite.wait_for_topology_snapshot(server_num=current_server_num - 1)

                    # continue load data during node offline
                    util_sleep(15)
                    self.ignite.start_node(node_id)
                    self.ignite.wait_for_topology_snapshot(server_num=current_server_num)

                PiClientIgniteUtils.wait_for_running_clients_num(self.ignite, current_client_num, 120)

                self.wait_transactions_finish()

                self.ignite.jmx.wait_for_finish_rebalance(self.rebalance_timeout,
                                                          self.group_names)

                self.idle_verify_check_conflicts_action()

            self.idle_verify_dump_action()

