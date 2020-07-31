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

from pprint import PrettyPrinter

from tiden_gridgain.piclient.piclient import PiClientException
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_streamer_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden.util import with_setup, print_blue, require, util_sleep_for_a_while, is_enabled, log_print, print_red, attr
from tiden.configuration_decorator import test_configuration

from random import random
from collections import deque
from tiden_gridgain.utilities.jmx_utility import JmxUtility
from tiden_gridgain.utilities.snapshot_utility import SnapshotUtility
from tiden.zookeeper import Zookeeper
from suites.pme.pme_utils import PmeUtils

from suites.sow.test_lrt import TestLrt

@test_configuration([
    'zookeeper_enabled'
])
class TestPmeRegress(PmeUtils):

    MAX_CLIENT_NODES = 45
    MAX_SERVER_NODES = 80

    def setup(self):
        self.zoo = Zookeeper(self.config, self.ssh)

        zookeeper_enabled = is_enabled(self.config.get('zookeeper_enabled'))
        default_context = self.contexts['default']
        default_context.add_context_variables(
            caches_file="caches.xml",
            zookeeper_enabled=zookeeper_enabled,
        )

        if zookeeper_enabled:
            self.zoo.deploy_zookeeper()
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString(),
            )

        super().setup()

    setup_test = PmeUtils.setup_test
    teardown_test = PmeUtils.teardown_test
    teardown_test_hard = PmeUtils.teardown_test_hard

    @require(min_ignite_version='2.5.1-p6')
    @require(min_client_nodes=10, min_server_nodes=2)
    @with_setup(setup_test, teardown_test, exchange_history_size=2)
    def test_ignite_8657(self):
        """
        This test is based on IGNITE-8657:
        1. start grid with EXCHANGE_HISTORY_SIZE smaller than N
        2. activate
        3. start simultaneously M > N clients
        4. all client nodes should and be able to perform cache put/get operations and transactions

        NB: this test hangs with 2.5.1-p6, due to piclient wait Ignition.start() forever
        """
        self.start_grid()
        self.load_random_data_with_streamer(0, 1000, nodes_num=2)
        self.cu.set_current_topology_as_baseline()

        nodes_before = set(self.ignite.get_all_common_nodes())

        with PiClient(self.ignite,
                      self.get_client_config(),
                      nodes_num=10,
                      jvm_options=self.jvm_options,
                      read_timeout=300) as piclient:
            nodes_after = set(self.ignite.get_all_common_nodes())
            nodes_started = list(nodes_after - nodes_before)
            node_ids = deque(nodes_started)
            node_id = node_ids[0]
            node_ids.rotate()

            for i in range(1, 5):
                gateway = piclient.get_gateway(node_id)
                ignite = piclient.get_ignite(node_id)
                tx = ignite.transactions().txStart()
                util_sleep_for_a_while(3)
                tx.commit()
                for concurrency in ['OPTIMISTIC', 'PESSIMISTIC']:
                    for isolation in ['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE']:
                        print_blue('Run transaction %s %s' % (concurrency, isolation))

                        node_id = node_ids[0]
                        node_ids.rotate()

                        gateway = piclient.get_gateway(node_id)
                        ignite = piclient.get_ignite(node_id)

                        concurrency_isolation_map = self._get_tx_type_map(gateway)

                        cache_names = ignite.cacheNames().toArray()

                        tx = ignite.transactions().txStart(concurrency_isolation_map.get(concurrency),
                                                           concurrency_isolation_map.get(isolation))

                        for cache_name in cache_names:
                            cache = ignite.getCache(cache_name)
                            val = cache.get(int(random()*1000))
                            # log_print('got %s' % repr(val))
                            if val:
                                cache.put(int(random()*1000), val)

                        tx.commit()
                node_id = node_ids[0]
                node_ids.rotate()

                ignite = piclient.get_ignite(node_id)
                async_ops = []
                for cache_name in ignite.cacheNames().toArray():
                    _async = create_async_operation(create_streamer_operation, cache_name, 1002, 2000)
                    _async.evaluate()
                    async_ops.append(_async)

                for async_op in async_ops:
                    async_op.getResult()

    @require(min_ignite_version='2.5.1-p13')
    @require(min_client_nodes=MAX_CLIENT_NODES, min_server_nodes=2)
    @with_setup(setup_test, teardown_test, exchange_history_size=2)
    def test_ignite_8855(self):
        """
         This test is based on IGNITE-8855:
         1. start grid with EXCHANGE_HISTORY_SIZE smaller than N
         2. activate
         3. start simultaneously M > N clients
         4. there should be throttling on client reconnects, e.g.
         client out of exchange should not try to reconnect all at once.
         NB: this test fails on 8.5.1-p13
        """
        nodes_num = TestPmeRegress.MAX_CLIENT_NODES
        self.start_grid()
        self.load_random_data_with_streamer(0, 1000, nodes_num=2)
        self.cu.set_current_topology_as_baseline()

        pp = PrettyPrinter()

        try:
            with PiClient(self.ignite,
                          self.get_client_config(),
                          nodes_num=nodes_num,
                          jvm_options=self.jvm_options,
                          read_timeout=300):    # that's enough to start if bug was fixed
                self.ignite.wait_for_topology_snapshot(None, nodes_num)
                n_tries = 0
                res = None
                while n_tries < 3:
                    util_sleep_for_a_while(5)
                    self.ignite.get_data_from_log(
                        'server',
                        'Client node tries to connect but its exchange info is cleaned up from exchange history',
                        '(.*)',
                        'log_kickoff'
                    )
                    res = self._collect_msg('log_kickoff', 'server')
                    log_print('Client kick off messages: \n' + pp.pformat(res))
                    if not res:
                        n_tries = n_tries + 1
                        continue

                    assert res, "There should be client kick off messages"

                    util_sleep_for_a_while(5)

                    self.ignite.get_data_from_log(
                        'client',
                        'Client node reconnected',
                        '(.*)',
                        'reconnect'  # and now Jinn would appear!
                    )
                    res = self._collect_msg('reconnect')
                    if not res:
                        n_tries = n_tries + 1
                        continue

                    log_print('Reconnect attempts: \n' + pp.pformat(res))

                    self.ignite.wait_for_topology_snapshot(None, nodes_num, "Ensure topology is stable")
                    break

        except PiClientException as e:
            log_print("Got client exception: %s" % str(e))
            assert False, "IGNITE-8855 reproduced"

    @require(min_ignite_version='2.5.1-p13')
    @require(min_server_nodes=MAX_SERVER_NODES)
    @with_setup(setup_test, teardown_test_hard)
    @attr('time')
    def test_ignite_9398_activate(self):
        """
        https://ggsystems.atlassian.net/browse/IGN-11435
        https://issues.apache.org/jira/browse/IGNITE-9398

        Fixed in: 8.5.1-p14

        This ticket optimizes dispatching of Custom Discovery Messages by offloading their processsing
        to separate thread. If the fix is ok, then all nodes must mention custom discovery messages faster

        :return:
        """
        self.start_grid_no_activate()

        util_sleep_for_a_while(3)
        jmx = JmxUtility(self.ignite)
        jmx.activate(1)

        max_time = self._get_last_exchange_time()

        print_blue(
            "Max time diff between 'Started exchange init' and 'Finish exchange future' at all nodes: %s msec" %
            max_time
        )

        self._dump_exchange_time(max_time, "cluster activate")

        jmx.deactivate(1)
        jmx.kill_utility()

    @require(min_ignite_version='2.5.1-p13')
    @require(min_server_nodes=MAX_SERVER_NODES)
    @with_setup(setup_test, teardown_test_hard)
    @attr('time')
    def test_ignite_9398_deactivate(self):
        """
        https://ggsystems.atlassian.net/browse/IGN-11435
        https://issues.apache.org/jira/browse/IGNITE-9398

        Fixed in: 8.5.1-p14

        This ticket optimizes dispatching of Custom Discovery Messages by offloading their processsing
        to separate thread. If the fix is ok, then all nodes must mention custom discovery messages faster

        :return:
        """
        self.start_grid()

        util_sleep_for_a_while(3)
        jmx = JmxUtility(self.ignite)
        jmx.deactivate(1)

        max_time = self._get_last_exchange_time()

        print_blue(
            "Max time diff between 'Started exchange init' and 'Finish exchange future' at all nodes: %s msec" %
            max_time
        )

        self._dump_exchange_time(max_time, "cluster deactivate")

        jmx.kill_utility()

    @require(min_ignite_version='2.5.1-p13')
    @require(min_server_nodes=MAX_SERVER_NODES)
    @with_setup(setup_test, teardown_test_hard)
    @attr('time')
    def test_ignite_9398_snapshot(self):
        """
        https://ggsystems.atlassian.net/browse/IGN-11435
        https://issues.apache.org/jira/browse/IGNITE-9398

        Fixed in: 8.5.1-p14

        This ticket optimizes dispatching of Custom Discovery Messages by offloading their processsing
        to separate thread. If the fix is ok, then all nodes must mention custom discovery messages faster

        :return:
        """
        self.start_grid()

        su = SnapshotUtility(self.ignite)

        su.snapshot_utility('SNAPSHOT', '-type=FULL')

        # max_time = self._get_last_exchange_time("message='snapshot")

        max_time = self._get_last_exchange_time()

        print_blue(
            "Max time diff between 'Started exchange init' and 'Finish exchange future' at all nodes: %s msec" %
            max_time
        )

        self._dump_exchange_time(max_time, "snapshot created")

    @with_setup(setup_test, teardown_test)
    def test_ignite_10128(self):
        """
        https://ggsystems.atlassian.net/browse/IGN-12187
        https://issues.apache.org/jira/browse/IGNITE-10128

        IO race during read\write cache configurations.
        :return:
        """
        self.start_grid_no_activate()
        max_iterations = 100
        for i in range(0, max_iterations):
            self.cu.activate()
            util_sleep_for_a_while(3)
            self.cu.deactivate()
            util_sleep_for_a_while(3)

    @require(min_ignite_version='2.5.1-p6')
    @with_setup(setup_test, teardown_test)
    def test_ignite_8897(self):
        """
        1. create LRT
        2. start PME
        3. try to add new node to baseline
        :return:
        """

        # start grid, remove one random node from baseline
        self.start_grid()
        self.ignite.cu.set_current_topology_as_baseline()

        grid_size = len(self.ignite.get_all_alive_nodes())
        stopping_node_id = self.ignite.get_random_server_nodes()[0]
        stopping_node_consistent_id = self.ignite.get_node_consistent_id(stopping_node_id)

        self.kill_node(stopping_node_id)
        self.ignite.wait_for_topology_snapshot(server_num=grid_size - 1, client_num=None)
        self.ignite.cu.remove_node_from_baseline(stopping_node_consistent_id)
        util_sleep_for_a_while(5)

        # create a long running transaction
        with PiClient(self.ignite,
                      self.get_client_config(),
                      nodes_num=1,
                      jvm_options=self.jvm_options):

            self.ignite.wait_for_topology_snapshot(None, 1)

            TestLrt.create_transactional_cache()
            lrt_operations = TestLrt.launch_transaction_operations()

            self.release_transactions(lrt_operations)

