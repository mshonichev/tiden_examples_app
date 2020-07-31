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

from tiden.configuration_decorator import test_configuration
from tiden.util import with_setup, print_blue, util_sleep_for_a_while, is_enabled, attr, test_case_id, require
from suites.pme.ignite_bench_adapter import IgniteBenchAdapter
from suites.pme.pme_utils import PmeUtils
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.helper.cache_utils import DynamicCachesFactory

@test_configuration([
    'zookeeper_enabled',
    'sbt_model_enabled',
])
class TestPmeBench(PmeUtils):
    setup_test = PmeUtils.setup_test
    teardown_test = PmeUtils.teardown_test

    LOAD_FACTOR = 1000

    num_backups = 1

    num_partitions = 32768
    # num_partitions = 28672
    # num_partitions = 24576
    # num_partitions = 20480
    # num_partitions = 16384
    # num_partitions = 12288
    # num_partitions = 8192
    # num_partitions = 4096
    # num_partitions = 1024

    num_loading_nodes = 2

    def get_source_resource_dir(self):
        return "%s/res/bench" % self.config['suite_dir']

    def setup(self):
        self.ignite = IgniteBenchAdapter(self.config, self.ssh)
        self.cu = self.ignite.cu
        self.su = self.ignite.su
        self.zoo = self.ignite.zoo

        self.consistency_check = is_enabled(self.config.get('consistency_check_enabled', False))

        self.optimistic_possibility = self.config.get('optimistic_possibility', 0.5)
        self.cross_cache_batch = self.config.get('cross_cache_batch', 4)

        self.tx_delay = self.config.get('tx_delay', 4)
        self.collect_timeout = self.config.get('collect_timeout', 100)
        self.collect_timeout_metrics_thread = self.config.get('collect_timeout_metrics_thread', 100)

        zookeeper_enabled = is_enabled(self.config.get('zookeeper_enabled'))
        sbt_model_enabled = is_enabled(self.config.get('sbt_model_enabled'))

        default_context = self.contexts['default']
        default_context.add_config('caches.xml', 'caches_default.xml')
        default_context.add_context_variables(
            caches_file="caches_default.xml",
            zookeeper_enabled=zookeeper_enabled,
            sbt_model_enabled=sbt_model_enabled,
            num_backups=self.num_backups,
            num_partitions=self.num_partitions,
        )

        if zookeeper_enabled:
            self.zoo.deploy_zookeeper()
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString(),
            )

        super().setup()

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    def test_pme_bench_activate(self):
        """
        Perform activate and deactivate benchmarks, save results to .csv in var_dir
        """
        self.start_grid_no_activate()
        self.last_top = self.ignite.ignite_srvs.last_topology_snapshot()
        self.last_topVer = max([_['ver'] for _ in self.last_top])
        last_minorTopVer = 0
        print_blue("Topology version before activate: %d" % self.last_topVer)

        util_sleep_for_a_while(3)
        self.ignite.ignite_srvs.jmx.activate(1)

        n_tries = 0
        max_tries = 5
        while n_tries < max_tries:
            n_tries += 1
            max_time = self._get_last_exchange_time()
            if self.exchange_finished:
                break
            util_sleep_for_a_while(5)

        self.new_top = self.ignite.ignite_srvs.last_topology_snapshot()
        self.new_topVer = max([_['ver'] for _ in self.new_top])
        assert self.new_topVer == self.last_topVer, "ERROR: major topology changed, possibly crash during activation"

        for exch_topVer, exch_data in self.agg_exch_x1.items():
            exch_major_topVer = int(exch_topVer / 10000)
            exch_minor_topVer = exch_topVer - exch_major_topVer * 10000
            if exch_major_topVer == self.last_topVer:
                x1_time = self.agg_exch_x1[exch_topVer]['max_duration']
                x2_time = self.agg_exch_x2[exch_topVer]['max_duration']
                self._dump_exchange_time(x1_time, x2_time, "activate [%d, %d]" % (exch_major_topVer, exch_minor_topVer),
                                         num_partitions=self.num_partitions)
                last_minorTopVer = exch_minor_topVer

        self.ignite.ignite_srvs.jmx.deactivate(1)

        n_tries = 0
        max_tries = 5
        while n_tries < max_tries:
            n_tries += 1
            max_time = self._get_last_exchange_time()
            if self.exchange_finished:
                break
            util_sleep_for_a_while(5)

        for exch_topVer, exch_data in self.agg_exch_x1.items():
            exch_major_topVer = int(exch_topVer / 10000)
            exch_minor_topVer = exch_topVer - exch_major_topVer * 10000
            if exch_major_topVer == self.last_topVer and exch_minor_topVer > last_minorTopVer:
                x1_time = self.agg_exch_x1[exch_topVer]['max_duration']
                x2_time = self.agg_exch_x2[exch_topVer]['max_duration']
                self._dump_exchange_time(x1_time, x2_time,
                                         "deactivate [%d, %d]" % (exch_major_topVer, exch_minor_topVer),
                                         num_partitions=self.num_partitions)

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('client')
    def test_pme_bench_1_client(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)
        self.do_pme_client_bench(1)

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('client')
    def test_pme_bench_5_client(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)
        self.do_pme_client_bench(5)

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('server')
    def test_pme_bench_1_server(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)
        self.do_pme_server_bench(1)

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('server')
    def test_pme_bench_1_server_coordinator(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)
        self.do_pme_server_bench(1, kill_coordinator=True)

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('server')
    def test_pme_bench_5_server(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)
        self.do_pme_server_bench(5)

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('snapshot')
    def test_pme_bench_snapshot(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)
        self.do_snapshot_bench()

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('snapshot')
    def test_pme_bench_dynamic_caches_client(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load,
                                     nodes_num=self.num_loading_nodes)

        expected_total_num_clients = len(
            self.ignite.get_all_client_nodes() +
            self.ignite.get_all_common_nodes()
        )

        num_clients = 32

        # start num_clients client nodes on 'flaky' hosts
        with PiClient(self.ignite.ignite_cli_flaky, self.get_client_config(), nodes_num=num_clients,
                      new_instance=True) as piclient:
            self.ignite.ignite_srvs.wait_for_topology_snapshot(
                client_num=expected_total_num_clients + num_clients)

            from itertools import cycle
            client_nodes = cycle(
                self.ignite.ignite_cli_flaky.get_all_common_nodes() +
                self.ignite.ignite_cli_flaky.get_all_client_nodes()
            )

            # data_model = ModelTypes.VALUE_ALL_TYPES.value
            # created_caches = []
            dynamic_caches_factory = DynamicCachesFactory()
            # async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                node_id = next(client_nodes)
                print('Start %s on node %s ... ' % (cache_name, node_id))

                gateway = piclient.get_gateway(node_id)
                ignite = piclient.get_ignite(node_id)

                ignite.getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name, gateway=gateway))

                # if with_index:
                # data_model = ModelTypes.VALUE_ALL_TYPES.value
                #             async_operation = create_async_operation(create_streamer_operation,
                #                                                      cache_name, 1, self.max_key + 2,
                #                                                      value_type=data_model
                #                                                      )
                #             async_ops.append(async_operation)
                #             async_operation.evaluate()
                #             created_caches.append(cache_name)
                #
                #         print_blue('Waiting async results...')
                #         wait for streamer to complete
                # for async_op in async_ops:
                #     async_op.getResult()
                #
                # log_print("Dynamic caches with data created")
                # return created_caches

    @require(min_ignite_version='2.5.1')
    @with_setup(setup_test, teardown_test)
    @attr('time')
    def test_pme_bench_all(self):
        self.start_grid_no_activate()
        self.ignite.jmx.activate(1)
        self.ignite.cu.set_current_topology_as_baseline()
        self.load_data_with_streamer(0, 1 * self.LOAD_FACTOR, ignite=self.ignite.ignite_cli_load, nodes_num=8)
        self.do_pme_client_bench(1)
        self.warmup_time = self.warmup_clients_time
        self.do_pme_client_bench(5)
        self.do_pme_server_bench(1)
        self.do_pme_server_bench(5)
        self.do_snapshot_bench()

    def do_snapshot_bench(self):
        ex = None
        metrics = None
        try:
            with PiClient(self.ignite.ignite_cli_load, self.get_client_config()):
                with TransactionalLoading(self,
                                          kill_transactions_on_exit=True,
                                          cross_cache_batch=self.cross_cache_batch,
                                          skip_atomic=True,
                                          skip_consistency_check=not self.consistency_check,
                                          collect_timeout=self.collect_timeout,
                                          collect_timeout_metrics_thread=self.collect_timeout_metrics_thread,
                                          loading_profile=LoadingProfile(
                                              delay=self.tx_delay,
                                              commit_possibility=0.97,
                                              start_key=1,
                                              end_key=self.LOAD_FACTOR - 1,
                                              transaction_timeout=10000
                                          ),
                                          tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                    metrics = tx_loading.metrics

                    self._prepare_before_test(tx_loading, 'snapshot')

                    self.ignite.ignite_srvs.su.snapshot_utility('SNAPSHOT', '-type=FULL')

                    self._measurements_after_test('snapshot', skip_minor_exch=0)

            self.ignite.ignite_srvs.wait_for_topology_snapshot(client_num=0)
        except Exception as e:
            ex = e
        if metrics:
            self.create_loading_metrics_graph('pme_snapshot', metrics, dpi_factor=0.75)
        if ex:
            raise ex

    def do_pme_client_bench(self, num_clients):
        metrics = None
        ex = None
        try:
            with PiClient(self.ignite.ignite_cli_load, self.get_client_config()):
                with TransactionalLoading(self,
                                          kill_transactions_on_exit=True,
                                          cross_cache_batch=self.cross_cache_batch,
                                          skip_atomic=True,
                                          skip_consistency_check=not self.consistency_check,
                                          loading_profile=LoadingProfile(
                                              delay=self.tx_delay,
                                              commit_possibility=0.97,
                                              start_key=1,
                                              end_key=self.LOAD_FACTOR - 1,
                                              transaction_timeout=1000
                                          ),
                                          tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                    metrics = tx_loading.metrics

                    expected_total_num_clients = len(
                        self.ignite.get_all_client_nodes() +
                        self.ignite.get_all_common_nodes()
                    )

                    self._prepare_before_test(tx_loading, 'JOIN %d client(s)' % num_clients)

                    # start num_clients client nodes on 'flaky' hosts
                    with PiClient(self.ignite.ignite_cli_flaky, self.get_client_config(), nodes_num=num_clients,
                                  new_instance=True):
                        self.ignite.ignite_srvs.wait_for_topology_snapshot(
                            client_num=expected_total_num_clients + num_clients)
                        tx_loading.metrics_thread.add_custom_event('%d client(s) joined' % num_clients)

                        self._measurements_after_test('JOIN %d client(s)' % num_clients, skip_exch=1)

                        util_sleep_for_a_while(self.stabilization_time)

                        # upon exit from with block, num_clients client nodes will be killed
                        self._prepare_before_test(tx_loading, 'LEAVE %d client(s)' % num_clients)

                    self.ignite.ignite_srvs.wait_for_topology_snapshot(client_num=expected_total_num_clients)
                    tx_loading.metrics_thread.add_custom_event('%d client(s) left' % num_clients)

                    self._measurements_after_test('LEAVE %d client(s)' % num_clients, skip_exch=1)
                    util_sleep_for_a_while(self.stabilization_time)

            self.ignite.ignite_srvs.wait_for_topology_snapshot(client_num=0)
        except Exception as e:
            ex = e
        if metrics:
            self.create_loading_metrics_graph('pme_%d_clients_join_left' % num_clients, metrics, dpi_factor=0.75)
        if ex:
            raise ex

    def do_pme_server_bench(self, num_servers, kill_coordinator=False):
        metrics = None
        ex = None
        self.ignite.ignite_srvs.make_cluster_heapdump([1], 'before_load')
        try:
            with PiClient(self.ignite.ignite_cli_load, self.get_client_config()):
                with TransactionalLoading(self,
                                          kill_transactions_on_exit=True,
                                          cross_cache_batch=self.cross_cache_batch,
                                          skip_atomic=True,
                                          skip_consistency_check=not self.consistency_check,
                                          loading_profile=LoadingProfile(
                                              delay=self.tx_delay,
                                              commit_possibility=0.97,
                                              start_key=1,
                                              end_key=self.LOAD_FACTOR - 1,
                                              transaction_timeout=1000
                                          ),
                                          tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:

                    metrics = tx_loading.metrics
                    node_ids = self.ignite.ignite_srvs.get_random_server_nodes(
                        num_servers,
                        use_coordinator=kill_coordinator
                    )
                    expected_total_server_num = len(self.ignite.get_all_default_nodes()) - len(node_ids)

                    self._prepare_before_test(tx_loading, 'LEAVE %d server(s)' % len(node_ids))

                    self.ignite.ignite_srvs.make_cluster_jfr(60)
                    util_sleep_for_a_while(2)
                    self.ignite.ignite_srvs.kill_nodes(*node_ids)

                    self.ignite.ignite_srvs.wait_for_topology_snapshot(server_num=expected_total_server_num)
                    tx_loading.metrics_thread.add_custom_event('%d server(s) left' % len(node_ids))

                    self._measurements_after_test('LEAVE %d server(s)' % len(node_ids), skip_exch=1)
                    # self.ssh.exec_on_host(self.ignite.ignite_srvs.nodes[1]['host'], [
                    #     'jmap -dump:format=b,file={testdir}/heapdump.{pid}.hprof {pid}'.format(
                    #         testdir=self.config['rt']['remote']['test_dir'],
                    #         pid=self.ignite.ignite_srvs.nodes[1]['PID'],
                    #     )
                    # ])
                    self.ignite.ignite_srvs.make_cluster_heapdump([1], 'after_server_leave')

                    util_sleep_for_a_while(self.stabilization_time)

                    self._prepare_before_test(tx_loading, 'JOIN %d server(s)' % len(node_ids))
                    self.ignite.ignite_srvs.start_nodes(*node_ids)
                    self.ignite.ignite_srvs.wait_for_topology_snapshot(
                        server_num=expected_total_server_num + len(node_ids))
                    tx_loading.metrics_thread.add_custom_event('%d server(s) joined' % len(node_ids))
                    util_sleep_for_a_while(int(3 * self.LOAD_FACTOR / 1000))

                    self._measurements_after_test('JOIN %d server(s)' % len(node_ids), skip_exch=1)
                    self.ignite.ignite_srvs.make_cluster_heapdump([1], 'after_server_join')

                    util_sleep_for_a_while(self.stabilization_time)

            self.ignite.ignite_srvs.wait_for_topology_snapshot(client_num=0)
        except Exception as e:
            ex = e
        if metrics:
            self.create_loading_metrics_graph('pme_%d_servers_left_join' % num_servers, metrics, dpi_factor=0.75)
        if ex:
            raise ex

