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

import re
from time import sleep
from collections import namedtuple

from tiden.error_maker import FileSystemErrorMaker, IOErrorMaker
from tiden_gridgain.piclient.helper.cache_utils import IgniteCache, IgniteCacheConfig

from tiden.apps.ignite.igniteexception import IgniteException
from tiden_gridgain.piclient.helper.class_utils import create_all_types, create_java_array, create_java_object, ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_streamer_operation, \
    create_put_all_operation, create_continuous_query_operation, create_transaction_control_operation, TxDescriptor, \
    create_starvation_in_fork_join_pool
from tiden_gridgain.piclient.loading import TransactionalLoading
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.case.regresstestcase import RegressTestCase
from tiden.tidenexception import TidenException
from tiden.util import with_setup, print_blue, require, attr, util_sleep_for_a_while, is_enabled, current_time_millis, \
    require_min_server_nodes, log_print
from tiden.configuration_decorator import test_configuration
from tiden.testconfig import test_config
from threading import Thread
from tiden.assertions import tiden_assert, tiden_assert_not_equal, tiden_assert_equal
from tiden.util import known_issue, test_case_id, enable_connections_between_hosts, disable_connections_between_hosts

@test_configuration(['shared_folder_enabled'])
class TestRegress(RegressTestCase):
    setup_test = RegressTestCase.setup_test
    teardown_test = RegressTestCase.teardown_test
    setup_pitr_test = RegressTestCase.setup_pitr_test
    teardown_pitr_test = RegressTestCase.teardown_pitr_test
    setup_pitr_shared_storage_test = RegressTestCase.setup_pitr_shared_storage_test
    teardown_pitr_shared_storage_test = RegressTestCase.teardown_pitr_shared_storage_test
    setup_shared_storage_test = RegressTestCase.setup_shared_storage_test
    teardown_shared_storage_test = RegressTestCase.teardown_shared_storage_test

    snapshot_list_output = 'ID=([0-9]+), ' \
                           'CREATED=([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1,3}), ' \
                           'TYPE=(FULL|INC), ' \
                           'CLUSTER SIZE=([0-9]+), ' \
                           'SERVER NODES=([0-9]+), ' \
                           'SNAPSHOT FOLDER=([a-zA-Z\/\<\>0-9\.]+), ' \
                           'CACHES COUNT=([0-9]+)'
    snapshot_list_output_snapshot_id_pos = 0
    snapshot_list_output_snapshot_date_pos = 1
    snapshot_list_output_snapshot_type_pos = 2
    snapshot_list_output_cluster_size_pos = 3
    snapshot_list_output_num_server_nodes_pos = 4
    snapshot_list_output_snapshot_folder_pos = 5
    snapshot_list_output_num_caches_pos = 6

    def setup(self):
        default_context = self.contexts['default']
        default_context.add_context_variables(
            caches_file="caches.xml",
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled', True)),
            community_edition_enabled = is_enabled(self.config.get('community_edition_enabled'))
        )

        other_caches = self.create_test_context('other_caches')
        other_caches.add_context_variables(
            caches_file="caches_2.xml",
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled', True)),
            community_edition_enabled = is_enabled(self.config.get('community_edition_enabled'))
        )

        self.ssl_enabled = is_enabled(self.config.get('ssl_enabled'))

        if self.ssl_enabled:
            keystore_pass = truststore_pass = '123456'
            ssl_config_path = self.config['rt']['remote']['test_module_dir']
            ssl_params = namedtuple('ssl_conn', 'keystore_path keystore_pass truststore_path truststore_pass')

            self.ssl_conn_tuple = ssl_params(keystore_path='{}/{}'.format(ssl_config_path, 'server.jks'),
                                             keystore_pass=keystore_pass,
                                             truststore_path='{}/{}'.format(ssl_config_path, 'trust.jks'),
                                             truststore_pass=truststore_pass)

            self.cu.enable_ssl_connection(self.ssl_conn_tuple)
            self.su.enable_ssl_connection(self.ssl_conn_tuple)

            for context_name in self.contexts:
                self.contexts[context_name].add_context_variables(
                    ssl_enabled=True,
                    ssl_config_path=ssl_config_path,
                )

        super().setup()

    @attr('regression')
    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_empty_transaction(self):
        """
        This test is based on GG-13683:
        1. Enable assertions.
        2. Create empty (not involving any tx ops) transaction of any type on SERVER node.
        3. Call commit. Without fix it will fail with assertion.
        """

        self.start_grid()
        self.load_data_with_streamer()

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            gateway = piclient.get_gateway()
            ignite = piclient.get_ignite()

            concurrency_isolation_map = self._get_tx_type_map(gateway)

            self.cu.set_current_topology_as_baseline()

            for i in range(1, 5):
                tx = ignite.transactions().txStart()
                util_sleep_for_a_while(3)
                tx.commit()
                for concurrency in ['OPTIMISTIC', 'PESSIMISTIC']:
                    for isolation in ['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE']:
                        print_blue('Run transaction %s %s' % (concurrency, isolation))
                        tx = ignite.transactions().txStart(concurrency_isolation_map.get(concurrency),
                                                           concurrency_isolation_map.get(isolation))
                        util_sleep_for_a_while(3)
                        tx.commit()

    @attr('regression_IGN', 'IGN-10694')
    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_ign_10694(self):
        """
        This test is based on IGN-10694:
        1. Set IGNITE_EXCHANGE_HISTORY_SIZE on server.
        2. Start client more than IGNITE_EXCHANGE_HISTORY_SIZE.
        """

        self.start_grid()
        self.load_data_with_streamer()

        with PiClient(self.ignite, self.get_client_config(), nodes_num=4) as piclient:
            ignite = piclient.get_ignite()

            async_ops = []
            for cache_name in ignite.cacheNames().toArray():
                _async = create_async_operation(create_streamer_operation, cache_name, 1002, 2000)
                _async.evaluate()
                async_ops.append(_async)

            for async_op in async_ops:
                async_op.getResult()

    @attr('regression_IGN', 'IGN-12518')
    @require(min_server_nodes=2)
    @with_setup(setup_test, teardown_test)
    def test_stop_server_node_when_execute_idle_verify(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-12518
        """

        def shutdown_node(ignite):
            util_sleep_for_a_while(1)
            ignite.kill_node(2)

        self.start_grid()
        self.load_data_with_streamer(end_key=100000, nodes_num=2)
        t = Thread(target=shutdown_node, args=(self.ignite,))
        t.start()
        self.cu.control_utility('--cache', 'idle_verify')

    @attr('regression_IGN', 'continuous_query')
    @with_setup(setup_test, teardown_test)
    def test_continuous_query_example(self):
        """
        Check work-logic of piclient and ignite work according piclient.create_continuous_query_operation
        """
        cache_name = 'cache_group_3_088'
        test_range = range(1, 10)

        self.start_grid()
        with PiClient(self.ignite, self.get_client_config(), jvm_options=None, nodes_num=1, clear=False):
            cache_instance = IgniteCache(cache_name)
            with PiClient(self.ignite, self.get_client_config(), jvm_options=None, nodes_num=1, new_instance=True,
                          clear=False):
                async_operation = create_async_operation(create_continuous_query_operation,
                                                         cache_name=cache_name, duration=100)
                async_operation.evaluate()
                for i in test_range:
                    cache_instance.put(i, str(i), key_type="long")
                    util_sleep_for_a_while(1, 'put value={} to long key={}'.format(str(i), i))
            util_sleep_for_a_while(5, 'wait until all CQ listener notify client node')
        get_log = self.ignite.grep_all_data_from_log(
            'client',
            'Queried existing entry',
            'key=(\d+), val=(\d+)',
            'QC_logs',
        )
        tiden_assert_not_equal(len(get_log), 0, "len(result)")
        for node in get_log.keys():
            result_list = list(sorted(set(get_log[node])))
            for i in test_range:
                tiden_assert(str(i) == result_list[i - 1][0] and str(i) == result_list[i - 1][1],
                             'key={element}, value={element} in client_node with CQ'.format(element=i))

    @attr('regression_IGN', 'network_problem_emulation', 'client')
    @require(min_client_hosts=2)
    @require(min_server_hosts=2)
    @with_setup(setup_test, teardown_test)
    def test_broke_network_between_client_node_and_cluster_client_fail_after_clientFailureDetectionTimeout(self):
        """
        Test based on https://ggsystems.atlassian.net/browse/GG-14714
        work from 2.5.1-p161/2.5.5-p1
        """
        try:
            enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                                             self.config['environment']['server_hosts'][-1])
            self.start_grid(disable_host_list=[self.config['environment']['server_hosts'][-1]])
            with PiClient(self.ignite, self.get_client_config(), jvm_options=None,
                          nodes_num=1, new_instance=True, host=self.config['environment']['server_hosts'][-1]):
                util_sleep_for_a_while(15, 'wait until transaction start cluster-wide')
                disable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                                                  self.config['environment']['server_hosts'][-1])
                util_sleep_for_a_while(60, 'wait clientFailureDetectionTimeout')
                error_msg = 'Client node considered as unreachable and will be dropped from cluster, because no metrics update messages received in interval'
                get_log = self.ignite.grep_all_data_from_log(
                    'server',
                    error_msg,
                    '(.*)',
                    'TcpCommunicationSpi_resolver',
                )
                tiden_assert_not_equal(len(get_log), 0, "len(result)")
                tiden_assert(len(get_log[1][0]) > 0, 'found error {}'.format(error_msg))

        finally:
            self.set_current_context('default')
            enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                                             self.config['environment']['server_hosts'][-1])

    @attr('regression_IGN', 'network_problem_emulation', 'client')
    @require(min_client_hosts=2)
    @require(min_server_hosts=2)
    @with_setup(setup_test, teardown_test)
    def test_broke_network_between_client_node_and_cluster_with_DIGNITE_ENABLE_FORCIBLE_NODE_KILL_true(self):
        """
        Test based on https://ggsystems.atlassian.net/browse/IGN-9679
        requared: -DIGNITE_ENABLE_FORCIBLE_NODE_KILL=true
        work from 2.5.1-p161/2.5.5-p1
        """
        self.network_problem_emulation_between_node_and_cluster(node='client')

    @attr('regression_IGN', 'network_problem_emulation', 'server')
    @require(min_client_hosts=2)
    @require(min_server_hosts=2)
    @with_setup(setup_test, teardown_test)
    def test_broke_network_between_server_node_and_cluster_with_DIGNITE_ENABLE_FORCIBLE_NODE_KILL_true(self):
        """
        Test based on https://ggsystems.atlassian.net/browse/IGN-9679
        requared: -DIGNITE_ENABLE_FORCIBLE_NODE_KILL=true
        work from 2.5.1-p161/2.5.5-p1
        """
        self.network_problem_emulation_between_node_and_cluster(node='server')

    def network_problem_emulation_between_node_and_cluster(self, node):
        cache_name = 'cache_group_3_088'
        self.ignite.set_node_option('*', 'jvm_options',
                                    self.get_default_jvm_options() + ['-DIGNITE_ENABLE_FORCIBLE_NODE_KILL=true'])
        inner_PiClient_jvm_options = self.get_default_jvm_options() + \
                                     ['-DIGNITE_ENABLE_FORCIBLE_NODE_KILL=true'] \
            if node == 'server' else ['-DIGNITE_ENABLE_FORCIBLE_NODE_KILL=true']
        try:
            enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                               self.config['environment']['server_hosts'][-1])
            not_default_context = self.create_test_context('custom_timeout_settings')
            not_default_context.add_config('server.xml', 'server_custom_timeout_settings.xml')
            not_default_context.add_config('client.xml', 'client_custom_timeout_settings.xml')
            default_context_vars = self.contexts['default'].get_context_variables()
            not_default_context.add_context_variables(
                **default_context_vars,
                connectTimeout=5000,
                maxConnectTimeout=10000,
                reconnectCount=2,
                idleConnectionTimeout=1000,
                failureDetectionTimeout=160000,
                clientFailureDetectionTimeout=150000,
                socketTimeout=10000,
                usePairedConnections=True
            )
            self.rebuild_configs()
            not_default_context.build_and_deploy(self.ssh)
            self.set_current_context('custom_timeout_settings')
            self.start_grid(disable_host_list=[self.config['environment']['server_hosts'][-1]])
            node_config = self.get_client_config() if node == 'client' else self.get_server_config()
            with PiClient(self.ignite, node_config, jvm_options=inner_PiClient_jvm_options,
                          nodes_num=1, new_instance=True, host=self.config['environment']['server_hosts'][-1]):
                lrt_operations = PiClientIgniteUtils.launch_transaction_operations(cache_name,
                                                                                   start_key=1000, end_key=1010)
                util_sleep_for_a_while(30, 'wait until transaction start cluster-wide')
                disable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                                    self.config['environment']['server_hosts'][-1])
                util_sleep_for_a_while(30, 'wait until transaction start cluster-wide')
                PiClientIgniteUtils.release_transactions(lrt_operations)
                self.cu.kill_transactions(force=True)
            error_msg = 'TcpCommunicationSpi failed to establish connection to node, node will be dropped from cluster'
            get_log = self.ignite.grep_all_data_from_log(
                'server',
                error_msg,
                '(.*)',
                'TcpCommunicationSpi_resolver',
            )
            tiden_assert_not_equal(len(get_log), 0, "len(result)")
            tiden_assert(len(get_log[1][0]) > 0 if node == 'client' else
                         len(get_log[1][0]) == 0, 'found error {}'.format(error_msg))
        finally:
            self.set_current_context('default')
            enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                               self.config['environment']['server_hosts'][-1])

    # @require(~test_config.compaction_enabled)
    @test_case_id('167650')
    @attr('regression_IGN', 'IGN-12756')
    # @known_issue('IGN-13033')
    @with_setup(setup_test, teardown_test)
    def test_enable_wal_compaction_after_huge_load(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-12756
        """
        try:
            not_default_context = self.create_test_context('disable_wal_compaction_data_region')
            not_default_context.add_config('server.xml', 'server_disable_wal_compaction_data_region.xml')
            not_default_context.add_config('client.xml', 'client_disable_wal_compaction_data_region.xml')
            default_context_vars = self.contexts['default'].get_context_variables()
            not_default_context.add_context_variables(
                **default_context_vars,
                data_region_enabled=True,
                compaction_enabled=False
            )
            self.rebuild_configs()
            not_default_context.build_and_deploy(self.ssh)
            self.set_current_context('disable_wal_compaction_data_region')
            self.start_grid()
            self.load_data_with_streamer(end_key=100000, config_file="client_disable_wal_compaction_data_region.xml")
            checksum_before_restart = self.calc_checksums_distributed(
                config_file="client_disable_wal_compaction_data_region.xml")

            self.stop_grid_hard()

            not_default_context = self.create_test_context('enable_wal_compaction_data_region')
            not_default_context.add_config('server.xml', 'server_enable_wal_compaction_data_region.xml')
            not_default_context.add_config('client.xml', 'client_enable_wal_compaction_data_region.xml')
            default_context_vars = self.contexts['default'].get_context_variables()

            not_default_context.add_context_variables(
                **default_context_vars,
                data_region_enabled=True,
                compaction_enabled=True
            )
            self.rebuild_configs()
            not_default_context.build_and_deploy(self.ssh)
            self.set_current_context('enable_wal_compaction_data_region')
            self.start_grid()
            util_sleep_for_a_while(60)
            exception = self.find_exception_in_logs('org.apache.ignite.IgniteCheckedException')
            tiden_assert(exception < 1, 'Find problem in log after enable wal_compaction')
            checksum_after_restart = self.calc_checksums_distributed(
                config_file="client_enable_wal_compaction_data_region.xml")
            tiden_assert(checksum_after_restart == checksum_before_restart, 'Checksums are the same')
        finally:
            self.set_current_context('default')

    @attr('regression_IGN', 'IGN-10582')
    @with_setup(setup_test, teardown_test)
    @require(min_server_nodes=3)
    @require(~test_config.shared_folder_enabled)
    def test_ign_10582(self):
        """
        This test is based on IGN-10582:
        1. Set -DISABLE_WAL_DURING_REBALANCING=true on server.
        """

        self.start_grid()
        self.load_data_with_streamer()
        self.cu.control_utility('--cache', 'idle_verify')
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            ignite = piclient.get_ignite()

            async_ops = []
            for cache_name in ignite.cacheNames().toArray():
                _async = create_async_operation(create_streamer_operation, cache_name, 1002, 20000)
                _async.evaluate()
                async_ops.append(_async)

            self.ignite.kill_node(3)
            self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))
            sleep(10)
            self.ignite.start_node(3)
            self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))

            for async_op in async_ops:
                async_op.getResult()
            sleep(20)

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            ignite = piclient.get_ignite()
            sleep(20)

    @attr('regression')
    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_sql_hangs_with_other_node_in_baseline(self):
        """
        This test is to check GG-13679:
        SqlQuery hangs indefinitely while additional node registered in topology but still not in baseline.
        IMPORTANT: distributedJoins=true mast be added as parameter for JDBC connection.
        """
        from pt.utilities.sqlline_utility import Sqlline
        self.start_grid()

        self.load_data_with_streamer(value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)

        initial_setup = ['!tables', '!index',
                         '\'select * from \"cache_group_1_001\".ALLTYPESINDEXED a, '
                         '\"cache_group_1_001\".ALLTYPESINDEXED b WHERE a.LONGCOL=b.LONGCOL;\'',
                         '\'select count(*) from \"cache_group_1_001\".ALLTYPESINDEXED a, '
                         '\"cache_group_1_001\".ALLTYPESINDEXED b WHERE a.LONGCOL=b.LONGCOL;\'']

        expected = ['^\\\'COUNT\(\*\)\\\'\$|\'COUNT\(\*\)\'', '^\\\'1001\\\'$|\'1001\'']

        # Add node but do not include it to baseline
        node_id = list(self.ignite.add_additional_nodes(self.get_server_config(), 1))
        self.ignite.set_node_option('*', 'jvm_options', self.get_default_jvm_options())
        self.ignite.start_additional_nodes(node_id)
        util_sleep_for_a_while(2)
        self.cu.control_utility('--baseline')

        sqlline = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        output = sqlline.run_sqlline(initial_setup, driver_flags=['distributedJoins=true'])
        # output = self.run_sqlline(initial_setup)
        self.su.check_content_all_required(output, expected, maintain_order=True)

    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_ign_9715(self):
        """
        Start grid with default options, populate with data, then restart with IGNITE_USE_ASYNC_FILE_IO_FACTORY option.
        If IGN-9715 reproduces, UnsupportedOperationException should be thrown upon activation.
        Otherwise, issue is fixed.
        :return:
        """
        self.start_grid()
        self.preloading()
        self.stop_grid()
        self.ignite.set_node_option('*', 'jvm_options', self.get_async_io_jvm_options())

        try:
            self.start_grid(activation_timeout=30)
        except IgniteException as e:
            # activation failed, check it is failed because of proper error in logs
            assert e.args[0] == 'Activation failed', \
                "Unexpected exception: %s" % e
            expected_exception = 'java.lang.UnsupportedOperationException'
            expected_cause = "AsynchronousFileChannel doesn't support mmap."
            result = self.ignite.find_exception_in_logs(expected_exception)
            assert result > 0, \
                "Expected exception not '%s' found!" % expected_exception
            server_nodes = self.ignite.get_all_default_nodes()
            for node_idx in server_nodes:
                assert expected_cause in self.ignite.nodes[node_idx]['exception'], \
                    "Unexpected cause in expected exception found: expected '%s', found '%s' " % \
                    (expected_cause, self.ignite.nodes[node_idx]['exception'])

            assert False, \
                "IGN-9715 reproduced"

    @with_setup(setup_pitr_test, teardown_pitr_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13301_full(self):
        """
        Last snapshot in PITR mode should not be DELETable.
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        snapshot_id = self.get_last_snapshot_id()
        try:
            self.su.snapshot_utility('DELETE', '-id=%s' % snapshot_id)
            assert False, "GG-13301 reproduced: last snapshot must be unable to DELETE in PITR mode"
        except TidenException as e:
            assert "[DELETE] failed with error: 7000" in str(e), \
                "DELETE must fail with proper error code"
            assert "failed to DELETE snapshot (the snapshot is last)" in str(e), \
                "DELETE must fail with proper error message"

    @with_setup(setup_pitr_test, teardown_pitr_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13301_inc(self):
        """
        Last snapshot in PITR mode should not be DELETable.
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        # full_snapshot_id = self.get_last_snapshot_id()
        self.remove_random_from_caches()
        self.su.snapshot_utility('SNAPSHOT', '-type=inc')
        inc_snapshot_id = self.get_last_snapshot_id()
        try:
            self.su.snapshot_utility('DELETE', '-id=%s' % inc_snapshot_id)
            assert False, \
                "GG-13301 reproduced: last snapshot must be unable to DELETE in PITR mode"
        except TidenException as e:
            assert "[DELETE] failed with error: 7000" in str(e), \
                "DELETE must fail with proper error code"
            assert "failed to DELETE snapshot (the snapshot is last)" in str(e), \
                "DELETE must fail with proper error message"

    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13469_list_corrupted_full(self):
        """
        LIST must not throw exception, when one of snapshots has broken metadata.
        Sub-test case #1 : create and corrupt FULL snapshot
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        s1_snapshot_id = self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.get_last_snapshot_id()
        self.corrupt_snapshot(s1_snapshot_id)
        try:
            self.su.snapshot_utility('LIST')
        except TidenException as e:
            assert False, "GG-13469 reproduced: for FULL snapshot: %s" % str(e)
        lines = self.su.latest_utility_output.split('\n')
        print_blue(lines)
        for line in lines:
            if line.startswith('ID='):
                assert str(s1_snapshot_id) not in line, \
                    "Snapshot '%s' should not be printed in LIST output due to it is corrupted." % s1_snapshot_id

    @with_setup(setup_pitr_shared_storage_test, teardown_pitr_shared_storage_test)
    @require(test_config.shared_folder_enabled)
    def test_gg_13457(self):
        """
        Point In Time Recovery must be enabled.
        Create a sequence of 1 FULL and 2 incremental snapshots (+Create 1 FULL due to PITR legacy).
        MOVE last incremental.
        MOVE previous incremental. If issue is reproduced, 'WAL archive segment deleted' exception raised
        and MOVE fails with 8000. Otherwise, issue is fixed.
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.remove_random_from_caches()
        self.su.snapshot_utility('SNAPSHOT', '-type=inc')
        s1_snapshot_id = self.get_last_snapshot_id()
        self.remove_random_from_caches()
        self.su.snapshot_utility('SNAPSHOT', '-type=inc')
        s2_snapshot_id = self.get_last_snapshot_id()
        self.remove_random_from_caches()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.su.snapshot_utility('MOVE', '-id=%s' % s2_snapshot_id, '-dest=%s' % self.get_shared_folder_path())
        try:
            self.su.snapshot_utility('MOVE', '-id=%s' % s1_snapshot_id, '-dest=%s' % self.get_shared_folder_path())
        except TidenException as e:
            assert "Error code: 8000. " in str(e), \
                "Expected MOVE command output 8000 not found."
            assert "WAL archive segment has been deleted" in str(e), \
                "Expected error text 'WAL archive segment has been deleted' not found."
            assert False, \
                "GG-13457 reproduced"
        self.stop_grid()

    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13469_list_corrupted_inc(self):
        """
        LIST must not throw exception, when one of snapshots has broken metadata.
        Sub-test case #2 : create and corrupt INC snapshot
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=inc')
        s2_snapshot_id = self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.get_last_snapshot_id()
        self.corrupt_snapshot(s2_snapshot_id)
        try:
            self.su.snapshot_utility('LIST')
        except TidenException as e:
            assert False, "GG-13469 reproduced: for INC snapshot: %s" % str(e)
        lines = self.su.latest_utility_output.split('\n')
        print_blue(lines)
        for line in lines:
            if line.startswith('ID='):
                assert str(s2_snapshot_id) not in line, \
                    "Snapshot '%s' should not be printed in LIST output due to it is corrupted." % s2_snapshot_id

    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13469_list_corrupted_parent(self):
        """
        LIST must not throw exception, when one of snapshots has broken metadata.
        Sub-test case #3 : create and corrupt FULL snapshot while having dependant INC snapshot
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        s1_snapshot_id = self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=inc')
        self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.get_last_snapshot_id()
        self.corrupt_snapshot(s1_snapshot_id)
        try:
            self.su.snapshot_utility('list')
        except TidenException as e:
            assert False, "GG-13469 reproduced: for dependant snapshot: %s" % str(e)
        lines = self.su.latest_utility_output.split('\n')
        print_blue(lines)
        for line in lines:
            if line.startswith('ID='):
                assert str(s1_snapshot_id) not in line, \
                    "Snapshot '%s' should not be printed in LIST output due to it is corrupted." % s1_snapshot_id

    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    @require(test_config.shared_folder_enabled)
    def test_gg_13469_list_corrupted_src(self):
        """
        LIST must not throw exception, when one of snapshots has broken metadata.
        Sub-test case #4 : create and corrupt FULL snapshot found with SRC
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        s1_snapshot_id = self.get_last_snapshot_id()
        self.su.snapshot_utility('MOVE', '-id=%s' % s1_snapshot_id, '-dest=%s' % self.get_shared_folder_path())
        self.corrupt_snapshot(s1_snapshot_id, src=self.get_shared_folder_path(), is_aggregated_snapshot=True)
        try:
            self.su.snapshot_utility('LIST', '-src=%s' % self.get_shared_folder_path())
        except TidenException as e:
            assert False, "GG-13469 reproduced: for LIST -src: %s" % str(e)
        lines = self.su.latest_utility_output.split('\n')
        print_blue(lines)
        for line in lines:
            if line.startswith('ID='):
                assert str(s1_snapshot_id) not in line, \
                    "Snapshot '%s' should not be printed in LIST output due to it is corrupted." % s1_snapshot_id

    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13459(self):
        """
        mock test, TODO: should load more data
        :return:
        """
        self.start_grid()
        self.preloading()
        self.cu.control_utility('--cache', 'idle_verify')

    @with_setup(setup_pitr_test, teardown_pitr_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13524_check(self):
        """
        CHECK -force should not fail when additional caches present at grid compared to caches in checked snapshot.
        Start grid with first set of caches, populate with data, make snapshot, stop grid, clean lfs.
        Start grid with second, different set of caches, try to CHECK snapshot. Without '-force' it should complain
        about missing caches (in the same cache group). With '-force', CHECK must succeed.
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        snapshot_id = self.get_last_snapshot_id()
        self.stop_grid()
        self.delete_lfs(delete_snapshots=False)

        # start config with other set of caches
        # in finally we should set the default context otherwise it could affect other tests
        try:
            self.set_current_context('other_caches')
            self.ignite.set_node_option('*', 'config', self.get_server_config())
            self.start_grid()
            try:
                self.su.snapshot_utility('CHECK', '-id=%s' % str(snapshot_id))
                assert False, \
                    "CHECK was expected to fail!"
            except TidenException:
                assert "[CHECK] failed with error: 6510" in str(self.su.latest_utility_output), \
                    "CHECK was expected to fail with 6510"

            self.stop_grid()
            self.delete_lfs(delete_snapshots=False)
            self.start_grid()
            try:
                self.su.snapshot_utility('CHECK', '-id=%s' % str(snapshot_id), '-force')
            except TidenException as e:
                assert "[CHECK] failed with error: 6510" in str(self.su.latest_utility_output), \
                    "CHECK -force was expected to either pass or fail with 6510"
                assert False, \
                    "GG-13524 reproduced on CHECK"
        finally:
            self.set_current_context('default')

    @with_setup(setup_pitr_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13524_restore(self):
        """
        RESTORE -force should not fail when additional caches present at grid compared to caches in checked snapshot.
        Start grid with first set of caches, populate with data, make snapshot, stop grid, clean lfs.
        Start grid with second, different set of caches, try to RESTORE snapshot. Without '-force' it should complain
        about missing caches (in the same cache group). With '-force', RESTORE must succeed.
        Additional caches in the same caches groups must be destroyed. Caches in different cache groups should be
        left intact.
        :return:
        """
        pass

        # TODO Behaviour changed between 2.4.2 - 2.5.1
        # self.start_grid()
        # self.preloading()
        # self.su.snapshot_utility('SNAPSHOT', '-type=full')
        # snapshot_id = self.get_last_snapshot_id()
        # self.stop_grid()
        # self.delete_lfs(delete_snapshots=False)
        #
        # # start config with other set of caches
        # self.set_current_context('other_caches')
        # self.ignite.set_node_option('*', 'config', self.get_server_config())
        # self.start_grid()
        # try:
        #     self.su.snapshot_utility('RESTORE', '-id=%s' % str(snapshot_id))
        #     assert False, \
        #         "RESTORE was expected to fail!"
        # except TidenException as e:
        #     assert "[RESTORE] failed with error: 5510" in str(e), \
        #         "RESTORE was expected to fail with 5510"
        #
        # self.stop_grid()
        # self.delete_lfs(delete_snapshots=False)
        # self.start_grid()
        # try:
        #     self.su.snapshot_utility('RESTORE', '-id=%s' % str(snapshot_id), '-force')
        # except TidenException as e:
        #     assert "[RESTORE] failed with error: 5510" in str(e), \
        #         "RESTORE -force was expected to either pass or fail with 5510"
        #     assert False, \
        #         "GG-13524 reproduced on RESTORE"

    @require(min_server_nodes=2)
    @with_setup(setup_pitr_test, teardown_pitr_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13529(self):
        """
        start grid with point in time recovery enabled
        activate - grid would create activation snapshot automatically
        use some atomic volatile data structure - e.g. semaphore, countdown latch or reentrant lock to be sure
            'default-volatile-ds-group' cache created.
        create full snapshot
        kill one node
        remove node from baseline - grid would create new snapshot automatically

        the number of caches in all three snapshots should be equal.
        :return:
        """
        self.start_grid()
        # self.preloading()

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            ignite = piclient.get_ignite()
            ignite.semaphore('test_semaphore', 1, True, True)

            self.su.snapshot_utility('LIST')
            self.su.snapshot_utility('SNAPSHOT', '-type=FULL')
            num_server_nodes = self.ignite.get_nodes_num('server')
            node_idx_to_kill = 2  # tiden can't kill node #1 because it uses it for REST access
            self.ignite.kill_node(node_idx_to_kill)
            self.ignite.wait_for_topology_snapshot(
                server_num=num_server_nodes - 1,
                comment=", kill node"
            )
            self.cu.remove_node_from_baseline(self.ignite.get_node_consistent_id(node_idx_to_kill))
            sleep(5)  # TODO: make sure automated snapshot created
            self.su.snapshot_utility('LIST')
            output = self.su.latest_utility_output
            log_print(output, color='debug')
            r = re.compile('ID=(\d+),')
            m = r.findall(output)
            assert m is not None, "No snapshots found!"
            assert len(m) == 3, "Expected to have 3 snapshots, got %d" % len(m)

            number_of_caches = None
            for snapshot_data in m:
                if number_of_caches is None:
                    number_of_caches = snapshot_data[self.snapshot_list_output_num_caches_pos]
                else:
                    assert number_of_caches == snapshot_data[self.snapshot_list_output_num_caches_pos], \
                        "GG_13529 Reproduced: expected to have same number of caches in all three snapshots. " \
                        "Got {} and {}".format(number_of_caches, snapshot_data[self.snapshot_list_output_num_caches_pos])

    # @with_setup('setup_test', 'teardown_test')
    # def test_gg_12916(self):
    #     """
    #     mock test, TODO: fix expected error message when ticket is fixed.
    #     :return:
    #     """
    #     expected = self.load_expected_regexp('test_GG_12916.txt')
    #     self.start_grid()
    #     try:
    #         self.su.snapshot_utility('LIST', '-src=/err/no/such/folder')
    #     except TidenException as e:
    #         output = str(e)
    #         assert not self.match_expected(expected, output), "GG-12916 reproduced"

    @require(min_ignite_version='2.4.2-p6')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    @require(test_config.shared_folder_enabled)
    def test_gg_13613(self):
        expected = "There are clashes between existing caches and that ones which could be restored from snapshot"
        self.start_grid()
        try:
            with PiClient(self.ignite, self.get_client_config(), nodes_num=1):
                cache1_config = IgniteCacheConfig()
                cache1_config.set_group_name('grp1')
                cache1_config.set_atomicity_mode('transactional')
                cache1_config.set_backups(2)

                cache1 = IgniteCache('grp1', cache1_config)

                for i in range(1, 100):
                    cache1.put(i, i)

            self.ignite.wait_for_topology_snapshot(client_num=0)

            self.su.snapshot_utility('snapshot', '-type=full')
            snapshot_id = self.get_last_snapshot_id()
            self.su.snapshot_utility('snapshot', '-type=full')

            snapshot_store = self.get_shared_folder_path()
            self.su.snapshot_utility('MOVE', '-id=%s -dest=%s' % (snapshot_id, snapshot_store))

            self._restart_empty_grid()

            with PiClient(self.ignite, self.get_client_config(), nodes_num=1):
                cache2_config = IgniteCacheConfig()
                cache2_config.set_group_name('grp2')
                cache2_config.set_atomicity_mode('transactional')
                cache2_config.set_backups(2)

                cache2 = IgniteCache('grp1', cache2_config)

                for i in range(1, 100):
                    cache2.put(i, i * 2)

            self.ignite.wait_for_topology_snapshot(client_num=0)

            self.su.snapshot_utility('restore', '-id=%s -src=%s' % (snapshot_id, snapshot_store))

            self.restart_grid()

            self.cu.control_utility('--cache', 'idle_verify')

            with PiClient(self.ignite, self.get_client_config()):
                cache_assert = IgniteCache('grp1')

                for i in range(1, 100):
                    assert cache_assert.get(i) == i

            raise TidenException('GG-13613 Reproduced')
        except TidenException as e:
            assert expected in self.su.latest_utility_output, \
                "GG-13613 Reproduced: %s" % str(e)

    @require(min_ignite_version='2.5.1-p6')
    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13643(self):
        self.start_grid()
        try:
            self.load_data_with_streamer(start_key=0, end_key=1001)

            self.su.snapshot_utility('snapshot', '-type=full')

            with PiClient(self.ignite, self.get_client_config()):
                pass

            self.su.snapshot_utility('restore', '-id=%s' % self.get_last_snapshot_id())

            self.restart_grid()

            self.cu.control_utility('--cache', 'idle_verify')
        except TidenException as e:
            assert "Timeout" in self.su.latest_utility_output, \
                "GG-13643 Reproduced: %s" % str(e)

    @require(min_ignite_version='2.4.2-p7')
    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_13653(self):
        self.start_grid()
        try:
            with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
                gateway = piclient.get_gateway()
                ignite = piclient.get_ignite()

                # Configure cache1
                cache_config = IgniteCacheConfig()
                cache_config.set_name('cache1')
                cache_config.set_cache_mode('replicated')
                cache_config.set_atomicity_mode('transactional')
                cache_config.set_write_synchronization_mode('full_sync')
                cache_config.set_affinity(False, 32)

                # set query entities
                query_indices_names = gateway.jvm.java.util.ArrayList()
                query_indices_names.add("strCol")
                query_indices = gateway.jvm.java.util.ArrayList()
                query_indices.add(
                    gateway.jvm.org.apache.ignite.cache.QueryIndex().setFieldNames(query_indices_names, True))

                query_entities = gateway.jvm.java.util.ArrayList()
                query_entities.add(
                    gateway.jvm.org.apache.ignite.cache.QueryEntity("java.lang.Integer",
                                                                    ModelTypes.VALUE_ALL_TYPES.value)
                        .addQueryField("strCol", "java.lang.String", None)
                        .addQueryField("longCol", "java.lang.Long", None)
                        .setIndexes(query_indices)
                )

                cache_config.get_config_object().setQueryEntities(query_entities)
                cache_config.get_config_object().setStatisticsEnabled(False)

                ignite.getOrCreateCache(cache_config.get_config_object())

                # Configure cache2
                cache_config2 = IgniteCacheConfig()
                cache_config2.set_name('cache2')
                cache_config2.set_cache_mode('partitioned')
                cache_config2.set_backups(3)
                cache_config2.set_atomicity_mode('transactional')
                cache_config2.set_write_synchronization_mode('full_sync')
                cache_config2.set_affinity(False, 32)

                # set query entities
                query_indices_names2 = gateway.jvm.java.util.ArrayList()
                query_indices_names2.add("strCol")
                query_indices2 = gateway.jvm.java.util.ArrayList()
                query_indices2.add(
                    gateway.jvm.org.apache.ignite.cache.QueryIndex().setFieldNames(query_indices_names2, True))
                query_entities2 = gateway.jvm.java.util.ArrayList()
                query_entities2.add(
                    gateway.jvm.org.apache.ignite.cache.QueryEntity("java.lang.Integer",
                                                                    ModelTypes.VALUE_ALL_TYPES.value)
                        .addQueryField("strCol", "java.lang.String", None)
                        .addQueryField("longCol", "java.lang.Long", None)
                        .setIndexes(query_indices)
                )

                cache_config2.get_config_object().setQueryEntities(query_entities2)
                cache_config2.get_config_object().setStatisticsEnabled(False)

                ignite.getOrCreateCache(cache_config2.get_config_object())

            # Restart client
            with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
                cache1 = IgniteCache('cache1')
                cache2 = IgniteCache('cache2')

                run_num = 3
                for i in range(0, run_num):
                    print_blue('Run %s from %s' % (str(i + 1), run_num))
                    for j in range(i * 100, i * 100 + 101):
                        cache1.put(j, create_all_types(j))
                        cache2.put(j, create_all_types(j))

                    print_blue('Create sqlFieldsQueries')
                    sqlFieldsQuery1 = piclient.get_gateway().jvm.org.apache.ignite.cache.query \
                        .SqlFieldsQuery('select * from "cache1".AllTypes')
                    sqlFieldsQuery2 = piclient.get_gateway().jvm.org.apache.ignite.cache.query \
                        .SqlFieldsQuery('select * from "cache2".AllTypes')

                    print_blue('Assert sqlFieldsQuery is not empty')
                    assert not cache1.cache.query(sqlFieldsQuery1).getAll().isEmpty(), "%s" % str(i + 1)
                    assert not cache2.cache.query(sqlFieldsQuery2).getAll().isEmpty(), "%s" % str(i + 1)

                    util_sleep_for_a_while(5)
        except TidenException as e:
            assert "Some new problem arises during reproducing GG-13653: %s" % e

    @with_setup(setup_pitr_shared_storage_test, teardown_pitr_shared_storage_test)
    @require(test_config.shared_folder_enabled)
    def test_ign_10086(self):
        """
        Exception during checkpoint concurrent changes in topology.
        Set checkpoint frequency to 1 sec. Restore from shared folder 10 times.
        :return:
        """
        self.start_grid()
        self.preloading()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        snapshot_id = self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=full')
        self.su.snapshot_utility('MOVE', '-id=%s' % snapshot_id, '-dest=%s' % self.get_shared_folder_path())

        num_iterations = 10

        try:
            for i in range(0, num_iterations):
                log_print('Iteration {} from {}'.format(i, num_iterations), color='debug')
                self.su.snapshot_utility(
                    'RESTORE',
                    '-id=%s' % str(snapshot_id),
                    '-src=%s' % self.get_shared_folder_path()
                )

            expected_exception = "java.lang.IllegalStateException"
            exception_cause = ""
            result = self.ignite.find_exception_in_logs(expected_exception)
            print_blue(result)
            server_nodes = self.ignite.get_all_default_nodes()
            for node_idx in server_nodes:
                if 'exception' in self.ignite.nodes[node_idx]:
                    exception_cause = self.ignite.nodes[node_idx]['exception']
                    break

            assert result == 0, \
                "IGN-10086 reproduced: %s: %s" % (expected_exception, exception_cause)

        except TidenException as e:
            assert "Some new problem arises during reproducing IGN-10086: %s" % str(e)

        self.stop_grid()
        self.delete_lfs()

    @require(min_ignite_version='2.4.5')
    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_ign_10541(self):
        """
        Put data to cache with configured TTL Accessed Expiry Policy and ensure that
        data leaves cache after restart and timeout.
        https://ggsystems.atlassian.net/browse/IGN-10541
        https://issues.apache.org/jira/browse/IGNITE-8503
        :return:
        """
        self.start_grid()
        ttl_timeout_seconds = 150
        num_entries = 20000
        cache_config = None
        cache_name = 'ttl_cache'

        start_time = None

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            gateway = piclient.get_gateway()
            # ignite = piclient.get_ignite()

            # create TTL cache
            cache_config = IgniteCacheConfig(gateway=gateway)
            cache_config.set_name(cache_name)
            cache_config.set_group_name('ttl_group')
            cache_config.set_eager_ttl(True)
            cache_config.set_expiry_policy_factory_accessed(ttl_timeout_seconds)
            cache_config.set_atomicity_mode('transactional')
            cache_config.set_write_synchronization_mode('full_sync')
            cache_config.set_rebalance_mode('sync')
            cache_config.set_affinity(False, 32)

            # cache = ignite.getOrCreateCache(cache_config.get_config_object())
            cache = IgniteCache(cache_name, config=cache_config, gateway=gateway)

            # put data to cache
            operation = create_put_all_operation(cache_name, 1, num_entries, 100,
                                                 value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                 gateway=gateway)
            operation.evaluate()
            if start_time is None:
                start_time = current_time_millis()
            for i in range(1, num_entries):
                value = cache.get(i, key_type='long')
                if i < 10:
                    print("key=%s, value=%s" % (i, value))

        # restart grid
        self.stop_grid()
        self.start_grid()

        # sleep for expiration timeout
        end_time = current_time_millis()
        grid_restart_time = int((end_time - start_time) / 1000)
        if grid_restart_time > ttl_timeout_seconds + 1:
            print("Grid was restarting for %d seconds, TTL %d seconds, everything should be expired as of now" % (
                grid_restart_time, ttl_timeout_seconds
            ))
        else:
            sleep_seconds = ttl_timeout_seconds - grid_restart_time + 1

            print("sleeping %d seconds" % sleep_seconds)
            sleep(sleep_seconds)
            print("awoke")

        with PiClient(self.ignite, self.get_client_config()):
            cache = IgniteCache(cache_name)
            for i in range(1, num_entries):
                value = cache.get(i, key_type='long')
                if value is not None:
                    print("(!) key=%s, value=%s" % (i, value))
                assert value is None, "IGN-10541 reproduced: value for key %d was resurrected after restart" % i

        self.stop_grid()
        self.delete_lfs()

    @with_setup(setup_test, teardown_test)
    @require(~test_config.shared_folder_enabled)
    def test_gg_14219_additional_logging(self):
        """
        Added additional logging when snapshot creation fails due to page corruption.
        :return:
        """
        self.start_grid()
        self.preloading()

        try:
            self.corrupt_page_store()
            self.su.snapshot_utility('SNAPSHOT', '-type=full')
            s1_snapshot_id = self.get_last_snapshot_id()
        except TidenException as e:
            assert True, "GG-13469 reproduced: for FULL snapshot: %s" % str(e)
        else:
            assert False, "Page store was corrupted, but snapshot was created!"

    @attr('regression')
    @require_min_server_nodes(3)
    # @require(~test_config.shared_folder_enabled)
    @with_setup(setup_test, teardown_test)
    def test_data_loss_on_the_node_with_empty_lfs(self):
        """
        Based on IGN-12031. Possible data loss during starting of the nodes with empty pds.

        Scenario:
        1)Start 3 data nodes and activate the cluster with cache with 1 backup and PartitionLossPolicy.READ_ONLY_SAFE.
        2)Start client and add the data to your cache. Stop the client
        3)Stop DN2 and clear it pds and wal
        4)Start DN2. Rebalance will start.
        5)During rebalance stop DN3.
        6)Start DN3.
        """
        self.ignite.jmx.start_utility()
        self.start_grid()
        self.preloading()

        cluster_nodes = self.ignite.get_alive_default_nodes()

        # As we need 3 data nodes, let's stop other nodes.
        for node_id in cluster_nodes[3:]:
            self.ignite.kill_node(node_id)

        # And reset baseline
        self.cu.set_current_topology_as_baseline()

        cache_to_test = 'test_cache_with_loss_policy'
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            from pt.piclient.helper.cache_utils import IgniteCacheConfig

            ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()
            cache_config = IgniteCacheConfig(gateway)
            cache_config.set_name(cache_to_test)
            cache_config.set_partition_loss_policy('READ_ONLY_SAFE')
            cache_config.set_cache_mode('PARTITIONED')
            cache_config.set_backups(1)
            cache_config.set_atomicity_mode('transactional')
            cache_config.set_write_synchronization_mode('full_sync')
            cache_config.set_affinity(False, 32)

            ignite.getOrCreateCache(cache_config.get_config_object())
            operation = create_streamer_operation(cache_to_test, 1, 1001,
                                                  value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                                  allow_overwrite=True)
            operation.evaluate()

        # Restart node 2 with cleanup LFS
        self.ignite.kill_node(2)
        self.ignite.cleanup_work_dir(node_id=2)
        self.ignite.start_node(2)

        util_sleep_for_a_while(3)

        # Restart node 3
        self.ignite.kill_node(3)
        self.ignite.start_node(3)

        # Let them rebalance
        util_sleep_for_a_while(10)
        self.ignite.jmx.wait_for_finish_rebalance(90, [cache_to_test])

    @attr('regression')
    @with_setup(setup_test, teardown_test)
    def test_destroy_cache_during_rebalance(self):
        """
        Based on IGN-12305. NPE in GridDhtPartitionDemander#handleSupplyMessage occurs when concurrently
        rebalancing and stopping cache in same cache group.
        """
        self.start_grid()
        self.preloading()

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            from pt.piclient.helper.cache_utils import IgniteCacheConfig

            ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()

            cache_to_test = 'test_cache_2'

            # Create caches on one cache group.
            for cache_name in ['test_cache_1', 'test_cache_2', 'test_cache_3']:
                cache_config = IgniteCacheConfig(gateway)
                cache_config.set_name(cache_name)
                cache_config.set_group_name('test_group')
                cache_config.set_cache_mode('replicated')
                cache_config.set_atomicity_mode('transactional')
                cache_config.set_write_synchronization_mode('full_sync')
                cache_config.set_affinity(False, 32)

                ignite.getOrCreateCache(cache_config.get_config_object())
                operation = create_streamer_operation(cache_name, 1, 100001,
                                          value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                          allow_overwrite=True)
                operation.evaluate()

            # Start additional node and add it to topology to trigger rebalance.
            node_id = list(self.ignite.add_additional_nodes(self.get_server_config(), 1))
            self.ignite.set_node_option('*', 'jvm_options', self.get_default_jvm_options())
            self.ignite.start_additional_nodes(node_id)
            util_sleep_for_a_while(5)
            self.cu.set_current_topology_as_baseline()
            util_sleep_for_a_while(2)

            # Destroy cache.
            ignite.cache(cache_to_test).destroy()

            caches = piclient.get_ignite().cacheNames().toArray()
            log_print(caches)

        tiden_assert_equal(0, self.ignite.find_exception_in_logs('java.lang.NullPointerException'),
                           "# Exception in log")

    # @with_setup('setup_pitr_test', 'teardown_pitr_test')
    # def test_ign_9660(self):
    #     # self.util_copy_test_tools_to_libs()
    #     self.start_grid()
    #
    #     with PiClient(self.ignite, self.client_config) as piclient:
    #         self.load_data_with_streamer_piclient(value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)
    #
    #         self.ignite.add_additional_nodes(self.server_config, 1)
    #         self.ignite.set_node_option('*', 'jvm_options', self.get_default_jvm_options())
    #         self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())
    #
    #         cache_under_test = IgniteCache("cache_group_3_088")
    #         print_blue("Create query")
    #         query = piclient.gateway.jvm.org.apache.ignite.cache.query \
    #             .SqlQuery("AllTypesIndexed",
    #                       'select B.* from "cache_group_3_088".AllTypesIndexed as B '
    #                       'inner join "cache_group_3_084".AllTypesIndexed '
    #                       'as A on B.longCol = A.longCol '
    #                       'where A.longCol = 1')
    #         query.setDistributedJoins(True)
    #
    #         print_blue("Get results of the query")
    #         cache_under_test.cache.query(query).getAll()

    def preloading(self, preloading_size=None):
        self.load_data_with_streamer()

    # @attr('debug')
    # def test_gg_13689(self):
    #     """
    #     [IGNITE-7912] control.sh script should show used WAL-segments
    #     :return:
    #     """
    #     pass
    #
    # @attr('debug')
    # def test_gg_13647(self):
    #     """
    #     [IGNITE-7090] Semaphore Stuck when no acquirers to assign permit
    #     :return:
    #     """
    #     pass
    #
    # @attr('debug')
    # def test_gg_13657(self):
    #     """
    #     Partition map hang in incorrect state when backup filter is assigned
    #     :return:
    #     """
    #     pass
    #
    # @attr('debug')
    # def test_gg_13660(self):
    #     """
    #     Backport [GG-13552] Strategy calculation depends on order of receiving compute results
    #     :return:
    #     """
    #
    # @attr('debug')
    # def test_ign_10082(self):
    #     """
    #     [IGNITE-8049] Limit the number of operation cycles in B+Tree
    #     :return:
    #     """

    @test_case_id('203978')
    @attr('regression_IGN', 'IGN-13645', 'mute')
    @with_setup(setup_test, teardown_test)
    # @known_issue('IGN-13645')
    def test_checkpoint_behaviour_under_starvation(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-13645
        It checks transaction behaviour when checkpoints is blocked by ForkJoinPool#commonPool
        In old version transaction can't be finished.
        """
        self.start_grid()
        try:
            PiClient.read_timeout = 60
            with PiClient(self.ignite, self.get_client_config(), jvm_options=None, nodes_num=1, clear=False):
                async_starvation_operation = create_async_operation(create_starvation_in_fork_join_pool)
                async_starvation_operation.evaluate()
                util_sleep_for_a_while(3, 'Wait until operation started')
                self.load_data_with_streamer(end_key=1000, nodes_num=1, new_instance=True)
            self.cu.control_utility('--cache', 'idle_verify')
        finally:
            PiClient.read_timeout = 3600

    @test_case_id('186435')
    @require(min_ignite_version='2.5.6')
    @attr('regression_IGN', 'IGN-13476', 'jmx_exclude')
    @with_setup(setup_test, teardown_test)
    def test_exclude_node_via_jmx(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-13645
        It checks an ability to stop node via jvm
        """
        test_node = 2
        self.start_grid()
        jmx_node_id = self.ignite.jmx.start_utility()
        with PiClient(self.ignite, self.get_client_config(),
                      nodes_num=1, new_instance=True) as piclient:
            cache_name = piclient.get_ignite().cacheNames().toArray()[0]
            lrt_operations = PiClientIgniteUtils.launch_transaction_operations(cache_name,
                                                                               start_key=1000, end_key=1010)
            util_sleep_for_a_while(5, msg='wait LRT start')
            log_print('Call func exclude server node {}'.format(test_node))
            self.ignite.jmx.evaluate_operation(
                1,
                'SPIs',
                'TcpDiscoverySpi',
                'excludeNode',
                self.ignite.nodes[test_node]['id']
            )
            PiClientIgniteUtils.release_transactions(lrt_operations)
            with PiClient(self.ignite, self.get_client_config(),
                          nodes_num=1, new_instance=True):
                get_log = self.ignite.grep_all_data_from_log(
                    'client',
                    'Local node \[ID=.*, order',
                    'Local node \[ID=(.*), order',
                    'id',
                )
                for node in get_log.keys():
                    self.ignite.jmx.evaluate_operation(
                        1,
                        'SPIs',
                        'TcpDiscoverySpi',
                        'excludeNode',
                        ''.join(get_log[node])
                    )
                util_sleep_for_a_while(20, 'wait after client kill')
                self.cu.control_utility('--cache', 'idle_verify')
                self.su.snapshot_utility('SNAPSHOT', '-type=full')
                for node in [node for node in self.ignite.nodes.keys() if node != jmx_node_id]:
                    if node != test_node:
                        tiden_assert(self.ignite.check_node_is_alive(node), 'expected Node {} alive'.format(node))
                    else:
                        tiden_assert(not self.ignite.check_node_is_alive(node), 'expected Node {} dead'.format(node))

    @attr('regression_IGN', 'IGN-13493')
    @with_setup(setup_test, teardown_test)
    def test_stop_node_and_return_back_on_same_port(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-13493
        It checks cluster behaviour when one of node is unreachable because of network issue and at the same time
        another node starts on this remote host with same consistent id and ports
        """
        enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                            self.config['environment']['server_hosts'][-1])
        not_default_context = self.create_test_context('config_with_custom_communication_port')
        not_default_context.add_config('server.xml', 'server_config_with_custom_communication_port.xml')
        not_default_context.add_config('client.xml', 'client_config_with_custom_communication_port.xml')
        default_context_vars = self.contexts['default'].get_context_variables()
        not_default_context.add_context_variables(
            **default_context_vars,
            communicationSpiLocalPort=47205,
            failureDetectionTimeout=100000,
            clientFailureDetectionTimeout=100000,
            connectTimeout=100000,
            maxConnectTimeout=120000,
            idleConnectionTimeout=120000,
        )
        self.rebuild_configs()
        not_default_context.build_and_deploy(self.ssh)

        try:
            self.set_current_context('config_with_custom_communication_port')
            self.start_grid(disable_host_list=[self.config['environment']['server_hosts'][-1]])
            inner_PiClient_jvm_options = self.get_default_jvm_options()
            with PiClient(self.ignite, "server_config_with_custom_communication_port.xml", nodes_num=1,
                          jvm_options=inner_PiClient_jvm_options, new_instance=True, consistent_id='node_1_20001',
                          host=self.config['environment']['server_hosts'][-1], clear=False):
                util_sleep_for_a_while(10, 'wait after server entered topology')
                self.cu.set_current_topology_as_baseline()
                util_sleep_for_a_while(10, 'wait after BLT change')
                disable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                                                 self.config['environment']['server_hosts'][-1])

            def enable_connection(self):
                sleep(5)
                enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                                                 self.config['environment']['server_hosts'][-1])

            t = Thread(target=enable_connection, args=(self,))
            t.start()
            with PiClient(self.ignite, "server_config_with_custom_communication_port.xml", nodes_num=1,
                          jvm_options=inner_PiClient_jvm_options, new_instance=True,
                          host=self.config['environment']['server_hosts'][-1], consistent_id='node_1_20001'):
                util_sleep_for_a_while(30, 'wait')
        finally:
            enable_connections_between_hosts(self.ssh, self.config['environment']['server_hosts'][0],
                               self.config['environment']['server_hosts'][-1])
            self.set_current_context('default')

