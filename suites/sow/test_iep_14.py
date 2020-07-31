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

from random import randint, choice

from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_account_runner_operation, \
    create_streamer_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.case.singlegridzootestcase import SingleGridZooTestCase
from tiden.logger import get_logger
from tiden.error_maker import IOErrorMaker
from tiden.tidenexception import TidenException
from tiden.util import is_enabled, attr, test_case_id, with_setup, print_green, util_sleep_for_a_while, require
from tiden.assertions import *

from re import search, match
from os.path import dirname
from tiden.testconfig import test_config
from os import path


class Iep14Resources:
    BREAK_PARTITION_EXCEPTION = 'Caused by: java.io.IOException: Failed to verify'
    BREAK_CRC_EXCEPTION = 'class org.apache.ignite.internal.processors.cache.persistence.wal.crc.' \
                          'IgniteDataIntegrityViolationException'


class TestIep14(SingleGridZooTestCase):
    max_key = 1001
    io_maker = None
    small_storage = '/storage/tmpfs'
    full_threads_list = [
        'tcp-disco-srvr',
        'tcp-disco-msg-worker',
        'tcp-comm-worker',
        'grid-nio-worker-tcp-comm',
        'exchange-worker',
        'sys-stripe',
        'grid-timeout-worker',
        'db-checkpoint-thread',
        # 'wal-file-archiver',
        'wal-write-worker',
        'file-decompressor',
        'ttl-cleanup-worker',
        'nio-acceptor-tcp-comm'
    ]

    def setup(self):
        default_context = self.contexts['default']
        default_context.add_context_variables(
            snapshots_enabled=True,
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            compaction_enabled=is_enabled(self.config.get('compaction_enabled')),
        )
        super().setup()

        self.io_maker = IOErrorMaker(self.ignite, self.ssh)
        if is_enabled(self.config.get('zookeeper_enabled')):
            self.start_zookeeper()
            default_context = self.contexts['default']
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString()
            )
            default_context.build_and_deploy(self.ssh)

        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestIEP_14]')

    def teardown(self):
        if is_enabled(self.config.get('zookeeper_enabled')):
            self.stop_zookeeper()

    def setup_testcase(self):
        self.setup_testcase_no_grid_start()
        self.start_grid(activate_on_particular_node=1)
        self.load_data_with_streamer()
        log_print(repr(self.ignite), color='debug')

    def setup_testcase_with_jmx(self):
        self.setup_testcase_no_grid_start()
        self.ignite.jmx.start_utility()

    def setup_testcase_no_grid_start(self):
        self.logger.info('TestSetup is called')

        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()
            default_context = self.contexts['default']
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString()
            )
            default_context.build_and_deploy(self.ssh)

        self.set_current_context()
        self.ignite.set_activation_timeout(20)
        self.ignite.set_snapshot_timeout(120)
        self.util_copy_piclient_model_to_libs()
        self.ignite.set_node_option('*', 'jvm_options', ['-DIGNITE_TEST_FEATURES_ENABLED=true'])

    def teardown_testcase(self):
        self.logger.info('TestTeardown is called')
        if self.get_context_variable("zookeeper_enabled"):
            self.stop_zookeeper()
        self.ignite.set_activation_timeout(10)
        self.ignite.set_snapshot_timeout(30)
        self.stop_grid_hard()
        self.cleanup_lfs()
        self.util_cleanup_on_all_nodes(self.small_storage)
        self.remove_additional_nodes()
        self.reset_cluster()
        self.ignite.set_grid_name(1)
        self.set_current_context()
        log_print(repr(self.ignite), color='debug')

    def teardown_testcase_with_jmx(self):
        if self.ignite.jmx.is_started():
            self.ignite.jmx.kill_utility()
        self.teardown_testcase()

    @attr('iep-14')
    @test_case_id('91955')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    @require(test_config.zookeeper_enabled)
    def test_failure_handler_during_node_segmentation(self):
        """
        This test checks that node segmentation (with Zookeeper discovery) handled with default FailureHandler.
        To test this we block in/out traffic from node_under_test to Zookeeper nodes. This should cause segmentation
        for node_under_test.

        NOTE: this works only with Zookeeper Discovery Spi.
        """
        if not is_enabled(self.config.get('zookeeper_enabled')):
            raise TidenException('This test required Zookeeper Discovery Spi')

        # to be able to segment node you should have nodes on hosts different from clients hosts.
        # Otherwise client will fail.
        if len(self.config['environment']['server_hosts']) <= len(self.config['environment']['client_hosts']):
            raise TidenException('You should have more server hosts than clients')

        log_print(repr(self.ignite), color='debug')
        self.start_grid()
        self.load_data_with_streamer()
        sum_1 = self.calculate_sum_by_field()
        log_print(sum_1, color='green')
        zk_hosts = [host.split(':')[0] for host in self.zoo._get_zkConnectionString().split(',')]
        node_under_test = self.util_pick_random_node(exclude_client_hosts=True)
        host_under_test = self.ignite.nodes[node_under_test]['host']
        try:
            with PiClient(self.ignite, self.get_client_config()) as piclient:
                cache_names = piclient.get_ignite().cacheNames()

                async_operations = []
                for cache_name in cache_names.toArray():
                    async_operation = create_async_operation(create_account_runner_operation,
                                                             cache_name, 1, 1000, 0.5,
                                                             delay=3,
                                                             run_for_seconds=20)
                    async_operations.append(async_operation)
                    async_operation.evaluate()

                # disable connection from/to node under test to Zookeeper hosts
                self._disable_connections(zk_hosts, host_under_test)

                util_sleep_for_a_while(10)

                for async_op in async_operations:
                    try:
                        async_op.getResult()
                    except Exception as e:
                        log_print(e, color='red')

                util_sleep_for_a_while(10)

                str_to_check = 'Critical system error detected. Will be handled accordingly to configured handler'
                search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
                tiden_assert('StopNodeFailureHandler' in search_result,
                             'String \'%s\' could be found in node log' % str_to_check)

            log_print('Node %s stopped with StopNodeFailureHandler' % node_under_test, color='debug')
            sum_2 = self.calculate_sum_by_field()
            log_print(sum_2, color='green')
            tiden_assert_equal(sum_1, sum_2, 'Sums are not the same!!!')
        finally:
            self._enable_connections(zk_hosts, host_under_test)

    @attr('iep-14')
    @test_case_id('91952')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_no_free_space_on_wal_with_jvm_crash(self):
        """
        Based on https://gridgain.freshdesk.com/a/tickets/7064
        Check this flag helps: -DIGNITE_WAL_MMAP=false
        """
        server_config = 'server_with_custom_db_path.xml'
        node_under_test = self.util_pick_random_node()
        host_under_test = self.ignite.nodes[node_under_test]['host']

        custom_context = self.create_test_context('server_with_custom_db_path')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            wal_path=True,
            wal_path_val=self.small_storage
        )
        custom_context.set_client_result_config("client_with_custom_db_path.xml")
        custom_context.set_server_result_config("server_with_custom_db_path.xml")

        custom_context.build_and_deploy(self.ssh)

        # self.set_current_context('server_with_custom_db_path')

        # cleanup storage
        self.util_cleanup_storage(host_under_test, self.small_storage)

        # Update configuration for node_under_test
        self.ignite.set_node_option(node_under_test, 'config', server_config)

        # Allocate 48M from 50 to get IOException on preloading
        self.io_maker.allocate_disc_space_on_node(node_under_test, self.small_storage, '96M')

        # Start grid and make preloading
        log_print(repr(self.ignite), color='debug')
        self.start_grid()
        self.load_data_with_streamer()

        checksum_1 = self.calc_checksums_on_client()

        str_to_find = 'No space left on device'
        search_result = self.find_in_node_log(str_to_find, node_id=node_under_test)
        log_print(search_result)

        tiden_assert(str_to_find in search_result,
                     'String \'%s\' could be found in node log' % str_to_find)

        checksum_2 = self.calc_checksums_on_client()
        log_print(checksum_2, color='green')
        self.stop_grid()

        tiden_assert_equal(checksum_1, checksum_2, 'Checksums are the same')

    @attr('iep-14', 'thread_termination')
    @test_case_id('91954')
    @with_setup(setup_testcase_with_jmx, teardown_testcase_with_jmx)
    def test_emulate_thread_termination_StopNodeOrHaltFailureHandler(self):
        """
        Test StopNodeOrHaltFailureHandler Failure Handler handles situation when thread terminated over JMX.
        To be able to use this functionality this property should be added: '-DIGNITE_TEST_FEATURES_ENABLED=true

        If thread_filter is set used threads from this filter. Otherwise all threads are tested.
        """
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.StopNodeOrHaltFailureHandler'

        # Old version. Now context is used.
        # self.util_prepare_remote_config(server_config, failure_handler=failure_handler)
        # failure_handler = 'org.apache.ignite.failure.StopNodeOrHaltFailureHandler'

        # generate config with proper failure handler
        default_context = self.contexts['default']
        default_context.add_context_variables(
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        default_context.build_and_deploy(self.ssh)

        full_test_threads = self.util_get_threads_from_cluster_over_jmx(node_under_test)

        full_threads_list = self.util_check_all_threads_presented(self.full_threads_list, full_test_threads)
        thread_filter = full_threads_list[randint(0, len(full_threads_list)-1)]

        if thread_filter:
            test_threads = self.util_get_threads_to_test(full_test_threads, thread_filter)
        else:
            test_threads = full_test_threads

        log_print('Going to run tests for threads %s' % test_threads, color='green')
        for thread_under_test in test_threads:
            self.util_test_thread_termination(thread_under_test, node_under_test=node_under_test,
                                              thread_mask=thread_filter)

    @attr('iep-14_do_not_include', 'thread_termination')
    @with_setup(setup_testcase_with_jmx, teardown_testcase_with_jmx)
    def test_emulate_thread_termination_RestartProcessFailureHandler(self):
        """
        Test RestartProcessFailureHandler Failure Handler handles situation when thread terminated over JMX.
        To be able to use this functionality this property should be added: '-DIGNITE_TEST_FEATURES_ENABLED=true

        If thread_filter is set used threads from this filter. Otherwise all threads are tested.
        """
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.RestartProcessFailureHandler'

        # generate config with proper failure handler
        default_context = self.contexts['default']
        default_context.add_context_variables(
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        default_context.build_and_deploy(self.ssh)

        full_test_threads = self.util_get_threads_from_cluster_over_jmx(node_under_test)
        thread_filter = ['wal-file-decompressor']

        if thread_filter:
            test_threads = self.util_get_threads_to_test(full_test_threads, thread_filter)
        else:
            test_threads = full_test_threads

        for thread_under_test in test_threads:
            self.util_test_thread_termination(thread_under_test, node_under_test=node_under_test,
                                              thread_mask=thread_filter)

    @attr('thread_termination', 'current')
    @test_case_id('91953')
    @with_setup(setup_testcase_with_jmx, teardown_testcase_with_jmx)
    def test_emulate_thread_termination_StopNodeFailureHandler(self):
        """
        Test StopNodeFailureHandler Failure Handler handles situation when thread terminated over JMX.
        To be able to use this functionality this property should be added: '-DIGNITE_TEST_FEATURES_ENABLED=true

        If thread_filter is set used threads from this filter. Otherwise all threads are tested.
        """
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.StopNodeFailureHandler'

        # generate config with proper failure handler
        default_context = self.contexts['default']
        default_context.add_context_variables(
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        default_context.build_and_deploy(self.ssh)
        self.ignite.set_node_option('*', 'config', self.get_server_config())

        full_test_threads = self.util_get_threads_from_cluster_over_jmx(node_under_test)

        full_threads_list = self.util_check_all_threads_presented(self.full_threads_list, full_test_threads)
        thread_filter = full_threads_list[randint(0, len(full_threads_list)-1)]

        if thread_filter:
            test_threads = self.util_get_threads_to_test(full_test_threads, thread_filter)
        else:
            test_threads = full_test_threads

        log_print('Going to run tests for threads %s' % test_threads, color='green')
        for thread_under_test in test_threads:
            self.util_test_thread_termination(thread_under_test, node_under_test=node_under_test,
                                              thread_mask=thread_filter)

    @attr('do_not_include')
    @with_setup(setup_testcase_with_jmx, teardown_testcase_with_jmx)
    def test_emulate_thread_termination_NoOpFailureHandler(self):
        """
        Do not run this test in suite as the handler NoOpFailureHandler do nothing. So node is just halted.
        To be able to use this functionality this property should be added: '-DIGNITE_TEST_FEATURES_ENABLED=true
        """
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.NoOpFailureHandler'

        # generate config with proper failure handler
        default_context = self.contexts['default']
        default_context.add_context_variables(
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        default_context.build_and_deploy(self.ssh)
        self.ignite.set_node_option('*', 'config', self.get_server_config())

        full_test_threads = self.util_get_threads_from_cluster_over_jmx(node_under_test)
        thread_filter = ['wal-file-decompressor']

        if thread_filter:
            test_threads = self.util_get_threads_to_test(full_test_threads, thread_filter)
        else:
            test_threads = full_test_threads

        for thread_under_test in test_threads:
            self.util_test_thread_termination(thread_under_test, thread_mask=thread_filter)

    @attr('deprecated')
    @with_setup(setup_testcase_with_jmx, teardown_testcase_with_jmx)
    def test_emulate_thread_termination_StopNodeFailureHandler_thread_id(self):
        """
        This test do the same job as test with JMX. This test is not used any more as all thread are now available
        over JMX. So this mechanism is deprecated.
        """
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.StopNodeFailureHandler'
        test_threads = ['file-decompressor']

        # generate config with proper failure handler
        default_context = self.contexts['default']
        default_context.add_context_variables(
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        default_context.build_and_deploy(self.ssh)

        for thread_under_test in test_threads:
            self.util_test_thread_termination_by_id(thread_under_test, node_under_test=node_under_test)

    def util_test_thread_termination(self, thread_to_terminate, node_under_test=2, thread_mask=[]):
        """
        Start grid and make initial load.
        Calculating checksum and sums over caches.
        Start load with AccountRunner.
        Terminate thread using JMX and check exception in logs.
        Wait for Account Runner finish it's work.
        Calculate sums again and check it's the same as before.
        Start node that was stopped by failure handler.
        Calculate sums again and check it's the same as before.

        :param thread_to_terminate: Thread name that should be terminated.
        :param node_under_test: Node ID on which thread is terminated.
        :param thread_mask: Mask to filter threads in case threads id's have changed.
        :return:
        """
        self.ignite.set_grid_name(thread_to_terminate)
        self.delete_lfs()
        # Start grid and make preloading
        self.start_grid()

        self.load_data_with_streamer()

        balances_before = self.calculate_sum_by_field()
        log_print(balances_before)

        checksum_before = self.calculate_sum_by_field()
        log_print(checksum_before, color='green')
        self.ignite.wait_for_topology_snapshot(client_num=0)

        log_print(repr(self.ignite), color='debug')

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache_names = piclient.get_ignite().cacheNames()

            async_operations = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_account_runner_operation,
                                                         cache_name, 1, 1000, 0.5,
                                                         delay=3,
                                                         run_for_seconds=20)
                async_operations.append(async_operation)
                async_operation.evaluate()

            util_sleep_for_a_while(5)

            log_print("Going to terminate worker %s on node %s" % (thread_to_terminate, node_under_test), color='green')

            current_threads = self.util_get_threads_from_cluster_over_jmx_on_running_cluster(node_under_test)
            # in case there are threads with another id's in cluster, just pick another one thread with the same mask
            if thread_to_terminate not in current_threads:
                log_print('FAIL to find thread %s in list %s' % (thread_to_terminate, current_threads), color='red')
                test_threads = self.util_get_threads_to_test(current_threads, thread_mask)
                thread_to_terminate = test_threads[0]
                log_print('Thread replaced to %s' % thread_to_terminate, color='red')

            op_result = self.ignite.jmx.evaluate_operation(
                node_under_test, 'Kernal', 'WorkersControlMXBean', 'terminateWorker', thread_to_terminate
            )

            log_print("got '%s'" % op_result, color='green')

            str_to_find = 'Critical system error detected. Will be handled accordingly to configured handler'
            search_result = self.find_in_node_log(str_to_find, node_id=node_under_test)
            log_print(search_result)

            tiden_assert(thread_to_terminate in search_result,
                         'String \'%s\' could be found in node log' % thread_to_terminate)

            for async_op in async_operations:
                try:
                    async_op.getResult()
                except Exception as e:
                    log_print(e, color='red')

            util_sleep_for_a_while(10)

            self.cu.control_utility('--cache', 'idle_verify', node=1)

            checksum_after = self.calc_checksums_on_client()
            util_sleep_for_a_while(2)
            checksum_after_1 = self.calc_checksums_on_client()

            tiden_assert_equal(checksum_after_1, checksum_after, 'Checksums after/Checksums after')
            balances_after = self.calculate_sum_by_field()

            balances_after_1 = self.calculate_sum_by_field()

            self.util_compare_dicts(balances_after_1, balances_after)
            self.util_compare_dicts(balances_before, balances_after)

            tiden_assert_equal(balances_after_1, balances_after, 'Balances after/Balances after')

            # If everything ok, balances should be the same and checksum - not (as there were transfers)
            tiden_assert_equal(balances_before, balances_after, 'Balance before/balance after')
            tiden_assert(checksum_before != checksum_after, 'Checksums are not the same')

        # Stop grid
        self.ignite.wait_for_topology_snapshot(client_num=0, exclude_nodes_from_check=[node_under_test])
        util_sleep_for_a_while(3)
        # Starting node to check node can be run after crush
        self.ignite.start_node(node_under_test, force=True)
        util_sleep_for_a_while(10)
        balances_after_3 = self.calculate_sum_by_field()
        log_print(balances_after_3)
        self.stop_grid()

    @attr('iep-14')
    @test_case_id('91950')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_no_free_space_on_db(self):
        """
        This test checks the failure handler handles the situation when there is no free space on the storage used for
        storagePath. Check server.xml config file for details.

        To run this test one of the nodes should be started on a server with mount storage /storage/tmpfs.
        """
        server_config = 'server_with_custom_db_path.xml'

        node_under_test = self.util_pick_random_node()
        host_under_test = self.ignite.nodes[node_under_test]['host']
        small_storage = '/storage/tmpfs'

        custom_context = self.create_test_context('server_with_custom_db_path')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            db_storage_path=True,
            db_storage_path_val=small_storage,
            custom_cp_freq=30000
        )
        custom_context.set_client_result_config("client_with_custom_db_path.xml")
        custom_context.set_server_result_config("server_with_custom_db_path.xml")

        custom_context.build_and_deploy(self.ssh)

        # cleanup storage
        self.util_cleanup_storage(host_under_test, small_storage)

        self.ignite.set_node_option(node_under_test, 'config', server_config)

        # Start grid and make preloading
        log_print(self.ignite, color='debug')
        self.start_grid()
        self.load_data_with_streamer(end_key=30000)

        util_sleep_for_a_while(35, 'wait for checkpoint')
        str_to_check = 'java.io.IOException: No space left on device'
        search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)

        tiden_assert(str_to_check in search_result, 'String \'%s\' could be found in node log' % str_to_check)

        str_to_check = 'Critical system error detected. Will be handled accordingly to configured handler'
        search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
        # StopNodeOrHaltFailureHandler is the default failure handler, so if there was not set any, we are expecting
        # this one will be called
        tiden_assert('StopNodeOrHaltFailureHandler' in search_result,
                     'String \'%s\' could be found in node log' % str_to_check)

    @attr('iep-14')
    @test_case_id('91951')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_no_free_space_on_wal(self):
        """
        This test checks the failure handler handles the situation when there is no free space on the storage used for
        wal archive path. Check server.xml config file for details.

        IMPORTANT!!! we can not emulate the same case for wal path as node preallocate all wal segments on start,
        so node will create all wal files or will fail on start. But for wal archive we can emulate this behaviour.

        To run this test one of the nodes should be started on a server with mount storage /storage/tmpfs.
        """
        server_config = 'server_with_custom_db_path.xml'
        node_under_test = self.util_pick_random_node()
        host_under_test = self.ignite.nodes[node_under_test]['host']

        custom_context = self.create_test_context('server_with_custom_db_path')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            wal_path=True,
            wal_path_val=self.small_storage
        )
        custom_context.set_client_result_config("client_with_custom_db_path.xml")
        custom_context.set_server_result_config("server_with_custom_db_path.xml")

        custom_context.build_and_deploy(self.ssh)

        # cleanup storage
        self.util_cleanup_storage(host_under_test, self.small_storage)
        self.ignite.set_node_option(node_under_test, 'config', server_config)

        # Allocate 48M from 50 to get IOException on preloading
        file_path = self.io_maker.allocate_disc_space_on_node(node_under_test, self.small_storage, '88M')

        # Start grid and make preloading
        log_print('Node under test is %s' % node_under_test, color='green')
        self.start_grid()
        self.load_data_with_streamer()

        checksum_1 = self.calc_checksums_on_client()
        log_print(checksum_1, color='green')

        str_to_check = 'Critical system error detected. Will be handled accordingly to configured handler'
        search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
        tiden_assert('StopNodeOrHaltFailureHandler' in search_result,
                     'String \'%s\' could be found in node log' % str_to_check)

        self.io_maker.cleanup_disc_space_on_node(node_under_test, file_path)
        self.ignite.start_node(node_under_test, force=True)

        checksum_2 = self.calc_checksums_on_client()
        log_print(checksum_2, color='green')
        self.stop_grid()

        tiden_assert_equal(checksum_1, checksum_2, 'Checksums are the same')

    @attr('iep-14')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_no_free_space_on_snapshot_creation(self):
        """
        This test checks if snapshot operation fails cause of 'java.io.IOException: No space left on device'
        cluster keeps working and next snapshot operation is successful if there is enough space.

        Scenario:
        1. Cluster is up and running.
        2. Allocate space on storage to get java.io.IOException: No space left on device during snapshot creation
        on some node.
        3. Load data into caches.
        4. Try to create snapshot. Snapshot operation should fail.
        5. Cleanup storage.
        6. Create snapshot. Creation should be successful.
        7. Try to restore snapshot. Restore should be successful.
        """
        server_config = 'server_with_custom_db_path.xml'
        node_under_test = self.util_pick_random_node()
        host_under_test = self.ignite.nodes[node_under_test]['host']

        custom_context = self.create_test_context('server_with_custom_db_path')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            change_snapshot_path=True,
            change_snapshot_path_value=self.small_storage
        )
        custom_context.set_client_result_config("client_with_custom_db_path.xml")
        custom_context.set_server_result_config("server_with_custom_db_path.xml")

        custom_context.build_and_deploy(self.ssh)

        # cleanup storage
        self.util_cleanup_storage(host_under_test, self.small_storage)
        self.ignite.set_node_option(node_under_test, 'config', server_config)

        # Allocate 88 from 100 to get IOException on snapshot operation
        file_path = self.io_maker.allocate_disc_space_on_node(node_under_test, self.small_storage, '88M')

        # Start grid and make preloading
        log_print('Node under test is %s' % node_under_test, color='green')
        self.start_grid()
        self.load_data_with_streamer(end_key=300)

        checksum_1 = self.calc_checksums_on_client()
        log_print(checksum_1, color='green')

        expected_error = ['Command \[SNAPSHOT\] failed with error: 4730 - storage device is full']
        self.su.snapshot_utility('snapshot', '-type=full', all_required=expected_error)

        util_sleep_for_a_while(10)

        self.io_maker.cleanup_disc_space_on_node(node_under_test, file_path)

        self.su.snapshot_utility('snapshot', '-type=full')

        checksum_2 = self.calc_checksums_on_client()
        log_print(checksum_2, color='green')

        self.su.snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(1))
        self.stop_grid()

        tiden_assert_equal(checksum_1, checksum_2, 'Checksums are the same')

    @attr('iep-14')
    @test_case_id('91948')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_access_denied_errors_on_wal(self):
        """
        This test emulates IO errors in WAL path. In this particular test we emulate scenario when file system
        goes to read-only state. In this case (AccessDeniedException) should be handled by Failure Handler.
        """
        files, node_idx = None, None
        self.server_config = 'server_with_custom_failure_handler.xml'
        node_under_test = self.util_pick_random_node(include_coordinator=True)
        failure_handler = 'org.apache.ignite.failure.StopNodeOrHaltFailureHandler'

        custom_context = self.create_test_context('server_with_custom_failure_handler')
        default_context_vars = self.contexts['default'].get_context_variables()
        log_print(default_context_vars)
        custom_context.add_context_variables(
            **default_context_vars,
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        custom_context.set_client_result_config("client_with_custom_failure_handler.xml")
        custom_context.set_server_result_config("server_with_custom_failure_handler.xml")

        custom_context.build_and_deploy(self.ssh)

        self.ignite.set_node_option('*', 'config', self.server_config)

        # Start grid and make preloading
        self.start_grid()
        self.load_data_with_streamer()

        try:
            node_idx, files = self.io_maker.make_wal_files_read_only(node_id=node_under_test)
            if len(files) > 1:
                node_dir = dirname(files[0])
                print_files = [file.replace(node_dir, '') for file in files]
                log_print('On node = %s\nWAL directory: %s\nWAL files: %s' % (node_idx, node_dir, print_files),
                          color='blue')
            else:
                log_print('On node = %s', color='blue')
                log_print(files, color='blue')

            util_sleep_for_a_while(10)

            self.load_data_with_streamer(end_key=3000)

            str_to_check = 'java.nio.file.AccessDeniedException'
            search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
            log_print(search_result, color='green')

            tiden_assert(str_to_check in search_result,
                         'String \'%s\' could be found in node log' % str_to_check)

            checksum_2 = self.calc_checksums_on_client()
            log_print(checksum_2, color='green')
            self.stop_grid()
        finally:
            self.io_maker.fix_lfs_access(node_idx, files)

    @attr('iep-14')
    @test_case_id('91949')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_access_denied_on_pagestore(self):
        """
        This test emulates IO errors in page store. In this particular test we emulate scenario when file system
        goes to read-only state. In this case (AccessDeniedException) should be handled by Failure Handler.
        """
        remote_path = None
        node_idx = None
        self.server_config = 'server_with_custom_failure_handler.xml'
        node_under_test = self.util_pick_random_node(include_coordinator=True)
        failure_handler = 'org.apache.ignite.failure.StopNodeOrHaltFailureHandler'

        custom_context = self.create_test_context('server_with_custom_failure_handler')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            failure_handler=True,
            failure_handler_val=failure_handler,
            custom_cp_freq=30000
        )
        custom_context.set_client_result_config("client_with_custom_failure_handler.xml")
        custom_context.set_server_result_config("server_with_custom_failure_handler.xml")

        custom_context.build_and_deploy(self.ssh)

        self.ignite.set_node_option('*', 'config', self.server_config)

        self.start_grid()
        self.load_data_with_streamer()

        try:
            node_idx, remote_path = self.io_maker.make_cache_folder_read_only(node_id=node_under_test)

            self.load_data_with_streamer(end_key=30000)
            util_sleep_for_a_while(35, 'wait for checkpoint')
            str_to_check = 'java.nio.file.AccessDeniedException'
            search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
            log_print(search_result, color='green')

            tiden_assert(str_to_check in search_result,
                         'String \'%s\' could be found in node log' % str_to_check)

        finally:
            self.io_maker.fix_lfs_access(node_idx, remote_path)

    @attr('iep-14')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_io_errors_on_binary_meta(self):
        """
        This test emulates IO errors in binary meta store. In this particular test we emulate scenario when file system
        goes to read-only state. In this case (AccessDeniedException) should be handled by Failure Handler.

        NOTE: fixed in 2.5.5
        """
        remote_path = None
        node_idx = None
        self.server_config = 'server_with_custom_failure_handler.xml'
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.StopNodeOrHaltFailureHandler'

        custom_context = self.create_test_context('server_with_custom_failure_handler')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        custom_context.set_client_result_config("client_with_custom_failure_handler.xml")
        custom_context.set_server_result_config("server_with_custom_failure_handler.xml")

        custom_context.build_and_deploy(self.ssh)

        self.ignite.set_node_option('*', 'config', self.server_config)

        self.start_grid()
        self.load_data_with_streamer(value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)

        try:
            node_idx, remote_path = self.io_maker.make_binary_meta_read_only(node_id=node_under_test)

            # Load data with another value_type to trigger binary_meta files creation
            self.load_data_with_streamer(start_key=1001, end_key=1050,
                                         value_type=ModelTypes.VALUE_ALL_TYPES.value)

            str_to_check = 'java.nio.file.AccessDeniedException'
            search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
            log_print(search_result, color='green')

            tiden_assert('StopNodeOrHaltFailureHandler' in search_result,
                         'String \'%s\' could be found in node log' % str_to_check)

        finally:
            self.io_maker.fix_lfs_access(node_idx, remote_path)

    @attr('iep-14_do_not_include')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_emulate_io_errors_on_metastorage(self):
        """
        We are trying to emulate io error in metastore. But this does not work for now.
        There is no way to 'touch' metastore when grid is started. The only way is during activation / deactivation
        process.
        :return:
        """

        remote_path = None
        node_idx = None
        self.server_config = 'server_with_custom_failure_handler.xml'
        node_under_test = self.util_pick_random_node()
        failure_handler = 'org.apache.ignite.failure.StopNodeOrHaltFailureHandler'

        custom_context = self.create_test_context('server_with_custom_failure_handler')
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            failure_handler=True,
            failure_handler_val=failure_handler
        )
        custom_context.set_client_result_config("client_with_custom_failure_handler.xml")
        custom_context.set_server_result_config("server_with_custom_failure_handler.xml")

        custom_context.build_and_deploy(self.ssh)

        self.ignite.set_node_option('*', 'config', self.server_config)

        self.start_grid()
        self.load_data_with_streamer()

        try:
            node_idx, remote_path = self.io_maker.make_metastorage_read_only(node_id=node_under_test)

            log_print("Going to start additional node", color='green')
            self.ignite.add_additional_nodes(self.server_config, 1)
            self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())
            self.cu.set_current_topology_as_baseline()

            self.cu.deactivate()

            str_to_check = 'java.nio.file.AccessDeniedException'
            search_result = self.find_in_node_log('java.nio.file.AccessDeniedException', node_id=node_under_test)
            log_print(search_result, color='green')

            tiden_assert('StopNodeOrHaltFailureHandler' in search_result,
                         'String \'%s\' could be found in node log' % str_to_check)
        finally:
            self.io_maker.fix_lfs_access(node_idx, remote_path)

    @attr('SOW4', 'IS_3_1_part_bin', 'iep-14')
    @with_setup(setup_testcase, teardown_testcase)
    def test_sow4_3_1_break_bin_partition(self):
        """
        Test options --check-crc and --exclude-caches for idle_verify command.
        :return:
        """
        seek = 8192
        cache_name = 'test_cache_group'
        no_conflicts_msg = 'idle_verify check has finished, no conflicts have been found'
        failed_msg = 'idle_verify failed on 1 node'
        crc_failed_msg = 'CRC check of partition: {}, for cache group {} failed.'
        node_under_test = 1
        host = self.ignite.nodes[node_under_test]['host']

        self.cu.control_utility('--cache', 'idle_verify --check-crc')
        tiden_assert(no_conflicts_msg in self.cu.latest_utility_output, 'No conflicts have been found')

        # let's break partition
        node_idx = list(self.ignite.nodes.keys())[0]

        command = [
            'find {}/work/db/{}/cacheGroup-{} -name \'part-*.bin\''.format(self.ignite.nodes[node_idx]['ignite_home'],
                                                                           self.ignite.get_node_consistent_id(node_idx),
                                                                           cache_name)
        ]
        results = self.ssh.exec_on_host(host, command)
        bin_files = [path.basename(f) for f in results[host][0].split('\n') if f]
        m = match('.*-(\d+).bin', bin_files[0])
        partition_id = m.group(1)

        command = [
            'printf "\\x31\\xc0\\xc3" | dd of={}/work/db/{}/cacheGroup-{}/{} bs=1 seek={} count=3 conv=notrunc'.format(
                self.ignite.nodes[node_idx]['ignite_home'],
                self.ignite.get_node_consistent_id(node_idx),
                cache_name,
                bin_files[0],
                seek),
            'printf "\\x31\\xc0\\xc3" | dd of={}/work/db/{}/cacheGroup-{}/{} bs=1 seek={} count=3 conv=notrunc'.format(
                self.ignite.nodes[node_idx]['ignite_home'],
                self.ignite.get_node_consistent_id(node_idx),
                cache_name,
                bin_files[1],
                seek)
        ]
        log_print('command execute on node {} - {}'.format(node_under_test, command), color='green')
        results = self.ssh.exec_on_host(host, command)
        result = ' '.join(results[host])
        log_print('command execute result - {}'.format(result))
        tiden_assert(result != ' ', 'break command execute correct')

        for try_count in range(5):
            util_sleep_for_a_while(5, 'waiting for checkpoint')
            self.cu.control_utility('--cache', 'idle_verify --check-crc')
            output = self.cu.latest_utility_output
            if failed_msg in self.cu.latest_utility_output:
                break

        tiden_assert(failed_msg in self.cu.latest_utility_output, 'Conflict has been found')

        m = search('See log for additional information. (.*)', output)
        if m:
            conflict_file = m.group(1)
            host = self.cu.latest_utility_host
            results = self.ssh.exec_on_host(host, ['cat {}'.format(conflict_file)])
            log_print(results, color='blue')
            tiden_assert(crc_failed_msg.format(partition_id, cache_name) in results[host][0],
                         'CRC failed message \n{} is not found in the output \n{}'.
                         format(crc_failed_msg.format(partition_id, cache_name), results[host][0]))

        else:
            tiden_assert_is_none(False, 'Conflict file is not found in output:\n{}'.format(output))

        # check --exclude-caches option
        self.cu.control_utility('--cache', 'idle_verify --check-crc --exclude-caches {}'.format(cache_name))
        tiden_assert(no_conflicts_msg in self.cu.latest_utility_output, 'No conflicts have been found')

    @attr('SOW4', 'IS_3_1_part_bin', 'iep-14')
    @test_case_id('91956')
    @with_setup(setup_testcase, teardown_testcase)
    def test_sow4_IS_3_1_break_part_bin(self):
        """
        Based on SOW4 document
        Check the process with partition bin corrupt due node stop
        """
        self.break_bin('part', '1', Iep14Resources.BREAK_PARTITION_EXCEPTION)

    @attr('SOW4', 'IS_3_1_part_bin', 'iep-14')
    @test_case_id('134121')
    @with_setup(setup_testcase, teardown_testcase)
    def test_sow4_IS_3_1_break_index_bin_header(self):
        """
        Based on SOW4 document
        Check the process with index bin corrupt due node stop
        """
        self.break_bin('index', '5', Iep14Resources.BREAK_PARTITION_EXCEPTION)

    @attr('SOW4', 'IS_3_1_part_bin', 'iep-14')
    @test_case_id('134122')
    @with_setup(setup_testcase, teardown_testcase)
    def test_sow4_IS_3_1_break_index_bin_crc_zero_page(self):
        """
        Based on SOW4 document
        Check the process with index bin corrupt due node stop
        """
        self.break_bin('index', '4100', Iep14Resources.BREAK_CRC_EXCEPTION)

    @attr('SOW4', 'IS_3_1_part_bin', 'iep-14')
    @test_case_id('134123')
    @with_setup(setup_testcase, teardown_testcase)
    def test_sow4_IS_3_1_break_index_bin_crc_n_page(self):
        """
        Based on SOW4 document
        Check the process with index bin corrupt due node stop
        """
        self.break_bin('index', '10000', Iep14Resources.BREAK_CRC_EXCEPTION)

    def break_bin(self, partition_name, seek, error_in_logs):
        self.cu.control_utility('--cache', 'idle_verify')
        self.cu.deactivate()
        self.ignite.stop_nodes()

        node_idx = list(self.ignite.nodes.keys())[0]
        host = self.ignite.nodes[node_idx]['host']
        if 'part' in partition_name:
            command = [
                'find {}/work/db/{}/cacheGroup-test_cache_group -name \'part-*.bin\''.format(
                    self.ignite.nodes[node_idx]['ignite_home'],
                    self.ignite.get_node_consistent_id(node_idx)
                )
            ]
            results = self.ssh.exec_on_host(host, command)
            bin_files = [path.basename(f) for f in results[host][0].split('\n') if f]
            m = match('.*-(\d+).bin', bin_files[0])
            partition_name = '{}-{}'.format(partition_name, m.group(1))

        command = ['printf "\\x31\\xc0\\xc3" | dd of=%s/work/db/%s/cacheGroup-test_cache_group/%s.bin bs=1 seek=%s '
                   'count=3 conv=notrunc' % (self.ignite.nodes[node_idx]['ignite_home'],
                                             self.ignite.get_node_consistent_id(node_idx),
                                             partition_name,
                                             seek),
                   ]
        log_print('command execute on node %s - %s' % (node_idx, command), color='green')
        results = self.ssh.exec_on_host(self.ignite.nodes[node_idx]['host'], command)
        result = ' '.join(results[self.ignite.nodes[node_idx]['host']])
        log_print('command execute result - %s' % result)
        tiden_assert(result != ' ', 'break command execute correct')
        exception = 0
        try:
            found_broken_index = False
            self.ignite.start_nodes()
            self.cu.activate()
            self.cu.control_utility('--cache', 'idle_verify')
            self.cu.control_utility('--cache', 'validate_indexes')
            output = self.cu.latest_utility_output
            for line in output.split('\n'):
                m = search('issues found (listed above)', line)
                if m:
                    found_broken_index = True
            if not found_broken_index:
                raise TidenException('')
        except TidenException:
            util_sleep_for_a_while(10)
            exception = self.find_exception_in_logs(error_in_logs)
            log_print('Found correct exception in logs - %s' % exception)
            tiden_assert(exception >= 1, 'no error message found in the log')
        else:
            tiden_assert(False, 'cluster pass test, excepted error')

    @attr('SOW4', 'IS_3_1_wal', 'iep-14')
    @test_case_id('91957')
    @with_setup(setup_testcase_no_grid_start, teardown_testcase)
    def test_sow4_IS_3_1_break_wal(self):
        """
        Based on SOW4 document.
        Check the start process with corrupted WAL file.
        """
        # self.util_prepare_remote_config('server_with_custom_frequency.xml')
        self.server_config = 'server_with_custom_frequency.xml'

        default_context = self.contexts['default']
        default_context.add_context_variables(
            custom_cp_freq=300000
        )
        default_context.set_client_result_config("client_with_custom_frequency.xml")
        default_context.set_server_result_config("server_with_custom_frequency.xml")

        default_context.build_and_deploy(self.ssh)
        self.ignite.set_node_option('*', 'config', self.server_config)

        self.start_grid()
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            cache_names = piclient.get_ignite().cacheNames()

            async_ops = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_streamer_operation,
                                                         cache_name, 0, 400000)
                async_ops.append(async_operation)
                async_operation.evaluate()

        util_sleep_for_a_while(10)
        self.ignite.stop_nodes()
        node_under_test = list(self.ignite.nodes.keys())[0]
        command = [
            'printf "\\x31\\xc0\\xc3" | dd of=%s/work/db/wal/%s/0000000000000000.wal bs=1 seek=1 count=3 conv=notrunc'
            % (self.ignite.nodes[node_under_test]['ignite_home'], self.ignite.get_node_consistent_id(node_under_test)),
        ]
        log_print('command execute on node %s - %s' % (node_under_test, command), color='debug')
        results = self.ssh.exec_on_host(self.ignite.nodes[node_under_test]['host'], command)
        log_print('command execute result {}'.format(results), color='debug')
        tiden_assert(results != ' ', 'command \n{}\nto corrupt wal not executed'.format(command))
        search_result = None
        try:
            self.ignite.start_nodes()
            self.cu.activate()
        except TidenException:
            util_sleep_for_a_while(10)
            # exception = self.find_exception_in_logs('class org.apache.ignite.internal.pagemem.wal.StorageException')
            str_to_check = 'StorageException:'
            search_result = self.find_in_node_log(str_to_check, node_id=node_under_test)
            log_print('Found correct exception in logs:\n{}'.format(search_result), color='debug')
            tiden_assert('StorageException' in search_result,
                         'String \'%s\' could be found in node log' % str_to_check)

        tiden_assert(search_result, 'Cluster activation with corrupted wal is success, expected error')

    def find_in_node_log(self, text, node_id):
        node = self.ignite.nodes.get(node_id)
        search_result = None
        if node:
            host = node['host']
            log = node['log']
            cmd = ['cat %s | grep \'%s\'' % (log, text)]
            log_print(cmd, color='blue')
            output = self.ssh.exec_on_host(host, commands=cmd)
            log_print(output[host], color='green')
            search_result = output[host][0]

        else:
            log_print('There is no node with id %s in ignite' % node_id, color='red')
            log_print(self.ignite.nodes, color='red')

        return search_result

    def find_exception_in_logs(self, exception, result_node_option='exception', node_id=None):
        self.ignite.find_exception_in_logs(exception, result_node_option=result_node_option)
        exceptions_cnt = 0
        nodes_to_check = self.ignite.nodes.keys()
        if node_id:
            nodes_to_check = [node_id]

        for node_idx in nodes_to_check:
            if result_node_option in self.ignite.nodes[node_idx] \
                    and self.ignite.nodes[node_idx][result_node_option] is not None \
                    and self.ignite.nodes[node_idx][result_node_option] != '':
                log_print(self.ignite.nodes[node_idx][result_node_option], color='red')
                exceptions_cnt = exceptions_cnt + 1
        return exceptions_cnt

    def util_cleanup_on_all_nodes(self, path_to_clear):
        for node_id in self.ignite.nodes.keys():
            self.util_cleanup_storage(self.ignite.nodes[node_id].get('host'), path_to_clear)

    def util_cleanup_storage(self, host, path_to_clear):
        log_print('Cleaning up folder %s on host %s' % (path_to_clear, host), color='green')
        commands = ['rm -fr %s/*' % path_to_clear]
        response = self.ssh.exec_on_host(host, commands)
        log_print(response, color='green')

    def util_update_server_config(self, config_file, replace_with, host=None):
        commands = []
        for pattern, replacer in replace_with:
            if replacer == '':
                commands.append('sed -i \'/%s/d\' %s' % (pattern, config_file))
            else:
                commands.append('sed -i \'s|%s|%s|g\' %s' % (pattern, replacer, config_file))

        log_print(commands, color='green')

        hosts = self.config['environment']['server_hosts']
        if host:
            hosts = [host]

        for host_to_change in hosts:
            response = self.ssh.exec_on_host(host_to_change, commands)
            log_print(response, color='green')

    def remove_additional_nodes(self):
        additional_nodes = self.ignite.get_all_additional_nodes()
        for node_idx in additional_nodes:
            self.ignite.kill_node(node_idx)
            del self.ignite.nodes[node_idx]

    def _disable_connections(self, to_hosts, on_host):
        log_print('Disabling node on host: %s' % to_hosts, color='red')
        cmd = []
        if isinstance(to_hosts, list):
            for host in set(to_hosts):
                cmd += self._iptables_rule(host)
        else:
            cmd = self._iptables_rule(to_hosts)

        log_print(cmd, color='red')
        results = self.ssh.exec_on_host(on_host, cmd)
        print(results)

    def _enable_connections(self, to_hosts, on_host):
        cmd = []
        if isinstance(to_hosts, list):
            for host in set(to_hosts):
                cmd += self._iptables_rule(host, add_rule=False)
        else:
            cmd = self._iptables_rule(to_hosts, add_rule=False)
        log_print(cmd, color='red')
        results = self.ssh.exec_on_host(on_host, cmd)
        print(results)

    @staticmethod
    def _iptables_rule(host, add_rule=True):
        if add_rule:
            cmd = [
                'sudo iptables -w -A INPUT -s %s -j DROP' % host,
                'sudo iptables -w -A OUTPUT -d %s -j DROP' % host]
        else:
            cmd = [
                'sudo iptables -w -D INPUT -s %s -j DROP' % host,
                'sudo iptables -w -D OUTPUT -d %s -j DROP' % host
            ]
        return cmd

    def util_get_threads_from_cluster_over_jmx(self, node_id):
        """
        Start grid and get thread names over JMX
        """
        # Start grid to get thread names
        self.start_grid()

        log_print("Requesting 'Kernal.WorkersControlMXBean.WorkerNames' from node # %d" % node_id, color='green')
        op_result = self.ignite.jmx.get_attributes(
            node_id, 'Kernal', 'WorkersControlMXBean', 'WorkerNames'
        )

        log_print("got '%s'" % op_result, color='green')
        test_threads = op_result.get('WorkerNames')
        log_print(test_threads, color='green')
        test_threads = [s.strip() for s in test_threads[1:-1].split(',')]
        # Stop grid as we don't need it for now
        self.stop_grid()

        return test_threads

    def util_get_threads_from_cluster_over_jmx_on_running_cluster(self, node_id):
        """
        Get thread names over JMX from running cluster
        """
        log_print("Requesting 'Kernal.WorkersControlMXBean.WorkerNames' from node # %d" % node_id, color='green')
        op_result = self.ignite.jmx.get_attributes(
            node_id, 'Kernal', 'WorkersControlMXBean', 'WorkerNames'
        )

        log_print("got '%s'" % op_result, color='green')
        test_threads = op_result.get('WorkerNames')
        log_print(test_threads, color='green')
        test_threads = [s.strip() for s in test_threads[1:-1].split(',')]

        return test_threads

    def util_get_threads_from_jstack(self, node_id, thread_name):
        """
        Run jstack and find thread id using thread name.
        """
        # Start grid to get thread names

        commands = []
        host = self.ignite.nodes[node_id]['host']
        pid = self.ignite.nodes[node_id]['PID']
        test_dir = self.config['rt']['remote']['test_dir']
        commands.append('jstack %s > %s/thread_dump.txt; cat %s/thread_dump.txt | grep %s' %
                        (pid, test_dir, test_dir, thread_name))

        log_print(commands, color='green')
        response = self.ssh.exec_on_host(host, commands)
        log_print(response, color='green')
        out = response[host][0]

        thread_name = out.split()[0][1:-1]
        thread_id = out.split()[1][1:] if out.split()[1].startswith("#") else out.split()[1]

        log_print('Thread id = %s, thread name = %s' % (thread_id, thread_name), color='green')

        return thread_id, thread_name

    def util_test_thread_termination_by_id(self, thread_to_terminate, node_under_test=1):
        """
        Start grid and make initial load.
        Calculating checksum and sums over caches.
        Start load with AccountRunner.
        Terminate thread using JMX and check exception in logs.
        Wait for Account Runner finish it's work.
        Calculate sums again and check it's the same as before.

        :param thread_to_terminate: Thread name that should be terminated.
        :param node_under_test: Node ID on which thread is terminated.
        :return:
        """
        self.ignite.set_grid_name(thread_to_terminate)
        self.delete_lfs()
        # Start grid and make preloading
        self.start_grid()

        self.load_data_with_streamer()

        balances_before = self.calculate_sum_by_field()
        print(balances_before)

        checksum_before = self.calc_checksums_on_client()
        log_print(checksum_before, color='green')
        self.ignite.wait_for_topology_snapshot(client_num=0)

        log_print(repr(self.ignite), color='debug')

        thread_id, thread_name = self.util_get_threads_from_jstack(node_under_test, thread_to_terminate)

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache_names = piclient.get_ignite().cacheNames()

            async_operations = []
            for cache_name in cache_names.toArray():
                ar_operation = create_account_runner_operation(cache_name, 1, 1000, 0.05, delay=1)
                ar_operation.setTimeOut(5000)
                async_operation = create_async_operation(ar_operation)
                async_operations.append(async_operation)
                async_operation.evaluate()

            log_print("Going to terminate worker %s" % thread_name, color='green')

            op_result = self.ignite.jmx.evaluate_operation(
                node_under_test, 'Kernal', 'WorkersControlMXBean', 'stopThreadById', thread_id
            )

            log_print("got '%s'" % op_result, color='green')

            exception = self.find_exception_in_logs('Thread %s is terminated unexpectedly' % thread_to_terminate,
                                                    node_id=node_under_test)
            print(exception)

            for async_op in async_operations:
                try:
                    async_op.getResult()
                except Exception as e:
                    print(e)

            util_sleep_for_a_while(10)

            #self.su.snapshot_utility('idle_verify', run_on_node=1)
            self.cu.control_utility('--cache idle_verify', run_on_node=1)

            checksum_after = self.calc_checksums_on_client()
            print_green(checksum_after)
            balances_after = self.calculate_sum_by_field()
            print(balances_after)

            self.util_compare_dicts(balances_before, balances_after)

            # If everything ok, balances should be the same and checksum - not (as there were transfers)
            tiden_assert_equal(balances_before, balances_after, 'Balance before/balance after')
            tiden_assert(checksum_before != checksum_after, 'Checksums are not hte same')

        self.ignite.wait_for_topology_snapshot(client_num=0)
        util_sleep_for_a_while(3)
        self.stop_grid()

    @staticmethod
    def util_compare_dicts(dict_1, dict_2):
        for key in dict_1.keys():
            if not dict_2.get(key):
                log_print('There is no key %s in dict %s' % (key, dict_2), color='red')
            if dict_1[key] != dict_2[key]:
                log_print('For key %s value %s is not equal %s' % (key, dict_1[key], dict_2[key]), color='red')

    @staticmethod
    def util_get_threads_to_test(full_test_threads, thread_filter):
        final_list = []
        log_print('Thread filter %s' % thread_filter, color='blue')

        if not isinstance(thread_filter, list):
            thread_filter = [thread_filter]

        test_threads = list(filter(lambda t_name: [item for item in thread_filter if item in t_name],
                                   full_test_threads))
        # it could be that amount of same threads is huge. For example for sys-stripe or grid-nio-worker-tcp-comm,
        # in this case we need to reduce this amount.
        if len(test_threads) > len(thread_filter):
            for pattern in thread_filter:
                pattern_threads = [found_thread for found_thread in test_threads if pattern in found_thread]
                # if more then two threads fit the pattern pick up first and random one
                if len(pattern_threads) > 2:
                    final_list.append(pattern_threads[0])
                    final_list.append(pattern_threads[randint(1, len(pattern_threads) - 1)])
        else:
            final_list = test_threads

        return final_list

    def util_pick_random_node(self, include_coordinator=False, exclude_client_hosts=False):
        available_nodes = self.ignite.get_all_default_nodes()

        if not include_coordinator:
            available_nodes.remove(1)

        if exclude_client_hosts:
            available_hosts = [host for host in self.config['environment']['server_hosts']
                               if host not in self.config['environment']['client_hosts']]

            available_nodes = []
            for node_id in self.ignite.get_all_default_nodes():
                if self.ignite.nodes[node_id]['host'] in available_hosts:
                    available_nodes.append(node_id)

            if not available_nodes:
                raise TidenException('There is no nodes to use in this test.')

        random_node = choice(available_nodes)
        log_print('Node under test = %s' % random_node, color='green')

        return random_node

    @staticmethod
    def util_check_all_threads_presented(threads_to_check, all_threads):
        mask_to_exclude = []
        for thread_mask in threads_to_check:
            found_threads = [thread for thread in all_threads if thread_mask in thread]
            if not found_threads:
                log_print('WARNING: thread with mask %s is not found' % thread_mask, color='red')
                mask_to_exclude.append(thread_mask)

        return list(set(threads_to_check) - set(mask_to_exclude))

