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

import random
import traceback
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import ExitStack
from time import sleep

from tiden.apps import NodeStatus
from tiden.apps.zookeeper import ZkNodesRestart
from tiden_gridgain.piclient.helper.cache_utils import IgniteCacheConfig, IgniteCache
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_distributed_atomic_long, \
    create_async_operation
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden_gridgain.case.singlegridzootestcase import SingleGridZooTestCase
from tiden.util import is_enabled, get_logger, log_print, print_debug, attr, with_setup, util_sleep, repeated_test, \
    test_case_id, util_sleep_for_a_while
from tiden.utilities import Sqlline


class TestStressGrid(SingleGridZooTestCase):
    glue_timeout = None

    group_names = []  # group names to wait to be rebalanced
    rebalance_timeout = 240  # completed rebalance timeout
    iterations = 5  # number of rebalance test iterations
    logger = get_logger('tiden')

    def setup(self):
        self.ignite.jmx.rebalance_collect_timeout = 5

        self.ignite.activate_module('ignite-log4j2')

        self.the_glue_timeout = self.config.get('the_glue_timeout', None)

        default_context = self.contexts['default']
        default_context.add_context_variables(
            caches_file='caches.xml',
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled')),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            rebalance_pool_size=4,
            caches=True,
            logger=True,
            logger_path='%s/ignite-log4j2.xml' % self.config['rt']['remote']['test_module_dir'],
        )

        indexed_types = self.create_test_context('indexed_types')
        indexed_types.add_context_variables(
            caches_file='caches_index.xml',
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled')),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            rebalance_pool_size=4,
            caches=True,
            logger=True,
            logger_path='%s/ignite-log4j2.xml' % self.config['rt']['remote']['test_module_dir'],
        )

        in_memory_context = self.create_test_context('in_memory')
        in_memory_context.add_context_variables(
            caches_file='caches.xml',
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled')),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            rebalance_pool_size=4,
            in_memory=True,
            caches=False,
            logger=True,
            logger_path='%s/ignite-log4j2.xml' % self.config['rt']['remote']['test_module_dir'],
        )

        super().setup()

        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestStressGrid]')

    def teardown(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.stop_zookeeper()

    def setup_testcase(self):
        self.logger.info('TestSetup is called')

        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()

        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.su.clear_snapshots_list()
        self.start_grid(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite, self.get_client_config(), end_key=1000)

        if self.get_context_variable('pitr_enabled') and self.get_context_variable('snapshots_enabled'):
            self.su.wait_no_snapshots_activity_in_cluster()

        self.group_names = PiClientIgniteUtils.collect_cache_group_names(self.ignite,
                                                                         self.get_client_config())

        # if not self.ignite.jmx.is_started():
        #     self.ignite.jmx.start_utility()

        # wait for no client on cluster
        self.ignite.wait_for_topology_snapshot(client_num=0)

        print_debug(repr(self.ignite))

    def empty_setup(self):
        self.logger.info('TestSetup is called')

        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()

            # self.zoo.run_zk_client_command("create /apacheIgnite content sasl:zoocli@EXAMPLE.COM:cdrwa",
            #                                assert_result="Created /apacheIgnite")

            in_memory_context = self.contexts['in_memory']
            in_memory_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString()
            )
            in_memory_context.build_and_deploy(self.ssh)

    def teardown_testcase(self):
        self.logger.info('TestTeardown is called')

        print_debug(repr(self.ignite))
        log_print('All additional nodes: %s Alive additional nodes: %s' %
                  (self.ignite.get_all_additional_nodes(), self.ignite.get_alive_additional_nodes()), color='blue')

        for additional_node in self.ignite.get_alive_additional_nodes():
            self.ignite.kill_node(additional_node)

        # kill utility if exists
        if self.ignite.jmx.is_started():
            self.ignite.jmx.kill_utility()

        self.stop_grid_hard()
        self.su.copy_utility_log()

        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()

        self.ignite.cleanup_work_dir()
        self.set_current_context()
        self.ignite.reset(hard=True)
        print_debug(repr(self.ignite))

    @with_setup(empty_setup, teardown_testcase)
    def test_massive_index_rebuild(self):
        """
        1) 2 nodes, backupCnt = 1, persistenceEnabled
        2) Load (A, B) type into a cache with defined (A, B) types in index config
        3) Load new type of data into a cache (C,D)
        4) Kill one node
        5) Create new index in alive cluster
        6) Start node again

        :return:
        """

        PiClient.read_timeout = 1200

        self.set_current_context('indexed_types')

        self.util_copy_piclient_model_to_libs()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid(skip_activation=True)

        self.ignite.cu.activate(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    value_type=ModelTypes.VALUE_ALL_TYPES_30_INDEX.value,
                                                    end_key=5000)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    value_type=ModelTypes.VALUE_ACCOUNT.value,
                                                    start_key=5000,
                                                    end_key=10000)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    value_type=ModelTypes.VALUE_EXT_ALL_TYPES_30_INDEX.value,
                                                    start_key=10000,
                                                    end_key=15000)

        # PiClientIgniteUtils.load_data_with_streamer(self.ignite,
        #                                             self.get_client_config(),
        #                                             cache_names_patterns=
        #                                             ['cache_group_3'],
        #                                             value_type=ModelTypes.VALUE_EXT_ALL_TYPES_30_INDEX.value,
        #                                             end_key=10000)

        iterations = 50

        sqlline = Sqlline(self.ignite)

        columns = ['longCol', 'doubleCol', 'stringCol', 'booleanCol', 'longCol1',
                   # 'doubleCol1', 'stringCol1', 'intCol', 'intCol1',  # 'booleanCol1',
                   # 'index', 'longCol2', 'doubleCol2', 'stringCol2', 'booleanCol2',
                   # 'longCol12', 'doubleCol12', 'stringCol12', 'intCol12', 'intCol2',
                   # 'shortCol2', 'longCol3', 'doubleCol3', 'stringCol3', 'booleanCol3',
                   # 'longCol13', 'doubleCol13', 'stringCol13', 'intCol13', 'intCol3', 'shortCol3'
                   ]

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache_names = piclient.get_ignite().cacheNames().toArray()

            for i in range(0, iterations):
                log_print('Current iteration %s from %s' % (i, iterations), color='debug')

                update_table = []

                self.ignite.kill_node(2)

                indexed_columns = ','.join(columns)

                for cache_name in cache_names:
                    # self.ssh.exec_on_host('REMOVE')
                    vtype = 'ALLTYPES30INDEX'  # if 'cache_group_3' not in cache_name else 'EXTALLTYPES30INDEX'

                    update_table.append(f'\'CREATE INDEX IF NOT EXISTS {cache_name}_{vtype} on '
                                        f'\"{cache_name}\".{vtype}({indexed_columns}) INLINE_SIZE 32 PARALLEL 28;\'')

                update_table.append('!index')

                sqlline.run_sqlline(update_table)

                self.ignite.start_node(2)

                util_sleep_for_a_while(30)

                self.verify_no_assertion_errors()

                self.cu.control_utility('--cache validate_indexes', all_required='no issues found.')

    @repeated_test(10)
    @with_setup(empty_setup, teardown_testcase)
    def test_full_cluster_blinking(self):
        """

        Enable indexes

        Start servers with PDS, start clients, start some light tx loading.
        In loop try to blink with all cluster at the same time. Logically there should be no data loss:
            full cluster blinking - so there shouldn't be any data loss

        :return:
        """

        PiClient.read_timeout = 240

        self.set_current_context('indexed_types')

        self.util_copy_piclient_model_to_libs()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid(skip_activation=True)

        self.ignite.cu.activate(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    end_key=100000)

        nodes_before = self.ignite.get_alive_default_nodes()
        iterations = 50

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, loading_profile=LoadingProfile(delay=1000)):
                for i in range(0, iterations):
                    log_print('Current iteration %s from %s' % (i, iterations), color='debug')

                    for node_id in nodes_before:
                        self.ignite.kill_node(node_id)
                        sleep(float(self.the_glue_timeout)
                              if self.the_glue_timeout else round(random.uniform(0.1, 0.5), 1))

                    for node_id in nodes_before:
                        self.ignite.start_node(node_id, skip_topology_check=True)
                        sleep(float(self.the_glue_timeout)
                              if self.the_glue_timeout else round(random.uniform(0.1, 0.5), 1))

                    self.ignite.wait_for_topology_snapshot(server_num=len(nodes_before))

                    for node_id in self.ignite.get_all_default_nodes():
                        self.ignite.update_started_node_status(node_id)

                    sleep(10)

                    self.cu.control_utility('--cache validate_indexes', all_required='no issues found.')

                    self.verify_cluster(nodes_before, 0)

    @repeated_test(10)
    @with_setup(empty_setup, teardown_testcase)
    def test_nodes_connecting_to_dead_cluster(self):
        """
        https://ggsystems.atlassian.net/browse/IGN-13800

        Two nodes are trying to connect to cluster, meanwhile cluster killed.
        They should send join request but didn't get self NodeAdded

        """
        PiClient.read_timeout = 240

        # sleep_for_time = float(random.randrange(1, 15, 1)) / 5

        self.set_current_context('in_memory')

        self.util_copy_piclient_model_to_libs()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid(skip_activation=True)

        last_loaded_key = 1000
        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    end_key=last_loaded_key)

        nodes_before = self.ignite.get_alive_default_nodes()

        additional_nodes = self.ignite.add_additional_nodes(config=self.get_client_config(), num_nodes=2)

        def start_piclients():
            try:
                self.ignite.start_additional_nodes(additional_nodes, client_nodes=True, skip_topology_check=True)
            except Exception as err:
                print(err)
            finally:
                sleep(10)

                self.ignite.update_started_node_status(additional_nodes)

                for add_node in additional_nodes:
                    self.ignite.kill_node(add_node)

        log_print("Starting clients under load", color="green")

        executor = ThreadPoolExecutor()

        executor.submit(start_piclients)

        try:
            time_to_sleep = round(random.uniform(3.5, 4.9), 1)
            sleep(time_to_sleep)

            log_print("Time to sleep: %s" % time_to_sleep, color='green')

            self.ignite.kill_nodes()

            sleep(30)

            self.verify_cluster(nodes_before, 0)
        except Exception as e:
            raise e
        finally:
            executor.shutdown(wait=True)

        self.ssh.killall('java')

    @repeated_test(50)
    @with_setup(empty_setup, teardown_testcase)
    @test_case_id(201857)
    def test_clients_killed_few_coordinators(self):
        """
        1. Start grid, load some data
        2. Repeat:
            2.1. Start clients thread with loading (put all operation)
            2.2. Kill first node (coordinator) + second node (possible coordinator)
            2.3. Kill 4 next coordinators with some small timeout
            2.4. Sleep for 2 minutes to make cluster process failure
            2.5. Launch verify procedure
            2.6. Stop clients put thread

        :return:
        """

        self.set_current_context('default')

        self.util_copy_piclient_model_to_libs()
        self.util_deploy_sbt_model()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid()

        sleep_for_time = float(self.the_glue_timeout) if self.the_glue_timeout else round(random.uniform(0.1, 2.9), 1)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    value_type=ModelTypes.VALUE_ACCOUNT.value,
                                                    end_key=1000)

        nodes_before = self.ignite.get_alive_default_nodes()

        def start_piclients():
            for _ in range(0, 3):
                try:
                    PiClientIgniteUtils.load_data_with_putall(self.ignite,
                                                              self.get_client_config(),
                                                              value_type=ModelTypes.VALUE_ACCOUNT.value,
                                                              nodes_num=24,
                                                              end_key=1000)
                except Exception as err:
                    print(err)

        with PiClient(self.ignite, self.get_client_config()) as load:
            log_print("Starting clients under load", color="green")

            executor = ThreadPoolExecutor()

            executor.submit(start_piclients)

            sleep(5)

            try:
                self.ignite.kill_node(1)

                for i in range(0, 4):
                    sleep(sleep_for_time)

                    i = int(load.get_ignite().cluster().forOldest().node().consistentId().replace('node_1_', ''))

                    if self.ignite.nodes[i]['status'] in [NodeStatus.KILLED, NodeStatus.KILLING]:
                        sleep(sleep_for_time)

                        i = int(load.get_ignite().cluster().forOldest().node().consistentId().replace('node_1_', ''))

                    self.ignite.kill_node(i)
                    log_print("Killing node %s" % i)

            except Exception as e:
                print(e)

            sleep(120)

            self.verify_cluster(nodes_before, 0)

            executor.shutdown(wait=True)

    @attr('the_glue', 'lost_topology')
    @repeated_test(10)
    @with_setup(empty_setup, teardown_testcase)
    @test_case_id(188707)
    def test_cycling_restart_grid_dynamic_caches_no_client(self):
        """
        Scenario The Glue
        (Assertions should be enabled)

        1. Start grid, load some data
        2. In the loop:
            2.1 define node restart timeout (0.5 - 2.0 seconds)
            2.2 Load more data
            2.3 Restart each node with defined timeout (DOES NOT LOOK ON TOPOLOGY SNAPSHOT)
            2.4 Try to activate, check AssertionErrors
            2.5 Try to baseline (If 2 operation failed -> PME, kill all nodes, start new test iteration)
            2.6 Try to load data
            2.7 Try to calculate checksum

        :return:
        """
        import random

        PiClient.read_timeout = 240

        # sleep_for_time = float(random.randrange(1, 15, 1)) / 5

        self.set_current_context('in_memory')

        self.util_copy_piclient_model_to_libs()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid(skip_activation=True)

        self.start_dynamic_caches_with_node_filter()

        last_loaded_key = 1000
        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config(),
                                                    end_key=last_loaded_key,
                                                    jvm_options=['-ea']
                                                    )

        nodes_before = self.ignite.get_alive_default_nodes()

        iterations = 50
        last_loaded_key += 1
        for i in range(0, iterations):
            with ExitStack() as stack:
                # load data before start zk restart thread
                self.start_dynamic_caches_with_node_filter()
                # PiClientIgniteUtils.wait_for_running_clients_num(self.ignite, 0, 120)
                PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                            self.get_client_config(),
                                                            start_key=last_loaded_key,
                                                            end_key=last_loaded_key + 500,
                                                            jvm_options=['-ea'],
                                                            check_clients=True)
                last_loaded_key += 500

                if self.get_context_variable('zookeeper_enabled') and \
                        is_enabled(self.config.get('zookeeper_nodes_restart')):
                    stack.enter_context(ZkNodesRestart(self.zoo, 3))

                log_print('Current iteration %s from %s' % (i, iterations), color='debug')

                sleep_for_time = float(self.the_glue_timeout) if self.the_glue_timeout else round(
                    random.uniform(0.5, 2.5), 1)
                log_print(
                    "In this run we are going to sleep for {} seconds after each node restart".format(sleep_for_time),
                    color='green')

                log_print('Trying to load data into created/existing caches', color='yellow')

                log_print("Round restart")
                for node_id in self.ignite.get_alive_default_nodes():
                    self.ignite.kill_node(node_id)
                    self.ignite.start_node(node_id, skip_topology_check=True)
                    sleep(sleep_for_time)

                log_print("Wait for topology messages")
                for node_id in self.ignite.get_all_default_nodes():
                    self.ignite.update_started_node_status(node_id)

                sleep(15)

            last_loaded_key = self.verify_cluster(nodes_before, last_loaded_key)

    @attr('the_glue', 'atomic')
    @repeated_test(10)
    @with_setup(empty_setup, teardown_testcase)
    @test_case_id(188708)
    def test_cycling_restart_grid_dynamic_caches_with_atomic_iter(self):
        """
        Scenario The Glue
        (Assertions should be enabled)

        1. Start grid, load some data
        2. In the loop:
            2.1 define node restart timeout (0.5 - 2.0 seconds)
            2.2 Load more data
            2.3 Restart each node with defined timeout (DOES NOT LOOK ON TOPOLOGY SNAPSHOT)
            2.4 Try to activate, check AssertionErrors
            2.5 Try to baseline (If 2 operation failed -> PME, kill all nodes, start new test iteration)
            2.6 Try to load data
            2.7 Try to calculate checksum

        :return:
        """
        import random

        PiClient.read_timeout = 240

        # sleep_for_time = float(random.randrange(1, 15, 1)) / 5

        self.set_current_context('in_memory')

        self.util_copy_piclient_model_to_libs()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid(skip_activation=True)

        with PiClient(self.ignite, self.get_client_config(), jvm_options=['-ea']) as piclient:
            ignite = piclient.get_ignite()

            self.start_dynamic_caches_with_node_filter()

            last_loaded_key = 1000
            PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                        self.get_client_config(),
                                                        end_key=last_loaded_key,
                                                        jvm_options=['-ea']
                                                        )

            nodes_before = self.ignite.get_alive_default_nodes()

            iterations = 50
            last_loaded_key += 1
            for i in range(0, iterations):
                log_print('Current iteration %s from %s' % (i, iterations), color='debug')

                sleep_for_time = float(self.the_glue_timeout) if self.the_glue_timeout else round(
                    random.uniform(0.5, 2.5), 1)
                log_print(
                    "In this run we are going to sleep for {} seconds after each node restart".format(sleep_for_time),
                    color='green')

                log_print('Trying to load data into created/existing caches', color='yellow')
                self.start_dynamic_caches_with_node_filter()
                PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                            self.get_client_config(),
                                                            start_key=last_loaded_key,
                                                            end_key=last_loaded_key + 500,
                                                            jvm_options=['-ea'])
                last_loaded_key += 500

                atomic_name = None
                atomic = None
                try:
                    log_print("Incrementing atomics")
                    for c in range(0, 10):
                        atomic_name = "nodeId_%s" % c
                        atomic = ignite.atomicLong(atomic_name, 0, True)
                        for j in range(0, 50):
                            atomic.incrementAndGet()
                except Exception:
                    log_print("Failed to increment atomics: assert that atomic.removed() and recreating it",
                              color='red')

                    # recreate atomic (https://issues.apache.org/jira/browse/IGNITE-11535)
                    if atomic:
                        ignite.atomicLong(atomic_name, 100, True)

                log_print("Round restart")
                for node_id in self.ignite.get_alive_default_nodes():
                    self.ignite.kill_node(node_id)
                    self.ignite.start_node(node_id, skip_topology_check=True)
                    sleep(sleep_for_time)

                log_print("Wait for topology messages")
                for node_id in self.ignite.get_all_default_nodes():
                    self.ignite.update_started_node_status(node_id)

                sleep(15)

                last_loaded_key = self.verify_cluster(nodes_before, last_loaded_key)

    @attr('the_glue', 'atomic')
    @repeated_test(10)
    @with_setup(empty_setup, teardown_testcase)
    @test_case_id(188709)
    def test_cycling_restart_grid_dynamic_caches_with_atomic_on_restart(self):
        """
        Scenario The Glue
        (Assertions should be enabled)

        1. Start grid, load some data
        2. In the loop:
            2.1 define node restart timeout (0.5 - 2.0 seconds)
            2.2 Load more data
            2.3 Restart each node with defined timeout (DOES NOT LOOK ON TOPOLOGY SNAPSHOT)
            2.4 Try to activate, check AssertionErrors
            2.5 Try to baseline (If 2 operation failed -> PME, kill all nodes, start new test iteration)
            2.6 Try to load data
            2.7 Try to calculate checksum

        :return:
        """
        import random

        PiClient.read_timeout = 240

        # sleep_for_time = float(random.randrange(1, 15, 1)) / 5

        self.set_current_context('in_memory')

        self.util_copy_piclient_model_to_libs()
        self.ignite.set_activation_timeout(240)
        self.ignite.set_snapshot_timeout(240)
        self.ignite.set_node_option('*', 'jvm_options', ['-ea'])
        self.su.clear_snapshots_list()
        self.start_grid(skip_activation=True)

        with PiClient(self.ignite, self.get_client_config(), jvm_options=['-ea']) as piclient:
            # ignite = piclient.get_ignite()

            self.start_dynamic_caches_with_node_filter()

            last_loaded_key = 1000
            PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                        self.get_client_config(),
                                                        end_key=last_loaded_key,
                                                        jvm_options=['-ea']
                                                        )

            nodes_before = self.ignite.get_alive_default_nodes()

            iterations = 50
            last_loaded_key += 1
            for i in range(0, iterations):
                log_print('Current iteration %s from %s' % (i, iterations), color='debug')
                # sleep_for_time = float(self.the_glue_timeout) if self.the_glue_timeout else random.choice([0.7, 0.9, 2.0])
                sleep_for_time = float(self.the_glue_timeout) if self.the_glue_timeout else round(
                    random.uniform(0.5, 2.5), 1)
                log_print(
                    "In this run we are going to sleep for {} seconds after each node restart".format(sleep_for_time),
                    color='green')

                log_print('Trying to load data into created/existing caches', color='yellow')
                self.start_dynamic_caches_with_node_filter()
                PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                            self.get_client_config(),
                                                            start_key=last_loaded_key,
                                                            end_key=last_loaded_key + 500,
                                                            jvm_options=['-ea'])
                last_loaded_key += 500

                log_print("Round restart")
                for node_id in self.ignite.get_alive_default_nodes():
                    self.ignite.kill_node(node_id)
                    self.ignite.start_node(node_id, skip_topology_check=True)
                    sleep(sleep_for_time)

                    try:
                        log_print("Incrementing atomics using distributed compute")
                        create_async_operation(create_distributed_atomic_long).evaluate()
                    except Exception as e:
                        log_print("Failed to increment atomics")

                        # just print exception (https://issues.apache.org/jira/browse/IGNITE-11535)
                        traceback.print_exc()

                log_print("Wait for topology messages")
                for node_id in self.ignite.get_all_default_nodes():
                    self.ignite.update_started_node_status(node_id)

                sleep(15)

                log_print("Validating cluster")
                last_loaded_key = self.verify_cluster(nodes_before, last_loaded_key)

    def verify_cluster(self, nodes_before, last_loaded_key=None):
        if len(nodes_before) != self.ignite.get_nodes_num('server'):
            log_print("There are missing nodes on cluster.", color='yellow')

            self.verify_no_assertion_errors()

            log_print("Wait for topology messages again.", color='yellow')
            for node_id in self.ignite.get_all_default_nodes():
                self.ignite.update_started_node_status(node_id)

            log_print("Missing nodes case confirmed. Trying to restart node.", color='red')
            if len(nodes_before) != self.ignite.get_nodes_num('server'):
                nodes_to_start = []

                for node_id in self.ignite.get_alive_default_nodes():
                    # assert that node is not dead otherwise kill/restart again
                    if not self.ignite.check_node_status(node_id):
                        log_print("Restarting node %s" % node_id, color='yellow')
                        nodes_to_start.append(node_id)

                for node_id in nodes_to_start:
                    self.ignite.start_node(node_id, skip_nodes_check=True, check_only_servers=True)

                if len(nodes_before) != self.ignite.get_nodes_num('server'):
                    for node_id in self.ignite.get_alive_default_nodes():
                        self.util_get_threads_from_jstack(node_id, "FAILED")

                    assert False, "Failed to restart node"

        self.cu.control_utility('--activate')
        self.verify_no_assertion_errors()

        activate_failed = False
        log_print('Check that there is no Error in activate logs', color='yellow')
        if 'Error' in self.cu.latest_utility_output:
            activate_failed = True
            log_print('Failed!', color='red')
        sleep(5)

        self.cu.control_utility('--baseline')
        self.verify_no_assertion_errors()
        log_print('Check that there is no Error in control.sh --baseline logs', color='yellow')

        if 'Error' in self.cu.latest_utility_output:
            log_print('Failed! Second try after sleep 60 seconds', color='red')
            sleep(60)

            self.cu.control_utility('--baseline')

            if 'Error' in self.cu.latest_utility_output or activate_failed:
                log_print('Cluster looks hang.')

        log_print('Check that there is no AssertionError in logs', color='yellow')
        self.verify_no_assertion_errors()

        if last_loaded_key:
            try:
                log_print('Trying to load data into survivor caches', color='yellow')
                PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                            self.get_client_config(),
                                                            start_key=last_loaded_key,
                                                            end_key=last_loaded_key + 500,
                                                            allow_overwrite=True,
                                                            check_clients=False,
                                                            )

                last_loaded_key += 500

                log_print('Printing checksums of existing caches', color='yellow')

                print(PiClientIgniteUtils.calc_checksums_distributed(self.ignite, self.get_client_config(),
                                                                     check_clients=False))

                log_print('Check that there is no AssertionError in logs', color='yellow')
            except Exception as e:
                for node_id in self.ignite.get_alive_default_nodes():
                    self.util_get_threads_from_jstack(node_id, "FAILED")

                assert False, "Unable to connect client"
            finally:
                self.verify_no_assertion_errors()

        return last_loaded_key

    def restart_clients(self, nodes, iterations):
        for i in range(1, iterations):
            log_print('Going to start client nodes %s' % nodes)
            self.ignite.start_additional_nodes(nodes, client_nodes=True, skip_topology_check=True)
            util_sleep(2)
            self.ignite.update_starting_node_attrs()
            log_print('Going to kill client nodes %s' % nodes)
            self.ignite.kill_nodes(*nodes)

    def assert_nodes_alive(self):
        expected = len(self.ignite.get_alive_default_nodes() + self.ignite.get_alive_additional_nodes())
        actual = self.ignite.get_nodes_num('server')
        assert expected == actual, \
            "Missing nodes found. Check logs!\nActual: %s\nExpected: %s\n" % (actual, expected)

    def idle_verify_action(self, sum_after):
        self.cu.control_utility('--cache', 'idle_verify')
        self.verify_no_assertion_errors()

    def start_dynamic_caches_with_node_filter(self):
        modifiers = ['', '_first_copy', '_second_copy', '_third_copy', '_forth_copy']

        with PiClient(self.ignite, self.get_client_config(), jvm_options=['-ea']) as piclient:
            gateway = piclient.get_gateway()

            for modifier in modifiers:
                string_class = gateway.jvm.java.lang.String
                string_array = gateway.new_array(string_class, 3)

                for i, arg in enumerate(['node_1_1', 'node_1_2', 'node_1_3']):
                    string_array[i] = arg

                node12_filter = gateway.jvm.org.apache.ignite.piclient.affinity.ConsistentIdNodeFilter(
                    string_array)

                string_class = gateway.jvm.java.lang.String
                string_array = gateway.new_array(string_class, 3)

                for i, arg in enumerate(['node_1_4', 'node_1_5', 'node_1_6']):
                    string_array[i] = arg

                node34_filter = gateway.jvm.org.apache.ignite.piclient.affinity.ConsistentIdNodeFilter(
                    string_array)

                cfg = IgniteCacheConfig(gateway=gateway)
                cfg.set_name('req' + modifier)
                cfg.set_cache_mode('partitioned')
                cfg.set_atomicity_mode('transactional')
                cfg.set_backups(1)
                cfg.set_rebalance_mode('async')
                cfg.set_affinity(False, 64)
                cfg.set_write_synchronization_mode('full_sync')

                # transient?
                cfg.cacheConfig.setLoadPreviousValue(True)
                cfg.cacheConfig.setStoreKeepBinary(True)
                cfg.cacheConfig.setNodeFilter(node12_filter)
                cfg.cacheConfig.setDataRegionName('handled')
                # cfg.cacheConfig.setReadThrough(True)
                # cfg.cacheConfig.setWriteThrough(True)

                IgniteCache('req' + modifier, cfg, gateway=gateway)

                cfg = IgniteCacheConfig(gateway=gateway)
                cfg.set_name('trans' + modifier)
                cfg.set_atomicity_mode('transactional')
                cfg.set_backups(1)
                cfg.set_rebalance_mode('async')
                cfg.set_affinity(False, 64)

                # transient?
                cfg.cacheConfig.setNodeFilter(node12_filter)
                cfg.cacheConfig.setLoadPreviousValue(True)
                cfg.cacheConfig.setStoreKeepBinary(True)
                cfg.cacheConfig.setDataRegionName('handled')

                IgniteCache('trans' + modifier, cfg, gateway=gateway)

                cfg = IgniteCacheConfig(gateway=gateway)
                cfg.set_name('id' + modifier)
                cfg.set_atomicity_mode('atomic')
                cfg.set_cache_mode('replicated')
                cfg.set_rebalance_mode('async')
                cfg.set_backups(0)
                cfg.set_affinity(False, 256)

                cfg.cacheConfig.setDataRegionName('no-evict')

                IgniteCache('id' + modifier, cfg, gateway=gateway)

                cfg = IgniteCacheConfig(gateway=gateway)
                cfg.set_name('tid' + modifier)
                cfg.set_atomicity_mode('transactional')
                cfg.set_backups(1)
                cfg.set_rebalance_mode('async')
                cfg.set_affinity(False, 64)
                cfg.set_write_synchronization_mode('full_sync')

                cfg.cacheConfig.setDataRegionName('handled')
                cfg.cacheConfig.setNodeFilter(node34_filter)

                IgniteCache('tid' + modifier, cfg, gateway=gateway)

                cfg = IgniteCacheConfig(gateway=gateway)
                cfg.set_name('config' + modifier)
                cfg.set_atomicity_mode('atomic')
                cfg.set_cache_mode('replicated')
                cfg.set_rebalance_mode('async')
                cfg.set_backups(0)
                cfg.set_affinity(False, 256)

                cfg.cacheConfig.setDataRegionName('no-evict')

                IgniteCache('config' + modifier, cfg, gateway=gateway)

    def util_get_threads_from_jstack(self, node_id, iteration, to_find=''):
        """
        Run jstack and find thread id using thread name.
        """
        # Start grid to get thread names

        commands = []
        host = self.ignite.nodes[node_id]['host']
        pid = self.ignite.nodes[node_id]['PID']
        test_dir = self.config['rt']['remote']['test_dir']
        commands.append('jstack %s > %s/thread_dump_%s_%s.txt; cat %s/thread_dump.txt | grep "%s"' %
                        (pid, test_dir, node_id, iteration, test_dir, to_find))

        # log_print(commands)
        response = self.ssh.exec_on_host(host, commands)
        # log_print(response)
        out = response[host][0]

        # log_print('Node locked id = %s' % out)

        return out

    def verify_no_assertion_errors(self):
        assertion_errors = self.ignite.find_exception_in_logs(".*java.lang.AssertionError.*")

        # remove assertions from ignite.nodes to prevent massive output
        for node_id in self.ignite.nodes.keys():
            if 'exception' in self.ignite.nodes[node_id] and self.ignite.nodes[node_id]['exception'] != '':
                log_print("AssertionError found on node %s, text: %s" %
                          (node_id, self.ignite.nodes[node_id]['exception'][:100]),
                          color='red')
                self.ignite.nodes[node_id]['exception'] = ''

        if assertion_errors != 0:
            for node_id in self.ignite.get_alive_default_nodes():
                self.util_get_threads_from_jstack(node_id, "FAILED")

            log_print("AssertionErrors found in server logs! Count %d" % assertion_errors)

    # todo code duplicate
    def remove_index_bin_files(self, node_id):
        """
        Remove all index.bin files except cache cache-ignite-sys-caches for particular node.
        :param node_id:
        :return:
        """
        if node_id in self.ignite.nodes.keys():
            host = self.ignite.nodes[node_id]['host']
            ignite_home = self.ignite.nodes[node_id]['ignite_home']

            commands = dict()
            dir_to_search = '{}/work/db/'.format(ignite_home)
            commands[host] = ['find {} -name \'index.bin\''.format(dir_to_search)]

            output = self.ignite.ssh.exec(commands)
            files = [file for file in output[host][0].split('\n') if file and 'cache-ignite-sys-caches' not in file]
            log_print(files, color='debug')
            log_print(commands, color='debug')
            commands[host] = [';'.join(['rm {}'.format(file) for file in files])]
            output = self.ignite.ssh.exec(commands)
            log_print(output, color='debug')
        else:
            log_print("Node id {} not found in server nodes {}".format(node_id, self.ignite.nodes.keys()),
                      color='red')

