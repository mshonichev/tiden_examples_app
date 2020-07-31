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

from tiden.apps.ignite import Ignite
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden.util import log_print, sleep, attr, with_setup, util_sleep_for_a_while
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.helper.cache_utils import DynamicCachesFactory
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_sum_operation, \
    create_put_all_operation, create_checksum_operation
from tiden_gridgain.piclient.piclient import PiClient
from suites.compatibility.abstract import AbstractCompatibilityTest


class MixedTestLoadingAdapter:
    def __init__(self, test_class):
        self.test_class = test_class

    def get_client_config(self):
        return self.test_class.client_config

    def __getattribute__(self, item):
        if item == 'ignite':
            return self.test_class.current_client_ignite
        elif item == 'cu':
            return self.test_class.current_client_ignite.cu
        elif item == 'calculate_sum_by_field':
            return self.test_class.calculate_sum_by_field
        return object.__getattribute__(self, item)


class TestMixedCluster(AbstractCompatibilityTest):

    def setup_new_version_cluster(self):
        super().setup_new_version_cluster()

    def setup_old_version_cluster(self):
        super().setup_old_version_cluster()

    def setup_mixed_cluster_testcase(self):
        super().setup_mixed_cluster_testcase()

    def teardown_mixed_cluster_testcase(self):
        super().teardown_mixed_cluster_testcase()

    def setup_for_24_fit(self):
        from pt.tidenfabric import TidenFabric

        if self.tiden.config.get('snapshot_storage'):
            nas_manager = TidenFabric().getNasManager()
            self.snapshot_storage = nas_manager.create_shared_folder(self.tiden.config.get('snapshot_storage'),
                                                                     cleanup=False)

        self.create_app_config_set(Ignite, '24_fit_no_consist_id',
                                   logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                       'test_module_dir'],
                                   caches='caches_24_fit.xml',
                                   disabled_cache_configs=True,
                                   zookeeper_enabled=False,
                                   change_snapshot_path=True if self.snapshot_storage else False,
                                   change_snapshot_path_value=self.snapshot_storage,
                                   deploy=True
                                   )
        self.create_app_config_set(Ignite, '24_fit_with_consist_id',
                                   logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                       'test_module_dir'],
                                   disabled_cache_configs=True,
                                   consistent_id=True,
                                   caches='caches_24_fit.xml',
                                   zookeeper_enabled=False,
                                   change_snapshot_path=True if self.snapshot_storage else False,
                                   change_snapshot_path_value=self.snapshot_storage,
                                   deploy=True
                                   )
        self.config_name = '24_fit_no_consist_id'
        self.setup_old_version_cluster()

    @attr('common')
    @with_setup(setup_mixed_cluster_testcase, teardown_mixed_cluster_testcase)
    def test_mixed_cluster_load_caches_old_client(self):
        """
        1. start mixed cluster (new version servers + old version servers)
        2. start old version client
        3. activate from old version control.sh
        4. smoke check:
        4.1. create dynamic caches
        4.2. do some load
        """
        created_caches = []
        with PiClient(self.ignite_old_version, self.client_config, nodes_num=1) as piclient:
            self.ignite_old_version.cu.activate()
            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')

                ignite = piclient.get_ignite()

                ignite.getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.client_config,
                                      loading_profile=LoadingProfile(delay=1, transaction_timeout=100000)):
                sleep(60)

    @attr('common')
    @with_setup(setup_mixed_cluster_testcase, teardown_mixed_cluster_testcase)
    def test_mixed_cluster_load_caches_new_client(self):
        """
        1. start mixed cluster (new version servers + old version servers)
        2. start new version client
        3. activate from new version control.sh
        4. smoke check:
        4.1. create dynamic caches
        4.2. do some load
        """
        created_caches = []
        with PiClient(self.ignite_new_version, self.client_config, nodes_num=1) as piclient:
            self.ignite_new_version.cu.activate()
            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.client_config,
                                      loading_profile=LoadingProfile(delay=1, transaction_timeout=100000)):
                sleep(60)

    @attr('common')
    @with_setup(setup_mixed_cluster_testcase, teardown_mixed_cluster_testcase)
    def test_mixed_cluster_load_caches_old_server(self):
        """
        1. start mixed cluster (new version servers + old version servers)
        2. activate from new version control.sh
        3. start old version server
        4. add it to baseline
        5. smoke check:
        5.1. create dynamic caches from old server node
        5.2. do some load from old server node
        """

        self.ignite_new_version.cu.activate()
        created_caches = []
        self.server_config = Ignite.config_builder.get_config('server', config_set_name='base')
        ignite = self.ignite_old_version
        with PiClient(ignite, self.server_config, nodes_num=1) as piclient:
            ignite.cu.add_node_to_baseline(ignite.get_node_consistent_id(piclient.node_ids[0]))

            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')

                ignite = piclient.get_ignite()

                ignite.getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.server_config,
                                      loading_profile=LoadingProfile(delay=1, transaction_timeout=100000)):
                sleep(60)

    @attr('common')
    @with_setup(setup_mixed_cluster_testcase, teardown_mixed_cluster_testcase)
    def test_mixed_cluster_load_caches_new_server(self):
        """
        1. start mixed cluster (new version servers + old version servers)
        2. activate from old version control.sh
        3. start new version server
        4. add it to baseline
        5. smoke check
        5.1. create dynamic caches from new server node
        5.2. do some load from new server node
        """

        self.ignite_old_version.cu.activate()
        created_caches = []
        self.server_config = Ignite.config_builder.get_config('server', config_set_name='base')

        ignite = self.ignite_new_version
        with PiClient(ignite, self.server_config, nodes_num=1) as piclient:
            ignite.cu.add_node_to_baseline(ignite.get_node_consistent_id(piclient.node_ids[0]))

            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.server_config,
                                      loading_profile=LoadingProfile(delay=1, transaction_timeout=100000)):
                sleep(60)

    @attr('common')
    @with_setup(setup_new_version_cluster, teardown_mixed_cluster_testcase)
    def test_new_cluster_load_caches_old_client(self):
        """
        1. start new version grid
        2. activate from new version control.sh
        3. start old version client
        4. smoke check
        4.1. create dynamic caches
        4.2. do some load
        """
        created_caches = []
        self.server_config = Ignite.config_builder.get_config('server', config_set_name='base')

        self.ignite_new_version.cu.activate()
        with PiClient(self.ignite_old_version, self.client_config, nodes_num=1) as piclient:
            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.client_config,
                                      loading_profile=LoadingProfile(delay=1, transaction_timeout=100000)):
                sleep(120)

    @attr('common')
    @with_setup(setup_old_version_cluster, teardown_mixed_cluster_testcase)
    def test_old_cluster_load_caches_new_client(self):
        """
        1. start old version grid
        2. activate from old version control.sh
        3. start new version client
        4. smoke check:
        4.1. create dynamic caches
        4.2. do some load
        """
        created_caches = []

        self.ignite_old_version.cu.activate()
        with PiClient(self.ignite_new_version, self.client_config, nodes_num=1) as piclient:
            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.client_config,
                                      loading_profile=LoadingProfile(delay=1, transaction_timeout=100000)):
                sleep(60)

    @attr('24_fit', 'current')
    @with_setup(setup_for_24_fit, teardown_mixed_cluster_testcase)
    def test_24_fitness_rolling_upgrade(self):
        """
        This test checks the main rolling upgrade scenario under the load:
            1. Old cluster up and running (consistent_id's are not set).
            2. First cycle (upgrade to new version and set property
                GG_DISABLE_SNAPSHOT_ON_BASELINE_CHANGE_WITH_ENABLED_PITR):
            3. Second cycle (set correct consistent_id with adding to baseline topology).

        """
        created_caches = []

        self.ignite_old_version.cu.activate()

        with PiClient(self.ignite_new_version, self.client_config, nodes_num=1) as piclient:

            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

        util_sleep_for_a_while(60)

        with PiClient(self.ignite_old_version, self.client_config, nodes_num=4) as piclient:
            cache_names = piclient.get_ignite().cacheNames()

            # Start transaction loading for TTL caches
            with TransactionalLoading(MixedTestLoadingAdapter(self), config_file=self.client_config,
                                      loading_profile=LoadingProfile(delay=0, transaction_timeout=100000,
                                                                     run_for_seconds=600)):
                util_sleep_for_a_while(20)
                log_print('Rolling upgrade', color='green')
                async_ops = []
                for cache_name in [cache_name for cache_name in cache_names.toArray()
                                   if cache_name.startswith("M2_PRODUCT")]:
                    async_operation = create_async_operation(create_put_all_operation,
                                                             cache_name, 1001, 400001, 10,
                                                             value_type=ModelTypes.VALUE_ALL_TYPES.value)
                    async_ops.append(async_operation)
                    async_operation.evaluate()

                # First cycle: upgrade version and set property.
                for i in range(1, 5):
                    self.ignite_old_version.cu.control_utility('--baseline')
                    log_print('Stopping node {}'.format(i), color='green')
                    self.ignite_old_version.kill_nodes(i)

                    self.ignite_new_version.cleanup_work_dir(i)
                    folder = self.ignite_old_version.get_work_dir(i)
                    log_print(folder, color='debug')
                    self.ignite_new_version.copy_work_dir_from(i, folder)

                    jvm_options = self.ignite_new_version.get_jvm_options(i)
                    jvm_options.append('-DGG_DISABLE_SNAPSHOT_ON_BASELINE_CHANGE_WITH_ENABLED_PITR=true')

                    util_sleep_for_a_while(10)
                    self.ignite_new_version.start_nodes(i, already_nodes=(4 - i), other_nodes=(4 - i), timeout=240)
                    self.ignite_new_version.cu.control_utility('--baseline')

                for async_op in async_ops:
                    async_op.getResult()

                util_sleep_for_a_while(30)
                log_print('Change consistent ID', color='green')

                self.ignite_new_version.set_node_option('*', 'config',
                                Ignite.config_builder.get_config('server', config_set_name='24_fit_with_consist_id'))

                # Second cycle - change consistent_id and add to baseline topology.
                for i in range(1, 5):
                    self.ignite_new_version.cu.control_utility('--baseline')
                    log_print('Stopping node {}'.format(i), color='green')
                    self.ignite_new_version.kill_nodes(i)
                    log_print("Starting node {} with new consistent id".format(i), color='debug')
                    self.ignite_new_version.start_nodes(i, timeout=240)
                    log_print("Changing baseline", color='debug')
                    self.ignite_new_version.cu.set_current_topology_as_baseline()
                    util_sleep_for_a_while(60, msg='Wait for rebalance to completed')

                log_print('Transactional loading done', color='green')

            # Just to check client node still can interact with cluster - calculate checksum from client node.
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()

            async_operations = []
            cache_operation = {}
            for cache_name in sorted_cache_names:
                async_operation = create_async_operation(create_checksum_operation, cache_name, 1, 10000)
                async_operations.append(async_operation)
                cache_operation[async_operation] = cache_name
                async_operation.evaluate()

            checksums = ''
            cache_checksum = {}
            for async_operation in async_operations:
                result = str(async_operation.getResult())
                cache_checksum[cache_operation.get(async_operation)] = result
                checksums += result

            log_print('Calculating checksums done')

    @attr('24_fit')
    @with_setup(setup_for_24_fit, teardown_mixed_cluster_testcase)
    def test_24_fitness_set_baseline_with_properties(self):
        """
        This test checks the cluster behaviour with option GG_DISABLE_SNAPSHOT_ON_BASELINE_CHANGE_WITH_ENABLED_PITR
        that could be set in different ways:
            1. Set at one of the server nodes.
            2. Set on some client node/nodes.
        """
        created_caches = []

        self.ignite_old_version.cu.activate()

        # Preloading
        with PiClient(self.ignite_new_version, self.client_config, nodes_num=1) as piclient:

            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

        util_sleep_for_a_while(20)

        new_client_config = Ignite.config_builder.get_config('client', config_set_name='24_fit_with_consist_id')
        jvm_options = self.ignite_new_version.get_jvm_options(1)
        jvm_options.append('-DGG_DISABLE_SNAPSHOT_ON_BASELINE_CHANGE_WITH_ENABLED_PITR=true')
        # with PiClient(self.ignite_new_version, self.client_config, jvm_options=jvm_options, nodes_num=1) as piclient:
        with PiClient(self.ignite_new_version, self.client_config, nodes_num=1) as piclient:
            for i in range(1, 5):
                self.ignite_old_version.cu.control_utility('--baseline')
                log_print('Stopping node {}'.format(i), color='green')

                jvm_options = self.ignite_new_version.get_jvm_options(i)
                jvm_options.append('-DGG_DISABLE_SNAPSHOT_ON_BASELINE_CHANGE_WITH_ENABLED_PITR=false')
                self.ignite_new_version.set_node_option('*', 'config',
                                                        Ignite.config_builder.get_config('server',
                                                                                         config_set_name='24_fit_with_consist_id'))
                log_print("Starting node {} with new consistent id".format(i), color='debug')
                self.ignite_new_version.start_nodes(i, already_nodes=4, other_nodes=4, timeout=240)
                log_print("Changing baseline", color='debug')
                self.ignite_old_version.cu.set_current_topology_as_baseline()
                util_sleep_for_a_while(60, msg='Wait for rebalance to completed')

        log_print('Test is done')

    @attr('24_fit')
    @with_setup(setup_new_version_cluster, teardown_mixed_cluster_testcase)
    def test_24_fitness_two_clients_with_snapshot(self):
        """
        """
        created_caches = []

        self.ignite_new_version.cu.activate()

        with PiClient(self.ignite_new_version, self.client_config, nodes_num=1) as piclient_new:

            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                log_print('Loading {}...'.format(cache_name), color='green')
                piclient_new.get_ignite().getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name))

                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name, 1, 1001, 10,
                                                         value_type=self.data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='debug')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

            with PiClient(self.ignite_old_version, self.client_config, nodes_num=2, new_instance=True) as piclient_old:

                log_print('Rolling upgrade', color='green')
                self.ignite_new_version.su.snapshot_utility('snapshot', '-type=full')
                log_print('Snapshot completed', color='debug')

                sorted_cache_names = []
                for cache_name in piclient_old.get_ignite().cacheNames().toArray():
                    sorted_cache_names.append(cache_name)

                sorted_cache_names.sort()

                async_operations = []
                cache_operation = {}
                cache_checksum = {}
                for cache_name in sorted_cache_names:
                    async_operation = create_async_operation(create_checksum_operation, cache_name, 1, 10000)
                    async_operations.append(async_operation)
                    cache_operation[async_operation] = cache_name
                    async_operation.evaluate()

                checksums = ''

                for async_operation in async_operations:
                    result = str(async_operation.getResult())
                    cache_checksum[cache_operation.get(async_operation)] = result
                    checksums += result

                log_print(checksums, color='debug')
                self.ignite_new_version.su.snapshot_utility('restore', '-id={}'.format(
                                                            self.ignite_new_version.su.get_created_snapshot_id(1)))
                log_print('Test completed', color='debug')

    def calculate_sum_by_field(self,
                               start_key=0, end_key=1001,
                               field='balance',
                               config_file=None, jvm_options=None):
        """
        Calculate checksum based on piclient

        :param start_key: start key
        :param end_key: end key
        :param field: field that used in sum operation
        :param config_file: client config
        :param jvm_options: jvm options
        :return:
        """
        log_print("Calculating sum over field '%s' in each cache value" % field)

        result = {}

        if not config_file:
            config_file = self.client_config

        with PiClient(self.current_client_ignite, config_file, jvm_options=jvm_options, nodes_num=1) as piclient:
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()

            log_print(sorted_cache_names, color='debug')
            for cache_name in sorted_cache_names:
                balance = 0
                log_print('Caches: {}'.format(cache_name), color='debug')
                balance += create_sum_operation(cache_name, start_key, end_key, field).evaluate()
                result[cache_name] = balance
                log_print('Bal {}'.format(balance), color='debug')

            log_print('Calculating over field done')

        return result

