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
from tiden.apps.netstat import Netstat
from tiden import log_print, sleep, is_enabled, util_sleep_for_a_while
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.generator_utils import AffinityCountKeyGeneratorBuilder, AffinityPartitionKeyGeneratorBuilder
from tiden_gridgain.piclient.helper.operation_utils import create_streamer_operation, create_async_operation
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile, LoadingUtils
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.utilities import JmxUtility
from suites.consumption.framework.scenarios.abstract import AbstractScenario
from suites.consumption.framework.utils import get_nodes_directory_size, convert_size

CHECKPOINT_SLEEP = 2
LOADING_STABLE_TIME = 60

DEFAULT_LOAD_TYPE = ModelTypes.VALUE_ALL_TYPES.value
CACHE_NAME = 'cache_group_4_091'
CACHE_GROUP = 'cache_group_4'
CACHE_NAME_NOT_IN_REBALANCE = 'cache_group_not_in_rebalance'
CACHE_GROUP_NOT_IN_REBALANCE = 'cache_group_not_in_rebalance_group'
GAGE_NODE = 1
NODE_TO_REBALANCE = 2
MAX_REBALANCE_TIMEOUT = 3600
PUT_DATA_STEP = 50000
PUT_DATA_STEP_LOADING = 1000000
REBALANCE_CONFIG_SET = 'rebalance'


class RebalanceScenario(AbstractScenario):

    def __init__(self, test_class):
        super().__init__(test_class)

        # default values for instance variables
        self.historical_rebalance = False
        self.metrics_idle = 30
        self.with_loading = False
        self.idle_verify = False
        self.load_type = DEFAULT_LOAD_TYPE
        self.put_data_step = PUT_DATA_STEP
        self.single_cache = False
        self.parts_distribution = None
        self.with_no_rebalance_cache = False
        self.jfr_settings = False

    def _validate_config(self):
        super()._validate_config()

        assert self.config.get('times_to_run'), 'There is no "times_to_run" variable in config'
        assert self.config.get('keys_to_load'), \
            'There is no "keys_to_load" variable in config'

    def run(self, artifact_name):
        super().run(artifact_name)

    def initialize_config(self):
        self.historical_rebalance = self.config.get('historical_rebalance', False)
        self.metrics_idle = self.config.get('metrics_idle', 30)
        self.with_loading = self.config.get('with_loading', False)
        self.idle_verify = is_enabled(self.config.get('idle_verify'))
        self.load_type = self.config.get('load_type', DEFAULT_LOAD_TYPE)
        self.single_cache = 'single_cache' in self.config
        self.parts_distribution = self.config.get('partition_distribution', None)
        self.with_no_rebalance_cache = self.config.get('with_no_rebalance_cache', False)
        self.jfr_settings = self.config.get('jfr_settings', None)

        if self.with_loading and self.historical_rebalance:
            log_print('There is no support historical rebalance with loading. Skipping loading.')
            self.with_loading = False
        if self.idle_verify and self.with_loading:
            log_print('Skipping idle_verify parameter because of with_loading used', color='yellow')

    def kill_cluster(self, ignite):
        ignite.kill_nodes()
        ignite.delete_lfs()
        log_print("Cleanup Ignite LFS ... ")
        commands = {}
        for node_idx in ignite.nodes.keys():
            host = ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
        self.test_class.tiden.ssh.exec(commands)
        log_print("Ignite LFS deleted.")
        log_print()

    def start_cluster_with_data(self, keys_to_load, historical_rebalance):

        if historical_rebalance:
            rebalance_jvm_options = ['-DIGNITE_PDS_WAL_REBALANCE_THRESHOLD=0', ]
        else:
            rebalance_jvm_options = ['-DIGNITE_PDS_WAL_REBALANCE_THRESHOLD=500000000', ]

        rebalance_jvm_options += self.artifact_jvm_properties

        version, ignite = self.test_class.start_ignite_grid(
            self.artifact_name,
            activate=True,
            config_set=REBALANCE_CONFIG_SET,
            jvm_options=rebalance_jvm_options
        )
        # load massive amount of data to CACHE_NAME cache
        last_end_key = self.load_amount_of_data(ignite, NODE_TO_REBALANCE, 0, keys_to_load)
        return ignite, last_end_key, version

    def load_amount_of_data(self, ignite, node, start_key, keys_to_load):
        """
        Tricky method that loads data into a cluster.

        The main idea here is to load data that will be rebalanced. Other data will not be written into a cluster.
        So for example: we collect rebalance time by killing node_2, in this case we load data that will be contains
        primary and backup partitions on node_2. Data that should not appear on node_2 will not be loaded.

        Also there is a case with cache that shouldn't be rebalanced, but used in TransactionalLoading. This cache
        needed to measure rebalance influence to a cache that not in rebalance.

        :param ignite: ignite instance
        :param node: node to rebalance
        :param start_key: start key to load
        :param keys_to_load: number os keys that should be loaded (NOT END_KEY!)
        :return:
        """
        client_config = Ignite.config_builder.get_config('client', config_set_name=REBALANCE_CONFIG_SET)
        log_print("Just load data from %s with %s affinity keys on node %s" %
                  (start_key, keys_to_load, NODE_TO_REBALANCE))

        with PiClient(ignite, client_config, nodes_num=2 if self.with_no_rebalance_cache else 1) as piclient:
            async_operations = []

            # load cache that will not be rebalanced but with loading
            if self.with_loading and self.with_no_rebalance_cache:
                gateway = piclient.get_gateway()

                log_print("Loading cache that will not be included into rebalance", color='red')

                operation_no_rebalance = create_async_operation(
                    create_streamer_operation,
                    CACHE_NAME_NOT_IN_REBALANCE, start_key, keys_to_load,
                    value_type=self.load_type,
                    gateway=gateway,
                )

                # here we define AffinityCountKeyGenerator to a loading operation
                # this specific loading allow us to measure loading to a non rebalanced cache
                operation_no_rebalance.getOperation().setKeyGenerator(
                    AffinityCountKeyGeneratorBuilder(
                        CACHE_NAME_NOT_IN_REBALANCE,
                        ignite.get_node_consistent_id(node),
                        start_key,
                        keys_to_load,
                        False
                    ).build()
                )

                operation_no_rebalance.evaluate()

                async_operations.append(operation_no_rebalance)

            gateway = piclient.get_gateway()

            # load cache that will be rebalanced
            log_print("Loading cache that will be rebalanced", color='red')
            operation = create_async_operation(
                create_streamer_operation,
                CACHE_NAME, start_key, keys_to_load,
                value_type=self.load_type,
                gateway=gateway,
            )

            if self.parts_distribution:
                # this is specific "all data in on partition case"
                operation.getOperation().setKeyGenerator(
                    AffinityPartitionKeyGeneratorBuilder(
                        CACHE_NAME,
                        self.parts_distribution,
                        start_key,
                        keys_to_load,
                    ).build()
                )
            else:
                # default partition distribution
                operation.getOperation().setKeyGenerator(
                    AffinityCountKeyGeneratorBuilder(
                        CACHE_NAME,
                        ignite.get_node_consistent_id(node),
                        start_key,
                        keys_to_load,
                        True
                    ).build()
                )

            operation.evaluate()

            async_operations.append(operation)

            for async_operation in async_operations:
                async_operation.getResult()

            last_key = operation.getResult()

        return last_key

    def calculate_rebalance_speed(self, ignite, prod_runs, warmup_runs, last_end_key, keys_to_load,
                                  tx_loading=None, version=None):
        """
        :param ignite: Ignite app instance
        :param prod_runs: production runs
        :param warmup_runs: warmup runs
        :param last_end_key: last inserted key (for historical rebalance)
        :param tx_loading: TransactionalLoading instance (for rebalance under loading)
        :param version: Ignite version (for non historical rebalance - full with/ and w/o loading)
        :param keys_to_load: number of key to load 5kk ~ 4.5 Gb traffic
        :return:
        """
        assert ignite or self.historical_rebalance, \
            'Error in scenario code! Ignite or historical_rebalance should be defined'

        netstat = Netstat('dft', {}, self.test_class.ssh)  # run netstat to collect network activity

        rebalance_speed = list()  # list of results for various runs

        jmx = None

        # Here we use artifact name to store LFS.
        # This needed to measure same versions with different JVM options/configs
        stored_lfs_name = 'rebalance_%s' % self.artifact_name

        try:
            warmup_time = 0

            # Be careful here - JMX utility must be properly killed after test
            jmx = JmxUtility(ignite)
            jmx.start_utility()

            for i in range(0, warmup_runs + prod_runs):
                host = ignite.nodes[NODE_TO_REBALANCE]['host']
                group_path = 'work/db/{}/cacheGroup-{}'.format(
                    ignite.get_node_consistent_id(NODE_TO_REBALANCE),
                    CACHE_GROUP
                )

                warmup_iteration = False if warmup_runs == 0 else i < warmup_runs
                log_print("Running iteration %s (%s)" % (i, 'warmup' if warmup_iteration else 'prod'))

                dir_size_before = get_nodes_directory_size(
                    ignite,
                    self.test_class.ssh,
                    group_path,
                    nodes=[NODE_TO_REBALANCE]
                )

                self.rebalance_event_write(i, tx_loading, 'iteration_{}: restart node'.format(i))

                ignite.kill_node(NODE_TO_REBALANCE)

                if self.historical_rebalance:
                    # For historical rebalance we store LFS for all cluster and then use this LFS to continue test
                    # There is some issues appears while restoring Ignite from stored LFS
                    # For 2.5.8 version for example there may be some failures

                    list_nodes_to_stop = list(node_id for node_id in ignite.nodes.keys()
                                              if node_id < 100000 and node_id != NODE_TO_REBALANCE)
                    if not ignite.exists_stored_lfs(stored_lfs_name):
                        self.load_amount_of_data(ignite, GAGE_NODE, last_end_key, keys_to_load)
                        # Avoid topology changes by client exiting
                        # https://ggsystems.atlassian.net/browse/GG-21629
                        ignite.wait_for_topology_snapshot(client_num=0)
                        util_sleep_for_a_while(30)
                        ignite.cu.deactivate()
                        util_sleep_for_a_while(30)
                        ignite.kill_nodes(*list_nodes_to_stop)
                        ignite.save_lfs(stored_lfs_name)
                        ignite.start_nodes()
                        ignite.cu.activate()
                    else:
                        util_sleep_for_a_while(30)
                        ignite.cu.deactivate()
                        util_sleep_for_a_while(30)
                        ignite.kill_nodes(*list_nodes_to_stop)
                        ignite.delete_lfs()
                        ignite.restore_lfs(stored_lfs_name)
                        ignite.start_nodes()
                else:
                    self._cleanup_lfs(ignite, NODE_TO_REBALANCE)

                    # sleep after cleanup LFS
                    sleep(10)

                start_rcvd_bytes = netstat.network()[host]['total']['RX']['bytes']
                start_sent_bytes = netstat.network()[host]['total']['TX']['bytes']

                ignite.start_node(NODE_TO_REBALANCE)

                log_print("Waiting for finish rebalance")

                # To collect JFRs we need to define time to collect JFRs.
                # Here we use warmup iteration rebalance time to set time
                if warmup_time and not warmup_iteration and self.jfr_settings:
                    ignite.make_cluster_jfr(int(warmup_time) + LOADING_STABLE_TIME, self.jfr_settings)

                rebalance_time = jmx.wait_for_finish_rebalance(
                    MAX_REBALANCE_TIMEOUT,
                    [CACHE_GROUP],
                    calculate_exact_time=True,
                    time_for_node=NODE_TO_REBALANCE,
                    log=False
                )

                self.rebalance_event_write(i, tx_loading, 'rebalance finished')

                finish_rcvd_bytes = netstat.network()[host]['total']['RX']['bytes']
                finish_sent_bytes = netstat.network()[host]['total']['TX']['bytes']

                self.write_time_event('iteration_%s rebalance finished' % i)

                # wait for checkpoint
                sleep(CHECKPOINT_SLEEP)

                dir_size_after = get_nodes_directory_size(
                    ignite,
                    self.test_class.ssh,
                    group_path,
                    nodes=[NODE_TO_REBALANCE]
                )

                if version:
                    log_print('Values for version: {}'.format(version))
                rcvd_bytes = finish_rcvd_bytes - start_rcvd_bytes
                avg_speed = float(rcvd_bytes) / rebalance_time
                log_print("Current rebalance time in {} seconds (Directory size: before/after - {}/{}, "
                          "Network activity: send/received - {}/{})\nRebalance speed (received network/time): {}/sec"
                          .format(rebalance_time,
                                  convert_size(dir_size_before),
                                  convert_size(dir_size_after),
                                  convert_size(finish_sent_bytes - start_sent_bytes, start_byte=True),
                                  convert_size(rcvd_bytes, start_byte=True),
                                  convert_size(avg_speed, start_byte=True)
                                  ),
                          color='yellow'
                          )

                if not warmup_iteration:
                    if tx_loading:
                        sleep(LOADING_STABLE_TIME)

                        self.write_time_event('~JFR collection ended')

                        sleep(LOADING_STABLE_TIME)

                    rebalance_speed.append(avg_speed)
                else:
                    warmup_time = rebalance_time
        finally:
            if jmx:
                jmx.kill_utility()

        return rebalance_speed

    def rebalance_event_write(self, i, tx_loading, text):
        self.write_time_event('iteration_%s %s' % (i, text))
        if tx_loading:
            LoadingUtils.custom_event(tx_loading, '%s' % text)

    def _cleanup_lfs(self, ignite, node_id=None):
        log_print('Cleanup Ignite LFS ... ')
        commands = {}

        nodes_to_clean = [node_id] if node_id else ignite.nodes.keys()

        for node_idx in nodes_to_clean:
            host = ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
        results = self.test_class.ssh.exec(commands)
        print(results)
        log_print('Ignite LFS deleted.')
        log_print()


class FullRebalanceScenario(RebalanceScenario):
    def run(self, artifact_name):
        """
        Run rebalance scenario for defined artifact

        Scenario is very simple
        1. start cluster
        2. load data to one cache with backups until size reached 'data_size_kb' from config (5GB is optimal)
        3. start or skip loading
        4. kill node with cache, clean lfs, start node again
        5. using JMX utility wait until LocalNodeMovingPartitionsCount for cache will be 0
        6. save this value and divide by spent time

        Also netstat metrics collected while running this scenario
        (In this case we don't need separate probe to collect more precise metrics)

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running rebalance benchmark with config: %s" % self.config, color='green')

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        ignite = None
        try:
            # collect properties from config
            self.initialize_config()

            in_memory = self.config.get('in_memory', False)
            xml_config_set_name = 'caches_%s.xml' % REBALANCE_CONFIG_SET \
                if 'single_cache' not in self.config else 'single_cache_%s.xml' % REBALANCE_CONFIG_SET
            self.test_class.create_app_config_set(
                Ignite, REBALANCE_CONFIG_SET,
                deploy=True,
                caches_list_file=xml_config_set_name,
                snapshots_enabled=True,
                logger=True,
                wal_segment_size=self.test_class.consumption_config.get('wal_segment_size',
                                                                        64 * 1024 * 1024),
                logger_path='%s/ignite-log4j2.xml' %
                            self.test_class.tiden.config['rt']['remote']['test_module_dir'],
                disabled_cache_configs=False,
                zookeeper_enabled=False,
                rebalance_pool_size=self.config.get('rebalance_pool_size', 8),
                system_pool_size=self.config.get('rebalance_pool_size', 8) + 8,
                checkpoint_read_lock_timeout=self.read_lock_property_value(version),
                wal_compaction_enabled=self.artifact_config_variables.get('wal_compaction_enabled', False),
                # caches related variables
                additional_configs=['caches.tmpl.xml', ] if 'single_cache' not in self.config else [
                    'single_cache.tmpl.xml', ],
                partitions=5 if self.parts_distribution else 1024,
                part_32=self.test_class.consumption_config.get('part_32',
                                                               32),  # see cache.tmpl.xml for more details
                part_64=self.test_class.consumption_config.get('part_64',
                                                               64),
                part_128=self.test_class.consumption_config.get('part_64',
                                                                128),
                in_memory=in_memory,
                backups=self.config.get('backups', 0),
                load_type=self.load_type,
            )

            # run ignite app
            keys_to_load = int(self.config.get('keys_to_load'))
            ignite, last_end_key, version = self.start_cluster_with_data(keys_to_load, False)

            ignite.set_snapshot_timeout(600)

            # wait for checkpoint
            sleep(CHECKPOINT_SLEEP)

            # dump idle_verify if need and no loading
            dump_before = None
            if self.idle_verify and not self.with_loading:
                dump_before = ignite.cu.idle_verify_dump()

            self.start_probes(artifact_name)

            warmup_runs, prod_runs = self._get_number_of_runs()

            # run rebalance calculation
            if self.with_loading:
                client_config = Ignite.config_builder.get_config('client', config_set_name=REBALANCE_CONFIG_SET)
                with PiClient(ignite, client_config, jvm_options=['-DPICLIENT_OPERATIONS_POOL_SIZE=64']) as piclient:
                    if self.parts_distribution:
                        # for partition distribution we need to pass config_loading_dict
                        cache_load_map = {
                            CACHE_NAME: {
                                # following keys are key generator builder arguments
                                # we build java object later, when we will knew exact gateway
                                'key_generator_builder': AffinityPartitionKeyGeneratorBuilder(
                                    CACHE_NAME,
                                    self.parts_distribution,
                                    1,
                                    keys_to_load,
                                ).set_collision_possibility(0.5),

                                # this is metrics postfix (need to separate different caches in plot)
                                'metric_postfix': 'rebalance',  # metrics postfix for plot
                            },
                        }
                    else:
                        cache_load_map = {
                            CACHE_NAME: {
                                # following keys are key generator builder arguments
                                # we build java object later, when we will knew exact gateway
                                'key_generator_builder':
                                    AffinityCountKeyGeneratorBuilder(
                                        CACHE_NAME,
                                        ignite.get_node_consistent_id(
                                            NODE_TO_REBALANCE),
                                        1,
                                        keys_to_load,
                                        True
                                    ).set_collision_possibility(0.5),

                                # this is metrics postfix (need to separate different caches in plot)
                                'metric_postfix': 'rebalance',  # metrics postfix for plot
                            },
                        }

                    caches_to_load = [CACHE_NAME, ]

                    # define tx_metrics for TransactionalLoading
                    tx_metrics = [
                        'txCreated_rebalance',
                        'txFailed_rebalance',
                    ]

                    if self.with_no_rebalance_cache:
                        # this cache will not be on NODE_TO_REBALANCE but will be under transactionalLoading
                        cache_load_map[CACHE_NAME_NOT_IN_REBALANCE] = {
                            # following keys are key generator builder arguments
                            # we build java object later, when we will knew exact gateway
                            'key_generator_builder': AffinityCountKeyGeneratorBuilder(
                                CACHE_NAME_NOT_IN_REBALANCE,
                                ignite.get_node_consistent_id(
                                    NODE_TO_REBALANCE),
                                1,
                                keys_to_load,
                                False
                            ).set_collision_possibility(0.5),

                            # this is metrics postfix (need to separate different caches in plot)
                            'metric_postfix': 'no_rebalance',  # metrics postfix for plot
                        }

                        caches_to_load.append(CACHE_NAME_NOT_IN_REBALANCE)

                        # mutate tx_metrics for TransactionalLoading
                        tx_metrics.append('txCreated_no_rebalance')
                        tx_metrics.append('txFailed_no_rebalance')

                    with TransactionalLoading(self.test_class,
                                              ignite=ignite,
                                              cu=ignite.cu,
                                              config_file=client_config,
                                              caches_to_run=caches_to_load,
                                              skip_consistency_check=True,
                                              cross_cache_batch=1,
                                              cache_load_map=cache_load_map,
                                              keys_count=keys_to_load,
                                              # multiply execution operations, because we load only in 1 or 2 caches
                                              load_threads=16 * piclient.nodes_num if self.single_cache else None,
                                              collect_timeout=5000,
                                              collision_possibility=0.5,
                                              with_exception=False,
                                              loading_profile=LoadingProfile(commit_possibility=0.8,
                                                                             end_key=last_end_key),
                                              tx_metrics=tx_metrics
                                              ) as tx_loading:
                        LoadingUtils.sleep_and_custom_event(tx_loading, 'Sleep before test', self.metrics_idle)

                        # define snapshot timeout for rebalance on loading
                        ignite.snapshot_timeout = 600

                        rebalance_speed = \
                            self.calculate_rebalance_speed(
                                ignite, prod_runs, warmup_runs, last_end_key, keys_to_load,
                                tx_loading=tx_loading,
                            )

                        metrics = tx_loading.metrics

                LoadingUtils.create_loading_metrics_graph(self.test_class.config['suite_var_dir'],
                                                          'rebalance_%s_%s' %
                                                          (
                                                              version,
                                                              'loading' if self.with_loading else ''
                                                          ),
                                                          metrics)
            else:
                rebalance_speed = self.calculate_rebalance_speed(
                    ignite, prod_runs, warmup_runs, last_end_key, keys_to_load,
                    version=version,
                )

            # dump idle_verify if need and no loading
            if self.idle_verify and not self.with_loading:
                dump_after = ignite.cu.idle_verify_dump()

                if dump_after != dump_before:
                    log_print('Failed idle_verify additional check', color='red')

            ignite.cu.deactivate()

            self.stop_probes(speed=rebalance_speed)

            self.results['evaluated'] = True
        finally:
            if ignite:
                self.kill_cluster(ignite)

            # remove config set
            self.test_class.remove_app_config_set(Ignite, REBALANCE_CONFIG_SET)


class HistoricalRebalanceScenario(RebalanceScenario):
    def run(self, artifact_name):
        """
        Run rebalance scenario for defined artifact (Historical rebalance)

        Scenario is very simple
        1. start cluster
        2. load data to one cache with backups until size reached 'data_size_kb' from config (5GB is optimal)
        3. start or skip loading
        4. kill node with cache, clean lfs, start node again
        5. using JMX utility wait until LocalNodeMovingPartitionsCount for cache will be 0
        6. save this value and divide by spent time

        Also netstat metrics collected while running this scenario
        (In this case we don't need separate probe to collect more precise metrics)

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running rebalance benchmark with config: %s" % self.config, color='green')

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        ignite = None
        try:
            # collect properties from config
            self.initialize_config()

            xml_config_set_name = 'caches_%s.xml' % REBALANCE_CONFIG_SET \
                if 'single_cache' not in self.config else 'single_cache_%s.xml' % REBALANCE_CONFIG_SET
            self.test_class.create_app_config_set(
                Ignite, REBALANCE_CONFIG_SET,
                deploy=True,
                caches_list_file=xml_config_set_name,
                snapshots_enabled=True,
                logger=True,
                wal_segment_size=self.test_class.consumption_config.get('wal_segment_size',
                                                                        64 * 1024 * 1024),
                logger_path='%s/ignite-log4j2.xml' %
                            self.test_class.tiden.config['rt']['remote']['test_module_dir'],
                disabled_cache_configs=False,
                zookeeper_enabled=False,
                rebalance_pool_size=self.config.get('rebalance_pool_size', 8),
                wal_compaction_enabled=self.artifact_config_variables.get('wal_compaction_enabled', False),
                system_pool_size=self.config.get('rebalance_pool_size', 8) + 8,
                checkpoint_read_lock_timeout=self.read_lock_property_value(version),
                # caches related variables
                additional_configs=['caches.tmpl.xml', ] if 'single_cache' not in self.config else [
                    'single_cache.tmpl.xml', ],
                partitions=5 if self.parts_distribution else 1024,
                part_32=self.test_class.consumption_config.get('part_32',
                                                               32),
                part_64=self.test_class.consumption_config.get('part_64',
                                                               64),
                part_128=self.test_class.consumption_config.get('part_64',
                                                                128),
                in_memory=False,
                backups=self.config.get('backups', 0),
                load_type=self.load_type,
            )

            keys_to_load = int(self.config.get('keys_to_load'))

            self.start_probes(artifact_name)

            warmup_runs, prod_runs = self._get_number_of_runs()

            ignite, last_end_key, version = self.start_cluster_with_data(keys_to_load, False)

            # run rebalance calculation
            # TODO currently there is no support for historical rebalance and loading
            rebalance_speed = self.calculate_rebalance_speed(
                ignite, prod_runs, warmup_runs, last_end_key, keys_to_load,
                version=version
            )

            self.stop_probes(speed=rebalance_speed)

            self.results['evaluated'] = True

            ignite.cu.deactivate()
        finally:
            if ignite:
                self.kill_cluster(ignite)

            # remove config set
            self.test_class.remove_app_config_set(Ignite, REBALANCE_CONFIG_SET)

