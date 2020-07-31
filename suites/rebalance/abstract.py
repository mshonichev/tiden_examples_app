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

from tiden import log_print, get_logger, is_enabled, TidenException
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden_gridgain.case.singlegridzootestcase import SingleGridZooTestCase


class AbstractRebalanceTest(SingleGridZooTestCase):
    group_names = []  # group names to wait to be rebalanced
    rebalance_timeout = 240  # completed rebalance timeout
    iterations = 5  # number of rebalance test iterations
    logger = get_logger('tiden')

    need_delete_lfs_on_teardown = True

    def setup(self):
        self.ignite.jmx.rebalance_collect_timeout = 5

        default_context = self.contexts['default']
        default_context.add_context_variables(
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled')),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            zoo_connection=self.zoo._get_zkConnectionString()
            if is_enabled(self.config.get('zookeeper_enabled')) else None,
            rebalance_pool_size=int(self.config.get('rebalance_pool_size', 4)),
            wal_segment_size=int(self.config.get('wal_segment_size', 1000000)),
            num_wal_segments=int(self.config.get('num_wal_segments', 5)),
            logger_enabled=False,
        )

        # context with configs for backup flickering test
        backup_context = self.create_test_context('backup')
        backup_context.add_config('caches.tmpl.xml', 'caches_backup.xml')
        self.prepare_backup_test_config(self.config)

        backup_context.add_context_variables(
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled')),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            zoo_connection=self.zoo._get_zkConnectionString()
            if is_enabled(self.config.get('zookeeper_enabled')) else None,
            rebalance_pool_size=int(self.config.get('rebalance_pool_size', 4)),
            cache_group_mult=int(self.config.get('cache_group_mult', 3)),
            wal_history_size=int(self.config.get('wal_history_size', 10000)),
            num_wal_segments=int(self.config.get('num_wal_segments', 20)),
            caches_file='backup',
            test_config=self.config,
            logger_enabled=True,
            logger_path='%s/ignite-log4j2.xml' % self.config['rt']['remote']['test_module_dir'],
        )

        # context with configs for 24 Hour Fitness
        fitness_context = self.create_test_context('fitness')
        fitness_context.add_context_variables(
            snapshots_enabled=True,
            pitr_enabled=True,
            zookeeper_enabled=False,
            caches_file='fitness',
            rebalance_pool_size=int(self.config.get('rebalance_pool_size', 8)),
            wal_segment_size=int(self.config.get('wal_segment_size', 1000000)),
            num_wal_segments=int(self.config.get('num_wal_segments', 5)),
            logger_enabled=True,
            logger_path='%s/ignite-log4j2.xml' % self.config['rt']['remote']['test_module_dir'],
        )

        diff_pool_context = self.create_test_context('diff_pool')
        diff_pool_context.add_context_variables(
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled')),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            zoo_connection=self.zoo._get_zkConnectionString()
            if is_enabled(self.config.get('zookeeper_enabled')) else None,
            rebalance_pool_size=1,
            wal_segment_size=int(self.config.get('wal_segment_size', 1000000)),
            num_wal_segments=int(self.config.get('num_wal_segments', 5)),
            logger_enabled=False,
        )

        super().setup()

        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestRebalance]')

    def teardown(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.stop_zookeeper()

    def _start_grid_no_preload(self, context_name, **kwargs):
        self.logger.info('TestSetup is called')
        self.set_current_context(context_name)
        self.rebuild_configs()
        self.contexts[context_name].build_and_deploy(self.ssh)

        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()

        jvm_options = [
            '-ea',
            # make sure that rebalance queue is big enough
            # '-DIGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE=19200000',
            '-DIGNITE_REBALANCE_THROTTLE_OVERRIDE=100',
        ]

        rebalance_type = self.config.get('rebalance_type', kwargs.get('rebalance_type', False))
        if rebalance_type == 'historic':
            # make sure that for no-clean-LFS cases there is always historical rebalance triggered
            wal_rebalance_treshold = '0'
        elif rebalance_type == 'full':
            # use full rebalancing instead of delta-historic rebalancing
            wal_rebalance_treshold = '10000000000'
        else:
            wal_rebalance_treshold = None

        if wal_rebalance_treshold:
            jvm_options.extend([
                '-DIGNITE_PDS_WAL_REBALANCE_THRESHOLD={}'.format(wal_rebalance_treshold),
            ])

        self.ignite.set_activation_timeout(120)
        self.ignite.set_snapshot_timeout(240)
        self.su.clear_snapshots_list()
        self.ignite.set_node_option('*', 'jvm_options', jvm_options)
        self.start_grid()

    def _wait_cluster_ready(self):
        if self.get_context_variable('pitr_enabled') and self.get_context_variable('snapshots_enabled'):
            self.su.wait_no_snapshots_activity_in_cluster()

        self.group_names = PiClientIgniteUtils.collect_cache_group_names(self.ignite,
                                                                         self.get_client_config())

        if not self.ignite.jmx.is_started():
            self.ignite.jmx.start_utility()

        # wait for no client on cluster
        self.ignite.wait_for_topology_snapshot(client_num=0)

        if is_enabled(self.config.get('disable_baseline_autoadjustment')):
            log_print("Going to disable baseline autoadjustment", color='green')
            if self.cu.is_baseline_autoajustment_supported():
                self.cu.disable_baseline_autoajustment()
                log_print("Baseline autoadjustment disabled", color='green')

        log_print(repr(self.ignite), color='debug')

    def _setup_with_context(self, context_name, **kwargs):
        self._start_grid_no_preload(context_name, **kwargs)

        if self.preloading_size > 0:
            if self.preloading_with_streamer:
                PiClientIgniteUtils.load_data_with_streamer(self.ignite, self.get_client_config(),
                                                            end_key=self.preloading_size)
            else:
                PiClientIgniteUtils.load_data_with_putall(self.ignite, self.get_client_config(),
                                                          end_key=self.preloading_size)

        self._wait_cluster_ready()

    def teardown_testcase(self):
        self.logger.info('TestTeardown is called')

        log_print(repr(self.ignite), color='debug')
        log_print(self.ignite.get_all_additional_nodes(), color='blue')
        log_print(self.ignite.get_alive_additional_nodes(), color='blue')
        for additional_node in self.ignite.get_alive_additional_nodes():
            self.ignite.kill_node(additional_node)

        # kill utility if exists
        if self.ignite.jmx.is_started():
            self.ignite.jmx.kill_utility()

        self.stop_grid_hard()
        self.su.copy_utility_log()

        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()
            # self.zoo.collect_logs_to_folder(self.ignite.config['rt']['remote']['test_dir'])

        # c'mon, I'm too lazy to do it right
        log_print(f'Value for self.need_delete_lfs_on_teardown is {self.need_delete_lfs_on_teardown}', color='debug')
        if self.need_delete_lfs_on_teardown:
            self.cleanup_lfs()
        self.set_current_context()
        self.reset_cluster()
        log_print(repr(self.ignite), color='debug')

    def setup_testcase(self):
        self.preloading_size = 100000
        self.preloading_with_streamer = True
        self._setup_with_context('default')

    def setup_fitness_testcase(self):
        self.preloading_size = 10000
        self.preloading_with_streamer = False
        self._setup_with_context('fitness')

    def prepare_backup_test_config(self, config):
        self.config['iteration_size'] = int(config.get('iteration_size', 80000))
        self.config['atomic_enabled'] = config.get('atomic_enabled', False)
        self.config['onheap_caches_enabled'] = config.get('onheap_caches_enabled', False)
        self.config['evictions_enabled'] = config.get('evictions_enabled', False)
        if self.config['evictions_enabled']:
            self.config['onheap_caches_enabled'] = True
        self.config['equal_partition_count'] = config.get('equal_partition_count', True)
        self.config['partition_count'] = int(config.get('partition_count', 64))

    def assert_nodes_alive(self):
        expected = len(self.ignite.get_alive_default_nodes() + self.ignite.get_alive_additional_nodes())
        actual = self.ignite.get_nodes_num('server')
        assert expected == actual, \
            "Missing nodes found. Check logs!\nActual: %s\nExpected: %s\n" % (actual, expected)

    def start_node(self, node_id):
        """
        Cause of Zookeeper if nodes restarts too quickly, we can get the error on nodes start
        like "Failed to add node to topology because it has the same hash code for partitioned affinity..."
        But after that if nodes starts again, it should be added to topology.
        This method does exactly this - Starts node, if there is a error, start node again.
        :param node_id:
        :return:
        """
        if is_enabled(self.config.get('zookeeper_nodes_restart')) or is_enabled(self.config.get('zookeeper_enabled')):
            self._util_start_node_several_times(node_id)
        else:
            if self.ignite.is_additional_node(node_id):
                self.ignite.start_additional_nodes(node_id)
            else:
                self.ignite.start_node(node_id)

    def _util_start_node_several_times(self, node_id):
        try:
            timeout = self.ignite.get_snapshot_timeout()
            self.ignite.set_snapshot_timeout(60)
            self.ignite.start_additional_nodes(node_id) \
                if self.ignite.is_additional_node(node_id) else self.ignite.start_node(node_id)

        except TidenException as e:
            check_msg = 'Failed to add node to topology because it has the same hash code for partitioned affinity ' \
                        'as one of existing nodes'
            result = self.ignite.find_in_node_log(node_id, ' grep "{}"'.format(check_msg))

            if result:
                self.ignite.start_additional_nodes(node_id) \
                    if self.ignite.is_additional_node(node_id) else self.ignite.start_node(node_id)
            else:
                raise e
        finally:
            self.ignite.set_snapshot_timeout(timeout)

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
            log_print("Node id {} does not found in server nodes {}".format(node_id, self.ignite.nodes.keys()),
                      color='red')

