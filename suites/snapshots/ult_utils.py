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
from re import search, findall

from tiden_gridgain.piclient.helper.cache_utils import DynamicCachesFactory, IgniteCache
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_streamer_operation, create_checksum_operation, \
    create_async_operation, create_account_runner_operation

from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden_gridgain.case.singlegridzootestcase import SingleGridZooTestCase
from tiden.util import log_print, is_enabled, util_sleep_for_a_while, print_green, print_blue, print_red, log_put
from tiden.assertions import tiden_assert_equal, tiden_assert
from tiden.tidenexception import TidenException
from tiden.logger import get_logger
from tiden.report.steps import step


class UltimateUtils(SingleGridZooTestCase):
    max_key = 1000
    load_multiplier = 1
    snapshot_storage = None
    gg_version = None
    reusable_lfs = False
    lfs_stored = False

    def setup(self):
        default_context = self.contexts['default']
        authentication_enabled = is_enabled(self.config.get('authentication_enabled'))
        self.reusable_lfs = is_enabled(self.config.get('reusable_lfs_enable'))

        default_context.add_context_variables(
            persistence_enabled=True,
            snapshots_enabled=is_enabled(self.config.get('snapshots_enabled', True)),
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            compaction_enabled=is_enabled(self.config.get('compaction_enabled')),
            authentication_enabled=authentication_enabled,
            sbt_model_enabled=is_enabled(self.config.get('sbt_model_enabled')),
            caches='caches_sbt.xml' if is_enabled(self.config.get('sbt_model_enabled')) else 'caches.xml',
            snapshot_archive_enabled=is_enabled(self.config.get('snapshot_arch')),
            dynamic_cache_enabled=is_enabled(self.config.get('dynamic_cache_enabled')),
            blt_auto_adjust_enabled=is_enabled(self.config.get('blt_auto_adjust_enabled')),
            community_edition_enabled=is_enabled(self.config.get('community_edition_enabled'))
        )

        if authentication_enabled:
            auth_login = self.config.get('auth_login', 'server_user')
            auth_password = self.config.get('auth_password', 'server_password')

            default_context.add_context_variables(
                auth_login=auth_login,
                auth_password=auth_password,
            )

            self.cu.enable_authentication(auth_login, auth_password),
            self.su.enable_authentication(auth_login, auth_password),
            self.ignite.enable_authentication(auth_login, auth_password),

        if not is_enabled(self.config.get('community_edition_enabled')):
            self.gg_version = self.config['artifacts']['ignite']['gridgain_version']

        super().setup()
        self.load_multiplier = float(self.config.get('pitr_load_multiplier', UltimateUtils.load_multiplier))
        self.ignite.set_activation_timeout(20)
        self.ignite.set_snapshot_timeout(60)
        self.blt_auto_adjust_timeout = 60000
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestSnapshots]')

    def teardown(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.stop_zookeeper()

    def setup_testcase(self):
        if self.lfs_stored:
            self.restore_lfs('snapshot_util', timeout=1200)
        self.setup_testcase_without_start_gid()

        activation_timeout = 60
        if self.get_context_variable('sbt_model_enabled'):
            activation_timeout = 200
        self.start_grid(timeout=activation_timeout, activate_on_particular_node=1)

        if not self.lfs_stored:
            if self.get_context_variable('dynamic_cache_enabled'):
                self.start_caches_dynamic(caches_file_name=self.get_context_variable('caches'), batch_size=10000)

            if self.get_context_variable('sbt_model_enabled'):
                PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                                   self.ignite,
                                                                   self.get_client_config(),
                                                                   only_caches_batch=None,
                                                                   end_key=int(self.max_key * self.load_multiplier))

            else:
                PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                            self.get_client_config(),
                                                            end_key=int(self.max_key * self.load_multiplier),
                                                            allow_overwrite=True)
        log_print(repr(self.ignite), color='debug')

    def change_grid_config(self, **kwargs):
        not_default_context = self.create_test_context('custom_config')
        not_default_context.add_config('server.xml', 'server_with_custom_config.xml')
        default_context_vars = self.contexts['default'].get_context_variables()
        not_default_context.add_context_variables(
            **default_context_vars,
            **kwargs,
        )
        self.rebuild_configs()
        not_default_context.build_and_deploy(self.ssh)
        self.set_current_context('custom_config')

    def create_context(self, name, **kwargs):
        not_default_context = self.create_test_context(name)
        not_default_context.add_config('server.xml', 'server_{}.xml'.format(name))
        not_default_context.add_config('client.xml', 'client_{}.xml'.format(name))
        default_context_vars = self.contexts['default'].get_context_variables()
        set_context = False

        if kwargs.get('set_context'):
            set_context = kwargs.pop('set_context')

        default_context_vars.update(kwargs)
        not_default_context.add_context_variables(
            **default_context_vars,
        )
        self.rebuild_configs()
        not_default_context.build_and_deploy(self.ssh)

        if set_context:
            self.set_current_context(name)

    def setup_testcase_without_activation(self, **kwargs):
        if kwargs.get('change_grid_config'):
            self.change_grid_config(**(kwargs.get('change_grid_config')))
        self.setup_testcase_without_start_gid()
        self.ignite.start_nodes()
        log_print(self.ignite.nodes, color='debug')

    def setup_testcase_without_start_gid(self):
        log_print('TestSetup is called', color='green')
        self.logger.info('TestSetup is called')

        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()
            default_context = self.contexts[self.get_current_context()]
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString()
            )
            default_context.build_and_deploy(self.ssh)

        if self.get_context_variable('sbt_model_enabled'):
            self.ignite.set_activation_timeout(240)
            self.ignite.set_snapshot_timeout(1800)
            self.util_deploy_sbt_model()

        self.su.clear_snapshots_list()
        self.util_copy_piclient_model_to_libs()

    def teardown_testcase(self):
        log_print('TestTeardown is called', color='green')
        if self.reusable_lfs and not self.lfs_stored:
            log_print('Going to stop nodes....')
            self.ignite.stop_nodes()
            util_sleep_for_a_while(5)
            self.save_lfs('snapshot_util', timeout=1200)
            self.lfs_stored = True
        else:
            self.stop_grid_hard()
        self.su.copy_utility_log()
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop()
        self.cleanup_lfs()
        self.remove_additional_nodes()
        self.ignite.set_node_option('*', 'config', self.get_server_config())
        self.set_current_context()
        self._reset_cluster()
        log_print(repr(self.ignite), color='debug')

    def setup_shared_storage_test(self, prefix_folder=None):
        self.snapshot_storage = self.su.create_shared_snapshot_storage(unique_folder=True, prefix=prefix_folder)
        self.setup_testcase()

    def teardown_shared_storage_test(self):
        self.teardown_testcase()
        self.su.remove_shared_snapshot_storage(self.snapshot_storage)
        self.snapshot_storage = None

    def cleanup_lfs(self):
        log_print("Cleanup Ignite LFS ... ")
        self.run_on_all_nodes('rm -rf ./work/*')
        log_print("Ignite LFS deleted.")

    def remove_additional_nodes(self):
        additional_nodes = self.ignite.get_all_additional_nodes()
        for node_idx in additional_nodes:
            self.ignite.kill_node(node_idx)
            del self.ignite.nodes[node_idx]

    def _reset_cluster(self):
        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()
        tmp_nodes = {}
        for node_idx in self.ignite.nodes.keys():
            if node_idx in server_nodes:
                tmp_nodes[node_idx] = dict(self.ignite.nodes[node_idx])
                if tmp_nodes[node_idx].get('log'):
                    del tmp_nodes[node_idx]['log']
                tmp_nodes[node_idx]['run_counter'] = 0
        self.ignite.nodes = tmp_nodes

    def get_baseline_nodes(self):
        """
        ask for BLT from cluster via CU --baseline
        :return: set of blt, non-blt nodes
        """
        self.cu.control_utility('--baseline')
        patterns = [
            ('ConsistentID=([^,;\n]+)', 'ConsistentID=(.*?), STATE='),  # old version
            ('ConsistentId=([^,;\n]+)', 'ConsistentId=(.*?), State='),  # new version
                    ]
        for all_nodes_pattern, blt_nodes_pattern in patterns:
            all_nodes = set(findall(all_nodes_pattern, self.cu.latest_utility_output))
            blt_nodes = set(findall(blt_nodes_pattern, self.cu.latest_utility_output))
            if all_nodes:
                return list(blt_nodes), list(all_nodes - blt_nodes)

        log_print('WARNING: nodes do not found in utility output:\n{}'.format(self.cu.latest_utility_output))
        return [], []

    @classmethod
    def util_get_restore_point(cls, seconds_ago=1):
        from datetime import datetime, timedelta
        from time import timezone

        util_sleep_for_a_while(2)
        time_format = "%Y-%m-%d-%H:%M:%S.%f"

        restore_point = (datetime.now() - timedelta(seconds=seconds_ago))
        # Hack to handle UTC timezone
        if int(timezone / -(60 * 60)) == 0:
            restore_point = restore_point + timedelta(hours=3)

        return restore_point.strftime(time_format)[:-3]

    @step('Restart empty inactive grid')
    def restart_empty_inactive_grid(self):
        self.cu.deactivate()
        util_sleep_for_a_while(5)
        self.ignite.stop_nodes()
        util_sleep_for_a_while(5)
        self.cleanup_lfs()
        self.ignite.start_nodes()

    @step('Restart grid with deleted WAL')
    def restart_grid_with_deleted_wal(self):
        self.cu.deactivate()
        util_sleep_for_a_while(5)
        self.ignite.stop_nodes()
        util_sleep_for_a_while(5)
        self.delete_lfs(delete_db=False, delete_binary_meta=False, delete_marshaller=False, delete_snapshots=False)
        self.ignite.start_nodes()
        self.cu.activate(activate_on_particular_node=1)

    @step('Restart grid without activation')
    def restart_grid_without_activation(self):
        self.cu.deactivate()
        util_sleep_for_a_while(5)
        self.ignite.stop_nodes()
        util_sleep_for_a_while(5)
        self.ignite.start_nodes()

    def _create_dynamic_caches_with_data(self, with_index=False):
        log_print("Create dynamic caches and load data")

        data_model = ModelTypes.VALUE_ALL_TYPES.value
        created_caches = []
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            dynamic_caches_factory = DynamicCachesFactory()
            async_ops = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method
                print_green('Loading %s...' % cache_name)

                gateway = piclient.get_gateway()
                ignite = piclient.get_ignite()

                ignite.getOrCreateCache(getattr(dynamic_caches_factory, method)(cache_name, gateway=gateway))

                if with_index:
                    data_model = ModelTypes.VALUE_ALL_TYPES.value
                async_operation = create_async_operation(create_streamer_operation,
                                                         cache_name, 1, self.max_key + 2,
                                                         value_type=data_model
                                                         )
                async_ops.append(async_operation)
                async_operation.evaluate()
                created_caches.append(cache_name)

            log_print('Waiting async results...', color='blue')
            # wait for streamer to complete
            for async_op in async_ops:
                async_op.getResult()

        log_print("Dynamic caches with data created")
        return created_caches

    def _calc_checksums_over_dynamic_caches(self):
        log_print("Calculating checksums")

        with PiClient(self.ignite, self.get_client_config()):
            dynamic_caches_factory = DynamicCachesFactory()

            async_operations = []
            for method in dynamic_caches_factory.dynamic_cache_configs:
                cache_name = "cache_group_%s" % method

                checksum_operation = create_checksum_operation(cache_name, 1, 1000)
                async_operation = create_async_operation(checksum_operation)
                async_operations.append(async_operation)
                async_operation.evaluate()

            checksums = ''

            for async_operation in async_operations:
                checksums += str(async_operation.getResult())

        log_print("Calculating checksums done")

        return checksums

    def util_verify(self, save_lfs_on_exception=False):
        util_sleep_for_a_while(10, 'Sleep before IDLE_VERIFY')
        from pt.util import version_num
        if version_num(self.cu.get_ignite_version()) < version_num('2.5.0'):
            idle_verify_pass = ['Command \[IDLE_VERIFY.*\] started',
                                'Partition verification finished, no conflicts have been found.',
                                'Command \[IDLE_VERIFY\] successfully finished in [0-9\.]+ seconds.']
            self.cu.control_utility('--cache', 'idle_verify', all_required=idle_verify_pass)
        else:
            idle_verify_pass = ['idle_verify check has finished, no conflicts have been found.']
            try:
                self.cu.control_utility('--cache', 'idle_verify', all_required=idle_verify_pass)
            except TidenException as e:
                self.cu.control_utility('idle_verify', '--analyse')
                self.stop_grid(fail=False)
                if save_lfs_on_exception:
                    self.save_lfs('bug')
                raise e

    @staticmethod
    def util_compare_check_sums(str1, str2):
        import difflib
        res = []
        if isinstance(str1, str):
            for line in difflib.unified_diff(str1.splitlines(True), str2.splitlines(True)):
                res.append(line)

            if len(res) != 0:
                log_print("Dicts are not the same:\ndiff:\n%s" % ''.join(res))
        else:
            log_print('WARNING: You are trying to compare check sums which are not strings: %s' % str1, color='red')

    @staticmethod
    def assert_check_sums(checksum_1, checksum_2):
        UltimateUtils.util_compare_check_sums(checksum_1, checksum_2)
        tiden_assert_equal(checksum_1, checksum_2, 'Check sums assertion')

    @staticmethod
    def util_get_random_nodes(count, nodes_amount):
        random_nodes = list()
        random_nodes.append(randint(2, nodes_amount))
        while len(random_nodes) < count:
            new_node_id = randint(2, nodes_amount)
            if new_node_id not in random_nodes:
                random_nodes.append(new_node_id)
        return random_nodes

    def util_start_additional_nodes(self, node_type='server', add_to_baseline=False, nodes_ct=None):
        nodes_count = nodes_ct if nodes_ct else randint(1, 3)

        if node_type == 'server':
            config = self.get_server_config()
        else:
            config = self.get_client_config()

        additional_nodes = self.ignite.add_additional_nodes(config, nodes_count)
        self.start_additional_nodes(additional_nodes)

        baseline_msg = ''
        if add_to_baseline:
            self.cu.set_current_topology_as_baseline()
            baseline_msg = 'Server nodes added to baseline.'
            util_sleep_for_a_while(10)

        util_sleep_for_a_while(5)
        self.cu.control_utility('--baseline')
        print_green('Started %s %s nodes. %s' % (nodes_count, node_type, '' if not add_to_baseline else baseline_msg))

        return additional_nodes

    def util_print_checksum(self, checksum):
        if not self.get_context_variable('sbt_model_enabled'):
            print_green(checksum)

    def util_run_money_transfer_task(self, time_to_run=10):
        log_print("Starting money transfer task", color='green')

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache_names = piclient.get_ignite().cacheNames()

            async_operations = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_account_runner_operation,
                                                         cache_name, 1, self.max_key, 0.5,
                                                         delay=1,
                                                         run_for_seconds=time_to_run)
                async_operations.append(async_operation)
                async_operation.evaluate()

            # wait operations to complete
            for async_op in async_operations:
                async_op.getResult()

        log_print("Money transfer is done", color='green')

    def sbt_case_upload_data_and_create_restore_point(self):
        # start_key = int(self.max_key * self.load_multiplier) + 1
        # self.sbt_like_load(start_key, 5000)
        self.upload_data_using_sbt_model()

        util_sleep_for_a_while(10)
        restore_point = self.util_get_restore_point()
        checksum = self.calc_checksums_distributed(config_file=self.get_client_config())

        # start_key = int(5000 * self.load_multiplier) + 1
        # self.sbt_like_load(start_key, 7000)
        self.upload_data_using_sbt_model(start_key=5000, end_key=7000)

        return restore_point, checksum

    def upload_data_using_sbt_model(self, start_key=None, end_key=5000):
        start_from = start_key if start_key else self.max_key
        start_from = int(start_from * self.load_multiplier) + 1
        self.common_load(start_from, end_key)

    def common_load(self, start_key, end_key):
        if self.get_context_variable('sbt_model_enabled'):
            # add transactional put into 1 cache 2k times total
            PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                               self.ignite,
                                                               self.get_client_config(),
                                                               only_caches_batch=1,
                                                               start_key=start_key,
                                                               end_key=start_key + 1)

            # load normal data into 200 caches 2k times total
            PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                               self.ignite,
                                                               self.get_client_config(),
                                                               only_caches_batch=200,
                                                               start_key=start_key + 2,
                                                               end_key=int(end_key * self.load_multiplier))
        else:
            PiClientIgniteUtils.load_data_with_putall(self.ignite,
                                                      self.get_client_config(),
                                                      start_key=start_key,
                                                      end_key=int(end_key * self.load_multiplier))

    def restart_empty_grid_with_nodes_count(self, nodes_count):
        self.cu.deactivate()
        util_sleep_for_a_while(5)
        current_nodes = self.ignite.get_alive_default_nodes()
        self.ignite.stop_nodes()
        util_sleep_for_a_while(5)
        self.delete_lfs()
        additional_nodes_count = nodes_count - len(current_nodes)

        if additional_nodes_count < 0:
            print_blue('Going to remove nodes %s' % current_nodes[additional_nodes_count:])
            for node_id in current_nodes[additional_nodes_count:]:
                current_nodes.remove(node_id)
                # if self.ignite.nodes.get(node_id):
                #     del self.ignite.nodes[node_id]

        log_print('Going to start nodes {}'.format(current_nodes))
        self.ignite.start_nodes(*current_nodes)

        if additional_nodes_count > 0:
            additional_nodes_count = nodes_count - len(current_nodes)
            print_blue('Starting %s additional nodes' % additional_nodes_count)
            node_id = list(self.ignite.add_additional_nodes(self.get_server_config(), additional_nodes_count))
            self.ignite.start_additional_nodes(node_id)

        self.cu.activate()

    def change_grid_topology(self, with_restart=True, increase_topology=True):
        # Just start additional node
        additional_nodes_count = 2

        if with_restart:
            self.restart_empty_inactive_grid()
            if increase_topology:
                self.ignite.add_additional_nodes(self.get_server_config(), additional_nodes_count)
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())
            else:
                default_nodes = self.ignite.get_all_default_nodes()
                self.ignite.kill_node(default_nodes[-1])
            self.cu.activate()
        else:
            if increase_topology:
                self.ignite.add_additional_nodes(self.get_server_config(), additional_nodes_count)
                self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())
            else:
                default_nodes = self.ignite.get_all_default_nodes()
                self.ignite.kill_node(default_nodes[-1])

    def _change_grid_topology_and_set_baseline(self, disable_auto_baseline=True):
        """
        Restart empty grid, start one additional node and add it to baseline topology.

        :param disable_auto_baseline:
        :return:
        """

        self.restart_empty_grid()
        self.ignite.add_additional_nodes(self.get_server_config(), 1)
        self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())

        util_sleep_for_a_while(5)

        if disable_auto_baseline and self.cu.is_baseline_autoajustment_supported():
            self.cu.disable_baseline_autoajustment()
            log_print("Baseline auto adjustment disabled", color='green')

        self._set_baseline_few_times()

    @step('Create cache "{cache_name}" with index data')
    def create_cache_with_indexed_data(self, cache_name):
        from pt.piclient.helper.cache_utils import IgniteCacheConfig

        # Configure cache1
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()

            cache_config = IgniteCacheConfig(gateway)
            cache_config.set_name(cache_name)
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
                gateway.jvm.org.apache.ignite.cache.QueryEntity("java.lang.Long",
                                                                ModelTypes.VALUE_ALL_TYPES_INDEXED.value)
                    .addQueryField("strCol", "java.lang.String", None)
                    .addQueryField("longCol", "java.lang.Long", None)
                    .addQueryField("intCol", "java.lang.Integer", None)
                    .setIndexes(query_indices))

            cache_config.get_config_object().setQueryEntities(query_entities)
            cache_config.get_config_object().setStatisticsEnabled(False)

            ignite.getOrCreateCache(cache_config.get_config_object())

    def _remove_footer(self, host, files):
        commands = []
        for file in files:
            commands.append('sed -i \'${/Total/d;}\' %s' % file)

        self.ssh.exec_on_host(host, commands)

    @staticmethod
    def prepare_caches_one_by_one_file(middle_file, result_file, caches_under_test, caches,
                                       empty_caches=False):
        empty_cache_str = 'cache %s. Size 0, lowest key not found, highest key not found, CRC32 0, ' \
                          'SHA256 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n'
        # prepare files
        caches.sort()

        final_result = {}
        for cache in caches:
            if cache in caches_under_test:
                final_result[cache] = result_file[cache]
            else:
                if empty_caches:
                    final_result[cache] = empty_cache_str % cache
                else:
                    final_result[cache] = middle_file[cache]

        return final_result

    def _remove_random_from_caches(self, easy=False):
        clients_num = self.ignite.get_nodes_num('client')

        with PiClient(self.ignite, self.get_client_config()):
            caches = self.ignite.get_cache_names('cache_group')

            print_blue('--- Caches ---')
            print_blue(caches)

            for cache_name in caches:
                cache = IgniteCache(cache_name)

                if easy:
                    for index in range(1,
                                       randint(1, int(len(caches) / 2))):
                        cache.remove(index, key_type='long')
                else:
                    for index in range(randint(1, int(self.max_key / 2)),
                                       randint(int(self.max_key / 2) + 1, self.max_key)):
                        cache.remove(index, key_type='long')

        self.wait_for_running_clients_num(clients_num, 120)

    def _put_additional_data_to_caches(self):
        self.load_data_with_streamer(1001, 100001)

    def _dump_particular_caches(self, caches):
        dump_caches = self.calc_checksums_on_client(dict_mode=True)

        particular_caches = {}
        for cache in caches:
            particular_caches[cache] = dump_caches[cache]

        return particular_caches

    def get_caches_from_log(self, node_id):
        cache_group_result = []
        regexp_str_caches = 'Started cache .*name=(\w+),'
        regexp_str_caches_groups = 'Started cache .*name=(\w+),.*group=(\w+),'
        output = self.ignite.grep_in_node_log(node_id, regexp_str_caches)

        for line in output.split('\n'):
            cache = search(regexp_str_caches, line)
            cache_group = search(regexp_str_caches_groups, line)
            if cache and not cache_group:
                cache_group_result.append((cache.group(1), None))
            elif cache_group:
                cache_group_result.append((cache_group.group(1), cache_group.group(2)))
        print_red(cache_group_result)
        return cache_group_result

    def get_caches_for_test(self):
        caches = self.ignite.get_cache_names('cache_group')
        group_caches = {}
        caches_under_test = []
        groups = []
        tmp_caches_under_test = caches[1: randint(2, len(caches) - 1)]

        print_red(caches)
        caches_groups = self.get_caches_from_log(1)
        print_red('Caches to start %s' % tmp_caches_under_test)
        for cache_name, group_name in caches_groups:
            if group_name:
                if group_caches.get(group_name):
                    group_caches[group_name].append(cache_name)
                else:
                    group_caches[group_name] = [cache_name]
                groups.append(group_name)

        for group in group_caches.keys():
            if set(group_caches[group]).intersection(set(tmp_caches_under_test)):
                caches_under_test += group_caches[group]

        caches_under_test += tmp_caches_under_test

        # if all caches are in caches_under_test just remove one group
        group_to_exclude = groups[randint(0, len(groups) - 1)]
        if len(set(caches_under_test)) == len(tmp_caches_under_test):
            caches_under_test = [cache for cache in caches_under_test if cache not in group_caches[group_to_exclude]]

        print_red(caches_under_test)
        return set(caches_under_test)

    def util_change_server_config_to_no_caches(self):
        commands = {}
        new_server_config = 'server_no_caches.xml'
        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()
        for node_idx in self.ignite.nodes.keys():
            if node_idx not in server_nodes:
                continue
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'cd %s; cp %s %s; sed -i \'/caches/d\' %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        self.get_server_config(), new_server_config, new_server_config),
                ]
        self.ssh.exec(commands)
        self.ignite.set_node_option('*', 'config', new_server_config)

    def util_change_snapshot_src(self, snapshot_dir, rename_dir=True, repair=False):
        files = self.util_change_snapshot_src_for_remote_grid(snapshot_dir, rename_dir, repair)
        log_print(files)
        return files

    def util_change_snapshot_src_for_remote_grid(self, snapshot_dir, rename_dir=True, repair=False):
        host = None

        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()
        for node_id in self.ignite.nodes.keys():
            if node_id in server_nodes:
                host = self.ignite.nodes[node_id]['host']
                ignite_home = self.ignite.nodes[node_id]['ignite_home']

        if repair:
            f_to_rename = [('/'.join(line.split('/')[:-1]) + '/_test_' + line.split('/')[-1], line)
                           for line in snapshot_dir]
        else:
            commands = dict()

            dir_to_search = '%s/work/snapshot/' % self.ignite.ignite_home
            if snapshot_dir:
                dir_to_search = snapshot_dir

            commands[host] = ['find %s -name \'part-1.bin\'' % dir_to_search]
            log_print(commands)
            output = self.ignite.ssh.exec(commands)
            print_blue(output)
            files = [file for file in output[host][0].split('\n') if file]
            print_blue(files)

            if rename_dir:
                f_to_rename = [('/'.join(line.split('/')[:-1]),
                                '/'.join(line.split('/')[:-2]) + '/_test_' + line.split('/')[-2]) for line in files]
            else:
                f_to_rename = [(line,
                                '/'.join(line.split('/')[:-1]) + '/_test_' + line.split('/')[-1]) for line in files]

        commands = set()
        remote_cmd = dict()
        files = []
        for src, dst in f_to_rename:
            commands.add('mv %s %s' % (src, dst))
            files.append(src)

        remote_cmd[host] = [';'.join(commands)]

        log_print(remote_cmd)
        output = self.ignite.ssh.exec(remote_cmd)
        log_print(output)
        print_red(remote_cmd)
        return files

    def check_snapshots_listing_on_all_nodes(self, snapshots, snapshot_path=None):
        path = 'work/snapshot/'
        if snapshot_path:
            path = snapshot_path
        output = self.run_on_all_nodes('ls %s' % path)

        if isinstance(snapshots, list):
            expecting_snapshots = list(snapshots)
        else:
            expecting_snapshots = [str(snapshots)]

        for node_id in output.keys():
            found = []
            snapshot_folders = [item for item in output[node_id].split('\n') if item]

            tiden_assert_equal(len(expecting_snapshots), len(snapshot_folders),
                               'Number of folders in work/snapshot on server'
                               )

            for snapshot in expecting_snapshots:
                log_print(snapshot)
                found += [folder for folder in snapshot_folders if snapshot in folder]

            tiden_assert_equal(len(expecting_snapshots), len(found),
                               'All folders in work/snapshot:\n%s\ncorrespond expected snapshots: %s' % (
                                   ','.join(found),
                                   ','.join(expecting_snapshots)
                               ))

    def run_on_all_nodes(self, command):
        output = dict()
        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()

        for node_id in server_nodes:
            commands = {}
            host = self.ignite.nodes[node_id]['host']
            ignite_home = self.ignite.nodes[node_id]['ignite_home']

            commands[host] = ['cd %s;%s' % (ignite_home, command)]
            log_print(commands, color='debug')
            tmp_output = self.ignite.ssh.exec(commands)
            output[node_id] = tmp_output[host][0]
        # log_print(output)
        return output

    def util_delete_snapshot_from_fs(self, snapshot_id=None, remote_dir=None):
        """
        Delete snapshot/snapshots from file system.
        If snapshot_id is not set ALL snapshots will be deleted.
        If remote_dir is not set, snapshot will be deleted from local directory (work/snapshots).

        :param snapshot_id: snapshot ID. If does not set, mask '*' used.
        :param remote_dir: remote direcory if you need to delete snapshot from shared store.
        :return:
        """
        msg = 'Going to delete local snapshots'
        if snapshot_id:
            msg = 'Going to delete local snapshot with ID = %s' % snapshot_id
        log_print(msg)
        commands = {}
        dir_on_node = {}

        if snapshot_id:
            dir_on_node = self.util_find_snapshot_folders_on_fs(snapshot_id)

        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()
        for node_idx in server_nodes:
            host = self.ignite.nodes[node_idx]['host']
            ignite_home = self.ignite.nodes[node_idx]['ignite_home']

            delete_dir = dir_on_node.get(node_idx) if snapshot_id else '*'
            relative_snapshot_path = remote_dir if remote_dir else '%s/work/snapshot' % ignite_home

            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/%s' % (relative_snapshot_path, delete_dir)
                ]
            else:
                commands[host].append('rm -rf %s/%s' % (relative_snapshot_path, delete_dir))

        self.ssh.exec(commands)
        snapshot_folders = self.util_find_snapshot_folders_on_fs(snapshot_id, remote_dir=remote_dir)
        tiden_assert(snapshot_folders == {}, 'Snapshot folders deleted %s' % snapshot_folders)

    def util_get_snapshot_size_on_lfs(self, snapshot_id, remote_dir=None):
        snapshot_folder = self.util_find_snapshot_folders_on_fs(snapshot_id=snapshot_id, remote_dir=remote_dir)
        snapshot_size = self.ssh.dirsize(snapshot_folder)
        return snapshot_size

    def util_find_snapshot_folders_on_fs(self, snapshot_id, remote_dir=None):
        snapshot_dirs = {}
        remote_snapshot_dir = None

        search_in_dir = remote_dir if remote_dir else './work/snapshot/'
        output = self.run_on_all_nodes('ls -1 %s | grep %s' % (search_in_dir, snapshot_id))

        if len(output) > 0:
            for node_idx in output.keys():
                snapshot_dir = output[node_idx].rstrip()
                if snapshot_dir:
                    # Add only if directory exists
                    snapshot_dirs[node_idx] = snapshot_dir
                    remote_snapshot_dir = snapshot_dirs[node_idx]
                print_green('Snapshot directory %s for snapshot ID=%s on node %s' %
                            (snapshot_dir if snapshot_dir else 'Not found', snapshot_id, node_idx))

        # if remote dir, it is the same for all servers, so don't need to iterate over all servers
        if remote_dir:
            return '%s/%s' % (remote_dir, remote_snapshot_dir)

        return snapshot_dirs

    def util_check_restoring_from_chain(self, snapshots_chain):
        for snapshot_id, dump_before in snapshots_chain:
            self.run_snapshot_utility('restore', '-id=%s -src=%s' % (snapshot_id, self.snapshot_storage))
            dump_after = self.calc_checksums_on_client()
            self.assert_check_sums(dump_before, dump_after)

    def util_create_snapshots_chain(self, snapshots_count):
        snapshots_chain = []
        self.run_snapshot_utility('snapshot', '-type=full')

        for i in range(0, snapshots_count):
            print_green('Iteration %s from %s' % (i + 1, snapshots_count))
            self._remove_random_from_caches()

            self.run_snapshot_utility('snapshot')
            current_dump = self.calc_checksums_on_client()
            snapshots_chain.append((self.get_latest_snapshot_id(), current_dump))
            self.load_data_with_streamer(end_key=5000, allow_overwrite=True)

        print_blue(self.su.snapshots_info())
        return snapshots_chain

    def _set_baseline_few_times(self, times=2):
        topology_changed = False
        lst_output = ''
        utility_baseline_log = 'control-utility-baseline.log'

        util_sleep_for_a_while(20)

        for _ in range(0, times):
            self.cu.set_current_topology_as_baseline(background=True, log=utility_baseline_log)

            check_command = {
                self.cu.latest_utility_host: [
                    'cat %s/%s' % (self.ignite.client_ignite_home, utility_baseline_log)
                ]
            }

            timeout_counter = 0
            baseline_timeout = 120
            completed = False
            while timeout_counter < baseline_timeout and not completed:
                lst_output = self.ignite.ssh.exec(check_command)[self.cu.latest_utility_host][0]

                log_put('Waiting for topology changed %s/%s' % (timeout_counter, baseline_timeout))

                if 'Connection to cluster failed.' in lst_output:
                    print_red('Utility unable to connect to cluster')
                    break

                if 'Number of baseline nodes: ' in lst_output:
                    completed = True
                    break

                util_sleep_for_a_while(5)
                timeout_counter += 5

            if completed:
                topology_changed = True
                break

            log_print()

        if not topology_changed:
            print_red(lst_output)
            raise TidenException('Unable to change grid topology')

        return topology_changed

    @staticmethod
    def escape_and_exclude_from_buffer(buffer, exclude_list):
        def custom_filter(item):
            return [pattern for pattern in exclude_list if pattern in item]

        return [line.replace('(', '\(').replace(')', '\)') for line in buffer.split('\n')
                if not custom_filter(line)][:-1]

    @staticmethod
    def check_for_exceptions(buffer, exceptions):
        for line in buffer:
            for exception in exceptions:
                if exception in line:
                    raise TidenException('Found exception: {}\nin buffer\n{}'.format(exception, '\n'.join(buffer)))

