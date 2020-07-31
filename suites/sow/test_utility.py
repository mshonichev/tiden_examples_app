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

from functools import reduce
from os.path import join
from re import search

from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden.util import util_sleep_for_a_while, log_print, is_enabled, with_setup, test_case_id, \
    attr, known_issue, require, if_applicable_ignite_version, parse_xml, merge_properties
from tiden.assertions import tiden_assert, tiden_assert_equal
from tiden.logger import get_logger
from tiden_gridgain.piclient.helper.operation_utils import create_broke_data_entry_operation, create_transaction_control_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.case.singlegridzootestcase import SingleGridZooTestCase
from tiden.tidenexception import TidenException
from tiden.utilities.sqlline_utility import Sqlline


class TestUtility(SingleGridZooTestCase):
    max_key = 1001

    def setup(self):
        super().setup()
        self.snapshot_arch_enabled = is_enabled(self.config.get('snapshot_arch'))
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TessUtils]')

    def setup_testcase(self):
        self.setup_testcase_no_grid_start()
        self.start_grid()
        self.load_data_with_streamer(end_key=self.max_key)
        log_print(repr(self.ignite), color='debug')

    def setup_testcase_with_sbt_model(self):
        self.util_deploy_sbt_model()
        self.server_config_name = 'server_with_sbt_model.xml'
        custom_context = self.create_test_context(self.server_config_name)
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            sbt_model_enabled='true',
            caches='caches_sbt.xml'
        )
        custom_context.set_server_result_config(self.server_config_name)
        custom_context.build_and_deploy(self.ssh)
        self.ignite.set_node_option('*', 'config', self.server_config_name)
        self.setup_testcase_no_grid_start()
        self.start_grid()
        log_print(repr(self.ignite), color='debug')

    def setup_with_custom_snapshot_folder(self):
        self.server_config_name = 'server_with_custom_snapshot_folder.xml'
        custom_context = self.create_test_context(self.server_config_name)
        default_context_vars = self.contexts['default'].get_context_variables()
        custom_context.add_context_variables(
            **default_context_vars,
            custom_snapshot_folder=True,
            custom_snapshot_folder_value=self.config['rt']['remote']['test_dir']
        )
        custom_context.set_server_result_config(self.server_config_name)
        custom_context.build_and_deploy(self.ssh)
        self.ignite.set_node_option('*', 'config', self.server_config_name)
        self.setup_testcase_no_grid_start()

    def setup_testcase_no_grid_start(self):
        log_print('TestSetup is called', color='green')
        self.logger.info('TestSetup is called')
        self.ignite.set_activation_timeout(20)
        self.ignite.set_snapshot_timeout(120)
        self.util_copy_piclient_model_to_libs()
        self.su.clear_snapshots_list()

    def teardown_testcase(self):
        log_print('TestTeardown is called', color='green')
        self.ignite.set_activation_timeout(10)
        self.ignite.set_snapshot_timeout(30)
        self.stop_grid_hard()
        self.cleanup_lfs()
        self.remove_additional_nodes()
        self.set_current_context()
        self.reset_cluster()
        log_print(repr(self.ignite), color='debug')

    @attr('tools', 'util-1-1')
    @test_case_id('91941')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_1_idle_verify(self):
        """
        Test idle_verify command detects problem if some key is corrupted (there is some difference between key
        on primary partition and backup).
        """

        partitions_to_break = [1, 2]
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1):
            cache_under_test = 'cache_group_1_028'
            log_print('Cache under test: %s' % cache_under_test, color='blue')

            operation = create_broke_data_entry_operation(cache_under_test, partitions_to_break[0], True,
                                                          'value', 'counter')
            log_print(operation.evaluate())
            operation = create_broke_data_entry_operation(cache_under_test, partitions_to_break[1], True, 'counter')
            log_print(operation.evaluate())

        util_sleep_for_a_while(10)

        expected = ['Conflict partition']
        self.cu.control_utility('--cache idle_verify', all_required=expected)

        log_print(self.cu.latest_utility_output)
        output = self.cu.latest_utility_output
        # m = search('See log for additional information. (.*)', self.cu.latest_utility_output)
        # if m:
        #     conflict_file = m.group(1)
        #     host = self.cu.latest_utility_host
        #     output = self.ssh.exec_on_host(host, ['cat {}'.format(conflict_file)])
        #     log_print(output, color='blue')
        # else:
        #     tiden_assert(False, 'Conflict file is not found in output:\n{}'.format(self.cu.latest_utility_output))

        grpId, partId = [], []
        for line in output.split('\n'):
            m = search('Conflict partition: (PartitionKey|PartitionKeyV2) \[grpId=(\d+),.*partId=(\d+)\]', line)
            if m:
                grpId.append(m.group(2))
                partId.append(int(m.group(3)))

        tiden_assert(grpId and partId, 'Could not find partition id in buffer %s' % output)
        tiden_assert(len(set(grpId)), 'Should be one group in output %s' % output)
        tiden_assert(set(partId) == set(partitions_to_break), 'Partition ids should match %s' % output)

    @attr('tools', 'dump')
    @test_case_id('91942')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_1_idle_verify_dump(self):
        """
        idle_verify --dump command dumped all data about current caches partitions on disk
        data should be different if caches was changed
        restored data must be equal previous one
        """
        with PiClient(self.ignite, self.get_client_config()):
            test_dir = self.config['rt']['remote']['test_dir']
            dump_before = self.cu.idle_verify_dump(copy_dir=test_dir)
            self.run_snapshot_utility('snapshot', '-type=full')

            self.load_data_with_streamer(end_key=5000,
                                         value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                         allow_overwrite=True)

            dump_after = self.cu.idle_verify_dump(copy_dir=test_dir)
            self.compare_lists_of_dicts(dump_before, dump_after)
            tiden_assert(dump_before != dump_after, 'dumps before and after data filling are equals')

            self.su.snapshot_utility('restore', '-id={}'.format(self.get_snapshot_id(1)))

            restored_dump = self.cu.idle_verify_dump(copy_dir=test_dir)
            self.compare_lists_of_dicts(dump_before, restored_dump)
            tiden_assert_equal(dump_before, restored_dump, 'restored cache dump are changed')

    @attr('tools', 'dump')
    @test_case_id('91942')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_1_idle_verify_dump_skip_zeros(self):
        """
        idle_verify --dump --skipZeros
        Test idle_verify command detects problem if some key is corrupted (there is some difference between key
        on primary partition and backup).
        """
        from pt.piclient.helper.cache_utils import IgniteCache

        cache_under_test = 'cache_group_4_118'
        test_dir = self.config['rt']['remote']['test_dir']
        dump = self.cu.idle_verify_dump(copy_dir=test_dir)
        dump_skip_zeros = self.cu.idle_verify_dump(skip_zeros=True, copy_dir=test_dir)

        tiden_assert(dump != dump_skip_zeros, "non zeroes dump have equal dump without zeroes ignore")

        for dump_item in dump_skip_zeros:
            for instance in dump_item["instances"]:
                if int(instance["updateCntr"]) == 0 or int(instance["size"]) == 0 or int(instance["partHash"]) == 0:
                    raise TidenException("Found zeros in non zeros dump")

        if if_applicable_ignite_version(self.config, '2.5.6'):
            with PiClient(self.ignite, self.get_client_config()):
                caches = self.ignite.get_cache_names('cache_group')

                for cache_name in [cache_name for cache_name in caches if cache_name == cache_under_test]:
                    cache = IgniteCache(cache_name)

                    for i in range(1, self.max_key):
                        cache.remove(i, key_type='long')

            dump_skip_zeros = self.cu.idle_verify_dump(skip_zeros=True, copy_dir=test_dir)

            tiden_assert(dump != dump_skip_zeros, "non zeroes dump have equal dump without zeroes ignore")

            for dump_item in dump_skip_zeros:
                for instance in dump_item["instances"]:
                    if int(instance["updateCntr"]) == 0 or int(instance["size"]) == 0 or int(instance["partHash"]) == 0:
                        raise TidenException("Found zeros in non zeros dump")

    @attr('tools', 'dump', 'IGN-12266')
    @test_case_id('169695')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_idle_verify_dump_exclude_filter(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-12036
        check that dump contain all declared cache groups according to using filter
        Known issue - IGN-13235  - "--cache-filter ALL" result doesn't contain ignite-sys-cache
        """
        self.test_util_specific_filter_exclude(key_dump=True)

    @attr('tools', 'dump', 'GG-19178')
    @test_case_id('169695')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_idle_verify_dump_exclude_filter(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/GG-19178
        check we can run command --exclude-caches without --dump
        """
        self.test_util_specific_filter_exclude(key_dump=False)

    def test_util_specific_filter_exclude(self, key_dump):
        limits = {"REPLICATED": 512, 'PARTITIONED': 1024}
        loaded_keys_count = 1001
        test_dir = self.config['rt']['remote']['test_dir']
        common_filter = self.cu.idle_verify_dump(copy_dir=test_dir)
        actual_groups_list = set([partition_dump["info"]["grpName"] for partition_dump in common_filter])
        _, _, _, expected_caches_names = self.parse_cache_xml(limits, loaded_keys_count, 'caches.xml')
        for cache_group in expected_caches_names.keys():
            exclude_filter = self.cu.idle_verify_dump(exclude_caches=','.join(expected_caches_names[cache_group]),
                                                      copy_dir=test_dir, key_dump=key_dump)
            if key_dump:
                exclude_groups_list = set([partition_dump["info"]["grpName"] for partition_dump in exclude_filter])
                tiden_assert_equal(sorted(actual_groups_list - set([cache_group])),
                                   sorted(exclude_groups_list),
                                   'Exclude {} group from control --cache idle_verify and check result'.format(
                                       cache_group),
                                   debug=True)

    @attr('tools', 'dump', 'IGN-12036')
    @test_case_id('167640')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_idle_verify_dump_cache_filter(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/IGN-12036
        check that dump contain all declared cache groups according to using filter

        Known issue - IGN-13235  - "--cache-filter ALL" result doesn't contain ignite-sys-cache
        """
        self.test_util_specific_cache_filter(key_dump=True)

    @attr('tools', 'dump', 'GG-19178')
    @test_case_id('167640')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_idle_verify_dump_cache_filter(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/GG-19178
        check we can run command --exclude-caches without --dump
        """
        self.test_util_specific_cache_filter(key_dump=False)

    def test_util_specific_cache_filter(self, key_dump):

        limits = {"REPLICATED": 512, 'PARTITIONED': 1024}
        loaded_keys_count = 1001
        expected_groups_list, _, _, _ = self.parse_cache_xml(limits, loaded_keys_count, 'caches.xml')
        filter = {
            'ALL': None,
            'SYSTEM': None,
            'PERSISTENT': None,
            'NOT_PERSISTENT': None
        }

        test_dir = self.config['rt']['remote']['test_dir']
        for filter_type in filter.keys():
            self.cu.control_utility('--cache', 'idle_verify --cache-filter {}'.format(filter_type))
        for filter_type in filter.keys():
            if not key_dump:
                self.cu.idle_verify_dump(cache_filter="--cache-filter {}".format(filter_type),
                                         copy_dir=test_dir, key_dump=key_dump)
            dump_filter = self.cu.idle_verify_dump(cache_filter="--cache-filter {}".format(filter_type),
                                                   copy_dir=test_dir)
            actual_groups_list = [partition_dump["info"]["grpName"] for partition_dump in dump_filter]
            filter[filter_type] = set(actual_groups_list)

        dump_filter = self.cu.idle_verify_dump(copy_dir=test_dir)
        actual_groups_list = [partition_dump["info"]["grpName"] for partition_dump in dump_filter]
        filter['WITHOUT_FILTER'] = set(actual_groups_list)
        tiden_assert('ignite-sys-cache' in filter['SYSTEM'] and 'ignite-sys-cache' not in filter[
            'PERSISTENT'] and 'ignite-sys-cache' not in filter['NOT_PERSISTENT'],
                     "ignite-sys-cache in SYSTEM filter and not in other specific filters")
        tiden_assert_equal(len(filter['PERSISTENT'] & filter['NOT_PERSISTENT']), 0,
                           "length of intersection PERSISTENTS and NOT_PERSISTENT sets == 0")
        tiden_assert_equal(sorted(filter['WITHOUT_FILTER'] | {'ignite-sys-cache'}), sorted(filter['ALL']),
                           "result of idle_verify filter - ALL and call without filter is equal "
                           "if add ignite-sys-cache")
        tiden_assert_equal(sorted(set(expected_groups_list) | {'ignite-sys-cache'}), sorted(filter['ALL']),
                           "caches.xml group list is equal filter['ALL'] group list"
                           "if add ignite-sys-cache")
        tiden_assert(filter['ALL'].issuperset(filter['SYSTEM'] | filter['PERSISTENT'] | filter['NOT_PERSISTENT']),
                     "Set of specific filters not equal set of all cache_group")

    def parse_cache_xml(self, limits, loaded_keys_count, file_name='caches.xml'):
        expected_groups_list = []
        expected_partitions_count = {}
        expected_caches_counts = {}
        expected_caches_names = {}
        config = parse_xml(join(self.config['rt']['test_resource_dir'], file_name))[0]
        for bean in config["_children"]:
            properties = reduce(lambda p, n: merge_properties(p, n), bean["_children"])

            group_name = properties.get("groupName", properties.get("name"))
            # groups
            expected_groups_list.append(group_name)

            if group_name not in expected_caches_names:
                expected_caches_names[group_name] = [properties.get("name", properties.get("name"))]
            else:
                expected_caches_names[group_name] += [properties.get("name", properties.get("name"))]

            # partitions
            affinity = properties.get('affinity')
            if affinity:
                caches_limit = [int(i["value"]) for i in affinity[0]["_children"] if i["value"].isnumeric()][0]
            else:
                caches_limit = limits.get(properties.get('cacheMode'), -1)
            expected_partitions_count[group_name] = caches_limit

            cache_mode = properties.get('cacheMode')

            # caches sizes
            nodes_num = int(len(self.config["environment"]["server_hosts"]) \
                            * self.config["environment"]["servers_per_host"])
            current_count = expected_caches_counts.get(group_name, 0)

            if cache_mode == 'REPLICATED':
                values = loaded_keys_count * nodes_num
            else:
                backups = int(properties.get('backups'))
                multiplier = backups + 1 if backups < nodes_num else nodes_num
                values = loaded_keys_count * multiplier

            expected_caches_counts[group_name] = current_count + values
        return expected_groups_list, expected_partitions_count, expected_caches_counts, expected_caches_names

    @attr('tools', 'dump')
    @test_case_id('91942')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_1_idle_verify_dump_correct_data(self):
        """
        idle_verify --dump must provide correct data about caches
        check that dump contain all declared cache groups data
        """
        patritions = self.cu.idle_verify_dump(copy_dir=self.config['rt']['remote']['test_dir'])

        limits = {"REPLICATED": 512, 'PARTITIONED': 1024}
        loaded_keys_count = 1001
        expected_groups_list, expected_partitions_count, expected_caches_counts, _ = \
            self.parse_cache_xml(limits, loaded_keys_count, 'caches.xml')

        actual_groups_list = [partition_dump["info"]["grpName"] for partition_dump in patritions]
        actual_partitions_count = {}
        actual_caches_count = {}
        for partition in patritions:
            group_name = partition["info"]["grpName"]

            # partitions
            count = actual_partitions_count.get(group_name, 0)
            count += 1
            actual_partitions_count[group_name] = count

            # caches sizes
            nodes_cache_size = [(item["consistentId"], int(item["size"])) for item in partition["instances"]]
            group_cache_size = sum([int(i[1]) for i in nodes_cache_size])
            sizes = {
                "nodes": nodes_cache_size,
                "all": group_cache_size
            }
            new_sizes = actual_caches_count.get(group_name, {
                "caches_size": 0,
                "nodes": []
            })
            new_sizes["nodes"] += [sizes]
            new_sizes["caches_size"] += group_cache_size
            actual_caches_count[group_name] = new_sizes

        tiden_assert_equal(sorted(set(expected_groups_list)), sorted(set(actual_groups_list)), "cache groups")
        tiden_assert_equal(expected_partitions_count, actual_partitions_count, 'partitions count')
        tiden_assert_equal(expected_caches_counts,
                           {k: v["caches_size"] for k, v in actual_caches_count.items()}, "caches count")

    @attr('pmi-tool')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_1_break_partition_value_using_utility(self):
        """
        This test is just check pmi-tool can break the value for some key.
        """
        self.util_break_partition_using_pmi_tool(break_value=True)

    @attr('pmi-tool')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_1_break_partition_counter_using_utility(self):
        """
        This test is just check pmi-tool can break the counter for some key.
        """
        self.util_break_partition_using_pmi_tool(break_value=False)

    @attr('pmi-tool', 'util-1-7')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_7_broke_index_from_utility(self):
        """
        This test is just check pmi-tool can break index for some cache.
        """
        cache_under_test = 'cache_group_1_028'
        self.util_deploy_pmi_tool()
        custom_context = self.create_test_context('pmi_tool')
        custom_context.add_context_variables(
            fix_consistent_id=True
        )
        custom_context.set_client_result_config("client_pmi.xml")
        custom_context.set_server_result_config("server_pmi.xml")

        custom_context.build_and_deploy(self.ssh)

        self.load_data_with_streamer(end_key=5000,
                                     value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)

        update_table = ['!tables', '!index',
                        '\'create index example_idx on \"%s\".ALLTYPESINDEXED(STRINGCOL);\'' % cache_under_test,
                        '!index']

        sqlline = Sqlline(self.ignite)
        sqlline.run_sqlline(update_table)

        util_sleep_for_a_while(10)

        self.run_pmi_tool(cache_under_test)

        self.cu.control_utility('--cache', 'validate_indexes')
        output = self.cu.latest_utility_output

        expected = ['idle_verify failed', 'Idle verify failed']
        self.cu.control_utility('--cache idle_verify',
                                '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
                                all_required=expected)
        #self.su.snapshot_utility('idle_verify',
        #                         '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
        #                         all_required=expected)

        log_print(output, color='debug')
        found_broken_index = False
        for line in output.split('\n'):
            m = search('IndexValidationIssue.*cacheName=%s, idxName=EXAMPLE_IDX' % cache_under_test, line)
            if m:
                found_broken_index = True
                log_print('Index EXAMPLE_IDX is broken', color='green')

        if not found_broken_index:
            raise TidenException('Expecting index broken, but it\'s not!!!')

    @attr('tools', 'util-1-3')
    @test_case_id('91943')
    @with_setup(setup_with_custom_snapshot_folder, teardown_testcase)
    def test_util_1_3_metadata(self):
        """
        Check METADATA and ANALYZE commands from snapshot-utility.sh shows correct information.

        For METADATA command we check that output file is generated and contains correct information about snapshot.

        To test ANALYZE command we update two keys in one cache in the same partition (for cache with 16 partitions
        keys 1 and 17 should be placed in the same partition) and check that analyze shows the correct
        update counter for this partition.
        """
        from pt.piclient.helper.cache_utils import IgniteCache

        node_under_test = 2
        self.start_grid()
        self.load_data_with_streamer(end_key=1000,
                                     value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                     allow_overwrite=True)

        self.run_snapshot_utility('snapshot', '-type=full', snapshot_archive_enabled=self.snapshot_arch_enabled)
        snapshot_local_path = '%s/work/snapshot/' % self.ignite.nodes[node_under_test]['ignite_home']
        log_print(snapshot_local_path, color='blue')

        self.run_snapshot_utility('metadata', '-action=print -src=%s -id=%s'
                                  % (self.config['rt']['remote']['test_dir'], self.get_snapshot_id(1)), standalone=True)

        self.run_snapshot_utility('analyze', '-src=%s -id=%s -output=%s/snapshot_analyze_1.out'
                                  % (self.config['rt']['remote']['test_dir'], self.get_snapshot_id(1),
                                     self.config['rt']['remote']['test_dir']),
                                  standalone=True)

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            from pt.piclient.helper.cache_utils import IgniteCacheConfig
            cache_name = 'test_cache_001'

            cache_assert = IgniteCache(cache_name)
            log_print(cache_assert.get(1))
            cache_assert.put(1, 1)
            log_print(cache_assert.get(17))
            cache_assert.put(17, 1)

            cache_name = 'cache_with_empty_partitions'
            ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()

            # this part is to test GG-14708
            cache_config = IgniteCacheConfig(gateway)
            cache_config.set_name(cache_name)
            cache_config.set_cache_mode('PARTITIONED')
            cache_config.set_backups(2)
            cache_config.set_atomicity_mode('transactional')
            cache_config.set_write_synchronization_mode('full_sync')
            cache_config.set_affinity(False, 1024)
            ignite.getOrCreateCache(cache_config.get_config_object())

            cache = IgniteCache(cache_name)
            cache.put(1, "One")
            cache.put(2, "Two")

        self.run_snapshot_utility('snapshot', '-type=full', snapshot_archive_enabled=self.snapshot_arch_enabled)
        self.run_snapshot_utility('analyze', '-src=%s -id=%s -output=%s/snapshot_analyze_2.out'
                                  % (self.config['rt']['remote']['test_dir'], self.get_snapshot_id(2),
                                     self.config['rt']['remote']['test_dir']),
                                  standalone=True)
        tiden_assert('Exception' not in self.su.latest_utility_output, 'Found Exception in utility output:\n{}'.
                     format(self.su.latest_utility_output))

    @attr('tools', 'util-1-6')
    @test_case_id('91944')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_6_utility_control_cache_contention(self):
        """
        Test control.sh --cache contention shows correct information about keys with contention.
        :return:
        """
        try:
            LRT_TIMEOUT = 10
            with PiClient(self.ignite, self.get_server_config(), nodes_num=1) as piclient:

                cache_name = 'test_cache_001'

                self.cu.control_utility('--cache contention 1')

                lrt_operations = self.launch_transaction_operations(cache_name)

                self.cu.control_utility('--cache contention 1')
                self.cu.control_utility('--cache contention 11')
                self.cu.control_utility('--cache contention 10')

                for operation in lrt_operations:
                    transaction_xid = operation.getTransaction().xid().toString()
                    if transaction_xid not in self.cu.latest_utility_output:
                        log_print("Transaction with XID %s is not in list. Key=%s, Value=%s"
                                  % (transaction_xid, operation.getKey(), operation.getValue()), color='debug')

                self.release_transactions(lrt_operations)

                log_print("Sleep for %s seconds" % LRT_TIMEOUT)
                util_sleep_for_a_while(LRT_TIMEOUT)

                self.cu.control_utility('--cache contention 1')

        except TidenException as e:
            log_print(e, color='red')
            pass

    @attr('tools', 'util-1-7')
    @test_case_id('91945')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_7_list_seq(self):
        """
        Test control.sh --cache seq shows correct information about sequences.
        :return:
        """
        from random import randint
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:

            seq_name = 'seq_name_%s'
            for i in range(1, 6):
                seq_ = self.util_get_or_create_atomic_sequence(piclient, seq_name % i)

                counter_num = randint(1, 100)
                log_print('Seq seq_%s will be incremented up to %s' % (i, counter_num), color='green')
                for j in range(1, counter_num):
                    seq_.incrementAndGet()

            for i in range(1, 6):
                seq_ = self.util_get_or_create_atomic_sequence(piclient, seq_name % i)
                seq_val = seq_.get()
                log_print('Seq seq_%s has value %s' % (i, seq_val), color='debug')

            for i in range(0, 3):
                self.cu.control_utility('--cache', 'list seq_name.* --seq')
                output = self.cu.latest_utility_output
                log_print(output)

            expected = ['idle_verify check has finished, no conflicts have been found.']
            self.cu.control_utility('--cache idle_verify', all_required=expected)
            #self.su.snapshot_utility('idle_verify',
            #                         '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
            #                         all_required=expected)

            log_print(self.cu.latest_utility_output, color='debug')

    @attr('tools', 'util-1-7', 'IGN-12408')
    @test_case_id('146617')
    @require(min_server_nodes=8, min_server_hosts=2)
    @with_setup(setup_testcase_with_sbt_model, teardown_testcase)
    def test_util_1_7_validate_index_connection_refuse(self):
        for x in range(0, 2):
            self.cu.control_utility('--cache', 'validate_indexes', use_same_host=True)
            self.check_validate_indexes_output(self.cu.latest_utility_output)
            self.cu.control_utility('--cache', 'validate_indexes', use_another_host=True)
            self.check_validate_indexes_output(self.cu.latest_utility_output)

    @attr('tools', 'util-1-7')
    @test_case_id('91946')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_7_broke_index(self):
        """
        Tests control.sh --cache validate_indexes detects broken index for some cache.
        :return:
        """
        self.load_data_with_streamer(end_key=5000,
                                     value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)

        update_table = ['!tables', '!index',
                        '\'create index example_idx on \"cache_group_1_028\".ALLTYPESINDEXED(STRINGCOL);\'',
                        '!index']

        sqlline = Sqlline(self.ignite)
        sqlline.run_sqlline(update_table)

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1):
            cache_under_test = 'cache_group_1_028'
            log_print('Cache under test: %s' % cache_under_test, color='blue')

            operation = create_broke_data_entry_operation(cache_under_test, 0, True, 'index')
            operation.evaluate()

            util_sleep_for_a_while(10)
            self.cu.control_utility('--cache', 'validate_indexes')
            output = self.cu.latest_utility_output

            expected = ['idle_verify failed', 'Idle verify failed']
            self.cu.control_utility('--cache idle_verify',
                                    '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
                                    all_required=expected)
            #self.su.snapshot_utility('idle_verify',
            #                         '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
            #                         all_required=expected)

            log_print(output, color='debug')
            found_broken_index = False
            for line in output.split('\n'):
                m = search('IndexValidationIssue.*cacheName=%s, idxName=EXAMPLE_IDX' % cache_under_test, line)
                if m:
                    found_broken_index = True
                    log_print('Index EXAMPLE_IDX is broken', color='green')

            if not found_broken_index:
                raise TidenException('Expecting index broken, but it\'s not!!!')

    def util_deploy_pmi_tool(self):
        commands = {}

        hosts = self.ignite.config['environment'].get('client_hosts', [])
        pmi_remote_path = self.config['artifacts']['pmi_tool']['remote_path']
        ignite_remote_path = self.config['artifacts']['ignite']['remote_path']
        for host in hosts:
            if commands.get(host) is None:
                commands[host] = [
                    "cp %s/bin/pmi-utility.sh %s/bin/pmi-utility.sh" % (
                        pmi_remote_path, ignite_remote_path
                    ),
                    "chmod +x %s/bin/pmi-utility.sh" % ignite_remote_path,
                    "cp -r %s/bin/include/pmi-utility/ %s/bin/include/" % (
                        pmi_remote_path, ignite_remote_path
                    )
                ]
            else:
                commands[host].append('cp %s/bin/pmi-utility.sh %s/bin/pmi-utility.sh' % (
                    pmi_remote_path, ignite_remote_path))
                commands[host].append('chmod +x %s/bin/pmi-utility.sh' % ignite_remote_path)
                commands[host].append('cp -r %s/bin/include/pmi-utility/ %s/bin/include/' % (
                    pmi_remote_path, ignite_remote_path
                ))

        log_print(commands)
        results = self.ssh.exec(commands)
        log_print(results)

    def run_pmi_tool(self, cache_name, operation='break', broke_type='index'):
        client_host = self.ignite.get_and_inc_client_host()

        config = '%s/client_pmi.xml' % self.config['rt']['remote']['test_module_dir']
        commands = {
            client_host: [
                "cd %s; bin/pmi-utility.sh %s -cache=%s -primary -partition=0 -broke_types=%s -cfg=%s" % (
                    self.ignite.client_ignite_home,
                    operation,
                    cache_name,
                    broke_type,
                    config
                )
            ]
        }
        log_print(commands)
        self.ignite.logger.debug(commands)
        results = self.ignite.ssh.exec(commands)
        lines = results[client_host][0]
        log_print(lines)

    @attr('tools', 'util-1-8')
    @test_case_id('91947')
    @with_setup(setup_testcase, teardown_testcase)
    def test_util_1_8_counters_detection_during_PME_node_from_baseline(self):
        """
        Tests PME synchronise partition counters if some detected.
        :return:
        """
        self.load_data_with_streamer(end_key=1000,
                                     value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:

            caches_before_lrt = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                caches_before_lrt.append(cache_name)

            cache_under_test = caches_before_lrt[0]
            log_print('Cache under test: %s' % cache_under_test, color='blue')

            operation = create_broke_data_entry_operation(cache_under_test, 1, True, 'counter')
            operation.evaluate()

            expected = ['Conflict partition']
            self.cu.control_utility('--cache idle_verify', all_required=expected)

            output = self.cu.latest_utility_output

            grp_id, part_id = None, None
            for line in output.split('\n'):
                m = search('Conflict partition: (PartitionKey|PartitionKeyV2) \[grpId=(\d+),.*partId=(\d+)\]', line)
                if m:
                    grp_id = m.group(2)
                    part_id = m.group(3)

            tiden_assert(grp_id and part_id,
                         'Expecting to find conflicts in output\n{}'.format(self.cu.latest_utility_output))

            # Start one more server node and change baseline to run PME
            log_print("Going to start additional node", color='green')
            self.ignite.add_additional_nodes(self.get_server_config(), 1)
            self.ignite.start_additional_nodes(self.ignite.get_all_additional_nodes())
            self.cu.control_utility('--baseline')
            self.cu.set_current_topology_as_baseline()
            self.cu.control_utility('--baseline')
            msg_in_log = self.find_in_node_log('Partition states validation has failed for group: %s'
                                               % cache_under_test, node_id=1)
            assert msg_in_log != []

            # Check there are no conflicts after PME
            util_sleep_for_a_while(30)
            self.cu.control_utility('--cache', 'idle_verify')

            # Stop one more server node and change baseline to run PME
            self.ignite.kill_node(self.ignite.get_alive_additional_nodes()[0])
            util_sleep_for_a_while(30)
            self.cu.control_utility('--baseline')
            self.cu.set_current_topology_as_baseline()
            self.cu.control_utility('--baseline')

            # Check there are no conflicts after PME
            self.cu.control_utility('--cache', 'idle_verify')

    def util_break_partition_using_pmi_tool(self, break_value=True):
        cache_under_test = 'cache_group_1_028'
        self.util_deploy_pmi_tool()
        custom_context = self.create_test_context('pmi_tool')
        custom_context.add_context_variables(
            fix_consistent_id=True
        )
        custom_context.set_client_result_config("client_pmi.xml")
        custom_context.set_server_result_config("server_pmi.xml")

        custom_context.build_and_deploy(self.ssh)

        self.load_data_with_streamer(end_key=5000,
                                     value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)

        util_sleep_for_a_while(10)

        broke_type = 'counter'
        if break_value:
            broke_type = 'value'

        self.run_pmi_tool(cache_under_test, broke_type=broke_type)

        util_sleep_for_a_while(10)

        expected = ['idle_verify failed', 'Idle verify failed']
        self.cu.control_utility('--cache idle_verify',
                                '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
                                all_required=expected)
        # self.su.snapshot_utility('idle_verify',
        #                         '-output=%s/idle_verify_output.txt' % self.config['rt']['remote']['test_dir'],
        #                         all_required=expected)
        log_print(self.su.latest_utility_output)

        output = self.util_get_info_from_conflicts_file()
        log_print(output, color='green')
        records_info = [line for line in output if 'Partition instances' in line]

        if len(records_info) > 0:
            records_info = records_info[0].split('],')
        else:
            records_info = []

        partition_hash_record = set()

        prim_part_hash, prim_update_cntr = None, None
        for line in records_info:
            m = search('PartitionHashRecord \[isPrimary=(.+), partHash=([\d,-]+), updateCntr=(\d+)', line)

            if m:
                if m.group(1) == 'true':
                    prim_part_hash = m.group(2)
                    prim_update_cntr = m.group(3)
                else:
                    partition_hash_record.add((m.group(1), m.group(2), m.group(3)))
        for isPrimary, partHash, updateCntr in partition_hash_record:
            if break_value:
                assert partHash != prim_part_hash
                assert updateCntr == prim_update_cntr
            else:
                assert partHash == prim_part_hash
                assert updateCntr != prim_update_cntr

        grpId, partId = None, None
        for line in output:
            m = search('Conflict partition: PartitionKey \[grpId=(\d+),.*partId=(\d+)\]', line)
            if m:
                grpId = m.group(1)
                partId = m.group(2)

        if grpId and partId:
            self.cu.control_utility('--cache idle_verify', '-analyze -grpId=%s -partId=%s' % (grpId, partId),
                                    all_required=expected)
            #self.su.snapshot_utility('idle_verify', '-analyze -grpId=%s -partId=%s' % (grpId, partId),
            #                         all_required=expected)

            output = self.util_get_info_from_conflicts_file(file_name='idle_verify_conflicts_2.txt')
            log_print(output, color='debug')
        else:
            log_print('Could not find partition id', color='red')
        log_print(self.su.latest_utility_output)

    def util_get_info_from_conflicts_file(self, file_name='idle_verify_conflicts.txt'):
        client_host = self.config['environment']['client_hosts'][0]
        if len(self.ignite.get_all_common_nodes()) > 0:
            node_id = self.ignite.get_all_common_nodes()[0]
            client_host = self.ignite.nodes[node_id]['host']
        client_home = self.ignite.client_ignite_home

        conflicts_file = '%s/idle_verify_conflicts.*' % client_home
        new_file = '%s/%s' % (self.ignite.config['rt']['remote']['test_dir'], file_name)

        commands = ['ls %s' % conflicts_file]
        response = self.ssh.exec_on_host(client_host, commands)
        log_print(response, color='debug')
        conflicts_file = [found_file for found_file in response[client_host][0].split('\n') if found_file][-1]

        commands = ['cp %s %s' % (conflicts_file, new_file),
                    'cat %s' % new_file]
        response = self.ssh.exec_on_host(client_host, commands)
        log_print(response, color='debug')

        return response[client_host][1].split('\n')

    @staticmethod
    def launch_transaction_operations(cache_name):
        log_print("Creating transactions")
        lrt_operations = []
        for i in range(0, 10):
            lrt_operation = create_transaction_control_operation(cache_name, 1, i)
            lrt_operation.evaluate()
            lrt_operations.append(lrt_operation)

            lrt_operation = create_transaction_control_operation(cache_name, 10, i)
            lrt_operation.evaluate()
            lrt_operations.append(lrt_operation)

        for i in range(0, 5):
            lrt_operation = create_transaction_control_operation(cache_name, 15, i)
            lrt_operation.evaluate()
            lrt_operations.append(lrt_operation)

        return lrt_operations

    @staticmethod
    def release_transactions(lrt_operations):
        log_print("Releasing transactions")
        for lrt_operation in lrt_operations:
            lrt_operation.releaseTransaction()

    def remove_additional_nodes(self):
        additional_nodes = self.ignite.get_all_additional_nodes()
        for node_idx in additional_nodes:
            self.ignite.kill_node(node_idx)
            del self.ignite.nodes[node_idx]

    def find_in_node_log(self, text, node_id):
        node = self.ignite.nodes.get(node_id)
        search_result = None
        if node:
            host = node['host']
            log = node['log']
            cmd = ['cat %s | grep \'%s\'' % (log, text)]
            output = self.ssh.exec_on_host(host, commands=cmd)
            log_print(output[host], color='green')
            search_result = output[host][0]

        else:
            log_print('There is no node with id %s in ignite' % node_id, color='red')
            log_print(repr(self.ignite), color='red')

        return search_result

    @staticmethod
    def compare_lists_of_dicts(l1, l2):
        for list_id in range(0, len(l1)):
            if l1[list_id] != l2[list_id]:
                log_print('not equal:\n%s\n%s' % (l1[list_id], l2[list_id]), color='blue')

    @staticmethod
    def util_get_or_create_atomic_sequence(piclient, sequence_name, reserve_size=1000, create=True):
        gateway = piclient.get_gateway()
        ignite = piclient.get_ignite()

        atomic_cfg = gateway.jvm.org.apache.ignite.configuration.AtomicConfiguration()
        atomic_cfg.setAtomicSequenceReserveSize(reserve_size)

        sequence = ignite.atomicSequence(sequence_name, atomic_cfg, 0, False)

        if sequence is None and create:
            sequence = ignite.atomicSequence(sequence_name, atomic_cfg, 0, True)

        return sequence

    @staticmethod
    def check_validate_indexes_output(output):
        for line in output.split('\n'):
            if 'connection failed' in line:
                raise TidenException('control.sh --cache validate_index fail with connection failed')

