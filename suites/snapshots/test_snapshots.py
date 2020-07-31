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
from tiden_gridgain.piclient.helper.operation_utils import create_streamer_operation, create_put_all_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden import *
from tiden.logger import get_logger
from tiden.report.steps import Step
from tiden.tidenexception import FeatureNotEnabled

from tiden.configuration_decorator import test_configuration
from tiden.testconfig import test_config
from suites.snapshots.ult_utils import UltimateUtils


@test_configuration([
    'zookeeper_enabled',
    'compaction_enabled',
    'pitr_enabled',
    'ssl_enabled'
], [
    # Public release
    [False, False, False, True],
    # Sber release
    [False, False, True, True],
    [False, True, True, True],
    [True, False, False, True],
    [True, False, True, True],
    [True, True, True, True],
])
class TestSnapshots(UltimateUtils):
    max_key = 1001
    # snapshot_storage = None

    teardown = UltimateUtils.teardown
    setup_testcase = UltimateUtils.setup_testcase
    setup_testcase_without_activation = UltimateUtils.setup_testcase_without_activation
    setup_testcase_without_start_gid = UltimateUtils.setup_testcase_without_start_gid
    setup_shared_storage_test = UltimateUtils.setup_shared_storage_test
    teardown_testcase = UltimateUtils.teardown_testcase
    teardown_shared_storage_test = UltimateUtils.teardown_shared_storage_test

    def setup_change_config_with_shared_storage_test(self):
        self.snapshot_storage = self.su.create_shared_snapshot_storage(unique_folder=True, prefix=None)

        not_default_context = self.create_test_context('custom_snapshot_path')
        not_default_context.add_config('server.xml', 'server_with_custom_snapshot_path.xml')
        default_context_vars = self.contexts['default'].get_context_variables()
        not_default_context.add_context_variables(
            **default_context_vars,
            change_snapshot_path=True,
            change_snapshot_path_value=self.snapshot_storage
        )
        self.rebuild_configs()
        not_default_context.build_and_deploy(self.ssh)
        self.set_current_context('custom_snapshot_path')
        UltimateUtils.load_multiplier = 0.01
        self.setup_testcase()

    def teardown_change_config_shared_storage_test(self):
        self.teardown_testcase()
        self.su.remove_shared_snapshot_storage(self.snapshot_storage)
        self.snapshot_storage = None

    def setup(self):
        # Enable SSL by default (if it's not overridden in the config or using test option)
        self.ssl_enabled = self.config.get('ssl_enabled', True)

        super().setup()
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestSnapshots]')

    @attr('regression')
    @test_case_id(79981)
    @with_setup(setup_testcase, teardown_testcase)
    def test_empty_transaction(self):
        """
        This test is based on GG-13683:
        1. Enable assertions.
        2. Create empty (not involving any tx ops) transaction of any type on SERVER node.
        3. Call commit. Without fix it will fail with assertion.
        """

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            gateway = piclient.get_gateway()

            concurrency_isolation_map = self._get_tx_type_map(gateway)

            self.cu.set_current_topology_as_baseline()

            for i in range(1, 5):
                with Step(self, f'Start {i} transaction'):
                    tx = piclient.get_ignite().transactions().txStart()
                    util_sleep_for_a_while(3)
                    tx.commit()
                    for concurrency in ['OPTIMISTIC', 'PESSIMISTIC']:
                        with Step(self, f'{concurrency} concurrency'):
                            for isolation in ['READ_COMMITTED', 'REPEATABLE_READ', 'SERIALIZABLE']:
                                with Step(self, f'{isolation} isolation'):
                                    print_green(f'Run transaction {concurrency} {isolation}')
                                    tx = piclient.get_ignite().transactions().txStart(concurrency_isolation_map.get(concurrency),
                                                                                      concurrency_isolation_map.get(isolation))
                                    util_sleep_for_a_while(3)
                                    tx.commit()

    @attr('regression')
    @require(min_server_nodes=4)
    @with_setup(setup_testcase_without_activation, teardown_testcase)
    def test_evicted_partitions_removed_from_lfs(self):

        self.ignite.kill_node(4)
        util_sleep_for_a_while(5)
        self.cu.activate()

        self.load_data_with_streamer(start_key=1, end_key=10001)
        util_sleep_for_a_while(40)

        self.ignite.kill_node(3)
        util_sleep_for_a_while(10)

        self.cu.remove_node_from_baseline(self.ignite.get_node_consistent_id(3))
        util_sleep_for_a_while(5)

        self.cu.control_utility('--baseline')

        util_sleep_for_a_while(30)
        self.ignite.start_node(3)

        self.cu.set_current_topology_as_baseline()

    @attr('regression', 'pitr', 'shared_store')
    @test_case_id(79982)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    @require(test_config.pitr_enabled)
    def test_move_snapshot_after_cleanup_lfs(self):
        """
        If PITR is enabled then WAL segments moved with snapshot to the shared folder. If some WAL files are absent
        move operation should be failed if -SKIP_WAL is not used. If flag is used, then only snapshot without WAL files
        should be moved to shared folder.
        """
        initial_dump = self.calc_checksums_on_client()
        print_green(initial_dump)
        self.su.snapshot_utility('snapshot', '-type=full')

        self.load_data_with_streamer(start_key=1001, end_key=2001)

        self.su.snapshot_utility('snapshot', '-type=full')

        self.restart_grid_with_deleted_wal()

        # self.delete_lfs(delete_db=False, delete_binary_meta=False, delete_marshaller=False, delete_snapshots=False)

        self.load_data_with_streamer(start_key=2001, end_key=5001)
        self.ignite.wait_for_topology_snapshot(client_num=0)
        # self.su.snapshot_utility('list')
        # first_list = self.su.latest_utility_output
        # compare_with += [line for line in first_list.split('\n') if 'started at' not in line][:-1]

        # Run it in background
        expected = ['Command \[MOVE\] failed with error: 8740 - .*WAL files have been deleted manually after snapshot '
                    'was created.']
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage),
                                 all_required=expected)

        # operation_id = self.su.wait_running_operation()

        # we need it here as we have to recreate directory (previous one should be cleaned up).
        try:
            self.su.snapshot_utility('move',
                                     '-id=%s -SKIP_WAL -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        except TidenException:
            log_print('Try again', color='red')
            self.su.snapshot_utility('move',
                                     '-id=%s -SKIP_WAL -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        util_sleep_for_a_while(5)
        self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        after_dump = self.calc_checksums_on_client()
        self.assert_check_sums(initial_dump, after_dump)

    @attr('common')
    @test_case_id(79973)
    @with_setup(setup_testcase, teardown_testcase)
    @require(test_config.pitr_enabled)
    def test_autogenerated_snapshots(self):
        """
        This test is based on GG-13648:
        1. Start cluster.
        2. Change baseline.
        3. Check snapshot created after changing baseline is valid.
        """
        nodes_range = []

        if self.get_context_variable('pitr_enabled'):
            snapshots = self.su.get_snapshots_from_list_command()
            print_red(snapshots)
            self.su.wait_no_snapshots_activity_in_cluster()
            self.su.snapshot_utility('CHECK', f'-id={snapshots[-1]}')
        else:
            raise FeatureNotEnabled('This test required PITR feature enabled')

        for i in range(1, 3):
            with Step(self, f'Start additional node {i}'):
                node_id = list(self.ignite.add_additional_nodes(self.get_server_config(), 1))
                nodes_range += node_id
                self.ignite.start_additional_nodes(node_id)
                util_sleep_for_a_while(30)
                self.cu.set_current_topology_as_baseline()
                util_sleep_for_a_while(30)
                self.su.wait_no_snapshots_activity_in_cluster()
                self.su.snapshot_utility('list')
                snapshots = self.su.get_snapshots_from_list_command()
                print_red(snapshots)
                self.su.snapshot_utility('CHECK', '-id=%s' % snapshots[-1])

        for i in range(1, 3):
            with Step(self, f'Kill additional node {i}'):
                node_id = nodes_range.pop()
                print_green('Going to kill node %s' % node_id)
                self.ignite.kill_node(node_id)

                util_sleep_for_a_while(60)
                self.cu.set_current_topology_as_baseline()
                util_sleep_for_a_while(60)
                self.su.wait_no_snapshots_activity_in_cluster()
                self.su.snapshot_utility('list')
                snapshots = self.su.get_snapshots_from_list_command()
                print_red(snapshots)
                self.su.snapshot_utility('CHECK', '-id=%s' % snapshots[-1])

    @attr('dynamic', 'common', 'smoke')
    @test_case_id(79979)
    # @known_issue('GG-14042')
    @with_setup(setup_testcase, teardown_testcase)
    def test_dynamic_index_restore_from_snapshot_dynamic_cache(self):
        self.dynamic_index_restore_from_snapshot()

    @attr('dynamic', 'common')
    @test_case_id(79979)
    # @known_issue('GG-14042')
    @with_setup(setup_testcase, teardown_testcase)
    def test_dynamic_index_restore_from_snapshot_static_cache(self):
        self.dynamic_index_restore_from_snapshot(check_dynamic_cache=False)

    @attr('dynamic', 'shared_store')
    @test_case_id(79980)
    # @known_issue('GG-14042')
    @require(min_ignite_version='2.5.1-p5')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_dynamic_index_restore_from_snapshot_and_next_restart_dynamic_caches(self):
        self.dynamic_index_restore_from_snapshot(with_restart=True)

    def dynamic_index_restore_from_snapshot(self, check_dynamic_cache=True, with_restart=False):
        """
        Test that dynamic index is restored from snapshot.
        """
        from pt.utilities import Sqlline

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1):
            cache_to_test = 'cache_group_1_001'

            if check_dynamic_cache:
                cache_to_test = 'test_cache_with_index'
                self.create_cache_with_indexed_data(cache_to_test)

            print_green("Overwrite values in cache %s" % cache_to_test)

            operation = create_streamer_operation(cache_to_test, 1, 1001,
                                                  value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                                  allow_overwrite=True)
            operation.evaluate()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        initial_setup = ['!tables', '!index',
                         '\'create index example_idx on \"%s\".ALLTYPESINDEXED(INTCOL);\'' % cache_to_test,
                         '\'explain select * from \"%s\".ALLTYPESINDEXED where INTCOL=22;\'' % cache_to_test,
                         '\'ALTER TABLE \"%s\".ALLTYPESINDEXED ADD COLUMN (TEST INT);\'' % cache_to_test,
                         '\'create index test_idx on \"%s\".ALLTYPESINDEXED(TEST);\'' % cache_to_test,
                         '\'explain select * from \"%s\".ALLTYPESINDEXED where TEST=22;\'' % cache_to_test]

        check_commands = ['!tables',
                          '!index',
                          '\'select count(*) from \"%s\".ALLTYPESINDEXED;\'' % cache_to_test,
                          '\'explain select * from \"%s\".ALLTYPESINDEXED where INTCOL=22;\'' % cache_to_test,
                          '\'explain select * from \"%s\".ALLTYPESINDEXED where TEST=22;\'' % cache_to_test]

        expected_versions_pattern = {
            '0': {
                'index_check': [
                    '/\* \"%s\".TEST_IDX: TEST = 22 \*/' % cache_to_test,
                    '/\* \"%s\".EXAMPLE_IDX: INTCOL = 22 \*/' % cache_to_test
                ]
            },
            '8.7.8': {
                'index_check': [
                    '/\* %s.EXAMPLE_IDX: INTCOL = 22 \*/' % cache_to_test,
                    '/\* %s.TEST_IDX: TEST = 22 \*/' % cache_to_test
                ]
            },
        }

        expected = get_from_version_dict(expected_versions_pattern, self.gg_version).get('index_check')

        # Create additional index, new field and index on this field
        sql_tool = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        sql_tool.run_sqlline(initial_setup)

        dump_before = self.calc_checksums_on_client()

        # Create full snapshot and move it
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.snapshot_storage:
            if self.get_context_variable('pitr_enabled'):
                self.su.snapshot_utility('snapshot', '-type=full')

            # snapshot_store = self.su.create_shared_snapshot_storage()
            self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        # Change data in the caches
        self._remove_random_from_caches()

        # Check new field and indexes are still there
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        # Restart clean grid
        log_print('First phase - checking indexes after grid restart and restore', color='green')

        if self.snapshot_storage:
            restore_command = ['restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage)]
            self.change_grid_topology()
        else:
            restore_command = ['restore', '-id=%s' % (self.get_snapshot_id(1))]
            self.restart_empty_grid(delete_snapshots=False)

        # Restore snapshot
        self.su.snapshot_utility(*restore_command)

        # Check data restored
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

        # Check dynamic created indexes are here
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        if with_restart:
            log_print('Second phase - checking indexes after grid restart', color='green')
            # Restart grid again and check indexes
            self.restart_grid()

            output = sql_tool.run_sqlline(check_commands)
            self.su.check_content_all_required(output, expected)

    @attr('dynamic')
    @test_case_id(79977)
    @with_setup(setup_testcase, teardown_testcase)
    def test_dynamic_caches_destroyed_after_restart(self):
        """
        Test that dynamic caches has not been deleted after cluster restart.
        """
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            created_caches = self._create_dynamic_caches_with_data()

            ignite = piclient.get_ignite()
            size_1 = ignite.cacheNames().size()
            log_print("Size %s" % size_1)

            caches_to_destroy = created_caches[0: int(randint(1, len(created_caches)) / 2)]
            for cache_name in caches_to_destroy:
                print_red('Destroying cache %s' % cache_name)
                ignite.cache(cache_name).destroy()

            size_2 = ignite.cacheNames().size()
            log_print("Size %s" % size_2)

        print_red("AssertExceptions: %s" % str(self.ignite.find_exception_in_logs("java.lang.AssertionError")))

        self.restart_grid()

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            size_3 = piclient.get_ignite().cacheNames().size()
            log_print("Size %s" % size_3)

            caches = piclient.get_ignite().cacheNames().toArray()
            log_print(caches)

            tiden_assert_equal(size_3, size_2, 'Caches size is equal')
            tiden_assert(set(caches).intersection(set(caches_to_destroy)) == set(), 'There are no caches that '
                                                                                    'was destroyed')

        print_red("AssertExceptions: %s" % str(self.ignite.find_exception_in_logs("java.lang.AssertionError")))

    @attr('dynamic')
    # @require(min_ignite_version="2.5.1-p5")
    @test_case_id(79978)
    @with_setup(setup_testcase, teardown_testcase)
    def test_dynamic_index_not_stored_on_node_join(self):
        """
        This test is to cover IGN-10081.
        Base scenario is:
            1) Start nodes, add some data
            2) Shutdown a node, create a dynamic index
            3) Shutdown the whole cluster, startup with the absent node, activate from the absent node
            4) Since the absent node did not 'see' the create index, index will not be active after cluster activation
            5) Update some data in the cluster
            6) Restart the cluster, but activate from the node which did 'see' the create index
            7) Attempt to update data. Depending on the updates in (5), this will either hang or result in an exception
        """
        from pt.utilities import Sqlline

        self.create_cache_with_indexed_data('cache1')

        initial_setup = ['!tables', '!index',
                         '\'select * from \"cache1\".ALLTYPESINDEXED;\'',
                         '\'create index example_idx on \"cache1\".ALLTYPESINDEXED(LONGCOL);\'',
                         '\'explain select * from \"cache1\".ALLTYPESINDEXED where LONGCOL=22;\'']

        check_commands = ['!tables',
                          '!index',
                          '\'explain select * from \"cache1\".ALLTYPESINDEXED where LONGCOL=22;\'']

        expected = ['/\* \"cache1\".EXAMPLE_IDX: LONGCOL = 22 \*/']

        expected_versions_pattern = {
            '0': {
                'index_check': [
                    '/\* \"cache1\".EXAMPLE_IDX: LONGCOL = 22 \*/'
                ]
            },
            '8.7.8': {
                'index_check': [
                    '/\* cache1.EXAMPLE_IDX: LONGCOL = 22 \*/'
                ]
            },
        }

        expected = get_from_version_dict(expected_versions_pattern, self.gg_version).get('index_check')
        sql_tool = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        self.ignite.kill_node(4)

        # Create additional index, new field and index on this field
        sql_tool.run_sqlline(initial_setup)

        # Restart grid and activate from node which was not in topology
        self.restart_grid_without_activation()
        nodes = self.ignite.get_alive_default_nodes()
        self.cu.activate(activate_on_particular_node=nodes[-1])

        # Check index is there
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        # Restart again and activate from another node
        self.restart_grid_without_activation()
        nodes = self.ignite.get_alive_default_nodes()
        self.cu.activate(activate_on_particular_node=nodes[0])

        # Check index is there
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

    @attr('common', 'shared_store')
    @test_case_id(24837)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_list_command(self):
        full_snap_ct, inc_snap_ct = 1, 0
        caches = self.ignite.get_cache_names('cache_')
        nodes_count = self.ignite.get_nodes_num('server')

        list_versions_pattern = {
            '0':
                {'main': 'ID={id}, .* TYPE={stype}, CLUSTER SIZE={nodes_num}, SERVER NODES={nodes_num}, '
                         'SNAPSHOT FOLDER={folder}, CACHES COUNT={caches_ct}'
                 },
            '8.5.1-p150':
                {'main': 'ID={id}, .* TYPE={stype}, CLUSTER SIZE={nodes_num}, BASELINE TOPOLOGY SIZE={nodes_num}, '
                         'CACHES COUNT={caches_ct}, COMPRESSION OPTION=NONE, COMPRESSION LEVEL=NONE',
                 },
        }

        list_pattern = get_from_version_dict(list_versions_pattern, self.gg_version).get('main')

        def _get_snapshot_list(full_snapshots_ct, inc_snapshots_ct):
            full_expected_output = [
                'Command \[LIST\] successfully finished in \d+ seconds.',
                'Number of full snapshots: %s' % full_snapshots_ct,
                'Number of incremental snapshots: %s' % inc_snapshots_ct,
                'Total number of snapshots: %s' % (full_snapshots_ct + inc_snapshots_ct)
            ]
            if get_from_version_dict(list_versions_pattern, self.gg_version).get('addition'):
                full_expected_output += \
                    get_from_version_dict(list_versions_pattern, self.gg_version).get('addition')
            return full_expected_output

        self.run_snapshot_utility('snapshot', '-type=full')
        first_full = [list_pattern.format(id=self.get_snapshot_id(1), stype='FULL', nodes_num=nodes_count,
                                          folder='\<LOCAL\>', caches_ct=str(len(caches)))]

        if self.get_context_variable('pitr_enabled'):
            snapshots = self.su.get_snapshots_from_list_command()
            first_snapshot_pattern = snapshots[0] if len(snapshots) > 1 else '\d+'
            first_full += [list_pattern.format(id=first_snapshot_pattern, stype='FULL', nodes_num=nodes_count,
                                               folder='\<LOCAL\>', caches_ct=str(len(caches)))]
            full_snap_ct += 1

        self.run_snapshot_utility('list', all_required=_get_snapshot_list(full_snap_ct, inc_snap_ct) + first_full)

        # Snapshot without parameter should be incremental
        self.run_snapshot_utility('snapshot')
        inc_snap_ct += 1

        second_inc = [list_pattern.format(id='\d+', stype='INCREMENTAL', nodes_num=nodes_count,
                                          folder='\<LOCAL\>', caches_ct=str(len(caches)))]
        self.run_snapshot_utility('list', all_required=_get_snapshot_list(full_snap_ct, inc_snap_ct) + second_inc)

        if self.get_context_variable('pitr_enabled'):
            self.run_snapshot_utility('snapshot', '-type=full')
            full_snap_ct += 1

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(2), self.snapshot_storage))

        inc_remote = [list_pattern.format(id='\d+', stype='INCREMENTAL', nodes_num=nodes_count,
                                          folder=self.snapshot_storage, caches_ct=str(len(caches)))]

        self.run_snapshot_utility('list', '-src=%s' % self.snapshot_storage,
                                  all_required=_get_snapshot_list(full_snap_ct, inc_snap_ct) + inc_remote)

        self.run_snapshot_utility('list',
                                  '-src=%s -output=%s/snapshot_utility.log'
                                  % (self.snapshot_storage, self.config['rt']['remote']['test_dir']),
                                  get_output_file_content=True)

        log_print(self.su.output_file_content, color='green')

        self.su.check_content_all_required(self.su.output_file_content, inc_remote)

    @attr('common')
    @test_case_id(24838)
    @with_setup(setup_testcase, teardown_testcase)
    def test_full_snapshot_restore(self):
        """
        Added part for GG-14437: Can't restore sql cache in snapshot.
        :return:
        """
        from pt.utilities import Sqlline

        sql_cache_name = 'sql_cache'
        # create cache using sql
        create_insert = ['!tables',
                         '\'create table \"%s\"(id int, val int, primary key(id));\'' % sql_cache_name,
                         '\'insert into \"%s\"(id, val) values (1, 1);\'' % sql_cache_name]

        # Create additional index, new field and index on this field
        sql_tool = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        sql_tool.run_sqlline(create_insert)

        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full')

        self._remove_random_from_caches()

        self.su.snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(1))
        dump_after = self.calc_checksums_on_client()

        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'smoke')
    @test_case_id(24840)
    @with_setup(setup_testcase, teardown_testcase)
    def test_inc_snapshot_restore(self):
        self.su.snapshot_utility('snapshot', '-type=full')

        self._remove_random_from_caches()

        dump_before = self.calc_checksums_on_client()

        # default snapshot should be INCREMENTAL
        self.su.snapshot_utility('snapshot')

        self._remove_random_from_caches()

        # restore incremental snapshot
        self.su.snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(2))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'shared_store')
    @test_case_id(24839)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_restore_with_move_command(self):
        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        self._remove_random_from_caches()
        self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'smoke')
    @test_case_id(24841)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_restore_particular_caches(self):
        caches = self.ignite.get_cache_names('cache_group')
        caches_uder_test = self.get_caches_for_test()

        print_blue('Caches under test: count %s from %s\nnames=%s' %
                   (len(caches_uder_test), len(caches), ','.join(caches_uder_test)))

        if not self.get_context_variable('pitr_enabled'):
            dump_before = self.calc_checksums_on_client(dict_mode=True)
            self.su.snapshot_utility('snapshot', '-type=full -caches=%s' % ','.join(caches_uder_test))

            self._remove_random_from_caches()

            dump_middle = self.calc_checksums_on_client(dict_mode=True)

            self.su.snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(1))

            dump_after = self.calc_checksums_on_client(dict_mode=True)

            final_checksum_file = self.prepare_caches_one_by_one_file(dump_middle, dump_before, caches_uder_test,
                                                                      caches)

            assert final_checksum_file == dump_after, "Checksums doesn't match"
        else:
            expected = ['Error code: 4000.* If point in time recovery is enabled then snapshot with explicit '
                        'cache is not possible!.',
                        'Command \[SNAPSHOT\] failed with error: 4000 - command failed.']
            self.su.snapshot_utility('snapshot', '-type=full -caches=%s' % ','.join(caches_uder_test),
                                     all_required=expected)

    @attr('common', 'change_topology', 'shared_store')
    @test_case_id(24844)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_restore_particular_caches_to_clean_topology(self):

        caches = self.ignite.get_cache_names('cache_group')

        caches_under_test = self.get_caches_for_test()

        self.logger.debug('Caches under test: count %s from %s\nnames=%s' %
                          (len(caches_under_test), len(caches), ','.join(caches_under_test)))
        print_blue('Caches under test: count %s from %s\nnames=%s' %
                   (len(caches_under_test), len(caches), ','.join(caches_under_test)))

        dump_before = self.calc_checksums_on_client(dict_mode=True)
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._remove_random_from_caches()

        self.change_grid_topology()

        self.su.snapshot_utility('restore', '-id=%s -caches=%s -src=%s' %
                                 (self.get_snapshot_id(1), ','.join(caches_under_test), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client(dict_mode=True)

        final_checksum_file = self.prepare_caches_one_by_one_file(dump_before, dump_after, caches_under_test,
                                                                  caches, empty_caches=True)

        self.assert_check_sums(final_checksum_file, dump_after)

    @attr('change_topology', 'shared_store')
    @test_case_id(24843)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_restore_to_another_clean_topology(self):
        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._remove_random_from_caches()

        self.change_grid_topology()

        self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('change_topology', 'shared_store')
    @test_case_id(24842)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_restore_to_another_topology(self):
        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._remove_random_from_caches()

        # dump_middle = self.calc_checksums_over_caches_piclient()

        self.change_grid_topology(with_restart=False, increase_topology=False)

        self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

        self.change_grid_topology(with_restart=False)

        self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'status')
    @test_case_id(79993)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_status_dynamic_with_client(self):
        """
        Test snapshot_utility status command works properly.
        """
        self.log_cache_entries()

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            piclient.get_ignite().getOrCreateCache('cache_group_dynamic')
            streamer = create_streamer_operation('cache_group_dynamic', 1, 100000)
            streamer.setValueType(ModelTypes.VALUE_ALL_TYPES.value)
            streamer.evaluate()

            self.su.snapshot_utility('snapshot', '-type=full', background=True, log='log.txt')
            for i in range(0, 10):
                self.su.snapshot_utility('status')

    @attr('common', 'status')
    @test_case_id(79990)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_delete_status(self):
        """
        Test snapshot_utility status command works properly for delete snapshot operation.
        """
        self.log_cache_entries()

        self.su.snapshot_utility('snapshot', '-type=full')

        self.su.snapshot_utility('delete', '-id=%s' % self.get_snapshot_id(1), background=True, log='log.txt')
        for i in range(0, 10):
            self.su.snapshot_utility('status')

    @attr('common', 'status')
    @test_case_id(79989)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_check_status(self):
        """
        Test snapshot_utility status command works properly for check snapshot operation.
        """
        self.log_cache_entries()

        self.su.snapshot_utility('snapshot', '-type=full')

        self.su.snapshot_utility('check', '-id=%s' % self.get_snapshot_id(1), background=True, log='log.txt')
        for i in range(0, 10):
            self.su.snapshot_utility('status')

    @attr('common', 'status', 'shared_store')
    @test_case_id(79992)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_move_status(self):
        """
        Test snapshot_utility status command works properly for move snapshot operation.
        """
        self.log_cache_entries()

        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage),
                                 background=True, log='log.txt')
        for i in range(0, 30):
            self.su.snapshot_utility('status')
            util_sleep_for_a_while(1)

    @attr('common', 'activation', 'left_node')
    @test_case_id(79991)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_left_node(self):
        """
        If server node left topology during snapshot creation, snapshot operation should failed.
        """
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            piclient.get_ignite().getOrCreateCache('cache_group_dynamic')
            streamer = create_streamer_operation('cache_group_dynamic', 1, 100000)
            streamer.setValueType('org.apache.ignite.piclient.model.values.AllTypes')
            streamer.evaluate()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        with PiClient(self.ignite, self.get_server_config()):
            self.su.snapshot_utility('snapshot', '-type=full', background=True, log='log.txt')

            util_sleep_for_a_while(2)

        for i in range(1, 10):
            self.su.snapshot_utility('status')

        self.su.snapshot_utility('list')

    @attr('common')
    @test_case_id(24847)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_delete_command(self):
        expected_behaviour = {
            '0': {
                'expected_error': ['Error code: 7000.*failed to DELETE snapshot.*the snapshot is in the middle of a '
                                   'snapshot chain.*Use \'force delete\' to DELETE the whole chain',
                                   'Command \[DELETE\] failed with error: 7000 - command failed.'],
                'args': '-force'
            },
            '8.5.1-p150': {
                'expected_error': ['Command \[DELETE\] failed with error: 7750 - snapshot has dependent snapshots '
                                   'and could not be removed in default mode. Use \'-chain=SINGLE|FROM\' '
                                   'to define how to work with snapshot and his dependent snapshots.'],
                'args': '-chain=FROM'
            }
        }

        expected_values = get_from_version_dict(expected_behaviour, self.gg_version)
        self.delete_command_general(expected_values.get('expected_error'), expected_values.get('args'))

    def delete_command_general(self, expected, command_param):
        final_expectation = None

        self.su.snapshot_utility('snapshot', '-type=full')

        self._remove_random_from_caches(easy=True)

        # default snapshot should be INCREMENTAL
        self.su.snapshot_utility('snapshot')

        self._remove_random_from_caches(easy=True)

        # default snapshot should be INCREMENTAL
        self.su.snapshot_utility('snapshot')

        if self.get_context_variable('pitr_enabled'):
            """
            If PITR is enabled, we have to create another full snapshot as latest full snapshot couldn't be deleted.
            Also in case of enabled PITR full snapshot was created during activation, so we should take this 
            into account. As a result there will be 3 full snapshots after deletion.
            """
            self.su.snapshot_utility('snapshot', '-type=full')
            final_expectation = ['ID=%s' % self.get_snapshot_id(1), 'Total number of snapshots: 3',
                                 'Command [LIST] successfully finished']

        self.su.snapshot_utility('delete', '-id=%s' % self.get_snapshot_id(2), all_required=expected)

        self.su.snapshot_utility('delete', '-id=%s %s' % (self.get_snapshot_id(2), command_param))

        self.su.snapshot_utility('list')

        if not final_expectation:
            final_expectation = ['ID=%s' % self.get_snapshot_id(1), 'Total number of snapshots: 1',
                                 'Command [LIST] successfully finished']

        # check only one (or three in case of PITR) snapshot is in the LIST command
        self.su.check_all_msgs_in_utility_output(final_expectation)

        # check only one snapshot folder on servers
        # TODO: Fix this fot PITR enabled case.
        if not self.get_context_variable('pitr_enabled'):
            self.check_snapshots_listing_on_all_nodes([str(self.get_snapshot_id(1))])

    @attr('common', 'check')
    @test_case_id(24846)
    @with_setup(setup_testcase, teardown_testcase)
    def test_snapshot_check_command_and_logical_structure(self):

        # Step 1: Create full snapshot
        self.su.snapshot_utility('snapshot', '-type=full')
        self.su.snapshot_utility('check', '-id=%s' % self.get_snapshot_id(1))

        # Step 2: Change data in the caches
        self._remove_random_from_caches(easy=True)

        # Step 3: Create second snapshot (it should be INCREMENTAL)
        self.su.snapshot_utility('snapshot')

        # Step 4: Change data in the caches
        self._remove_random_from_caches(easy=True)

        # Step 5: Create third snapshot (it should be INCREMENTAL)
        self.su.snapshot_utility('snapshot')
        self.su.snapshot_utility('check', '-id=%s' % self.get_snapshot_id(3))

        # Step 6: Delete second's snapshot folder. Now check for third snapshot should fail
        # self.util_delete_snapshot_folder(self.get_snapshot_id(2))
        self.util_delete_snapshot_from_fs(self.get_snapshot_id(2))

        # Step 10: Check should fail
        expected = ['Snapshot ID %s is broken. Found \d+ issues:' % self.get_snapshot_id(3),
                    'Command \[CHECK\] failed with error: 6510 - snapshot is broken.']
        self.su.snapshot_utility('check', '-id=%s' % self.get_snapshot_id(3), all_required=expected)

    @attr('common', 'check', 'shared_store')
    @test_case_id(24845)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_check_command_and_physical_structure(self):
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self.su.snapshot_utility('check', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        expected = ['Snapshot ID %s is broken. Found \d+ issues:' % self.get_snapshot_id(1),
                    'Command \[CHECK\] failed with error: 6510 - snapshot is broken.']
        fail_path = self.util_change_snapshot_src(self.snapshot_storage)
        self.su.snapshot_utility('check', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage),
                                 all_required=expected)
        print(fail_path)
        self.util_change_snapshot_src(fail_path, repair=True)
        self.su.snapshot_utility('check', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        fail_path = self.util_change_snapshot_src(self.snapshot_storage, rename_dir=False)
        self.su.snapshot_utility('check', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage),
                                 all_required=expected)
        self.util_change_snapshot_src(fail_path, repair=True)
        self.su.snapshot_utility('check', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

    @attr('common', 'shared_store')
    @test_case_id(79983)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_restore_snapshot_to_another_clean_topology_clean_config(self):
        """
        Test that snapshot could be restored from shared folder to another grid with some another configuration file.
        """
        from pt.utilities import Sqlline

        self.load_data_with_streamer(end_key=2000,
                                     value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)
        cache_name = 'cache_group_1_001'

        initial_setup = ['!tables', '!index',
                         f'\'select count(*) from \"{cache_name}\".ALLTYPESINDEXED;\'',
                         f'\'create index example_idx on \"{cache_name}\".ALLTYPESINDEXED(INTCOL);\'',
                         f'\'explain select * from \"{cache_name}\".ALLTYPESINDEXED where INTCOL=22;\'']

        check_commands = ['!tables',
                          '!index',
                          f'\'select count(*) from \"{cache_name}\".ALLTYPESINDEXED;\'',
                          f'\'explain select * from \"{cache_name}\".ALLTYPESINDEXED where INTCOL=22;\'']

        expected_versions_pattern = {
            '0': {
                'index_check': [
                    f'/\* \"{cache_name}\".EXAMPLE_IDX: INTCOL = 22 \*/'
                ]
            },
            '8.7.8': {
                'index_check': [
                    f'/\* {cache_name}.EXAMPLE_IDX: INTCOL = 22 \*/'
                ]
            },
        }
        expected = get_from_version_dict(expected_versions_pattern, self.gg_version).get('index_check')

        sql_tool = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        sql_tool.run_sqlline(initial_setup)

        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        self._remove_random_from_caches()

        self.util_change_server_config_to_no_caches()

        self.restart_empty_inactive_grid()
        self.cu.activate()

        self.su.snapshot_utility('restore', '-id={} -src={}'.format(self.get_snapshot_id(1), self.snapshot_storage))

        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

        config = '{}/caches_new.xml'.format(self.config['rt']['remote']['test_module_dir'])
        self.su.snapshot_utility('restore', '-id={} -config={} -src={}'.format(self.get_snapshot_id(1), config,
                                                                               self.snapshot_storage))

        util_sleep_for_a_while(30)
        sql_tool.run_sqlline(check_commands)

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('loading')
    @require(min_ignite_version="2.5.1-p8")
    @test_case_id(79994)
    @with_setup(setup_testcase, teardown_testcase)
    def test_create_incremental_snapshot_under_loading(self):
        """
        Test snapshot could be created when cluster is under the load.
        """
        # Step 1: Create full snapshot
        self.su.snapshot_utility('snapshot', '-type=full')

        with PiClient(self.ignite, self.get_client_config()):
            # Step 2: Start loading and create few incremental snapshots
            with TransactionalLoading(self, loading_profile=LoadingProfile(delay=1, run_for_seconds=40)):
                for _ in range(0, 7):
                    self.su.snapshot_utility('snapshot', '-type=INC')

            # Step 3: Change data in the caches
            self._remove_random_from_caches(easy=True)

            # Step 5: Restore from snapshot created in Step 2
            self.su.snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(2))

        self.cu.control_utility('--cache', 'idle_verify')

        self.verify_no_assertion_errors()

    @attr('common', 'smoke')
    @test_case_id(79976)
    @with_setup(setup_testcase, teardown_testcase)
    def test_create_restore_from_not_default_location(self):
        """
        This test is based on GG-12391
        """
        # Step 1: Create full snapshot
        snapshot_location = self.su.util_get_not_default_snapshot_location()
        self.su.snapshot_utility('snapshot', '-type=full -dest=%s' % snapshot_location)

        # Step 2: Change some data and create incremental snapshot
        self._remove_random_from_caches(easy=True)
        self.su.snapshot_utility('snapshot', '-type=INC -dest=%s' % snapshot_location)

        # Step 3: Create dump to compare after restore
        dump_before = self.calc_checksums_on_client()

        # Step 4: Change data in the caches
        self._remove_random_from_caches(easy=True)

        # Step 5: Restore from snapshot created in Step 2
        self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), snapshot_location))

        # Step 6: Check data is the same as before snapshot was created
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('cancel', 'shared_store')
    @test_case_id(79975)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_cancel_move_command(self):
        """
        Test snapshot_utility cancel command could cancel already started move snapshot operation.
        """
        compare_with = ['Command \[LIST\] successfully finished']
        expecting_snapshots = []

        self._create_dynamic_caches_with_data()
        self.su.snapshot_utility('snapshot', '-type=full')
        expecting_snapshots.append(str(self.get_snapshot_id(1)))

        if self.get_context_variable('pitr_enabled'):
            activation_snapshot = self.su.get_snapshots_from_list_command()[0]
            expecting_snapshots.append(activation_snapshot)
            self.su.snapshot_utility('snapshot', '-type=full')
            expecting_snapshots.append(str(self.get_snapshot_id(2)))

        self.check_snapshots_listing_on_all_nodes(expecting_snapshots)

        self.su.snapshot_utility('list')
        first_list = self.su.latest_utility_output
        compare_with += self.escape_and_exclude_from_buffer(first_list,
                                                            ['started at', 'Command [LIST] successfully finished in'])

        # Run it in background
        self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage),
                                 background=True)

        operation_id = self.su.wait_running_operation()

        if not operation_id:
            raise TidenException('There is no operation to cancel!!!')

        self.su.snapshot_utility('cancel', '-id=%s' % operation_id)

        self.su.wait_no_snapshots_activity_in_cluster()

        self.su.snapshot_utility('list', all_required=compare_with)

        self.check_snapshots_listing_on_all_nodes(expecting_snapshots)

    @attr('cancel')
    @test_case_id(24849)
    @with_setup(setup_testcase, teardown_testcase)
    def test_cancel_snapshot_command(self):
        compare_with = ['Command \[LIST\] successfully finished']

        self.su.snapshot_utility('snapshot', '-type=full')

        self.su.snapshot_utility('list')
        first_list = self.su.latest_utility_output
        compare_with += self.escape_and_exclude_from_buffer(first_list,
                                                            ['started at', 'Command [LIST] successfully finished in'])
        self._put_additional_data_to_caches()

        # Run it in background
        self.su.snapshot_utility('snapshot', background=True, log='test.txt')

        operation_id = self.su.wait_running_operation()

        if not operation_id:
            raise TidenException('There is no operation to cancel!!!')

        self.su.snapshot_utility('cancel', '-id=%s' % operation_id)

        self.su.wait_no_snapshots_activity_in_cluster()

        try:
            self.su.snapshot_utility('cancel', '-id=%s' % operation_id)
        except TidenException as e:
            log_print('Got exception running snapshot command \{}'.format(e), color='debug')

        self.su.snapshot_utility('list', all_required=compare_with)

    @attr('cancel')
    @test_case_id(24848)
    @require(min_ignite_version="2.5.1-p5")
    @with_setup(setup_testcase, teardown_testcase)
    def test_cancel_restore_command(self):
        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full')

        # Run it in background
        self.su.snapshot_utility('restore', '-id=%s' % self.get_snapshot_id(1), background=True, log='test.txt')

        try:
            operation_id = self.su.wait_running_operation()

            if not operation_id:
                raise TidenException('There is no operation to cancel!!!')

            self.su.snapshot_utility('cancel', '-id=%s' % operation_id)
        except TidenException as e:
            log_print('Got Tiden exception {}'.format(e))

        self.su.wait_no_snapshots_activity_in_cluster()

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    # @attr('cancel')
    # @test_case_id(79974)
    # @with_setup(setup_change_config_with_shared_storage_test, teardown_change_config_shared_storage_test)
    # def test_cancel_delete_command(self):
    #     """
    #     Test snapshot_utility cancel command could cancel already started delete snapshot operation.
    #     """
    #     compare_with = ['Command \[LIST\] successfully finished']
    #     expecting_snapshots = []
    #
    #     self.su.snapshot_utility('snapshot', '-type=full')
    #     expecting_snapshots.append(str(self.get_snapshot_id(1)))
    #     self.load_data_with_streamer(start_key=1001, end_key=10000)
    #     self.su.snapshot_utility('snapshot', '-type=full')
    #     expecting_snapshots.append(str(self.get_snapshot_id(2)))
    #
    #     if self.get_context_variable('pitr_enabled'):
    #         activation_snapshot = self.su.get_snapshots_from_list_command()[0]
    #         expecting_snapshots.append(activation_snapshot)
    #         self.su.snapshot_utility('snapshot', '-type=full')
    #         expecting_snapshots.append(str(self.get_snapshot_id(3)))
    #
    #     self.check_snapshots_listing_on_all_nodes(expecting_snapshots, snapshot_path=self.snapshot_storage)
    #
    #     self.su.snapshot_utility('list')
    #     first_list = self.su.latest_utility_output
    #     compare_with += [line for line in first_list.split('\n') if 'started at' not in line][:-1]
    #
    #     # Run it in background
    #     self.su.snapshot_utility('delete', '-id=%s' % self.get_snapshot_id(1), background=True)
    #
    #     operation_id = self.su.wait_running_operation()
    #
    #     if not operation_id:
    #         raise TidenException('There is no operation to cancel!!!')
    #
    #     self.su.snapshot_utility('cancel', '-id=%s -force' % operation_id)
    #
    #     if self.su.get_last_command_error_code() == '11960':
    #         self.snapshot_utility('status')
    #
    #     try:
    #         self.su.snapshot_utility('cancel', '-id=%s' % operation_id)
    #     except TidenException as e:
    #         log_print('Got exception running snapshot command \{}'.format(e), color='debug')
    #
    #     self.su.wait_no_snapshots_activity_in_cluster()
    #
    #     # self.su.snapshot_utility('list', all_required=compare_with)
    #     self.check_snapshots_listing_on_all_nodes(expecting_snapshots, snapshot_path=self.snapshot_storage)

    @attr('cancel')
    @test_case_id(79974)
    @with_setup(setup_testcase, teardown_testcase)
    def test_cancel_delete_command(self):
        """
        Test snapshot_utility cancel command could cancel already started delete snapshot operation.
        """
        compare_with = ['Command \[LIST\] successfully finished']
        expecting_snapshots = []

        self._create_dynamic_caches_with_data()
        self.su.snapshot_utility('snapshot', '-type=full')
        expecting_snapshots.append(str(self.get_snapshot_id(1)))
        self.load_data_with_streamer(start_key=1001, end_key=40000)
        self.su.snapshot_utility('snapshot', '-type=full')
        expecting_snapshots.append(str(self.get_snapshot_id(2)))

        if self.get_context_variable('pitr_enabled'):
            activation_snapshot = self.su.get_snapshots_from_list_command()[0]
            expecting_snapshots.append(activation_snapshot)
            self.su.snapshot_utility('snapshot', '-type=full')
            expecting_snapshots.append(str(self.get_snapshot_id(3)))

        self.check_snapshots_listing_on_all_nodes(expecting_snapshots)

        self.su.snapshot_utility('list')
        first_list = self.su.latest_utility_output
        compare_with += [line for line in first_list.split('\n') if 'started at' not in line][:-1]

        # Run it in background
        self.su.snapshot_utility('delete', '-id=%s' % self.get_snapshot_id(2), background=True)

        try:
            operation_id = self.su.wait_running_operation()

            if not operation_id:
                raise TidenException('There is no operation to cancel!!!')

            self.su.snapshot_utility('cancel', '-id=%s -force' % operation_id,
                                     all_required=['Snapshot operation canceled!'])

            # just give em the last chance
            if self.su.get_last_command_error_code() == '11960':
                self.snapshot_utility('status')

            self.su.snapshot_utility('cancel', '-id=%s' % operation_id,
                                     all_required=['The operation with such ID is not found'])
        except TidenException as e:
            log_print('Got TidenException {}'.format(e))
            log_print('Check snapshot deleted')
            self.su.snapshot_utility('list')
            list_output = self.su.latest_utility_output
            tiden_assert('{}'.format(self.get_snapshot_id(2)) not in list_output,
                         'Snapshot {} should not be in the list'.format(self.get_snapshot_id(2)))

        self.su.wait_no_snapshots_activity_in_cluster(retries=2)
        # check we can run another snapshot operation after cancel
        self.su.snapshot_utility('snapshot', '-type=full')

    @attr('dynamic', 'shared_store')
    @test_case_id(52941)
    @require(min_ignite_version='2.5.1-p5')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_full_dynamic_caches(self):
        """
        Test dynamic caches are fully restored from snapshot.
        """
        with PiClient(self.ignite, self.get_client_config(), nodes_num=1):
            self._create_dynamic_caches_with_data()

            caches_after = self.calc_checksums_on_client()

            self.su.snapshot_utility('snapshot', '-type=full')

            self.change_grid_topology(with_restart=False, increase_topology=True)

            if self.get_context_variable('pitr_enabled'):
                self.su.snapshot_utility('snapshot', '-type=full')

            self.su.snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

            self.restart_empty_grid()
            self.cu.activate()

            self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

            caches_finish = self.calc_checksums_on_client()

            tiden_assert_equal(caches_after, caches_finish, 'Caches checksum')
            self.cu.control_utility('--cache', 'idle_verify')

        log_print("AssertExceptions: %s" + str(self.ignite.find_exception_in_logs("java.lang.AssertionError")))

    @attr('shared_store')
    @test_case_id(79988)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_schedule_delete_with_pitr_enabled(self):
        """
        Test schedule works properly if PITR is enabled.
        """
        for i in range(1, 4):
            self.su.snapshot_utility('snapshot', '-type=full')

        self.su.snapshot_utility('list')

        # MOVE
        move_schedule = self.su.util_get_scheduled_datetime_in_cron_format(exact=True, delta=2)
        move_task = 'move_snapshots_default'
        task_param = '-command=move -name=%s -dest=%s -frequency=\'%s\' -ttl=30000' \
                     % (move_task, self.snapshot_storage, move_schedule)
        self.su.snapshot_utility('schedule', task_param)

        # Check all rules are here
        self.su.snapshot_utility('schedule', '-list')

        # Let's wait till MOVE and DELETE rules will fire
        util_sleep_for_a_while(180)

        # Step 6: Check snapshot list for local store is empty.
        self.su.snapshot_utility('list')
        self.su.snapshot_utility('list', '-src=%s' % self.snapshot_storage)

    @attr('schedule', 'shared_store')
    @test_case_id(79984)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_schedule_command_delete_move(self):
        """
        Test if schedule with delete and move is created. And delete should be run first, scheduler works properly.
        """
        self.schedule_command_move_delete(move_first=False)

    @attr('schedule', 'shared_store')
    @test_case_id(79987)
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_schedule_command_move_delete(self):
        """
        Test if schedule with move and delete is created. And delete should be run first, scheduler works properly.
        """
        self.schedule_command_move_delete()

    def schedule_command_move_delete(self, move_first=True):
        task_name = 'create_full'
        list_pattern = {
            '0': {
                'local': ['ID=\d+, .* TYPE=FULL,.* SNAPSHOT FOLDER=<LOCAL>'],
                'remote': ['ID=\d+, .* TYPE=FULL,.* SNAPSHOT FOLDER=%s' % self.snapshot_storage],
                'snap': ['ID=\d+, .* TYPE=FULL,.* SNAPSHOT FOLDER=<LOCAL>']
            },
            '8.5.1-p150': {
                'local': ['Path - <LOCAL>', 'List of snapshots:', 'ID=\d+, .* TYPE=FULL,.*'],
                'remote': ['Path - %s' % self.snapshot_storage, 'List of snapshots:', 'ID=\d+, .* TYPE=FULL,.*'],
                'snap': ['ID=\d+, .* TYPE=FULL,.*']
            }
        }

        expected_list_values = get_from_version_dict(list_pattern, self.gg_version)
        # snapshot_store = self.su.create_shared_snapshot_storage()
        task_param = '-command=create -name=%s -full_frequency=\'%s\'' \
                     % (task_name, self.su.util_get_scheduled_datetime_in_cron_format())

        self.su.snapshot_utility('schedule', task_param)

        # Let's sleep for some time to let fire the snapshot task
        util_sleep_for_a_while(130, msg='Waiting for scheduler')

        # Step 2. Disable full snapshot rule cause there could be conflict with move and delete rules.
        self.su.snapshot_utility('schedule', '-disable -name=%s' % task_name)

        # Step 3: Check there are full snapshots in local store.
        first_full_snapshot_list = ['List of snapshots:', 'ID=\d+, .* TYPE=FULL',
                                    'Command \[LIST\] successfully finished in \d+ seconds.']

        if self.get_context_variable('pitr_enabled'):
            activation_snapshot = self.su.get_snapshots_from_list_command()[0]
            escape = ['ID=%s' % activation_snapshot]
            self.su.snapshot_utility('list', all_required=first_full_snapshot_list, escape=escape)
        else:
            self.su.snapshot_utility('list', all_required=first_full_snapshot_list)

        delete_schedule = self.su.util_get_scheduled_datetime_in_cron_format(exact=True, delta=2)
        move_schedule = self.su.util_get_scheduled_datetime_in_cron_format(exact=True, delta=4)

        if move_first:
            move_schedule = self.su.util_get_scheduled_datetime_in_cron_format(exact=True, delta=2)
            delete_schedule = self.su.util_get_scheduled_datetime_in_cron_format(exact=True, delta=4)

        # Step 4. Create DELETE schedule rule
        delete_task = 'delete_snapshots_default'
        task_param = '-command=delete -name=%s -frequency=\'%s\' -ttl=30000' % (delete_task, delete_schedule)
        self.su.snapshot_utility('schedule', task_param)

        # Step 5. Create MOVE schedule rule
        move_task = 'move_snapshots_default'
        task_param = '-command=move -name=%s -dest=%s -frequency=\'%s\' -ttl=30000' \
                     % (move_task, self.snapshot_storage, move_schedule)
        self.su.snapshot_utility('schedule', task_param)

        # Check all rules are here
        schedule_listing_light = [
            'Schedules in cluster: 3'
            # , 'List of schedules:'
        ]

        for name, cmd in [(task_name, 'CREATE'), (delete_task, 'DELETE'), (move_task, 'MOVE')]:
            schedule_listing_light += ['Name: %s' % name, 'Command: %s' % cmd]

        self.su.snapshot_utility('schedule', '-list', all_required=schedule_listing_light)

        # Let's wait till MOVE and DELETE rules will fire
        util_sleep_for_a_while(180, msg='Waiting for scheduler')

        # Step 6: Check snapshot list for local store is empty.
        empty_snapshot_list = ['No snapshots found',
                               'Command \[LIST\] successfully finished in \d+ seconds.']

        # in case of PITR last snapshot can't be moved
        if self.get_context_variable('pitr_enabled'):
            empty_snapshot_list = ['List of snapshots:',
                                   'ID=\d+, .* TYPE=FULL',
                                   'Number of full snapshots: 1',
                                   'Number of incremental snapshots: 0',
                                   'Total number of snapshots: 1',
                                   'Command \[LIST\] successfully finished in \d+ seconds.']

        self.su.snapshot_utility('list', all_required=empty_snapshot_list)

        # Step 7: Check snapshot list for shared store is not empty as MOVE rule should be fired first.
        # if move was first, we should find snapshot in the shared folder.
        if move_first:
            snapshot_list = expected_list_values.get('remote') + \
                            ['Command \[LIST\] successfully finished in \d+ seconds.']
            if self.get_context_variable('pitr_enabled'):
                snapshot_list.insert(0, expected_list_values.get('snap')[0])
        else:
            snapshot_list = empty_snapshot_list

            if self.get_context_variable('pitr_enabled'):
                snapshot_list = expected_list_values.get('local') + \
                                ['Command \[LIST\] successfully finished in \d+ seconds.']

        self.su.snapshot_utility('list', '-src=%s' % self.snapshot_storage,
                                 all_required=snapshot_list,
                                 maintain_order=True)

        # Clean up
        self.su.util_delete_scheduled_task([task_name, delete_task, move_task], check_no_tasks=True)

    @attr('schedule')
    @test_case_id(79986)
    @with_setup(setup_testcase, teardown_testcase)
    def test_schedule_command_inc_snapshot(self):
        """
        Test schedule with create incremental snapshot works properly.
        """
        # Step 1: Create full snapshot manually as we need it to be able to create incremental one.
        self.su.snapshot_utility('snapshot', '-type=full')

        # Step 2. Create new rule for incremental snapshot.
        task_name = 'create_inc'
        after_2_mins = self.su.util_get_scheduled_datetime_in_cron_format(exact=True)
        after_5_mins = self.su.util_get_scheduled_datetime_in_cron_format(exact=True, delta=5)
        task_param = '-command=create -name=%s -full_frequency=\'%s\' -incremental_frequency=\'%s\'' % \
                     (task_name, after_5_mins, after_2_mins)
        self.su.snapshot_utility('schedule', task_param)

        # Step 2. Check new rule applied.
        # To match cron like tasks regexp [\*,\/, ,\d]+ is used
        schedule_listing = [
            'Schedules in cluster: %s' % '1',
            # 'List of schedules:',
            'Name: %s' % task_name,
            'Command: CREATE',
            'State: enabled',
            'Full snapshot frequency: %s' % '[\*,\/, ,\d]+',
            'Incremental snapshot frequency: %s' % '[\*,\/, ,\d]+'
        ]
        # To match cron like tasks regexp [\*,\/, ,\d]+ is used

        self.su.snapshot_utility('schedule', '-list', all_required=schedule_listing)

        # Step 3: Check snapshot list is empty.
        first_full_snapshot_list = ['List of snapshots:', 'ID=%s, .* TYPE=FULL' % self.get_snapshot_id(1),
                                    'Command \[LIST\] successfully finished in \d+ seconds.']
        self.su.snapshot_utility('list', all_required=first_full_snapshot_list)

        # Let's sleep for some time to let fire the snapshot task
        util_sleep_for_a_while(130, msg='Waiting for scheduler')

        # Step 4: Check snapshot list is not empty.
        snapshot_list = ['List of snapshots:', 'ID=\d+, .* TYPE=INCREMENTAL',
                         'Command \[LIST\] successfully finished in \d+ seconds.'] + first_full_snapshot_list[1:-1]
        self.su.snapshot_utility('list', all_required=snapshot_list)

        # Delete rule
        task_param = '-delete -name=%s' % task_name
        self.su.snapshot_utility('schedule', task_param)

        schedule_success_empty = ['No schedules found', 'Command \[SCHEDULE\] successfully finished in \d+ seconds.']
        self.su.snapshot_utility('schedule', '-list', all_required=schedule_success_empty)

    @attr('schedule')
    @test_case_id(79985)
    @with_setup(setup_testcase, teardown_testcase)
    def test_schedule_command_full_snapshot(self):
        """
        Test schedule with create full snapshot works properly.
        """
        # Step 1. Create schedule rule - full snapshot every 2 minutes.
        task_name = 'create_full'
        task_param = '-command=create -name=%s -full_frequency=\'%s\'' \
                     % (task_name, self.su.util_get_scheduled_datetime_in_cron_format())

        self.su.snapshot_utility('schedule', task_param)

        # Step 2. Check new rule applied.
        # To match cron like tasks regexp [\*,\/, ,\d]+ is used
        schedule_listing = [
            'Schedules in cluster: %s' % '1',
            # 'List of schedules:',
            'Name: %s' % task_name,
            'Command: CREATE',
            'State: enabled',
            'Full snapshot frequency: %s' % '[\*,\/, ,\d]+',
            'Incremental snapshot frequency: %s' % 'Never'
        ]
        # To match cron like tasks regexp [\*,\/, ,\d]+ is used

        self.su.snapshot_utility('schedule', '-list', all_required=schedule_listing)

        # Step 3: Check snapshot list is empty.
        empty_snapshot_list = ['No snapshots found', 'Command \[LIST\] successfully finished in \d+ seconds.']

        if self.get_context_variable('pitr_enabled'):
            empty_snapshot_list = ['Number of full snapshots: 1', 'Number of incremental snapshots: 0',
                                   'Total number of snapshots: 1',
                                   'Command \[LIST\] successfully finished in \d+ seconds.']

        self.su.snapshot_utility('list', all_required=empty_snapshot_list)

        # Let's sleep for some time to let fire the snapshot task
        util_sleep_for_a_while(130, msg='Waiting for scheduler')

        # Step 4: Check snapshot list is not empty.
        snapshot_list = ['List of snapshots:', 'ID=\d+, .* TYPE=FULL',
                         'Command \[LIST\] successfully finished in \d+ seconds.']
        if self.get_context_variable('pitr_enabled'):
            snapshot_list.append('Number of full snapshots: 2')
            activation_snapshot = self.su.get_snapshots_from_list_command()[0]
            escape = ['ID=%s' % activation_snapshot]
            self.su.snapshot_utility('list', all_required=snapshot_list, escape=escape)
        else:
            self.su.snapshot_utility('list', all_required=snapshot_list)

        # Delete rule
        task_param = '-delete -name=%s' % task_name
        self.su.snapshot_utility('schedule', task_param)

        schedule_success_empty = ['No schedules found', 'Command \[SCHEDULE\] successfully finished in \d+ seconds.']
        self.su.snapshot_utility('schedule', '-list', all_required=schedule_success_empty)

    @attr('common', 'shared_store', 'copy')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_inc_snapshot_restore_after_full_with_single_copy(self):
        """
        Test we can restore from copied snapshot if:
        1. There is the same snapshot in local snapshot storage.
        2. There is no snapshot in local storage.
        """
        self.run_snapshot_utility('snapshot', '-type=full')

        # if self.get_context_variable('pitr_enabled'):
        #     self.su.snapshot_utility('snapshot', '-type=full')
        self.upload_data_using_sbt_model()

        self.run_snapshot_utility('snapshot')
        dump_before = self.calc_checksums_on_client()

        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility('move', '-id=%s %s -single_copy -chain=SINGLE -dest=%s' %
                                  (self.get_snapshot_id(1),
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        self.run_snapshot_utility('list', '-src=%s' % self.snapshot_storage)
        self._remove_random_from_caches()
        # self.run_snapshot_utility('snapshot')

        self.run_snapshot_utility('copy', '-id=%s %s -dest=%s' %
                                  (self.get_snapshot_id(2),
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

        self.upload_data_using_sbt_model()
        # clean up local snapshots to be sure it restored from remote storage
        self.util_delete_snapshot_from_fs()

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), self.snapshot_storage))
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'shared_store', 'copy')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_snapshot_restore_from_copied_snapshot(self):
        """
        Test we can restore from copied snapshot if:
        1. There is the same snapshot in local snapshot storage.
        2. There is no snapshot in local storage.
        """
        dump_before = self.calc_checksums_on_client()
        self.run_snapshot_utility('snapshot', '-type=full')

        # if self.get_context_variable('pitr_enabled'):
        #     self.su.snapshot_utility('snapshot', '-type=full')

        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility('copy', '-id=%s %s -dest=%s' %
                                  (self.get_snapshot_id(1),
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        self.upload_data_using_sbt_model()

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        tiden_assert_equal(dump_before, dump_after, "Check sums doesn't match")

        self.upload_data_using_sbt_model()
        # clean up local snapshots to be sure it restored from remote storage
        self.util_delete_snapshot_from_fs(snapshot_id=self.get_snapshot_id(1))

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'shared_store', 'copy')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_single_copy_flag_for_move_command(self):
        """
        Test -single_copy for MOVE command. Snapshot with -single_copy should be faster and smaller that without
        -single_copy flag.
        """
        self.single_copy_flag_for_commands('move')

    @attr('common', 'shared_store', 'copy')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_single_copy_flag_for_copy_command(self):
        """
        Test -single_copy for COPY command. Snapshot with -single_copy should be faster and smaller that without
        -single_copy flag.
        """
        self.single_copy_flag_for_commands('copy')

    def single_copy_flag_for_commands(self, command):
        dump_before = self.calc_checksums_on_client()

        self.run_snapshot_utility('snapshot', '-type=full')

        # MOVE command can't move last snapshot if PITR is enabled.
        if command == 'move' and is_enabled(self.config.get('pitr_enabled')):
            self.run_snapshot_utility('snapshot', '-type=full')

        # copy/move snapshot
        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility(command, '-id=%s %s -dest=%s' %
                                  (self.get_snapshot_id(1),
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        self.run_snapshot_utility('snapshot', '-type=full')

        # copy/move snapshot with --single_copy flag
        # self.run_snapshot_utility('copy', '-id=%s -single_copy -skip_wal -dest=%s' %
        self.run_snapshot_utility(command, '-id=%s -single_copy %s -dest=%s' %
                                  (self.get_snapshot_id(2),
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        self.run_snapshot_utility('list', '-src=%s' % self.snapshot_storage)

        first_snapshot_size = self.util_get_snapshot_size_on_lfs(self.get_snapshot_id(1), self.snapshot_storage)
        second_snapshot_size = self.util_get_snapshot_size_on_lfs(self.get_snapshot_id(2), self.snapshot_storage)

        # as second snapshot was copied/moved with -single_copy option it should be smaller
        tiden_assert(second_snapshot_size < int(first_snapshot_size - first_snapshot_size * 0.3),
                     'Snapshot with -single_copy should be smaller, but %s is not smaller than %s'
                     % (second_snapshot_size, first_snapshot_size))

        self._remove_random_from_caches()

        # check we can restore to snapshot copied/moved with -single_copy option
        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(2), self.snapshot_storage))
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'copy', 'shared_store')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_can_copy_last_snapshots_with_pitr_enabled(self):
        """
        Test we can copy latest snapshot if PITR feature is enabled.
        """
        if not is_enabled(self.config.get('pitr_enabled')):
            TidenException('PIRT functionality should be enabled')

        self.run_snapshot_utility('snapshot', '-type=full')
        dump_before = self.calc_checksums_on_client()

        # TODO: copy command can not copy WAL files for now, so -skip_wal is using. We need to remove this flag.
        self.run_snapshot_utility('copy', '-id=%s -skip_wal -dest=%s' % (self.get_latest_snapshot_id(),
                                                                         self.snapshot_storage))

        # delete local snapshots
        self.util_delete_snapshot_from_fs()

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_latest_snapshot_id(), self.snapshot_storage))
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('change_topology', 'shared_store', 'copy')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_copy_snapshot_and_restore_to_another_smaller_topology(self):
        """
        This is the main case for COPY command:
        1. Run cluster and create full snapshots.
        2. COPY snapshot with -single_copy option.
        3. RESTORE to another smaller grid.
        """
        dump_before = self.calc_checksums_on_client()

        self.run_snapshot_utility('snapshot', '-type=full')
        self.run_snapshot_utility('snapshot', '-type=full')

        self.run_snapshot_utility('copy', '-id=%s -skip_wal -dest=%s' %
                                  (self.get_snapshot_id(1), self.snapshot_storage))

        self.run_snapshot_utility('copy', '-id=%s -single_copy -skip_wal -dest=%s' %
                                  (self.get_latest_snapshot_id(), self.snapshot_storage))

        self._remove_random_from_caches()

        self.change_grid_topology(increase_topology=False)

        self.run_snapshot_utility('restore', '-id=%s -src=%s' % (self.get_latest_snapshot_id(), self.snapshot_storage))

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('common', 'copy', 'shared_store', 'mute')
    @test_case_id(192426)
    # @known_issue('GG-14344')
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_copy_snapshots_chain_from(self):
        """
        Test -chain=FROM option for COPY command.
        1. Create snapshots chain (full snapshot + several incremental snapshots).
        2. Run COPY command with -chain=FROM and ID='first incremental snapshot'.
        3. Delete local folders with all incremental snapshots for nodes.
        So there should be FULL local snapshot and INC snapshots on the shared storage.
        4. Restore and check every snapshot from chain.
        """
        iterations = 2
        snapshots_chain = self.util_create_snapshots_chain(iterations)

        # TODO: enable -single_copy flag when it will be implemented for incremental snapshots
        single_copy = choice([True, False])
        single_copy = False
        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility('copy', '-id=%s %s %s -chain=FROM -dest=%s' %
                                  (self.get_snapshot_id(2),
                                   '-single_copy' if single_copy else '',
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        # delete all snapshots except first
        for snapshot_id in self.su.snapshots[1:]:
            self.util_delete_snapshot_from_fs(snapshot_id=snapshot_id['id'])

        self.util_check_restoring_from_chain(snapshots_chain)

    @attr('common', 'copy', 'shared_store')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_copy_snapshots_chain_from_with_delete_source(self):
        """
        Test -delete_source flag:
        1. Create snapshots chain (full snapshot + several incremental snapshots).
        2. Run COPY command with -delete_source flag.
        3. Check there is no local snapshots.
        4. Restore and check every snapshot from chain.
        """
        iterations = 2
        snapshots_chain = self.util_create_snapshots_chain(iterations)
        # TODO: enable -single_copy flag when it will be implemented for incremental snapshots
        single_copy = choice([True, False])
        single_copy = False
        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility('copy', '-id=%s %s %s -delete_source -chain=FROM -dest=%s' %
                                  (self.get_snapshot_id(1),
                                   '-single_copy' if single_copy else '',
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        # check there is no local snapshots
        for snapshot_id in self.su.snapshots:
            snapshot_dirs = self.util_find_snapshot_folders_on_fs(snapshot_id['id'])
            tiden_assert(snapshot_dirs == {}, 'There is no local snapshots %s' % snapshot_dirs)

        self.util_check_restoring_from_chain(snapshots_chain)

    @attr('common', 'copy', 'shared_store', 'mute')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_copy_snapshots_chain_to(self):
        """
        Test -chain=TO option for COPY command.
        1. Create snapshots chain (full snapshot + several incremental snapshots).
        2. Run COPY command with -chain=TO and ID='first incremental snapshot'.
        3. Delete local folders with all incremental snapshots for nodes.
        So there should be FULL local snapshot and INC snapshots on the shared storage.
        4. Restore and check every snapshot from chain.
        """
        iterations = 2
        snapshots_chain = self.util_create_snapshots_chain(iterations)

        # TODO: enable -single_copy flag when it will be implemented for incremental snapshots
        single_copy = choice([True, False])
        single_copy = False
        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility('copy', '-id=%s %s %s -chain=TO -dest=%s' %
                                  (self.get_snapshot_id(2),
                                   '-single_copy' if single_copy else '',
                                   '-skip_wal' if skip_wal else '',
                                   self.snapshot_storage))

        # delete all snapshots except first
        for snapshot_id in self.su.snapshots[:-1]:
            self.util_delete_snapshot_from_fs(snapshot_id=snapshot_id['id'])

        self.util_check_restoring_from_chain(snapshots_chain)

    @attr('common', 'shared_store')
    @test_case_id(1)
    @require(min_ignite_version='2.5.1-p151')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_copy_snapshots_chain(self):
        """
        Test snapshots chain (full snapshot + several incremental snapshots). Some of these chain snapshots
        could be done with -archive=ZIP option.
        Then restore every snapshot and check data correctness.
        """
        snapshots_chain = []
        iterations = 2
        self.run_snapshot_utility('snapshot', '-type=full')

        skip_wal = is_enabled(self.config.get('pitr_enabled'))
        self.run_snapshot_utility('copy', '-id=%s %s -dest=%s' % (self.get_latest_snapshot_id(),
                                                                  '-skip_wal' if skip_wal else '',
                                                                  self.snapshot_storage))

        for i in range(0, iterations):
            print_green('Iteration %s from %s' % (i + 1, iterations))
            self._remove_random_from_caches()
            archive = choice([True, False])
            # TODO: -single_copy does not work with inc snapshots for now
            single_copy = choice([True, False])
            single_copy = False
            self.run_snapshot_utility('snapshot', do_not_archive=archive)
            current_dump = self.calc_checksums_on_client()
            snapshots_chain.append((self.get_latest_snapshot_id(), current_dump))
            self.run_snapshot_utility('copy', '-id=%s %s %s -dest=%s' %
                                      (self.get_latest_snapshot_id(),
                                       '-single_copy' if single_copy else '',
                                       '-skip_wal' if skip_wal else '',
                                       self.snapshot_storage))
            self.load_data_with_streamer(end_key=5000, allow_overwrite=True)

        # check that we could restore from local snapshots
        for snapshot_id, dump_before in snapshots_chain:
            self.run_snapshot_utility('restore', '-id=%s' % snapshot_id)
            dump_after = self.calc_checksums_on_client()
            self.assert_check_sums(dump_before, dump_after)

        # delete local snapshots
        self.util_delete_snapshot_from_fs()

        # check that we could restore from remote snapshots
        for snapshot_id, dump_before in snapshots_chain:
            self.run_snapshot_utility('restore', '-id=%s -src=%s' % (snapshot_id, self.snapshot_storage))
            dump_after = self.calc_checksums_on_client()
            self.assert_check_sums(dump_before, dump_after)

    @attr('change_topology')
    # @test_case_id(24842)
    @with_setup(setup_testcase, teardown_testcase)
    def test_node_leave_during_restore(self):
        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('snapshot', '-type=full', background=True, log='log.txt')
        for i in range(1, 20):
            self.su.snapshot_utility('status')

        self.ignite.kill_node(4)

        self.su.wait_no_snapshots_activity_in_cluster(retries=40)

        snpshots = self.su.get_snapshots_from_list_command()

        self.su.snapshot_utility('check', '-id=%s' % snpshots[-1])

        dump_after = self.calc_checksums_on_client()

        self.assert_check_sums(dump_before, dump_after)

        self.change_grid_topology(with_restart=False)

        self.su.snapshot_utility('restore', '-id=%s ' % snpshots[-1])

        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

    @attr('regression')
    # @known_issue('GG-13772')
    @with_setup(setup_testcase, teardown_testcase)
    def test_restore_indexes_with_same_name_from_snapshot(self):
        """
        Test that snapshot with caches contains indexes with same name (but upper and lower cases or mixed) can be
        restored. Based on IGN-12564.
        Phase 1: Restore to not-empty grid.
        Phase 2: Restore to empty grid.
        """
        from pt.utilities import Sqlline

        with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
            cache_to_test = 'test_cache_with_index'
            self.create_cache_with_indexed_data(cache_to_test)

            print_green("Overwrite values in cache %s" % cache_to_test)

            ignite = piclient.get_ignite()

            for cache_name in [cache_to_test]:
                for idx in ['example_IDX', 'example_idx', 'Example_IDX', 'Example_Idx']:
                    sqlFieldsQuery1 = piclient.get_gateway().jvm.org.apache.ignite.cache.query \
                        .SqlFieldsQuery('create index \"%s\" on \"%s\".ALLTYPESINDEXED(LONGCOL)' % (idx, cache_name))
                    ignite.cache(cache_name).query(sqlFieldsQuery1).getAll();

            stream_operation = create_streamer_operation(cache_to_test, 1, 1001, allow_overwrite=True)
            stream_operation.evaluate()

        self.wait_for_running_clients_num(0, 90)

        initial_setup = ['!tables', '!index',
                         '\'create index \'example_idX\' on \"%s\".ALLTYPESINDEXED(LONGCOL);\'' % cache_to_test,
                         '\'create index \'exaMple_IDX\' on \"%s\".ALLTYPESINDEXED(LONGCOL);\'' % cache_to_test,
                         '\'explain select * from \"%s\".ALLTYPESINDEXED where LONGCOL=22;\'' % cache_to_test,
                         '\'ALTER TABLE \"%s\".ALLTYPESINDEXED ADD COLUMN (TEST INT);\'' % cache_to_test,
                         '\'create index test_idx on \"%s\".ALLTYPESINDEXED(TEST);\'' % cache_to_test,
                         '\'explain select * from \"%s\".ALLTYPESINDEXED where TEST=22;\'' % cache_to_test]

        check_commands = ['!tables',
                          '!index',
                          '\'select * from \"%s\".ALLTYPESINDEXED;\'' % cache_to_test,
                          '\'explain select * from \"%s\".ALLTYPESINDEXED where LONGCOL=22;\'' % cache_to_test,
                          '\'explain select * from \"%s\".ALLTYPESINDEXED where TEST=22;\'' % cache_to_test]

        expected_versions_pattern = {
            '0': {
                'index_check': [
                    '/\* \"%s\".TEST_IDX: TEST = 22 \*/' % cache_to_test,
                    '/\* \"%s\".EXAMPLE_IDX: LONGCOL = 22 \*/' % cache_to_test
                ]
            },
            '8.7.8': {
                'index_check': [
                    '/\* %s.EXAMPLE_IDX: LONGCOL = 22 \*/' % cache_to_test,
                    '/\* %s.TEST_IDX: TEST = 22 \*/' % cache_to_test
                ]
            },
        }
        expected = get_from_version_dict(expected_versions_pattern, self.gg_version).get('index_check')

        # Create additional index, new field and index on this field
        sql_tool = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        sql_tool.run_sqlline(initial_setup)

        dump_before = self.calc_checksums_on_client()

        # Create full snapshot and move it
        self.su.snapshot_utility('snapshot', '-type=full')

        if self.get_context_variable('pitr_enabled'):
            self.su.snapshot_utility('snapshot', '-type=full')

        log_print('Phase 1: Restore to not-empty grid:', color='green')
        self.restart_grid()

        # Restore snapshot
        # self.su.snapshot_utility('restore', '-id=%s -src=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        self.su.snapshot_utility('restore', '-id=%s' % (self.get_snapshot_id(1)))

        # Check data restored
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

        # Check dynamic created indexes are here
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        log_print('Phase 2: Restore to empty grid:', color='green')
        self.restart_empty_grid(delete_snapshots=False)

        self.su.snapshot_utility('restore', '-id=%s' % (self.get_snapshot_id(1)))

        # Check data restored
        dump_after = self.calc_checksums_on_client()
        self.assert_check_sums(dump_before, dump_after)

        # Check dynamic created indexes are here
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

    @attr('regression')
    @with_setup(setup_testcase, teardown_testcase)
    def test_create_index_on_static_cache(self):
        """
        Based on IGN-12707.
        This could be checked like this:
        1: Any DDL operation on static cache.
        2: Any DDL operation on dynamic cache after grid restart.
        """
        from pt.utilities import Sqlline

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            dynamic_cache = 'test_cache_with_index'
            self.create_cache_with_indexed_data(dynamic_cache)

            print_green("Overwrite values in cache %s" % dynamic_cache)
            stream_operation = create_streamer_operation(dynamic_cache, 1, 1001, allow_overwrite=True)
            stream_operation.evaluate()

        static_cache = 'cache_group_1_001'
        static_index = 'static_example_idx'
        dynamic_index = 'dynamic_example_idx'
        create_index_ddl = '\'create index \"{index_name}\" on \"{table_name}\".ALLTYPESINDEXED(LONGCOL);\''
        sql_errors = ['Cache doesn\'t exist', 'java.sql.SQLException:']
        initial_setup = ['!tables',
                         '!index']

        for index, cache in [(static_index, static_cache), (dynamic_index, dynamic_cache)]:
            initial_setup += [create_index_ddl.format(index_name=index, table_name=cache)]

        check_commands = ['!index']

        expected = [dynamic_index, static_index]

        sql_tool = Sqlline(self.ignite, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        output = sql_tool.run_sqlline(initial_setup)
        self.check_for_exceptions(output.split('\n'), sql_errors)

        # Check dynamic created indexes are here
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        log_print('Phase 1: Restore to not-empty grid:', color='green')
        self.restart_grid()

        for index in [static_index, dynamic_index]:
            initial_setup = [query.replace(index, 'new_{}'.format(index)) for query in initial_setup]

        output = sql_tool.run_sqlline(initial_setup)
        self.check_for_exceptions(output.split('\n'), sql_errors)

        expected = ['new_{}'.format(query) for query in expected]
        # Check dynamic created indexes are here
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

    @attr('regression')
    @require(min_ignite_version="2.5.1-p5")
    @with_setup(setup_testcase, teardown_testcase)
    def test_restore_compressed_snapshots(self):
        """
        Test compressed snapshots with different compression level could be restored.
        :return:
        """
        snapshots_chain = []
        compression_levels = [2, 4, 9]
        self.su.snapshot_utility('snapshot', '-type=full')
        current_dump = self.calc_checksums_on_client()
        snapshots_chain.append((self.get_latest_snapshot_id(), current_dump))

        for c_level in compression_levels:
            self.load_data_with_streamer(end_key=5000, allow_overwrite=True)
            self.run_snapshot_utility('snapshot', '-type=full -archive=ZIP -compression_level={}'.format(c_level))
            current_dump = self.calc_checksums_on_client()
            snapshots_chain.append((self.get_latest_snapshot_id(), current_dump))

        self.run_snapshot_utility('list')

        # check that we could restore from remote snapshots
        for snapshot_id, dump_before in snapshots_chain:
            self.run_snapshot_utility('restore', '-id={}'.format(snapshot_id))
            dump_after = self.calc_checksums_on_client()
            self.assert_check_sums(dump_before, dump_after)

    @attr('regression')
    # @known_issue('GG-14733')
    @with_setup(setup_testcase_without_activation, teardown_testcase,
                change_grid_config={'custom_checkpointFrequency': True})
    def test_restore_inc_snapshot_with_empty_partition(self):
        """
        Based on GG-14733.
        The main idea of this reproducer is to catch situation of put some value into empty partition after first
        checkpoint (with reason 'snapshot') and before second checkpoint (with reason 'timeout' or 'dirty pages')
        when incremental checkpoint is in progress.
        """
        from pt.piclient.helper.cache_utils import IgniteCache
        self.cu.activate()

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache_name = 'cache_group_1_001'
            cache_name_to_test = 'cache_group_4_118'
            gateway = piclient.get_gateway()

            self.su.snapshot_utility('snapshot', '-type=full')

            # put some values into another cache
            stream_operation = create_streamer_operation(cache_name, 1, 100001, allow_overwrite=True)
            stream_operation.evaluate()

            # put some values into this cache so it could be snapshoted
            stream_operation = create_put_all_operation(cache_name_to_test, 1, 11, 1,
                                                        value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                        gateway=gateway)
            stream_operation.evaluate()

            cache_to_test = IgniteCache(cache_name_to_test)

            # for i in range(1000, 1008):
            for i in range(28, 31):
                self.su.snapshot_utility('snapshot', '-type=inc', background=True, log='log.txt')
                log_print('Started snapshot {}'.format(datetime.now().isoformat()), color='debug')
                check = self.ignite.find_in_node_log(1, 'tail -n 20 | grep "Completed partition exchange"')

                iterations = 0
                while not check and iterations < 100:
                    check = self.ignite.find_in_node_log(1, 'tail -n 20 | grep "Completed partition exchange"')
                    iterations += 1

                log_print('Put into cache {}'.format(datetime.now().isoformat()), color='debug')
                cache_to_test.put(i, i)

                self.su.wait_no_snapshots_activity_in_cluster()

            self.su.snapshot_utility('list')

            # Just a check if snapshots were created, try to restore this snapshot
            snapshots = self.su.get_snapshots_from_list_command()
            self.su.snapshot_utility('restore', '-ID={}'.format(snapshots[2]))

        log_print('Done')

