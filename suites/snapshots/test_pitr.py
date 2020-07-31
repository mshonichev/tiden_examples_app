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

from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, \
    create_account_runner_operation, create_transaction_control_operation
from tiden import *
from tiden_gridgain.piclient.piclient import PiClient
from tiden.logger import get_logger
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden_gridgain.utilities.jmx_utility import JmxUtility
from tiden.configuration_decorator import test_configuration
from suites.snapshots.ult_utils import UltimateUtils
from tiden.testconfig import test_config


@test_configuration([
    'zookeeper_enabled',
    'compaction_enabled',
    'sbt_model_enabled',
    'ssl_enabled'
], [
    # Public release
    [False, False, False, False],
    # Sber release
    [False, False, True, False],
    [True, False, True, False],
    [False, True, False, False],
    [True, True, False, False],
])
class TestPitr(UltimateUtils):
    snapshot_storage = None

    setup_testcase = UltimateUtils.setup_testcase
    setup_testcase_without_activation = UltimateUtils.setup_testcase_without_activation
    setup_testcase_without_start_gid = UltimateUtils.setup_testcase_without_start_gid
    setup_shared_storage_test = UltimateUtils.setup_shared_storage_test
    teardown_testcase = UltimateUtils.teardown_testcase
    teardown_shared_storage_test = UltimateUtils.teardown_shared_storage_test

    def setup(self):
        super().setup()
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestPITR]')

        self.jmx = JmxUtility(self.ignite)

    def teardown(self):
        super().teardown()
        self.jmx.kill_utility()

    @require(~test_config.dynamic_cache_enabled)
    @attr('common', 'case_1')
    @test_case_id('79943')
    @with_setup(setup_testcase, teardown_testcase)
    def test_1_pitr_without_snapshot(self):
        """
        PITR without first snapshot.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Remember some time (Restore point) and change some data in the caches.
        3. Restore to the restore point.
        4. Check the data in the caches is the same as on step 2.
        """
        # 2. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # 3. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point, log='out.txt')

        util_sleep_for_a_while(3)

        # 4. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_2', 'smoke')
    @test_case_id('79944')
    @with_setup(setup_testcase, teardown_testcase)
    def test_2_pitr_after_full_snapshot(self):
        """
        PITR after first full snapshot.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Remember some time (Restore point) and change some data in the caches.
        4. Restore to the restore point.
        5. Check the data in the caches is the same as on step 2.
        """
        #  2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=40)

        # 4. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)

        util_sleep_for_a_while(3)

        # 5. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_3')
    @test_case_id('79945')
    @with_setup(setup_testcase, teardown_testcase)
    def test_3_pitr_after_full_snapshot_and_client_node(self):
        """
        PITR after first full snapshot and client node in topology.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Remember some time (Restore point) and change some data in the caches.
        4. Start client node.
        5. Restore to the restore point.
        6. Check the data in the caches is the same as on step 2.
        """
        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # 4. Start client node (random number from 1 to 3).
        with PiClient(self.ignite, self.get_client_config()):
            # 5. Restore to the restore point.
            self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)

            util_sleep_for_a_while(3)

            # 6. Check the data in the caches is the same as on step 2.
            self.util_verify()
            checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
            self.util_print_checksum(checksum_after_restore)
            # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

            # Check that data in the caches is good in general
            self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_4')
    @test_case_id('79947')
    @with_setup(setup_testcase, teardown_testcase)
    def test_4_pitr_empty_wal(self):
        """
        PITR after first full snapshot and additional node in topology (added to BLT).

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Start additional server nodes and add to baseline.
        4. Remember some time (Restore point) and change some data in the caches.
        5. Restore to the restore point.
        6. Check the data in the caches is the same as on step 2.
        """
        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # self.ignite.kill_node(2)
        restore_point_1, checksum_before_restore_1 = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        # self.ignite.start_node(2)
        # 3. Start additional server nodes and add to baseline.
        self.util_start_additional_nodes(add_to_baseline=True)

        restore_point_2 = self.util_get_restore_point(seconds_ago=3)
        checksum_before_restore_2 = self.calc_checksums_distributed(config_file=self.get_client_config())
        # 5. Restore to the restore point.

        for restore_point, checksum in [
            (restore_point_1, checksum_before_restore_1), (restore_point_2, checksum_before_restore_2)
        ]:
            self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)
            util_sleep_for_a_while(10)

            # 6. Check the data in the caches is the same as on step 2.
            self.util_verify()
            checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
            self.util_print_checksum(checksum_after_restore)
            # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

            # Check that data in the caches is good in general
            self.assert_check_sums(checksum, checksum_after_restore)

    @attr('common', 'case_4')
    @test_case_id('79947')
    @with_setup(setup_testcase, teardown_testcase)
    def test_4_pitr_on_changing_topology_add_nodes(self):
        """
        PITR after first full snapshot and additional node in topology (added to BLT).

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Start additional server nodes and add to baseline.
        4. Remember some time (Restore point) and change some data in the caches.
        5. Restore to the restore point.
        6. Check the data in the caches is the same as on step 2.
        """
        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Start additional server nodes and add to baseline.
        self.util_start_additional_nodes(add_to_baseline=True)

        # 3.1. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 4. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # 5. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)
        util_sleep_for_a_while(10)

        # 6. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_4', 'smoke')
    @test_case_id('79946')
    @with_setup(setup_testcase, teardown_testcase)
    def test_4_1_pitr_on_changing_topology_add_nodes(self):
        """
        PITR after first full snapshot and additional node in topology (additional node not in BLT).

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Start additional server nodes and add to baseline.
        4. Start additional server nodes and do not include them to baseline.
        5. Remember some time (Restore point) and change some data in the caches.
        6. Restore to the restore point.
        7. Check the data in the caches is the same as on step 2.
        """
        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Start additional server nodes and add to baseline.
        self.util_start_additional_nodes(add_to_baseline=True)

        # 4. Start additional server nodes and do not include them to baseline.
        self.util_start_additional_nodes()

        # 5. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # 6. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)
        util_sleep_for_a_while(10)

        # 7. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_5')
    @test_case_id('79948')
    @with_setup(setup_testcase, teardown_testcase)
    def test_5_beta_pitr_changing_topology_stop_node(self):
        """
        PITR after first full snapshot and some server nodes stopped.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Stop some server nodes.
        4. Remember some time (Restore point) and change some data in the caches.
        5. Restore to the restore point.
        6. Check the data in the caches is the same as on step 2.
        """
        # Prepare. Add some nodes to be able to stop them later.
        add_nodes = self.util_start_additional_nodes(add_to_baseline=True, nodes_ct=2)

        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Stop some server nodes.
        self.ignite.kill_node(add_nodes[0])

        # 4. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        self.ignite.kill_node(add_nodes[1])

        self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))

        # 6. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)
        util_sleep_for_a_while(10)

        # 7. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_6', 'smoke')
    @test_case_id('79949')
    @with_setup(setup_testcase, teardown_testcase)
    def test_6_pitr_between_snapshots_and_changing_topology(self):
        """
        PITR between snapshots (full then incremental) on changing topology.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Stop some server nodes.
        4. Remember some time (Restore point) and change some data in the caches.
        5. Stop another server node.
        6. Create incremental snapshot.
        7. Restore to the restore point.
        8. Check the data in the caches is the same as on step 2.
        """
        # Prepare. Add some nodes to be able to stop them later.
        add_nodes = self.util_start_additional_nodes(add_to_baseline=True, nodes_ct=2)

        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Stop some server nodes.
        self.ignite.kill_node(add_nodes[0])

        # 4. Remember some time (Restore point) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        self.util_print_checksum(checksum_before_restore)
        util_sleep_for_a_while(3)

        # 5. Stop another server node.
        self.ignite.kill_node(add_nodes[1])
        self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()))

        # 6. Create incremental snapshot.
        self.run_snapshot_utility('snapshot', '-type=inc')

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # 7. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)
        util_sleep_for_a_while(10)

        # 8. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_7', 'smoke')
    @test_case_id('79950')
    @with_setup(setup_testcase, teardown_testcase)
    def test_7_pitr_forward_on_changing_topology(self):
        """
        PITR on several points (point 1, point 2) and changing topology.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Stop some server nodes.
        4. Remember some time (Restore point 1) and change some data in the caches.
        5. Create incremental snapshot.
        6. Start some server nodes.
        7. Remember some time (Restore point 2) and change some data in the caches.
        8. Restore to the restore point 1.
        9. Check the data in the caches is the same as on step 4.
        10. Restore to the restore point 2.
        11. Check the data in the caches is the same as on step 7.
        """
        self.pitr_on_several_points_on_changing_topology()

    @attr('common', 'case_8', 'smoke')
    @test_case_id('79951')
    @with_setup(setup_testcase, teardown_testcase)
    def test_8_pitr_backward_on_changing_topology(self):
        """
        PITR on several points (point 2, point 1) and changing topology.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Stop some server nodes.
        4. Remember some time (Restore point 1) and change some data in the caches.
        5. Create incremental snapshot.
        6. Start some server nodes.
        7. Remember some time (Restore point 2) and change some data in the caches.
        8. Restore to the restore point 2.
        9. Check the data in the caches is the same as on step 7.
        10. Restore to the restore point 1.
        11. Check the data in the caches is the same as on step 4.
        """
        self.pitr_on_several_points_on_changing_topology(forward=False)

    @attr('common', 'case_9')
    @test_case_id('79952')
    @with_setup(setup_testcase, teardown_testcase)
    def test_9_pitr_check_transactions_algorithm(self):
        """
        Test transaction algorithm during PITR (if transaction started and does not finished before restore point, this
        transaction will not be restored).

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Change some data and create checksum dump.
        4. Run some long transactions for some caches.
        5. Remember the time (Restore point) inside the period of running transactions (before transactions
        were committed).
        6. Change the data in the caches.
        7. Restore to the restore point.
        8. Check the data in the caches is the same as on step 2.
        """
        #  2. Create full snapshot.
        restore_point = self.util_run_long_transactions()

        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Remember some time (Restore point) and change some data in the caches.

        if self.get_context_variable('sbt_model_enabled'):
            # add transactional put into 1 cache 2k times total
            PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                               self.ignite,
                                                               self.get_client_config(),
                                                               only_caches_batch=1,
                                                               start_key=int(self.max_key * self.load_multiplier),
                                                               end_key=int(self.max_key * self.load_multiplier) + 1)

            # load normal data into 200 caches 2k times total
            PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                               self.ignite,
                                                               self.get_client_config(),
                                                               only_caches_batch=200,
                                                               start_key=int(self.max_key * self.load_multiplier) + 2,
                                                               end_key=int(5000 * self.load_multiplier))
        else:
            PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                        self.get_client_config(),
                                                        start_key=self.max_key,
                                                        end_key=int(5000 * self.load_multiplier))

        util_sleep_for_a_while(10)
        checksum = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_verify()
        self.util_print_checksum(checksum)
        util_sleep_for_a_while(3)

        restore_point = self.util_run_long_transactions()

        if self.get_context_variable('sbt_model_enabled'):
            # add transactional put into 1 cache 2k times total
            PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                               self.ignite,
                                                               self.get_client_config(),
                                                               only_caches_batch=1,
                                                               start_key=int(5000 * self.load_multiplier),
                                                               end_key=int(5000 * self.load_multiplier) + 1)

            # load normal data into 200 caches 2k times total
            PiClientIgniteUtils.load_data_with_txput_sbt_model(self.config,
                                                               self.ignite,
                                                               self.get_client_config(),
                                                               only_caches_batch=200,
                                                               start_key=int(5000 * self.load_multiplier) + 2,
                                                               end_key=int(7000 * self.load_multiplier))

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=120)

        # 4. Restore to the restore point.
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)

        util_sleep_for_a_while(3)

        # 5. Check the data in the caches is the same as on step 2.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum, checksum_after_restore)

        # Check that data in the caches is good in general
        # assert checksum_after_restore == checksum
        self.assert_check_sums(checksum, checksum_after_restore)

    @attr('common', 'case_10')
    @test_case_id('79941')
    @with_setup(setup_testcase, teardown_testcase)
    def test_10_pitr_negative_case_on_changing_topology(self):
        """
        If number of stopped nodes more than (amount of backups for some cache + 1) then PITR should be failed as
        some data could be lost. If we start one of the stopped nodes then PITR should be completed successfully.

        Steps:
        1. Start cluster and upload some data to the caches (in test setup).
        2. Create full snapshot.
        3. Stop server nodes (amount equals to min amount of backups for partition cache).
        4. Remember some time (Restore point 1) and change some data in the caches.
        5. Stop one more server node.
        6. Restore to the restore point and get error.
        7. Start one server node (which was previously stopped).
        8. Restore to the restore point. This time restore should pass.
        9. Check the data in the caches is the same as on step 4.
        """
        replication_factor = 2
        if self.get_context_variable('sbt_model_enabled'):
            replication_factor = 3
            if len(self.ignite.get_alive_default_nodes()) <= 4:
                raise TidenException('This test with SBT model configuration must be run with more than 4 server nodes')

        #  2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Stop server nodes (amount equals to min amount of backups for partition cache).

        # alive nodes without coordinator
        alive_nodes = [node_id for node_id in self.ignite.get_alive_default_nodes() if node_id != 1]
        killed_nodes = []

        for i in range(0, replication_factor):
            node_id = alive_nodes.pop()
            self.ignite.kill_node(node_id)
            killed_nodes.append(node_id)

        # 4. Remember some time (Restore point 1) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        # 5. Stop one more server node.

        one_more_node = alive_nodes.pop()
        self.ignite.kill_node(one_more_node)

        # 6. Restore to the restore point and get error.
        restore_expected_output = [
            'Command \[RESTORE.*\] started',
            'Error code: 5000.',
            # 'Error code: 5000.* Not enough partitions in current topology to complete restore operation',
            'Command \[RESTORE\] failed with error: 5000 - command failed.']
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point, all_required=restore_expected_output)

        # 7. Start one server node (which was previously stopped).
        util_sleep_for_a_while(3)
        self.ignite.start_node(one_more_node)

        # 8. Restore to the restore point. This time restore should pass.
        util_sleep_for_a_while(10)
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point)

        # 9. Check the data in the caches is the same as on step 4.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        # assert checksum_after_restore == checksum_before_restore
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('common', 'case_11')
    @with_setup(setup_testcase, teardown_testcase)
    def test_11_sbt_fail_on_adding_node(self):
        #  2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Stop server nodes (amount equals to min amount of backups for partition cache).
        self.ignite.kill_node(4)
        self.run_snapshot_utility('snapshot', '-type=full')
        self.delete_lfs(node_ids=[4])
        self.ignite.start_node(4)

        # 4. Remember some time (Restore point 1) and change some data in the caches.
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()
        self.util_verify()

        util_sleep_for_a_while(5)
        self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point, timeout=800)

        # 9. Check the data in the caches is the same as on step 4.
        self.util_verify()
        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        self.util_print_checksum(checksum_after_restore)
        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        # assert checksum_after_restore == checksum_before_restore
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    def util_run_long_transactions(self, timeout=60):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()
            sorted_cache_names = ['cache_group_3_088']
            lrt_operations = []
            log_print('Running transactions started at %s' % datetime.now().strftime("%H:%M %B %d"))
            for cache_name in sorted_cache_names:
                for i in range(1, 10):
                    lrt_operation = create_transaction_control_operation(cache_name, i, i)
                    lrt_operation.evaluate()
                    lrt_operations.append(lrt_operation)

            util_sleep_for_a_while(timeout)
            restore_point = self.util_get_restore_point(seconds_ago=int(timeout / 2))

            for lrt_operation in lrt_operations:
                lrt_operation.releaseTransaction()

        log_print('Running transactions committed at %s. Restore point is %s'
                  % (datetime.now().strftime("%H:%M %B %d"), restore_point))
        self.wait_for_running_clients_num(0, 90)

        return restore_point

    def pitr_on_several_points_on_changing_topology(self, forward=True):
        restore_points = []
        stop_nodes_count = 1

        # 2. Create full snapshot.
        self.run_snapshot_utility('snapshot', '-type=full')

        # 3. Stop some server nodes.
        random_nodes = self.util_get_random_nodes(2, len(self.ignite.get_alive_default_nodes()))
        print_green('Stopping nodes %s' % random_nodes)
        for node in random_nodes:
            self.ignite.kill_node(node)

        self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_all_default_nodes()) +
                                                          len(self.ignite.get_alive_additional_nodes()) -
                                                          len(random_nodes))

        # 4. Remember some time (Restore point) and change some data in the caches.
        restore_points.append(self.sbt_case_upload_data_and_create_restore_point())
        self.util_verify()

        self.ignite.wait_for_topology_snapshot(client_num=0)

        # 6. Create incremental snapshot.
        self.run_snapshot_utility('snapshot', '-type=inc')

        print_green('Starting nodes %s' % random_nodes)
        for node in random_nodes:
            self.ignite.start_node(node)
            util_sleep_for_a_while(3)

        self.util_start_additional_nodes(nodes_ct=stop_nodes_count)
        # self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()) + 1)
        self.ignite.wait_for_topology_snapshot(server_num=len(self.ignite.get_alive_default_nodes()) +
                                                          len(self.ignite.get_alive_additional_nodes()))

        if forward:
            restore_points.append(self.sbt_case_upload_data_and_create_restore_point())
        else:
            restore_points.insert(0, self.sbt_case_upload_data_and_create_restore_point())

        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        for i, (restore_point, checksum) in enumerate(restore_points, start=1):
            self.su.wait_no_snapshots_activity_in_cluster()

            # 7. Restore to the restore point.
            self.run_snapshot_utility('RESTORE', '-to=%s' % restore_point, timeout=600)
            util_sleep_for_a_while(10)
            print_green('Restored to the point {i} timestamp {ts}'.format(i=str(i), ts=restore_point))

            # 8. Check the data in the caches is the same as on step 2.
            self.util_verify()
            checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
            self.util_print_checksum(checksum_after_restore)
            # self.util_compare_dicts(checksum, checksum_after_restore)

            # Check that data in the caches is good in general
            # assert checksum_after_restore == checksum
            self.assert_check_sums(checksum, checksum_after_restore)

    @attr('regression')
    @test_case_id('79953')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_pitr_with_update_same_partition(self):
        """
        This test is based on GG-13632:
        to reproduce this issue you need to load data with the specific distribution. You need to create transactions,
        with multiple keys which mapped to the same partition. As an example, if you have aff function with
        32 partitions, then key 0 will be mapped to partition with index 0, and key 32 will be mapped to the same
        partition, so you need to update both keys in one transaction.
        """
        self.load_data_with_streamer(end_key=1001, allow_overwrite=True)
        checksum_1 = self.calc_checksums_distributed()
        print_green(checksum_1)

        self.run_snapshot_utility('snapshot', '-type=full')

        self.util_run_money_transfer_task(10)

        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache = piclient.get_ignite().getOrCreateCache('cache_group_1_015')
            for i in range(1, 10):
                tx = piclient.get_ignite().transactions().txStart()
                cache.put(1, i)
                cache.put(65, i)
                tx.commit()

            # Check the values
            val_1 = cache.get(1)
            val_2 = cache.get(65)

            log_print("Vals = %s, %s" % (val_1, val_2))

        self.ignite.wait_for_topology_snapshot(client_num=0)

        checksum_before_restore = self.calc_checksums_distributed()
        print_green(checksum_before_restore)
        restore_point = self.util_get_restore_point()

        self.util_run_money_transfer_task(10)
        util_sleep_for_a_while(3)

        # Create another full snapshot to be able to move the previous one.
        self.run_snapshot_utility('snapshot', '-type=full')

        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))

        # PITR
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point, self.snapshot_storage))

        checksum_after_restore = self.calc_checksums_distributed()
        print_green(checksum_after_restore)

        # Check the values
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            cache = piclient.get_ignite().getOrCreateCache('cache_group_1_015')
            tx = piclient.get_ignite().transactions().txStart()
            val_1 = cache.get(1)
            val_2 = cache.get(65)
            tx.commit()
            log_print("Vals = %s, %s" % (val_1, val_2))
        # assert checksum_after_restore == checksum_before_restore
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('regression')
    @test_case_id('79954')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_recovery_not_hangs_during_incorrect_wal_segment_decompression(self):
        """
        Based on IGN-10466
        """
        if not self.get_context_variable('compaction_enabled'):
            from pt.tidenexception import FeatureNotEnabled
            raise FeatureNotEnabled

        # Step 1: Create restore point
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()

        self.util_verify()
        # Step 2: Create full snapshot
        util_sleep_for_a_while(10)
        self.run_snapshot_utility('snapshot', '-type=full')

        # Get snapshot id created during activation
        first_snapshot_id = self.su.get_snapshots_from_list_command()
        print_red(first_snapshot_id)

        # Step 3: Move snapshot to the shared store
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (first_snapshot_id[0], self.snapshot_storage))
        log_print('Snapshot store %s' % self.snapshot_storage)

        # let them go to archive
        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        self.change_wal_segment(self.snapshot_storage)

        # Step 4: Restore to the 'restore point'
        expected = ['Initiate recovery to time...',
                    #  'Error code: 5000. org.apache.ignite.IgniteCheckedException.',
                    'Command \[RESTORE\] failed with error: 5000 - command failed.']
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point, self.snapshot_storage),
                                 all_required=expected)

        util_sleep_for_a_while(3)

        self.run_snapshot_utility('RESTORE', '-id=%s -src=%s' % (first_snapshot_id[0], self.snapshot_storage))
        self.util_verify()

    @attr('shared_folder')
    @test_case_id('79956')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_first_snapshot(self):
        """
        Recovery from shared folder (first snapshot).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Remember time (restore point) and create caches dump.
            4. Change data in the caches.
            5. Create full snapshot.
            6. Move snapshot created during activation to the shared storage.
            7. Restore to the 'restore point' (Step 3).
            8. Dump caches and compare with the dump from Step 3.
        """
        # Step 1: Create restore point
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()

        self.util_verify()
        # Step 2: Create full snapshot
        util_sleep_for_a_while(10)
        self.run_snapshot_utility('snapshot', '-type=full')

        # Get snapshot id created during activation
        first_snapshot_id = self.su.get_snapshots_from_list_command()
        print_red(first_snapshot_id)

        # Step 3: Move snapshot to the shared store
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (first_snapshot_id[0], self.snapshot_storage))
        print('Snapshot store %s' % self.snapshot_storage)

        # let them go to archive
        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # Step 4: Restore to the 'restore point'
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point, self.snapshot_storage), timeout=1200)

        util_sleep_for_a_while(3)
        self.util_verify()

        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore)

        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that data in the caches is good in general
        # assert checksum_after_restore == checksum_before_restore
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('shared_folder')
    @test_case_id('79966')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_without_restart(self):
        """
        Recovery from shared folder (without restart).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point).
            6. Change some data in the caches and create incremental snapshot.
            7. Move snapshot 1 (from Step 3) to the shared storage.
            8. Restore to the 'restore point' (Step 5).
            9. Dump caches and compare with the dump from Step 4.
        """
        # Step 1: Create full snapshot
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 2: Create restore point
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()

        # Step 3: Create another snapshot
        # util_create_snapshot(snapshot_type='INCREMENTAL')
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 4: Move first snapshot to the shared store
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        print('Snapshot store %s' % self.snapshot_storage)

        # let them go to archive
        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # Step 5: Restore to the 'restore point'
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point, self.snapshot_storage))

        self.util_verify()

        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore)

        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)

        # Check that edge transactions produce consistent data
        # assert checksum_after_restore == checksum_before_restore
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)

    @attr('shared_folder')
    @test_case_id('79959')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_grid(self):
        """
        Recovery from shared folder (clean grid, same topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point).
            6. Change some data in the caches and create incremental snapshot.
            7. Move snapshot 1 (from Step 3) to the shared storage.
            8. Stop grid. Cleanup work directory.
            9. Start grid (4 nodes).
            10. Restore to the 'restore point' (Step 5).
            11. Dump caches and compare with the dump from Step 4.
        """
        self.util_recovery_from_shared_folder_on_clean_grid(nodes_count=4)

    @attr('shared_folder')
    @test_case_id('79957')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_bigger_grid(self):
        """
        Recovery from shared folder (clean grid, another bigger topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point).
            6. Change some data in the caches and create incremental snapshot.
            7. Move snapshot 1 (from Step 3) to the shared storage.
            8. Stop grid. Cleanup work directory.
            9. Start grid (5 nodes).
            10. Restore to the 'restore point' (Step 5).
            11. Dump caches and compare with the dump from Step 4.
        """
        self.util_recovery_from_shared_folder_on_clean_grid(nodes_count=5)

    @attr('shared_folder')
    @test_case_id('79964')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_smaller_grid(self):
        """
        Recovery from shared folder (clean grid, another smaller topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point).
            6. Change some data in the caches and create incremental snapshot.
            7. Move snapshot 1 (from Step 3) to the shared storage.
            8. Stop grid. Cleanup work directory.
            9. Start grid (1 node).
            10. Restore to the 'restore point' (Step 5).
            11. Dump caches and compare with the dump from Step 4.
        """
        self.util_recovery_from_shared_folder_on_clean_grid(nodes_count=1)

    @attr('shared_folder')
    @test_case_id('79963')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_grid_with_inc_snapshot(self):
        """
        Recovery from shared folder (clean grid, another same topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point 1).
            6. Change some data in the caches and create incremental snapshot.
            7. Change data in the caches and create caches dump.
            8. Remember time (restore point 2).
            9. Create full snapshot.
            10. Move snapshot 1 (from Step 3) to the shared storage with force flag.
            11. Stop grid. Cleanup work directory.
            12. Start grid (4 node).
            13. Restore to the 'restore point 1' (Step 5).
            14. Dump caches and compare with the dump from Step 4.
            15. Restore to the 'restore point 2' (Step 8).
            16. Dump caches and compare with the dump from Step 7.
        """
        self.util_recovery_from_shared_folder_on_clean_grid_and_inc_snapshot(nodes_count=4)

    @attr('shared_folder')
    @test_case_id('79965')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_smaller_grid_with_inc_snapshot(self):
        """
        Recovery from shared folder (clean grid, another smaller topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point 1).
            6. Change some data in the caches and create incremental snapshot.
            7. Change data in the caches and create caches dump.
            8. Remember time (restore point 2).
            9. Create full snapshot.
            10. Move snapshot 1 (from Step 3) to the shared storage with force flag.
            11. Stop grid. Cleanup work directory.
            12. Start grid (1 node).
            13. Restore to the 'restore point 1' (Step 5).
            14. Dump caches and compare with the dump from Step 4.
            15. Restore to the 'restore point 2' (Step 8).
            16. Dump caches and compare with the dump from Step 7.
        """
        self.util_recovery_from_shared_folder_on_clean_grid_and_inc_snapshot(nodes_count=1)

    @attr('shared_folder', 'current')
    @test_case_id('79958')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_bigger_grid_with_inc_snapshot(self):
        """
        Recovery from shared folder (clean grid, another bigger topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point 1).
            6. Change some data in the caches and create incremental snapshot.
            7. Change data in the caches and create caches dump.
            8. Remember time (restore point 2).
            9. Create full snapshot.
            10. Move snapshot 1 (from Step 3) to the shared storage with force flag.
            11. Stop grid. Cleanup work directory.
            12. Start grid (5 node).
            13. Restore to the 'restore point 1' (Step 5).
            14. Dump caches and compare with the dump from Step 4.
            15. Restore to the 'restore point 2' (Step 8).
            16. Dump caches and compare with the dump from Step 7.
        """
        self.util_recovery_from_shared_folder_on_clean_grid_and_inc_snapshot(nodes_count=5)

    @attr('shared_folder')
    @test_case_id('79960')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_grid_on_two_points(self):
        """
        Recovery from shared folder (clean grid, another same topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point 1).
            6. Change some data in the caches and create FULL snapshot.
            7. Change data in the caches and create caches dump.
            8. Remember time (restore point 2).
            9. Create full snapshot.
            10. Move snapshot 1 (from Step 3) to the shared storage with force flag.
            11. Move snapshot 2 (from Step 6) to the shared storage with force flag.
            12. Stop grid. Cleanup work directory. Start grid (4 node).
            13. Restore to the 'restore point 1' (Step 5).
            14. Dump caches and compare with the dump from Step 4.
            15. Restore to the 'restore point 2' (Step 8).
            16. Dump caches and compare with the dump from Step 7.
        """
        self.util_recovery_from_shared_folder_on_clean_grid_with_two_full_snapshots()

    @attr('shared_folder')
    @test_case_id('79962')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_grid_on_two_points_smaller_grid(self):
        """
        Recovery from shared folder (clean grid, another same topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point 1).
            6. Change some data in the caches and create FULL snapshot.
            7. Change data in the caches and create caches dump.
            8. Remember time (restore point 2).
            9. Create full snapshot.
            10. Move snapshot 1 (from Step 3) to the shared storage with force flag.
            11. Move snapshot 2 (from Step 6) to the shared storage with force flag.
            12. Stop grid. Cleanup work directory. Start grid (1 node).
            13. Restore to the 'restore point 1' (Step 5).
            14. Dump caches and compare with the dump from Step 4.
            15. Restore to the 'restore point 2' (Step 8).
            16. Dump caches and compare with the dump from Step 7.
        """
        self.util_recovery_from_shared_folder_on_clean_grid_with_two_full_snapshots(nodes_count=1)

    @attr('shared_folder')
    @test_case_id('79961')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_recovery_from_shared_folder_on_clean_grid_on_two_points_bigger_grid(self):
        """
        Recovery from shared folder (clean grid, another same topology).
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Change data in the caches and create caches dump.
            5. Remember time (restore point 1).
            6. Change some data in the caches and create FULL snapshot.
            7. Change data in the caches and create caches dump.
            8. Remember time (restore point 2).
            9. Create full snapshot.
            10. Move snapshot 1 (from Step 3) to the shared storage with force flag.
            11. Move snapshot 2 (from Step 6) to the shared storage with force flag.
            12. Stop grid. Cleanup work directory. Start grid (5 nodes).
            13. Restore to the 'restore point 1' (Step 5).
            14. Dump caches and compare with the dump from Step 4.
            15. Restore to the 'restore point 2' (Step 8).
            16. Dump caches and compare with the dump from Step 7.
        """
        self.util_recovery_from_shared_folder_on_clean_grid_with_two_full_snapshots(nodes_count=5)

    @attr('shared_folder')
    @test_case_id('79955')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_sbt_cannot_move_latest_snapshot(self):
        """
        Check we cannot move latest snapshot if PITR is enabled.
            1. Start grid (4 nodes).
            2. Activate grid and upload some data to the caches.
            3. Create full snapshot.
            4. Move full snapshot.
            5. Check that exception is risen and snapshot is not moved.
        """
        # Step 1: Create full snapshot
        util_sleep_for_a_while(2)
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 2: Try to move snapshot to the shared store. we should get exception here.
        snapshot_move_failed = ['Command \[MOVE .*] started...',
                                'Error code: 8000.*\(the snapshot is last\).*',
                                # 'Error code: 8000. failed to MOVE snapshot \(the snapshot is last\).*',
                                'Command \[MOVE\] failed with error: 8000 - command failed']

        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage),
                                 all_required=snapshot_move_failed)

    def util_recovery_from_shared_folder_on_clean_grid(self, nodes_count=4):
        # Step 1: Create full snapshot
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 2: Create restore point
        restore_point, checksum_before_restore = self.sbt_case_upload_data_and_create_restore_point()

        # Step 3: Create another snapshot
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 4: Move first snapshot to the shared store
        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        log_print('Snapshot store %s' % self.snapshot_storage)

        # Step 5: Stop grid, cleanup work dir and start 'clean' grid
        self.restart_empty_grid_with_nodes_count(nodes_count)

        # let them go to archive
        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        self.su.wait_no_snapshots_activity_in_cluster()
        # util_check_activity_in_the_cluster()

        # Step 5: Restore to the 'restore point'
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point, self.snapshot_storage))

        checksum_after_restore = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore)

        # self.util_compare_dicts(checksum_before_restore, checksum_after_restore)
        # Check that edge transactions produce consistent data
        # assert checksum_after_restore == checksum_before_restore
        self.assert_check_sums(checksum_before_restore, checksum_after_restore)
        self.util_verify()

    def util_recovery_from_shared_folder_on_clean_grid_and_inc_snapshot(self, nodes_count=4):
        # Step 1: Create full snapshot
        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 2: Create restore point
        restore_point_1, checksum_before_restore_1 = self.sbt_case_upload_data_and_create_restore_point()

        # Step 3: Create another snapshot
        self.run_snapshot_utility('snapshot', '-type=inc')

        restore_point_2, checksum_before_restore_2 = self.sbt_case_upload_data_and_create_restore_point()

        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 4: Move first snapshot to the shared store
        # snapshot_store = self.su.create_shared_snapshot_storage()
        self.run_snapshot_utility('move', '-id=%s -dest=%s -force' % (self.get_snapshot_id(1), self.snapshot_storage))
        log_print('Snapshot store %s' % self.snapshot_storage)

        # Step 5: Stop grid, cleanup work dir and start 'clean' grid
        self.restart_empty_grid_with_nodes_count(nodes_count)
        util_sleep_for_a_while(20)

        # let them go to archive
        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        self.su.wait_no_snapshots_activity_in_cluster()

        # Step 5: Restore to the 'restore point'
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point_1, self.snapshot_storage))

        util_sleep_for_a_while(15)
        self.util_verify()
        checksum_after_restore_1 = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore_1)

        # self.util_compare_dicts(checksum_before_restore_1, checksum_after_restore_1)
        # Check that edge transactions produce consistent data
        # assert checksum_after_restore_1 == checksum_before_restore_1
        self.assert_check_sums(checksum_before_restore_1, checksum_after_restore_1)

        # Step 5: Restore to the 'restore point'
        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point_2, self.snapshot_storage))
        self.util_verify()

        checksum_after_restore_2 = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore_2)

        # self.util_compare_dicts(checksum_before_restore_2, checksum_after_restore_2)
        # Check that edge transactions produce consistent data
        # assert checksum_after_restore_2 == checksum_before_restore_2
        self.assert_check_sums(checksum_before_restore_2, checksum_after_restore_2)

    def util_recovery_from_shared_folder_on_clean_grid_with_two_full_snapshots(self, nodes_count=4):
        # Step 1: Create full snapshot
        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 2: Create restore point
        restore_point_1, checksum_before_restore_1 = self.sbt_case_upload_data_and_create_restore_point()

        # Step 3: Create another snapshot
        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('snapshot', '-type=full')

        restore_point_2, checksum_before_restore_2 = self.sbt_case_upload_data_and_create_restore_point()

        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('snapshot', '-type=full')

        # Step 4: Move first snapshot to the shared store
        # snapshot_store = self.su.create_shared_snapshot_storage()
        log_print('Snapshot store %s' % self.snapshot_storage)
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(1), self.snapshot_storage))
        self.run_snapshot_utility('move', '-id=%s -dest=%s' % (self.get_snapshot_id(2), self.snapshot_storage))

        # Step 5: Stop grid, cleanup work dir and start 'clean' grid
        self.restart_empty_grid_with_nodes_count(nodes_count)
        util_sleep_for_a_while(20)

        # let them go to archive
        if self.get_context_variable('compaction_enabled') and not self.get_context_variable('sbt_model_enabled'):
            self.util_run_money_transfer_task(time_to_run=240)

        # Step 5: Restore to the 'restore point'
        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point_1, self.snapshot_storage))

        util_sleep_for_a_while(10)
        self.util_verify()

        checksum_after_restore_1 = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore_1)

        # self.util_compare_dicts(checksum_before_restore_1, checksum_after_restore_1)
        # Check that edge transactions produce consistent data
        # assert checksum_after_restore_1 == checksum_before_restore_1
        self.assert_check_sums(checksum_before_restore_1, checksum_after_restore_1)

        # Step 5: Restore to the 'restore point'
        self.su.wait_no_snapshots_activity_in_cluster()
        self.run_snapshot_utility('RESTORE', '-to=%s -src=%s' % (restore_point_2, self.snapshot_storage))
        util_sleep_for_a_while(10)
        self.util_verify()

        # Check that data in the caches is good in general
        checksum_after_restore_2 = self.calc_checksums_distributed(config_file=self.get_client_config())
        print_green(checksum_after_restore_2)

        # self.util_compare_dicts(checksum_before_restore_2, checksum_after_restore_2)
        # Check that edge transactions produce consistent data
        # assert checksum_after_restore_2 == checksum_before_restore_2
        self.assert_check_sums(checksum_before_restore_2, checksum_after_restore_2)

    def change_wal_segment(self, datastorage, node_idx=1):
        log_put("Changing wal segment ... ")

        host = self.ignite.nodes[node_idx]['host']
        consistent_id = self.ignite.get_node_consistent_id(node_idx)
        command = ['ls %s' % datastorage]

        results = self.ssh.exec_on_host(host, command)
        snapshot_folder = results[host][0].split('\n')[0]

        command = ['echo \'\' >  %s/%s/%s/wal/0000000000000001.wal.zip' % (datastorage, snapshot_folder, consistent_id)]
        results = self.ssh.exec_on_host(host, command)
        log_put("Changing done")
        log_print()

