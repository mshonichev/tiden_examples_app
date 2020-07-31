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

from tiden.assertions import tiden_assert_is_none, tiden_assert_equal, tiden_assert_is_not_none
from tiden.util import with_setup, attr, test_case_id
from tiden_gridgain.case.singlegridtestcase import SingleGridTestCase
from tiden_gridgain.piclient.piclient import PiClient


class TestSequences(SingleGridTestCase):

    def setup(self):
        super().setup()

    def setup_testcase(self):
        self.start_grid()
        self.load_data_with_streamer()

    def teardown_testcase(self):
        self.stop_grid()
        self.delete_lfs()

    def teardown(self):
        super().teardown()

    @staticmethod
    def get_or_create_atomic_sequence(piclient, sequence_name, reserve_size=1, create=True):
        gateway = piclient.get_gateway()
        ignite = piclient.get_ignite()

        atomic_cfg = gateway.jvm.org.apache.ignite.configuration.AtomicConfiguration()
        atomic_cfg.setAtomicSequenceReserveSize(reserve_size)

        sequence = ignite.atomicSequence(sequence_name, atomic_cfg, 0, False)
        if sequence is None and create:
            sequence = ignite.atomicSequence(sequence_name, atomic_cfg, 0, True)
        return sequence

    @attr('sequence')
    @test_case_id(153782)
    @with_setup(setup_testcase, teardown_testcase)
    def test_000_sequences_debug_after_shapshot_restore_and_grid_restart(self):

        test_seq_1 = 'myseq1'
        test_seq_2 = 'myseq2'
        with PiClient(self.ignite, self.get_client_config()) as piclient:

            seq1 = self.get_or_create_atomic_sequence(piclient, test_seq_1)

            for i in range(0, 5):
                seq1.incrementAndGet()

            seq1value1 = seq1.get()
            tiden_assert_equal(5, seq1value1, "sequence '%s' value" % test_seq_1)

            # create full snapshot
            full_snapshot_id = self.su.snapshot_utility('snapshot', '-type=full')

            seq2 = self.get_or_create_atomic_sequence(piclient, test_seq_2)

            tiden_assert_equal(0, seq2.get(), "sequence '%s' value" % test_seq_2)

            # increment sequence after snapshot
            for i in range(0, 5):
                seq1.incrementAndGet()

            seq1value2 = seq1.get()
            tiden_assert_equal(10, seq1value2, "sequence '%s' value" % test_seq_1)

            # restore full snapshot
            self.su.snapshot_utility('restore', '-id=%s' % full_snapshot_id)

            seq2 = self.get_or_create_atomic_sequence(piclient, test_seq_2, create=False)
            tiden_assert_is_none(seq2, "after restore from snapshot, sequence '%s'" % test_seq_2)

            seq1 = self.get_or_create_atomic_sequence(piclient, 'myseq1', create=False)
            tiden_assert_is_not_none(seq1, "after restore from snapshot, sequence '%s'" % test_seq_1)

            value = seq1.get()
            tiden_assert_equal(seq1value1, value, "sequence '%s' value restored from snapshot" % test_seq_1)

            seq2 = self.get_or_create_atomic_sequence(piclient, test_seq_2, create=True)

            for i in range(0, 3):
                seq2.incrementAndGet()

            seq2value = seq2.get()
            tiden_assert_equal(3, seq2value, "sequence '%s' value before restart" % test_seq_2)

            self.restart_grid()

            seq2value2 = seq2.get()
            tiden_assert_equal(seq2value, seq2value2, "sequence '%s' value after restart" % test_seq_2)

            seq1value3 = seq1.get()
            tiden_assert_equal(seq1value1, seq1value3, "sequence '%s' value after restart" % test_seq_1)

    @attr('sequence')
    @test_case_id(153783)
    @with_setup(setup_testcase, teardown_testcase)
    def test_client_hashes_sequence_values(self):
        test_seq = 'test_seq'
        with PiClient(self.ignite, self.get_client_config()) as piclient:

            seq = self.get_or_create_atomic_sequence(piclient, 'test_seq')

            seq.incrementAndGet()

            tiden_assert_equal(1, seq.get(), "sequence '%s' value" % test_seq)

            for i in range(1, 5):
                seq.incrementAndGet()

            tiden_assert_equal(5, seq.get(), "sequence '%s' value" % test_seq)
            # create full snapshot
            full_snapshot_id = self.su.snapshot_utility('snapshot', '-type=full')

            # increment sequence after snapshot
            for i in range(1, 5):
                seq.incrementAndGet()

            tiden_assert_equal(9, seq.get(), "sequence '%s' value" % test_seq)

            # restore full snapshot
            self.su.snapshot_utility('restore', '-id=%s' % full_snapshot_id)

        with PiClient(self.ignite, self.get_client_config()) as piclient:

            seq = self.get_or_create_atomic_sequence(piclient, 'test_seq', create=False)
            tiden_assert_is_not_none(seq, "after restore from snapshot, sequence '%s'" % test_seq)

            tiden_assert_equal(6, seq.get(), "sequence '%s' value" % test_seq)

            for i in range(1, 5):
                seq.incrementAndGet()

            tiden_assert_equal(10, seq.get(), "sequence '%s' value" % test_seq)

            self.restart_grid()

            seq = self.get_or_create_atomic_sequence(piclient, 'test_seq', create=False)
            tiden_assert_is_not_none(seq, "after restore from snapshot, sequence '%s'" % test_seq)

            tiden_assert_equal(12, seq.get(), "sequence '%s' value" % test_seq)

    # @attr('original')
    # @test_case_id('79967')
    # @with_setup('setup_testcase', 'teardown_testcase')
    # def test_001_sequences_after_shapshot_restore_and_grid_restart(self):
    #     """
    #     Test that sequences are restored from snapshot and have correct values after grid restart.
    #     """
    #     # inc and get sequence 1
    #     super().set_expecting_value(self.seq_1, 5)
    #     super().inc_and_get_sequence(sequence_name=self.seq_1)
    #
    #     # create full snapshot
    #     super().create_full_snapshot()
    #
    #     # get sequence 2
    #     super().set_expecting_value(self.seq_2, 0)
    #     super().get_sequence(self.seq_2)
    #
    #     seq_1_val = 4
    #     for i in range(1, 3):
    #         seq_1_val += 2
    #         super().set_expecting_value(self.seq_1, seq_1_val)
    #         super().get_sequence(self.seq_1)
    #
    #     # restore full snapshot
    #     super().restore_snapshot()
    #
    #     # check sequences after full restore
    #     super().set_expecting_values({self.seq_1: 6, self.seq_2: 0})
    #     # super().set_expecting_value(self.seq_2, 0)
    #     super().check_sequences()
    #
    #     # restart grid without clean up
    #     super().restart_grid()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 8, self.seq_2: 2})
    #     # super().set_expecting_value(self.seq_1, 8)
    #     # super().set_expecting_value(self.seq_2, 2)
    #     super().check_sequences()
    #
    # @attr('original')
    # @test_case_id('79968')
    # @with_setup('setup_testcase', 'teardown_testcase')
    # def test_002_sequences_after_shapshot_restore_and_grid_restart(self):
    #     """
    #     Test that sequences have correct values after grid restart and snapshot restore after that.
    #     """
    #     # inc and get sequence 1
    #     super().set_expecting_value(self.seq_1, 5)
    #     super().inc_and_get_sequence(sequence_name=self.seq_1)
    #
    #     # create full snapshot
    #     super().create_full_snapshot()
    #
    #     # get sequence 2
    #     super().set_expecting_value(self.seq_2, 0)
    #     super().get_sequence(self.seq_2)
    #
    #     seq_1_val = 4
    #     for i in range(1, 3):
    #         seq_1_val += 2
    #         super().set_expecting_value(self.seq_1, seq_1_val)
    #         super().get_sequence(self.seq_1)
    #
    #     # restart grid without clean up
    #     super().restart_grid()
    #
    #     super().set_expecting_values({self.seq_1: 10, self.seq_2: 2})
    #     super().check_sequences()
    #
    #     super().set_expecting_values({self.seq_1: 12, self.seq_2: 4})
    #     super().check_sequences()
    #
    #     # restore full snapshot
    #     super().restore_snapshot()
    #
    #     # check sequences after full restore
    #     super().set_expecting_values({self.seq_1: 6, self.seq_2: 0})
    #     super().check_sequences()
    #
    # @attr('original')
    # @test_case_id('79969')
    # @with_setup('setup_testcase', 'teardown_testcase')
    # def test_003_sequences_after_full_snapshot_restore_on_clean_grid(self):
    #     """
    #     Test that sequences are restored from snapshot on clean grid.
    #     """
    #     # inc and get sequence 1
    #     super().set_expecting_value(self.seq_1, 5)
    #     super().inc_and_get_sequence(sequence_name=self.seq_1)
    #
    #     # create full snapshot
    #     super().create_full_snapshot()
    #
    #     # get sequence 2
    #     super().set_expecting_value(self.seq_2, 0)
    #     super().get_sequence(self.seq_2)
    #
    #     seq_1_val = 4
    #     for i in range(1, 3):
    #         seq_1_val += 2
    #         super().set_expecting_value(self.seq_1, seq_1_val)
    #         super().get_sequence(self.seq_1)
    #
    #     # restart grid with clean up
    #     super().restart_empty_grid()
    #
    #     super().set_expecting_values({self.seq_1: 0, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restore full snapshot
    #     super().restore_snapshot()
    #
    #     # check sequences after full restore
    #     super().set_expecting_values({self.seq_1: 6, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restart grid without clean up
    #     super().restart_grid()
    #
    #     super().set_expecting_values({self.seq_1: 8, self.seq_2: 2})
    #     super().check_sequences()
    #
    # @attr('original')
    # @test_case_id('79970')
    # @with_setup('setup_testcase', 'teardown_testcase')
    # def test_004_sequences_after_cache_group_restore_from_snapshot(self):
    #     """
    #     Test that sequences are restored from snapshot if only cache group ignite-sys-atomic-cache@default-ds-group
    #     has been restored.
    #     """
    #     # inc and get sequence 1
    #     super().set_expecting_value(self.seq_1, 5)
    #     super().inc_and_get_sequence(sequence_name=self.seq_1)
    #
    #     # create snapshot for one cache
    #     super().create_particular_cache_snapshot('ignite-sys-atomic-cache@default-ds-group')
    #
    #     super().set_expecting_value(self.seq_2, 0)
    #     super().get_sequence(self.seq_2)
    #
    #     seq_1_val = 4
    #     for i in range(1, 3):
    #         seq_1_val += 2
    #         super().set_expecting_value(self.seq_1, seq_1_val)
    #         super().get_sequence(self.seq_1)
    #
    #     # restore snapshot
    #     super().restore_snapshot()
    #
    #     # check sequences after restore
    #     super().set_expecting_values({self.seq_1: 6, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restart grid with clean up
    #     super().restart_grid()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 8, self.seq_2: 2})
    #     super().check_sequences()
    #
    # @attr('original')
    # @test_case_id('79971')
    # @with_setup('setup_testcase', 'teardown_testcase')
    # def test_005_sequences_after_cache_group_restore_from_snapshot_and_grid_restart(self):
    #     """
    #     Test that sequences are restored from snapshot if only cache group ignite-sys-atomic-cache@default-ds-group
    #     has been restored and keep their values after grid restart.
    #     """
    #     # inc and get sequence 1
    #     super().set_expecting_value(self.seq_1, 5)
    #     super().inc_and_get_sequence(sequence_name=self.seq_1)
    #
    #     # create snapshot for one cache
    #     super().create_particular_cache_snapshot('ignite-sys-atomic-cache@default-ds-group')
    #
    #     super().set_expecting_value(self.seq_2, 0)
    #     super().get_sequence(self.seq_2)
    #
    #     seq_1_val = 4
    #     for i in range(1, 3):
    #         seq_1_val += 2
    #         super().set_expecting_value(self.seq_1, seq_1_val)
    #         super().get_sequence(self.seq_1)
    #
    #     # restore snapshot
    #     super().restore_snapshot()
    #
    #     # check sequences after restore
    #     super().set_expecting_values({self.seq_1: 6, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restart grid with clean up
    #     super().restart_empty_grid()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 0, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restore snapshot
    #     super().restore_snapshot()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 6, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restart grid with clean up
    #     super().restart_grid()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 8, self.seq_2: 2})
    #     super().check_sequences()
    #
    # @attr('original')
    # @test_case_id('79972')
    # @with_setup('setup_testcase', 'teardown_testcase')
    # def test_006_sequences_after_another_cache_group_restore_from_snapshot_and_grid_restart(self):
    #     """
    #     Test that sequences keep their values if some cache groups are restored from snapshot and grid restarts
    #     after that.
    #     """
    #     # inc and get sequence 1
    #     super().set_expecting_value(self.seq_1, 5)
    #     super().inc_and_get_sequence(sequence_name=self.seq_1)
    #
    #     # create snapshot for one cache
    #     super().create_full_snapshot()
    #
    #     super().set_expecting_value(self.seq_2, 0)
    #     super().get_sequence(self.seq_2)
    #
    #     seq_1_val = 4
    #     for i in range(1, 3):
    #         seq_1_val += 2
    #         super().set_expecting_value(self.seq_1, seq_1_val)
    #         super().get_sequence(self.seq_1)
    #
    #     # restore snapshot
    #     super().restore_caches_from_snapshot('cache_group_1_0001,cache_group_1_0002')
    #
    #     # check sequences after restore
    #     super().set_expecting_values({self.seq_1: 10, self.seq_2: 2})
    #     super().check_sequences()
    #
    #     # restart grid with clean up
    #     super().restart_empty_grid()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 0, self.seq_2: 0})
    #     super().check_sequences()
    #
    #     # restore snapshot
    #     super().restore_caches_from_snapshot('cache_group_1_0001,cache_group_1_0002')
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 2, self.seq_2: 2})
    #     super().check_sequences()
    #
    #     # restart grid with clean up
    #     super().restart_grid()
    #
    #     # check sequences after restart
    #     super().set_expecting_values({self.seq_1: 4, self.seq_2: 4})
    #     super().check_sequences()

