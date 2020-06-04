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

from time import sleep

from tiden import require, log_put, print_blue
from tiden_gridgain.piclient.helper.cache_utils import IgniteCache, IgniteCacheConfig
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_transaction_control_operation, TxDescriptor
from tiden_gridgain.piclient.piclient import PiClient, get_gateway
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.case.singlegridtestcase import get_logger, print_green, attr, test_case_id, \
    with_setup, log_print, TidenException, known_issue
from tiden_gridgain.case.singlegridzootestcase import SingleGridZooTestCase
from tiden.util import is_enabled, version_num, util_sleep_for_a_while

LRT_TIMEOUT = 10
CACHE_NAME = 'lrt_cache'


class TestLrt(SingleGridZooTestCase):

    def setup(self):
        default_context = self.contexts['default']
        default_context.add_context_variables(
            snapshots_enabled=True,
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            tx_timeout_on_pme=False,
        )

        tx_pme_context = self.create_test_context('tx_timeout_on_pme')
        tx_pme_context.add_context_variables(
            snapshots_enabled=True,
            pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            tx_timeout_on_pme=True,
        )

        super().setup()

        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestLrt]')

    def setup_testcase(self):
        self.logger.info('TestSetup is called')
        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()

        self.ignite.set_activation_timeout(20)
        self.ignite.set_snapshot_timeout(60)
        self.ignite.set_node_option(
            '*',
            'jvm_options',
            [
                '-ea',
            ]
        )
        self.su.clear_snapshots_list()
        self.ignite.start_nodes()
        self.cu.control_utility('--activate', '--user ignite --password ignite')
        self.load_data_with_streamer(
            start_key=0,
            end_key=1001,
            value_type=ModelTypes.VALUE_ACCOUNT.value,
        )
        self.ignite.jmx.start_utility()
        log_print(repr(self.ignite), color='debug')

    def teardown_testcase(self):
        self.logger.info('TestTeardown is called')
        if self.ignite.jmx.is_started():
            self.ignite.jmx.kill_utility()

        self.ignite.set_activation_timeout(10)
        self.ignite.set_snapshot_timeout(30)
        self.stop_grid_hard()
        self.cleanup_lfs()

        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()

        self.su.copy_utility_log()
        self.reset_cluster()
        log_print(repr(self.ignite), color='debug')

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62957)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_1_utility_output_format(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()

                self.assert_utility_output()

                for operation in lrt_operations.values():
                    transaction_xid = operation.getTransaction().xid().toString()
                    assert transaction_xid in self.cu.latest_utility_output, \
                        'Transaction with XID %s is not in list' % transaction_xid

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62958)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_2_utility_filter(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                tx_min_duration = int(LRT_TIMEOUT * 1.5)
                tx_min_size = 10

                self.cu.list_transactions(min_duration=str(tx_min_duration), order='DURATION')
                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                # no transactions on node1
                self.cu.list_transactions(nodes=self.ignite.get_node_consistent_id(1), order='DURATION')

                assert 'Error' not in self.cu.latest_utility_output, \
                    'Wrong transaction utility output: consistentId filter does not work'

                # no transactions on servers
                self.cu.list_transactions(servers=True, order='DURATION')
                assert 'Error' not in self.cu.latest_utility_output, \
                    'Wrong transaction utility output: servers/clients filter does not work'

                # empty values minSize 10 (all transactions with size 1
                self.cu.list_transactions(min_size=tx_min_size, order='SIZE')
                assert 'Error' not in self.cu.latest_utility_output, \
                    'Wrong transaction utility output: minSize does not work'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62959)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_3_utility_filter_label(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.cu.list_transactions(label='tx_.')
                self.assert_utility_output()
                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62960)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_4_transactions_list_killing_single_transaction(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            print_blue('Cache size %s' % cache.size('all'))

            assert cache.get(0, key_type='long') is None, 'Value for non-exists transaction exists: %s' % \
                                                          cache.get(0, key_type='long')

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()
                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                transaction_xid = lrt_operations[0].getTransaction().xid().toString()
                transaction_key = lrt_operations[0].getKey()
                transaction_value = lrt_operations[0].getValue()

                self.utility_drop_transaction(transaction_xid)
                print_blue(
                    'Removing transaction from monitoring list: %s with key, value: %s'
                    % (transaction_xid, transaction_key))
                lrt_operations.pop(0, None)

                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                assert transaction_xid not in self.cu.latest_utility_output, 'Transaction XID still exists in output'

                for lrt_operation in lrt_operations.values():
                    assert lrt_operation.getTransaction().xid().toString() in self.cu.latest_utility_output, \
                        'Transaction info about alive transaction does not exists in output'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                print_blue('Cache size %s' % cache.size('all'))

                assert cache.get(transaction_key, key_type='long') is None, \
                    'Value for removed transaction exists for tx{key:%s value:%s}: %s' % \
                    (cache.get(transaction_key, key_type='long'), transaction_key, transaction_value)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62961)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_5_all_nodes_transactions(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                for server_node in self.ignite.get_alive_default_nodes():
                    self.cu.control_utility('--tx', node=server_node)
                    self.assert_utility_output()

                    for lrt_operation in lrt_operations.values():
                        if lrt_operation.getTransaction().xid().toString() not in self.cu.latest_utility_output:
                            raise TidenException('Wrong transaction utility output')

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62962)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_6_wrong_transaction_xid(self):
        expected = 'Nothing found'

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.kill_transactions(xid='WRONG_XID')

                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62963)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_7_jmx(self):
        with PiClient(self.ignite, self.get_client_config()):

            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                out = self.ignite.jmx.evaluate_operation(
                    1, 'Transactions', 'TransactionsMXBeanImpl',
                    'getActiveTransactions', '0', '0', 'null', 'null', 'null',
                    'null', '20', 'DURATION', 'true', 'false')

                print_blue(out)

                for lrt_operation in lrt_operations.values():
                    assert lrt_operation.getTransaction().xid().toString() in out, \
                        'Unable to find transaction in JMX output'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)

            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi', 'smoke')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62964)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_8_stop_one_transaction_from_list(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            print_blue('Cache size %s' % cache.size('all'))

            assert cache.get(0, key_type='long') is None, 'Value for non-exists transaction exists: %s' % \
                                                          cache.get(0, key_type='long')

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()
                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                transaction_xid = lrt_operations[0].getTransaction().xid().toString()
                self.utility_drop_transaction(transaction_xid)
                transaction_key = lrt_operations[0].getKey()
                transaction_value = lrt_operations[0].getValue()

                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                assert transaction_xid not in self.cu.latest_utility_output, 'Transaction XID still exists in output'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                print_blue('Cache size %s' % cache.size('all'))

                assert cache.get(transaction_key, key_type='long') is None, \
                    'Value for removed transaction exists for tx{key:%s value:%s}: %s' % \
                    (cache.get(transaction_key, key_type='long'), transaction_key, transaction_value)

                self.wait_for_transaction_timeout()

                print_blue(
                    'Removing transaction from monitoring list: %s with key, value: %s'
                    % (transaction_xid, 0))
                lrt_operations.pop(0, None)
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi', 'smoke')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62965)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_9_stop_few_transaction_from_list(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            print_blue('Cache size %s' % cache.size('all'))

            assert cache.get(0, key_type='long') is None, 'Value for non-exists transaction exists: %s' % \
                                                          cache.get(0, key_type='long')

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()
                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                killed_xids = []
                transaction_keys = []

                # kill 0,1,2 transactions
                for i in range(0, 3):
                    transaction_xid = lrt_operations[i].getTransaction().xid().toString()
                    killed_xids.append(transaction_xid)
                    transaction_keys.append(lrt_operations[i].getKey())
                    self.utility_drop_transaction(transaction_xid)

                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                for transaction_xid in killed_xids:
                    assert transaction_xid not in self.cu.latest_utility_output, \
                        'Transaction XID still exists in output'

                for i in range(0, 3):
                    print_blue(
                        'Removing transaction from monitoring list: %s with key: %s, value: %s'
                        % (lrt_operations[i].getTransaction().xid().toString(),
                           lrt_operations[i].getKey(),
                           lrt_operations[i].getValue()))
                    lrt_operations.pop(i, None)

                for lrt_operation in lrt_operations.values():
                    assert lrt_operation.getTransaction().xid().toString() in self.cu.latest_utility_output, \
                        'Transaction info about alive transaction does not exists in output'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                print_blue('Cache size %s' % cache.size('all'))

                for transaction_key in transaction_keys:
                    assert cache.get(transaction_key,
                                     key_type='long') is None, 'Value for removed transaction exists: %s' % \
                                                               cache.get(transaction_key, key_type='long')

                self.wait_for_transaction_timeout()

                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62984)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_10_stop_few_transactions_using_filter(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            print_blue('Cache size %s' % cache.size('all'))

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()
                if expected not in self.cu.latest_utility_output:
                    raise TidenException('Wrong transaction utility output')

                self.cu.kill_transactions(label='tx_.', force=True)
                self.assert_utility_output()

                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()
                for transaction in lrt_operations.values():
                    print_green('Transaction {key:%s value:%s} should be None' %
                                (transaction.getKey(), transaction.getValue()))
                    assert transaction.getTransaction().xid().toString() not in self.cu.latest_utility_output
                    assert cache.get(transaction.getKey(), key_type='long') is None, \
                        'Unexpected data in cache'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                print_blue('Cache size %s' % cache.size('all'))

                assert cache.size('all') == 0, 'Cache is not empty'

                self.wait_for_transaction_timeout()
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(80011)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_11_not_stop_few_transactions_using_wrong_filter(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            print_blue('Cache size %s' % cache.size('all'))

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()

                self.cu.kill_transactions(label='UNKNOWN_TRANSACTION_ID', force=True)
                self.assert_utility_output()

                self.wait_for_transaction_timeout()

                self.cu.list_transactions()
                self.assert_utility_output()
                for transaction in lrt_operations.values():
                    print_green('Transaction {key:%s value:%s} should be None' %
                                (transaction.getKey(), transaction.getValue()))
                    assert transaction.getTransaction().xid().toString() in self.cu.latest_utility_output
                    assert cache.get(transaction.getKey(), key_type='long') is None, \
                        'Unexpected data in cache'

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                print_blue('Cache size %s' % cache.size('all'))

                self.wait_for_transaction_timeout()
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    def do_lrt_12_incorrect_filter(self, expected):
        cache = self.create_transactional_cache()

        print_blue('Cache size %s' % cache.size('all'))

        lrt_operations = self.launch_transaction_operations()

        try:
            self.wait_for_transaction_timeout()

            self.cu.list_transactions()
            self.assert_utility_output()
            if expected not in self.cu.latest_utility_output:
                raise TidenException('Wrong transaction utility output')

            kill_force = self.cu.get_kill_subcommand() + ' ' + self.cu.get_force_attr('tx')
            self.cu.control_utility('--tx', 'labFl tx ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', 'minDublin 0 ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', '--lael 0 ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', 'minSids 10 ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', 'sereIrs ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', 'lim1t 5 ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', 'odder SIZE ' + kill_force)
            assert 'Unexpected argument' in self.cu.latest_utility_output

            self.cu.control_utility('--tx', self.cu.get_command('tx', 'order') + ' WTF ' + kill_force)
            assert 'finished with code: 1' in self.cu.latest_utility_output

            self.wait_for_transaction_timeout()

            self.cu.list_transactions()
            self.assert_utility_output()
            for transaction in lrt_operations.values():
                print_green('Transaction {key:%s value:%s} should be None' %
                            (transaction.getKey(), transaction.getValue()))
                assert transaction.getTransaction().xid().toString() in self.cu.latest_utility_output
                assert cache.get(transaction.getKey(), key_type='long') is None, \
                    'Unexpected data in cache'

            self.release_transactions(lrt_operations)

            self.wait_for_transaction_timeout()
            print_blue('Cache size %s' % cache.size('all'))

            assert cache.size('all') != 0, 'Cache is not empty'

            self.wait_for_transaction_timeout()
        except TidenException as e:
            raise e
        finally:
            self.print_values_in_cache(lrt_operations, cache)

    @attr('lrt', 'pmi')
    @require(min_ignite_version='2.5.1')
    @test_case_id(80012)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_12_incorrect_filter(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):

            self.do_lrt_12_incorrect_filter(expected)

    @attr('pme', 'smoke')
    @require(min_ignite_version='2.5.1')
    @test_case_id(62966)
    def test_pmi_pme_1_stop_transaction_while_snapshot(self):
        try:
            self.set_current_context('tx_timeout_on_pme')

            self.setup_testcase()

            with PiClient(self.ignite, self.get_client_config()):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.su.snapshot_utility('snapshot', '-type=full', background=True, log='su.out')

                    self.su.wait_no_snapshots_activity_in_cluster(30)

                    self.release_transactions(lrt_operations)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)
        except Exception as e:
            raise e
        finally:
            self.set_current_context()

            self.teardown_testcase()

    @attr('pme', 'pmi')
    @require(min_ignite_version='2.5.1-p3')
    @test_case_id(62967)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_pme_2_stop_transaction_while_snapshot_dynamic_timeout_before(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                print_green('Enable big timeout')

                self.ignite.jmx.get_attributes(
                    1,
                    'Transactions',
                    'TransactionsMXBeanImpl',
                    'TxTimeoutOnPartitionMapExchange(0)'
                )

                self.su.snapshot_utility('snapshot', '-type=full', background=True, log='snapshot-out.log')

                print_green('Disable big timeout')

                # set attribute TxTimeoutOnPartitionMapExchange
                self.ignite.jmx.get_attributes(
                    1,
                    'Transactions',
                    'TransactionsMXBeanImpl',
                    'TxTimeoutOnPartitionMapExchange(10000)'
                )

                out = self.ignite.jmx.evaluate_operation(
                    1, 'Transactions', 'TransactionsMXBeanImpl',
                    'getActiveTransactions', '0', '0', 'null', 'null', 'null',
                    'null', '20', 'DURATION', 'true', 'true')

                print_green(out)

                # Wait for ~1 minute
                self.su.wait_no_snapshots_activity_in_cluster(30)

                self.release_transactions(lrt_operations)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81383)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_1_utility_output_format_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()

                    self.assert_utility_output()

                    for operation in lrt_operations.values():
                        transaction_xid = operation.getTransaction().xid().toString()
                        assert transaction_xid in self.cu.latest_utility_output, \
                            'Transaction with XID %s is not in list' % transaction_xid

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81384)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_2_utility_filter_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    tx_min_duration = int(LRT_TIMEOUT * 1.5)
                    tx_min_size = 10
                    '''
                    --tx 
                     [xid XID]
                     [minDuration SECONDS] 
                     [minSize SIZE]
                     [label PATTERN_REGEX] 
                     [servers|clients] 
                     [nodes consistentId1[,consistentId2,....,consistentIdN] 
                     [limit NUMBER] 
                     [order DURATION|SIZE] 
                     [kill] 
                     [--yes]
                    '''
                    self.cu.list_transactions(min_duration=tx_min_duration, min_size=tx_min_size, order='SIZE')
                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81385)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_3_utility_filter_label_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions(label='tx_.')
                    self.assert_utility_output()
                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81386)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_4_transactions_list_killing_single_transaction_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                print_blue('Cache size %s' % cache.size('all'))

                assert cache.get(0, key_type='long') is None, 'Value for non-exists transaction exists: %s' % \
                                                              cache.get(0, key_type='long')

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()
                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    transaction_xid = lrt_operations[0].getTransaction().xid().toString()
                    transaction_key = lrt_operations[0].getKey()
                    transaction_value = lrt_operations[0].getValue()

                    self.utility_drop_transaction(transaction_xid)
                    print_blue(
                        'Removing transaction from monitoring list: %s with key, value: %s'
                        % (transaction_xid, transaction_key))
                    lrt_operations.pop(0, None)

                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    assert transaction_xid not in self.cu.latest_utility_output, \
                        'Transaction XID "%s" still exists in output' % transaction_xid

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    print_blue('Cache size %s' % cache.size('all'))

                    assert cache.get(transaction_key, key_type='long') is None, \
                        'Value for removed transaction exists for tx{key:%s value:%s}: %s' % \
                        (cache.get(transaction_key, key_type='long'), transaction_key, transaction_value)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81387)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_5_all_nodes_transactions_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    for server_node in self.ignite.get_alive_default_nodes():
                        self.cu.control_utility('--tx', node=server_node)
                        self.assert_utility_output()

                        for lrt_operation in lrt_operations.values():
                            if lrt_operation.getTransaction().xid().toString() not in self.cu.latest_utility_output:
                                raise TidenException('Wrong transaction utility output')

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81388)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_6_wrong_transaction_xid_loading(self):
        expected = 'Nothing found.'

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.kill_transactions(xid='WRONG_XID')

                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81389)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_7_jmx_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    out = self.ignite.jmx.evaluate_operation(
                        1, 'Transactions', 'TransactionsMXBeanImpl',
                        'getActiveTransactions', '0', '0', 'null', 'null', 'null',
                        'null', '20', 'DURATION', 'true', 'false')

                    print_blue(out)

                    for lrt_operation in lrt_operations.values():
                        assert lrt_operation.getTransaction().xid().toString() in out, \
                            'Unable to find transaction in JMX output'

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)

                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81390)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_8_stop_one_transaction_from_list_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                print_blue('Cache size %s' % cache.size('all'))

                assert cache.get(0, key_type='long') is None, 'Value for non-exists transaction exists: %s' % \
                                                              cache.get(0, key_type='long')

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()
                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    transaction_xid = lrt_operations[0].getTransaction().xid().toString()
                    self.utility_drop_transaction(transaction_xid)
                    transaction_key = lrt_operations[0].getKey()
                    transaction_value = lrt_operations[0].getValue()

                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    assert transaction_xid not in self.cu.latest_utility_output, \
                        'Transaction XID still exists in output'

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    print_blue('Cache size %s' % cache.size('all'))

                    assert cache.get(transaction_key, key_type='long') is None, \
                        'Value for removed transaction exists for tx{key:%s value:%s}: %s' % \
                        (cache.get(transaction_key, key_type='long'), transaction_key, transaction_value)

                    self.wait_for_transaction_timeout()

                    print_blue(
                        'Removing transaction from monitoring list: %s with key, value: %s'
                        % (transaction_xid, 0))
                    lrt_operations.pop(0, None)
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81391)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_9_stop_few_transaction_from_list_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):

                cache = self.create_transactional_cache()

                print_blue('Cache size %s' % cache.size('all'))

                assert cache.get(0, key_type='long') is None, 'Value for non-exists transaction exists: %s' % \
                                                              cache.get(0, key_type='long')

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()
                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    killed_xids = []
                    transaction_keys = []

                    # kill 0,1,2 transactions
                    for i in range(0, 3):
                        transaction_xid = lrt_operations[i].getTransaction().xid().toString()
                        killed_xids.append(transaction_xid)
                        transaction_keys.append(lrt_operations[i].getKey())
                        self.utility_drop_transaction(transaction_xid)

                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    for transaction_xid in killed_xids:
                        assert transaction_xid not in self.cu.latest_utility_output, \
                            'Transaction XID still exists in output'

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    print_blue('Cache size %s' % cache.size('all'))

                    for transaction_key in transaction_keys:
                        assert cache.get(transaction_key,
                                         key_type='long') is None, 'Value for removed transaction exists: %s' % \
                                                                   cache.get(transaction_key, key_type='long')

                    for i in range(0, 3):
                        print_blue(
                            'Removing transaction from monitoring list: %s with key: %s, value: %s'
                            % (lrt_operations[i].getTransaction().xid().toString(),
                               lrt_operations[i].getKey(),
                               lrt_operations[i].getValue()))
                        lrt_operations.pop(i, None)

                    self.wait_for_transaction_timeout()

                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1-p2')
    @test_case_id(81392)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_10_stop_few_transactions_using_filter_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config(), nodes_num=2):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=10),
                                      post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                print_blue('Cache size %s' % cache.size('all'))

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()
                    if expected not in self.cu.latest_utility_output:
                        raise TidenException('Wrong transaction utility output')

                    self.cu.kill_transactions(label='tx_.', force=True)
                    self.assert_utility_output()

                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()
                    for transaction in lrt_operations.values():
                        print_green('Transaction {key:%s value:%s} should be None' %
                                    (transaction.getKey(), transaction.getValue()))
                        assert transaction.getTransaction().xid().toString() not in self.cu.latest_utility_output
                        assert cache.get(transaction.getKey(), key_type='long') is None, \
                            'Unexpected data in cache'

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    print_blue('Cache size %s' % cache.size('all'))

                    assert cache.size('all') == 0, 'Cache is not empty'

                    self.wait_for_transaction_timeout()
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81393)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_11_not_stop_few_transactions_using_wrong_filter_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=10),
                                      post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()

                print_blue('Cache size %s' % cache.size('all'))

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()

                    self.cu.kill_transactions(label='UNKNOWN_TRANSACTION_ID', force=True)
                    self.assert_utility_output()

                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()
                    self.assert_utility_output()
                    for transaction in lrt_operations.values():
                        print_green('Transaction {key:%s value:%s} should be None' %
                                    (transaction.getKey(), transaction.getValue()))
                        assert transaction.getTransaction().xid().toString() in self.cu.latest_utility_output
                        assert cache.get(transaction.getKey(), key_type='long') is None, \
                            'Unexpected data in cache'

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    print_blue('Cache size %s' % cache.size('all'))

                    self.wait_for_transaction_timeout()
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('loading')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81394)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_12_incorrect_filter_loading(self):
        expected = ''

        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(delay=10),
                                      post_checksum_action=self.additional_loading_actions):

                self.do_lrt_12_incorrect_filter(expected)

    @attr('other')
    @require(min_ignite_version='2.5.1-p7')
    @test_case_id(81397)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_utility_output_format_additional_node(self):
        # IGNITE-8476
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            self.ignite.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config(), 1),
                                               skip_nodes_check=True)

            try:
                self.wait_for_transaction_timeout()

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()
                self.validate_expected_output(lrt_operations, cache)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('other')
    @require(min_ignite_version='2.5.1-p3')
    @test_case_id(81398)
    @with_setup(setup_testcase, teardown_testcase)
    def test_lrt_utility_output_format_additional_node_loading(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                cache = self.create_transactional_cache()
                lrt_operations = self.launch_transaction_operations()

                self.ignite.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config(), 1),
                                                   skip_nodes_check=True)

                try:
                    self.wait_for_transaction_timeout()

                    self.cu.list_transactions()

                    self.assert_utility_output()

                    for operation in lrt_operations.values():
                        transaction_xid = operation.getTransaction().xid().toString()
                        assert transaction_xid in self.cu.latest_utility_output, \
                            'Transaction with XID %s is not in list' % transaction_xid

                    self.release_transactions(lrt_operations)

                    self.wait_for_transaction_timeout()
                    self.validate_expected_output(lrt_operations, cache)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)

    @attr('other')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81400)
    @with_setup(setup_testcase, teardown_testcase)
    def test_spreading_tx_timeout_through_cluster(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                self.su.snapshot_utility('snapshot', '-type=full', background=True, log='snapshot-out.log')

                # set attribute TxTimeoutOnPartitionMapExchange
                self.ignite.jmx.get_attributes(
                    1,
                    'Transactions',
                    'TransactionsMXBeanImpl',
                    'TxTimeoutOnPartitionMapExchange(9999)'
                )

                sleep(10)
                # attribute spreads in all nodes in cluster
                for node_id in self.ignite.get_alive_default_nodes():
                    attribute_value = \
                        self.ignite.jmx.get_attributes(
                            node_id,
                            'Transactions',
                            'TransactionsMXBeanImpl',
                            'TxTimeoutOnPartitionMapExchange'
                        )['TxTimeoutOnPartitionMapExchange']
                    assert '9999' in attribute_value, \
                        'Value of attribute does not match 9999: %s, value: %s' % (node_id, attribute_value)

                # Wait for ~1 minute
                self.su.wait_no_snapshots_activity_in_cluster(30)

                self.release_transactions(lrt_operations)

            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('other')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81401)
    def test_tx_utility_output_while_snapshot_and_tx_pme_timeout(self):
        try:
            self.set_current_context('tx_timeout_on_pme')

            self.setup_testcase()

            with PiClient(self.ignite, self.get_client_config()):
                cache = self.create_transactional_cache()

                lrt_operations = self.launch_transaction_operations()

                try:
                    self.wait_for_transaction_timeout()

                    self.su.snapshot_utility('snapshot', '-type=full', background=True, log='log.txt')

                    self.cu.list_transactions()
                    self.cu.list_transactions()
                    self.cu.list_transactions()

                    self.su.wait_no_snapshots_activity_in_cluster(30)

                    self.release_transactions(lrt_operations)
                except TidenException as e:
                    raise e
                finally:
                    self.print_values_in_cache(lrt_operations, cache)
        except Exception as e:
            raise e
        finally:
            self.set_current_context()

            self.teardown_testcase()

    @attr('other')
    @require(min_ignite_version='2.5.1-p10')
    @test_case_id(81395)
    def test_reduce_config_defined_tx_timeout_value(self):
        # TODO https://ggsystems.atlassian.net/browse/GG-13802
        try:
            self.set_current_context('tx_timeout_on_pme')

            self.setup_testcase()

            with PiClient(self.ignite, self.get_client_config()):
                with TransactionalLoading(self, post_checksum_action=self.additional_loading_actions):
                    cache = self.create_transactional_cache()

                    lrt_operations = self.launch_transaction_operations()

                    try:
                        self.wait_for_transaction_timeout()

                        log_print("Start additional nodes")
                        self.ignite.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config()),
                                                           skip_nodes_check=True)

                        # self.su.snapshot_utility('snapshot', '-type=full', background=True, log='snapshot-out.log')

                        # set attribute TxTimeoutOnPartitionMapExchange
                        log_print("Set timeout to 10 seconds")
                        self.ignite.jmx.get_attributes(
                            1,
                            'Transactions',
                            'TransactionsMXBeanImpl',
                            'TxTimeoutOnPartitionMapExchange(10000)'
                        )

                        # Wait for ~1 minute
                        log_print("Sleep for 120 seconds")
                        sleep(120)

                        # self.su.wait_no_snapshots_activity_in_cluster(30)

                        self.release_transactions(lrt_operations)
                    except TidenException as e:
                        raise e
                    finally:
                        self.print_values_in_cache(lrt_operations, cache)
        except Exception as e:
            raise e
        finally:
            self.set_current_context()

            self.teardown_testcase()

    @attr('other')
    @require(min_ignite_version='2.5.1-p5')
    @test_case_id(167651)
    @with_setup(setup_testcase, teardown_testcase)
    @known_issue('IGN-11586')
    def test_blink_node_kill_transactions(self):
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self,
                                      loading_profile=LoadingProfile(transaction_timeout=50)):
                self.wait_for_transaction_timeout()

                for _ in range(0, 10):
                    log_print("Kill node")
                    self.ignite.kill_node(2)
                    log_print("Sleep")
                    sleep(10)
                    log_print("Start node")
                    self.ignite.start_node(2)

                    self.cu.kill_transactions(force=True)

                log_print("Sleep for 5 seconds")
                sleep(5)

                self.cu.list_transactions()
                log_print("Sleep for 120 seconds")
                sleep(120)

                self.cu.list_transactions()
                self.cu.list_transactions()
                self.cu.list_transactions()

                self.verify_no_assertion_errors()

    @attr('other')
    @require(min_ignite_version='2.5.5')
    # @test_case_id(1)
    @with_setup(setup_testcase, teardown_testcase)
    # @known_issue('IGN-12281')
    def test_blink_node_rollback_transactions(self):

        for i in range(0, 2):
            with PiClient(self.ignite, self.get_client_config()):
                ai_ver = self.config['artifacts']['ignite']['ignite_version']
                if version_num(ai_ver) >= version_num("2.7"):
                    atomicity_mode = 'transactional_snapshot'
                else:
                    atomicity_mode = 'transactional'
                self.create_transactional_cache(
                    atomicity_mode=atomicity_mode,
                    cache_mode='replicated' if i == 0 else 'partitioned',
                    backups=(i + 1) * 2,
                )
                for _ in range(0, 5):
                    lrt_operations = self.launch_transaction_operations()
                    self.ignite.kill_node(2)
                    self.rollback_transactions(lrt_operations)
                    self.release_transactions(lrt_operations)
                    util_sleep_for_a_while(10)
                    self.ignite.start_node(2)

                    util_sleep_for_a_while(10)
                    self.ignite.jmx.wait_for_finish_rebalance(120,
                                                              [CACHE_NAME])

                    self.cu.list_transactions()
                    lrt_operations2 = self.launch_transaction_operations()
                    self.release_transactions(lrt_operations2)
                self.delete_transactional_cache()

        self.verify_no_assertion_errors()
        self.idle_verify_check_conflicts_action()

            # self.wait_for_transaction_timeout()
            #
            # for _ in range(0, 10):
            #     log_print("Kill node")
            #     self.ignite.kill_node(2)
            #     log_print("Sleep")
            #     sleep(10)
            #     log_print("Start node")
            #     self.ignite.start_node(2)
            #
            #     self.cu.kill_transactions(force=True)
            #
            # log_print("Sleep for 5 seconds")
            # sleep(5)
            #
            # self.cu.list_transactions()
            # log_print("Sleep for 120 seconds")
            # sleep(120)
            #
            # self.cu.list_transactions()
            # self.cu.list_transactions()

    @attr('other')
    @require(min_ignite_version='2.5.1-p5')
    @test_case_id(81396)
    @with_setup(setup_testcase, teardown_testcase)
    def test_reduce_manual_defined_tx_timeout_value(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                log_print("Set timeout to many seconds")
                self.ignite.jmx.get_attributes(
                    1,
                    'Transactions',
                    'TransactionsMXBeanImpl',
                    'TxTimeoutOnPartitionMapExchange(6000000)'
                )

                log_print("Start additional nodes")
                self.ignite.start_additional_nodes(self.ignite.add_additional_nodes(self.get_server_config()),
                                                   skip_nodes_check=True)

                # self.su.snapshot_utility('snapshot', '-type=full', background=True, log='snapshot-out.log')

                # set attribute TxTimeoutOnPartitionMapExchange
                log_print("Set timeout to 10 seconds")
                self.ignite.jmx.get_attributes(
                    1,
                    'Transactions',
                    'TransactionsMXBeanImpl',
                    'TxTimeoutOnPartitionMapExchange(10000)'
                )

                # Wait for ~1 minute
                log_print("Sleep for 120 seconds")
                sleep(120)

                # self.su.wait_no_snapshots_activity_in_cluster(30)

                self.release_transactions(lrt_operations)
            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    @attr('other')
    @require(min_ignite_version='2.5.1')
    @test_case_id(81399)
    @with_setup(setup_testcase, teardown_testcase)
    def test_pmi_lrt_jmx_kill_transactions_while_snapshot(self):
        with PiClient(self.ignite, self.get_client_config()):
            cache = self.create_transactional_cache()

            lrt_operations = self.launch_transaction_operations()

            try:
                self.wait_for_transaction_timeout()

                out = self.ignite.jmx.evaluate_operation(
                    1, 'Transactions', 'TransactionsMXBeanImpl',
                    'getActiveTransactions', '0', '0', 'null', 'null', 'null',
                    'null', '20', 'DURATION', 'true', 'true')

                print_blue(out)

                self.cu.list_transactions()
                for lrt_operation in lrt_operations.values():
                    xid = lrt_operation.getTransaction().xid().toString()
                    assert xid not in self.cu.latest_utility_output, \
                        'Transaction with XID %s still exists in output' % xid

                self.release_transactions(lrt_operations)

                self.wait_for_transaction_timeout()

                for i in range(0, 10):
                    assert cache.get(i, key_type='long') is None, \
                        'Value for key %s for rolled back transaction exists on cluster' % i

            except TidenException as e:
                raise e
            finally:
                self.print_values_in_cache(lrt_operations, cache)

    def assert_utility_output(self):
        assert 'Error' not in self.cu.latest_utility_output, 'Error found in control.sh utility output'

    def utility_drop_transaction(self,
                                 transaction_xid='_TAKEN_FROM_LAST_OUTPUT_'):
        self.cu.kill_transactions(xid=transaction_xid, force=True)
        return transaction_xid

    def cleanup_lfs(self):
        log_put('Cleanup Ignite LFS ... ')
        commands = {}

        server_nodes = self.ignite.get_all_default_nodes() + self.ignite.get_all_additional_nodes()
        for node_idx in self.ignite.nodes.keys():
            if node_idx not in server_nodes:
                continue
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' %
                    self.ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append(
                    'rm -rf %s/work/*' %
                    self.ignite.nodes[node_idx]['ignite_home'])
        self.ssh.exec(commands)
        # print(results)
        log_put('Ignite LFS deleted.')
        log_print()

    @staticmethod
    def create_transactional_cache(atomicity_mode='transactional', cache_mode='replicated', backups=2):
        gateway = get_gateway()

        cache_config = IgniteCacheConfig(gateway=gateway)
        cache_config.set_name(CACHE_NAME)
        cache_config.set_atomicity_mode(atomicity_mode)
        cache_config.set_backups(backups)
        cache_config.set_cache_mode(cache_mode)

        return IgniteCache(CACHE_NAME, cache_config, gateway=gateway)

    @staticmethod
    def delete_transactional_cache():
        gateway = get_gateway()
        cache = gateway.entry_point.getIgniteService().getIgnite().getOrCreateCache(CACHE_NAME)
        cache.destroy()

    @staticmethod
    def release_transactions(lrt_operations):
        log_print('Releasing transactions')
        for lrt_operation in lrt_operations.values():
            lrt_operation.releaseTransaction()

    @staticmethod
    def rollback_transactions(lrt_operations):
        log_print('Set transactions to rollback')
        for lrt_operation in lrt_operations.values():
            lrt_operation.rollbackTransactionAfterRelease()

    @staticmethod
    def launch_transaction_operations():
        log_print('Creating transactions')
        lrt_operations = {}
        for i in range(0, 10):
            tx_d = TxDescriptor(label='tx_%s' % i)
            lrt_operation = create_transaction_control_operation(CACHE_NAME, i, i,
                                                                 tx_description=tx_d)
            lrt_operation.evaluate()

            lrt_operations[i] = lrt_operation

        return lrt_operations

    @staticmethod
    def wait_for_transaction_timeout():
        log_print('Sleep for %s seconds' % LRT_TIMEOUT)
        sleep(LRT_TIMEOUT)

    @staticmethod
    def validate_expected_output(lrt_operations, cache):
        for transaction in lrt_operations.values():
            print_green('Validating {key:%s value:%s}' %
                        (transaction.getKey(), transaction.getValue()))
            assert transaction.getValue() == cache.get(transaction.getKey(), key_type='long'), \
                'Corrupted data; value in cache - %s' % cache.get(transaction.getKey(), key_type='long')

    @staticmethod
    def print_values_in_cache(lrt_operations, cache):
        for transaction in lrt_operations.values():
            print_blue('Value in cache tx {key:%s value:%s} value = %s' %
                       (transaction.getKey(), transaction.getValue(),
                        cache.get(transaction.getKey(), key_type='long')))

    @staticmethod
    def additional_loading_actions(sum_after):
        del sum_after[CACHE_NAME]

    # @attr('pme')
    # @require(min_ignite_version='2.5.8')
    # # @test_case_id()
    # @with_setup(setup_testcase, teardown_testcase)
    # def test_pme_duration_jmx_attr(self):
    #     with PiClient(self.ignite, self.get_client_config()):
    #
    #         self.create_transactional_cache()
    #
    #         lrt_operations = self.launch_transaction_operations()
    #
    #         clients = set(self.ignite.get_alive_common_nodes())
    #
    #
    #         with PiClient(self.ignite, self.get_server_config(),
    #                       nodes_num=1,
    #                       new_instance=True,
    #                       wait_for_ignition_start=False,  # TODO: requires piclient fix
    #                       jvm_options=[
    #                           '-Dcom.sun.management.jmxremote',
    #                           '-Dcom.sun.management.jmxremote.authenticate=false',
    #                           '-Dcom.sun.management.jmxremote.ssl=false',
    #                       ]):
    #
    #             started_client = list(set(self.ignite.get_alive_common_nodes()) - clients)[0]
    #
    #             sleep(3)
    #
    #             for i in range(0, 3):
    #                 out = self.ignite.jmx.get_attributes(
    #                     1, 'Kernal', 'ClusterMetricsMXBeanImpl',
    #                     'CurrentPmeDuration')
    #                 print_blue(out)
    #                 out = self.ignite.jmx.get_attributes(
    #                     1, 'Kernal', 'ClusterLocalNodeMetricsMXBeanImpl',
    #                     'CurrentPmeDuration')
    #                 print_blue(out)
    #                 out = self.ignite.jmx.get_attributes(
    #                     started_client, 'Kernal', 'ClusterLocalNodeMetricsMXBeanImpl',
    #                     'CurrentPmeDuration')
    #                 print_blue(out)
    #                 out = self.ignite.jmx.get_attributes(
    #                     started_client, 'Kernal', 'ClusterMetricsMXBeanImpl',
    #                     'CurrentPmeDuration')
    #                 print_blue(out)
    #                 sleep(1)
    #
    #             self.release_transactions(lrt_operations)
    #
    #         for i in range(0, 5):
    #             out = self.ignite.jmx.get_attributes(
    #                 1, 'Kernal', 'ClusterMetricsMXBeanImpl',
    #                 'CurrentPmeDuration')
    #             print_blue(out)
    #             sleep(3)
    #

