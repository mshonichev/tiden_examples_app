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

from py4j.protocol import Py4JJavaError

from tiden import TidenException
from tiden.case.apptestcase import AppTestCase
from tiden.apps.ignite import Ignite
from tiden_gridgain.piclient.helper.cache_utils import IgniteTransaction
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.piclient import PiClient
from tiden.util import *
from concurrent.futures import ThreadPoolExecutor
from difflib import unified_diff
from re import search
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden.assertions import tiden_assert


class TestTxDr(AppTestCase):
    ignite_master_app = None
    ignite_replica_app = None
    success_run = False
    sbt_model_config = None

    ignite_app_names = {
        'master': '',
        'replica': '',
    }

    config_params = {
        'master': {
            'disco_prefix': '475',
            'comm_prefix': '471',
            'auth': True
        },
        'replica': {
            'disco_prefix': '476',
            'comm_prefix': '472',
            'auth': True
        }
    }

    def __init__(self, *args):
        super().__init__(*args)

        self.artifacts = []
        for name, artf in args[0]['artifacts'].items():
            if artf.get('type', '') == 'ignite':
                print(name, artf)
                if artf.get('base', False):
                    self.ignite_app_names['master'] = name
                    self.add_app(
                        name,
                        app_class_name='ignite',
                        grid_name='master',
                        additional_grid_name=False
                    )
                else:
                    self.ignite_app_names['replica'] = name
                    self.add_app(
                        name,
                        app_class_name='ignite',
                        grid_name='replica',
                        additional_grid_name=False
                    )

    def get_dr_jvm_options(self, role='master'):
        jvm_options = ["-DIGNITE_PENDING_TX_TRACKER_ENABLED=true",
                       '-DTX_DR_DEBUG_OUTPUT_ENABLED=true',
                       # '-DIGNITE_MIN_TX_TIMEOUT=300000',
                       '-DCONSISTENT_CUT_GC_DISABLED=true',
                       '-DIGNITE_TEST_FEATURES_ENABLED=true',
                       # '-DTX_DR_WAL_BUFFER_SIZE=1073741824',
                       "-DDR_STORAGE=%s" % self.dr_storage]
        return jvm_options

    def setup(self):
        self.create_app_config_set(Ignite, 'master',
                                   zookeeper_enabled=False,
                                   sbt_model_enabled=True,
                                   discovery_port_mask=self.config_params['master']['disco_prefix'],
                                   communication_port_mask=self.config_params['master']['comm_prefix'],
                                   authentication_enabled=True,
                                   ssl_enabled=True,
                                   ssl_config_path=self.config['rt']['remote']['test_module_dir'],
                                   auth_login='server_user',
                                   auth_password='{}'.format(self.config_params['master']['disco_prefix']),
                                   caches='caches_sbt.xml')
        self.create_app_config_set(Ignite, 'replica',
                                   zookeeper_enabled=False,
                                   sbt_model_enabled=True,
                                   authentication_enabled=True,
                                   ssl_enabled=True,
                                   ssl_config_path=self.config['rt']['remote']['test_module_dir'],
                                   auth_login='server_user',
                                   auth_password='{}'.format(self.config_params['replica']['disco_prefix']),
                                   discovery_port_mask=self.config_params['replica']['disco_prefix'],
                                   communication_port_mask=self.config_params['replica']['comm_prefix'],
                                   caches='caches_sbt.xml')
        self.use_local_shared_directory = True if len(self.config['environment']['server_hosts']) == 1 else False
        super().setup()
        self.util_deploy_sbt_model('master')
        self.util_deploy_sbt_model('replica')

    def get_client_config(self, role='master'):
        return Ignite.config_builder.get_config('client', config_set_name=role)

    def start_ignite_grid(self, name):
        app = self.tiden.apps[self.ignite_app_names[name]]
        app.set_node_option('*', 'config',
                            Ignite.config_builder.get_config('server', config_set_name=name))
        app.set_node_option('*', 'jvm_options',
                            self.get_dr_jvm_options(role=name)
                            )
        app.reset()
        app.add_artifact_lib('piclient')
        app.start_nodes()
        if self.config_params[name]['auth']:
            app.cu.enable_authentication('server_user', '{}'.format(self.config_params[name]['disco_prefix']))
            app.su.enable_authentication('server_user', '{}'.format(self.config_params[name]['disco_prefix']))
            app.ru.enable_authentication('server_user', '{}'.format(self.config_params[name]['disco_prefix']))
            app.enable_authentication('server_user', '{}'.format(self.config_params[name]['disco_prefix']))
        app.cu.activate()
        return app

    @staticmethod
    def future_result(future):
        future.result()

    def setup_shared_storage_test(self):
        self.success_run = False
        log_print('create transfer_folder')
        if self.use_local_shared_directory:
            self.dr_storage = self.local_shared_dir('transfer_folder_{}'.format(self.config['rt']['test_method']),
                                                    create=True)
        log_print('set transfer_folder path - {}'.format(self.dr_storage))
        log_print('start clusters in parallel')
        futures = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures.append(executor.submit(self.start_ignite_grid, 'master'))
            futures.append(executor.submit(self.start_ignite_grid, 'replica'))
        self.ignite_master_app = futures[0].result()
        self.ignite_replica_app = futures[1].result()
        additional_node_id = self.ignite_master_app.add_additional_nodes(
            config=Ignite.config_builder.get_config('server', config_set_name='master'), name='base-ignite-2.8.1.b8')
        self.ignite_master_app.set_node_option(additional_node_id, 'jvm_options',
                            self.get_dr_jvm_options(role='master')
                            )
        self.ignite_master_app.start_additional_nodes(additional_node_id)
        PiClientIgniteUtils.load_data_with_txput_sbt_model(only_caches_batch=None, start_key=0,
                                                           end_key=100,
                                                           jvm_options=self.get_dr_jvm_options('master'),
                                                           config=self.get_client_config('master'),
                                                           tiden_config=self.ignite_master_app.config,
                                                           ignite=self.ignite_master_app)
        print_green('Preload done')

    def setup_shared_storage_test_with_replication_init(self):
        self.setup_shared_storage_test()
        try:
            self.ignite_master_app.ru.replication_utility('bootstrap',
                                                          '-role=master -archive=ZIP -single_copy -parallelism=4 -snapshot_folder=%s/snapshot' % self.dr_storage,
                                                          timeout=1200)
            self.ignite_replica_app.ru.replication_utility('bootstrap',
                                                           '-role=replica -snapshot_folder=%s/snapshot -snapshot_id=%s' % (
                                                               self.dr_storage,
                                                               self.ignite_master_app.ru.get_session_id_from_bootstrap_command()),
                                                           timeout=1200)
            additional_node_id = self.ignite_master_app.add_additional_nodes(
                config=Ignite.config_builder.get_config('server', config_set_name='master'),
                name='base-ignite-2.8.1.b8')
            self.ignite_master_app.set_node_option(additional_node_id, 'jvm_options',
                                                   self.get_dr_jvm_options(role='master')
                                                   )
            self.ignite_master_app.start_additional_nodes(additional_node_id)
            self.check_read_only_cluster('replica')
        except Exception as detail:
            log_print('Handling run-time error: %s' % detail)
            self.teardown_shared_storage_test()
        print_green('Bootstrap master/replica completed')

    def teardown_shared_storage_test(self):
        try:
            log_print('stop master/replica in parallel')
            futures = []
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures.append(executor.submit(self.stop_ignite_grid, 'master'))
                futures.append(executor.submit(self.stop_ignite_grid, 'replica'))
            map(self.future_result, futures)
        finally:
            if self.use_local_shared_directory:
                self.dr_storage = self.local_shared_dir('transfer_folder_{}'.format(self.config['rt']['test_method']),
                                                        create=False)
            self.ignite_master_app.ssh.killall('java')
            self.ignite_replica_app.ssh.killall('java')
            self.ignite_master_app.cleanup_work_dir()
            self.ignite_replica_app.cleanup_work_dir()

    def stop_ignite_grid(self, role):
        app = self.ignite_master_app if role == 'master' else self.ignite_replica_app
        app.cu.deactivate()
        app.stop_nodes()

    def diff_idle_verify_dump(self, raise_flag=False):
        dump_master = self.idle_verify_dump(role='master')
        log_print('Fount path in output - {} '.format(dump_master))
        dump_replica = self.idle_verify_dump(role='replica')
        log_print('Fount path in output - {} '.format(dump_replica))
        dump_diff = unified_diff(
            open(dump_master).readlines(),
            open(dump_replica).readlines(),
            fromfile='master',
            tofile='replica'
        )
        try:
            tiden_assert(''.join(dump_diff) == '', \
                         "files idle_verify dump master, replica not match")
            log_print("IDLE_VERIFY DUMP IS EQUAL", color='green')
        except AssertionError:
            log_print("IDLE_VERIFY DUMP IS NOT EQUAL", color='red')
            if raise_flag:
                raise

    def idle_verify_dump(self, role='master'):
        app = self.ignite_master_app if role == 'master' else self.ignite_replica_app
        app.cu.control_utility('--cache idle_verify --dump --skip-zeros')
        file_name = search('(\'.*\')', app.cu.latest_utility_output).group(0).replace("'", "")
        file_name_local = file_name.split('/')[-1].replace('idle-dump', '%s-idle-dump' % role)
        app.ssh.download_from_host(self.util_get_host_with_file(file_name), file_name,
                                   '%s/%s' % (app.config['rt']['test_resource_dir'], file_name_local))
        return '%s/%s' % (app.config['rt']['test_resource_dir'], file_name_local)

    def restart_ignite_grid(self, role='master'):
        app = self.ignite_master_app if role == 'master' else self.ignite_replica_app

        app.cu.deactivate()
        app.stop_nodes()
        app.start_nodes()
        app.cu.activate()

    def make_data_loading(self, duration, role='master', func_on_load=None):
        app = self.ignite_master_app if role == 'master' else self.ignite_replica_app
        with PiClient(app, self.get_client_config(role), jvm_options=self.get_dr_jvm_options(role),
                      cu=app.cu) as piclient:
            with TransactionalLoading(piclient, ignite=app, skip_consistency_check=True,
                                      cross_cache_batch=100, skip_atomic=True,
                                      config_file=self.get_client_config(role),
                                      wait_before_kill=False,
                                      loading_profile=LoadingProfile(delay=1,
                                                                     start_key=0,
                                                                     end_key=100,
                                                                     transaction_timeout=500,
                                                                     run_for_seconds=duration + 10),
                                      tx_metrics=['txCreated', 'txCommit', 'txRollback', 'txFailed']) as tx_loading:
                sleep(20)
                if func_on_load == 'switch':
                    self.ignite_master_app.ru.replication_utility('switch')
                elif func_on_load == 'bootstrap':
                    self.ignite_master_app.ru.replication_utility('bootstrap',
                                                                  '-role=master -archive=ZIP -single_copy -parallelism=4 -snapshot_folder=%s/snapshot' % self.dr_storage,
                                                                  timeout=1200)
                    self.ignite_replica_app.ru.replication_utility('bootstrap',
                                                                   '-role=replica -snapshot_folder=%s/snapshot -snapshot_id=%s' % (
                                                                       self.dr_storage,
                                                                       self.ignite_master_app.ru.get_session_id_from_bootstrap_command()),
                                                                   timeout=1200)
                elif func_on_load == 'restart_on_load':
                    self.ignite_replica_app.ru.replication_utility('pause')
                    sleep(10)
                    self.restart_ignite_grid('replica')
                    sleep(10)
                    self.ignite_replica_app.ru.replication_utility('resume')
                elif func_on_load == 'pitr':
                    cache = piclient.get_ignite().getOrCreateCache(
                        'com.sbt.bm.ucp.published.api.retail.PublishedIndividual')
                    cache.put(10000, 1)
                    sleep(45)
                    self.ignite_replica_app.ru.replication_utility('stop', '-recovery')
                sleep(duration)
            log_print(tx_loading.metrics['txCommit'])
        app.wait_for_topology_snapshot(
            None,
            0,
            ''
        )
        log_print('Loading done')

    @test_case_id('158331')
    @attr('DR', 'bootstrap_on_load')
    @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    def test_data_replication_bootstrap_on_load(self):
        self.make_data_loading(duration=180, func_on_load='bootstrap')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.success_run = True

    @test_case_id('134120')
    @attr('DR', 'PMI_DR', 'DR_1')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_1(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.success_run = True

    @test_case_id('158332')
    @attr('DR', 'PMI_DR', 'DR_2')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_2(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Call switch on master cluster
            6. Run transactional loading on 'Replica' cluster
            7. Wait for 'Replica' cluster applies all changes.
            8. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_master_app.ru.replication_utility('switch')
        # self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app, to_cluster=self.ignite_replica_app, timeout=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_replica_app,
                                                     to_cluster=self.ignite_master_app)
        self.ignite_replica_app.ru.replication_utility('status')
        self.make_data_loading(duration=180, role='replica')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_replica_app,
                                                     to_cluster=self.ignite_master_app)
        checksum_master_after, checksum_replica_after = self.calculate_checksums()
        tiden_assert(checksum_master_after == checksum_replica_after, 'Hash sum master and replica not match')
        tiden_assert(checksum_master_before != checksum_master_after,
                     'Master not change after load on replica with change')
        self.success_run = True

    @test_case_id('158333')
    @attr('DR', 'PMI_DR', 'DR_2_ON_LOAD')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_2_on_load(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Call switch on master cluster due load
            6. Run transactional loading on 'Replica' cluster
            7. Wait for 'Replica' cluster applies all changes.
            8. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.make_data_loading(duration=180, func_on_load='switch')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_replica_app,
                                                     to_cluster=self.ignite_master_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_replica_app.ru.replication_utility('status')
        self.make_data_loading(duration=180, role='replica')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_replica_app,
                                                     to_cluster=self.ignite_master_app)
        checksum_master_after, checksum_replica_after = self.calculate_checksums()
        tiden_assert(checksum_master_after == checksum_replica_after, 'Hash sum master and replica not match')
        tiden_assert(checksum_master_before != checksum_master_after,
                     'Master not change after load on replica with change')
        self.success_run = True

    @test_case_id('158334')
    @attr('DR', 'PMI_DR', 'DR_3')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_3(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Stop one node in master
            6. Run transactional loading on 'Master' cluster
            7. Wait for 'Replica' cluster applies all changes.
            8. Return node back to master (stop at step 5)
            9. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        util_sleep_for_a_while(180)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_master_app.kill_node(2)
        self.make_data_loading(duration=180, role='master')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        self.ignite_replica_app.ru.replication_utility('status')
        self.ignite_replica_app.ru.replication_utility('stop')
        self.ignite_replica_app.ru.replication_utility('status')
        self.ignite_master_app.start_node(2)
        util_sleep_for_a_while(360)
        checksum_master_after, checksum_replica_after = self.calculate_checksums()
        tiden_assert(checksum_master_after == checksum_replica_after, 'Hash sum master and replica not match')
        tiden_assert(checksum_master_before != checksum_master_after,
                     'Master not change after load on replica with change')
        self.success_run = True

    @test_case_id('158336')
    @attr('DR', 'DR_4', 'DR_4_BLT_NODE_ONLINE')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_4_replica_node_up(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Stop one node in master (delete lfs)
            6. Change master blt
            7. Run transactional loading on 'Master' cluster.
            6. Kill same node (step 5) on replica
            7. Wait for 'Replica' cluster applies all changes.
            8. Return node back to master (stop at step 5)
            9. Return node back to replica (stop at step 5)
            10. Change master blt
            11. Run transactional loading on 'Master' cluster.
            12. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_master_app.kill_node(2)
        self.ignite_master_app.delete_lfs(node_id=2)
        self.ignite_master_app.cu.control_utility('--baseline remove node_{} --yes'.format(2))
        self.make_data_loading(duration=180)
        self.ignite_replica_app.kill_node(2)
        self.ignite_replica_app.delete_lfs(node_id=2)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_master_app.start_node(2)
        self.ignite_replica_app.start_node(2)
        self.ignite_master_app.cu.control_utility('--baseline add node_{} --yes'.format(2))
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.success_run = True

    @test_case_id('158335')
    @attr('DR', 'DR_4', 'DR_4_BLT_NODE_OFFLINE')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_4_replica_node_down(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Stop one node in master (delete lfs)
            6. Kill same node (step 5) on replica
            7. Change master blt
            8. Run transactional loading on 'Master' cluster.
            9. Wait for 'Replica' cluster applies all changes.
            10. Return node back to master (stop at step 5)
            11. Change master blt
            11. Return node back to replica (stop at step 5)
            11. Run transactional loading on 'Master' cluster.
            12. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        util_sleep_for_a_while(180)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_master_app.kill_node(2)
        self.ignite_master_app.delete_lfs(node_id=2)
        self.ignite_replica_app.kill_node(2)
        self.ignite_replica_app.delete_lfs(node_id=2)
        self.ignite_master_app.cu.control_utility('--baseline remove node_{} --yes'.format(2))
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_master_app.start_node(2)
        self.ignite_master_app.cu.control_utility('--baseline add node_{} --yes'.format(2))
        self.ignite_replica_app.start_node(2)
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.success_run = True

    @test_case_id('158337')
    @attr('DR', 'PMI_DR', 'DR_5')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_5(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Stop replication
            6. Check replica sums not change
            7. Check master sums change
        """
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_replica_app.ru.replication_utility('stop')
        self.ignite_master_app.ru.replication_utility('stop')
        self.make_data_loading(duration=60)
        checksum_master_after, checksum_replica_after = self.calculate_checksums(idle_verify=False)
        tiden_assert(checksum_replica_before == checksum_replica_after, 'Replica change after stop replication')
        tiden_assert(checksum_master_before != checksum_master_after, 'Master not change after load')
        self.success_run = True

    @test_case_id('158338')
    @attr('DR', 'PMI_DR', 'DR_RESTART_PITR', 'DR_6_PITR')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_6_pitr(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Check check sums on both clusters are the same.
            5. Put variable in master, wait 40 seconds and check on replica
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.make_data_loading(duration=0, func_on_load='pitr')
        self.check_replica_value('replica')
        self.success_run = True

    @test_case_id('158339')
    @attr('DR', 'PMI_DR', 'DR_7')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_7(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Call pause on replica
            5. Run transactional loading on 'Master' cluster.
            6. Call resume on replica
            7. Check check sums on both clusters are the same.
        """
        checksum_master_before, checksum_replica_before = self.calculate_checksums()
        tiden_assert(checksum_master_before == checksum_replica_before, 'Hash sum master and replica not match')
        self.ignite_replica_app.ru.replication_utility('pause')
        self.make_data_loading(duration=60)
        self.ignite_replica_app.ru.replication_utility('resume')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master_after, checksum_replica_after = self.calculate_checksums()
        tiden_assert(checksum_replica_after == checksum_master_after, 'Hash sum master and replica not match')
        tiden_assert(checksum_master_before != checksum_master_after, 'Master not change after load')
        self.success_run = True

    @test_case_id('158340')
    @attr('DR', 'PMI_DR', 'DR_RESTART_MASTER', 'DR_8', 'DR_RESTART_CLUSTER')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_8(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Restart master
            5. Run transactional loading on 'Master' cluster.
            6. Check check sums on both clusters are the same.
            --------------- additional steps
            7. stop master
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.restart_ignite_grid()
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.ignite_master_app.ru.replication_utility('stop', background=True)
        self.success_run = True

    @test_case_id('158341')
    @attr('DR', 'PMI_DR', 'DR_RESTART_REPLICA', 'DR_9', 'DR_RESTART_CLUSTER')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_9(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Restart replica
            5. Run transactional loading on 'Master' cluster.
            6. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.restart_ignite_grid(role='replica')
        self.diff_idle_verify_dump()
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.success_run = True

    @test_case_id('158342')
    @attr('DR', 'PMI_DR', 'DR_RESTART_REPLICA', 'DR_9_ON_LOAD', 'DR_RESTART_CLUSTER')
    @with_setup(setup_shared_storage_test_with_replication_init, teardown_shared_storage_test)
    def test_data_replication_pmi_9_on_load(self):
        """
            1. Bootstrap replication
            2. Run transactional loading on 'Master' cluster.
            3. Wait for 'Replica' cluster applies all changes.
            4. Restart replica on load
            5. Run transactional loading on 'Master' cluster.
            6. Check check sums on both clusters are the same.
        """
        self.make_data_loading(duration=180)
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.make_data_loading(duration=180, func_on_load='restart_on_load')
        self.wait_for_another_cluster_apply_last_cut(from_cluster=self.ignite_master_app,
                                                     to_cluster=self.ignite_replica_app)
        checksum_master, checksum_replica = self.calculate_checksums()
        tiden_assert(checksum_master == checksum_replica, 'Hash sum master and replica not match')
        self.success_run = True

    def calculate_checksums(self, idle_verify=True):
        if idle_verify:
            self.diff_idle_verify_dump()

        checksum_for_master = PiClientIgniteUtils.calc_checksums_distributed(self.ignite_master_app,
                                                                             config=self.get_client_config('master'),
                                                                             jvm_options=self.get_dr_jvm_options(
                                                                                 'master'))
        checksum_for_replica = PiClientIgniteUtils.calc_checksums_distributed(self.ignite_master_app,
                                                                              config=self.get_client_config('replica'),
                                                                              jvm_options=self.get_dr_jvm_options(
                                                                                  'replica'))
        if idle_verify:
            self.diff_idle_verify_dump(raise_flag=True)
        return checksum_for_master, checksum_for_replica

    # @attr('check_cancel')
    # @with_setup(setup_shared_storage_test, teardown_shared_storage_test)
    # def test_data_replication_cancel_bootstrap_snapshot(self):
    #     # PiClientIgniteUtils.load_data_with_txput_sbt_model(only_caches_batch=None, start_key=100,
    #     #                                                    end_key=1000,
    #     #                                                    jvm_options=self.get_dr_jvm_options('master'),
    #     #                                                    config=self.get_client_config('master'),
    #     #                                                    tiden_config=self.tiden.config,
    #     #                                                    ignite=self.ignite_master_app)
    #     self.ignite_master_app.ru.replication_utility('bootstrap',
    #                                                   '-role=master -archive=ZIP -snapshot_folder=%s/snapshot' % self.dr_storage,
    #                                                   timeout=1200, background=True)
    #     bootstrap_session_id = self.ignite_master_app.ru.get_session_id_from_bootstrap_command()
    #     sleep(45)
    #     self.ignite_master_app.su.snapshot_utility('status')
    #
    #     operation_id = self.ignite_master_app.su.wait_running_operation()
    #
    #     if not operation_id:
    #         raise TidenException('There is no operation to cancel!!!')
    #
    #     self.ignite_master_app.su.snapshot_utility('cancel', '-id=%s' % operation_id)
    #     # self.ignite_replica_app.ru.replication_utility('bootstrap',
    #     #                                                '-role=replica -snapshot_folder=%s/snapshot -snapshot_id=%s' % (
    #     #                                                    self.dr_storage,
    #     #                                                    bootstrap_session_id),
    #     #                                                timeout=1200)
    #     sleep(1000)

    def util_deploy_sbt_model(self, ignite_name):
        """
        Deploy sbt_model.zip to all servers. sbt_model.zip contains:
            model.jar - should be placed to ignite/libs folder;
            caches.xml - should be renamed to caches_sbt.xml and placed to remote resource dir;
            json_model.json - should be placed to remote resource dir.
        """
        ignite = self.tiden.apps[self.ignite_app_names[ignite_name]]
        sbt_model_remote = ignite.config['artifacts']['sbt_model']['remote_path']
        remote_res = ignite.config['rt']['remote']['test_module_dir']
        cmd = [
            'cp %s/model.jar %s/libs/' % (
                sbt_model_remote, ignite.config['artifacts'][self.ignite_app_names[ignite_name]]['remote_path']),
            'cp %s/caches.xml %s/caches_sbt.xml' % (sbt_model_remote, remote_res),
            'cp %s/json_model.json %s' % (sbt_model_remote, remote_res),
        ]
        self.util_exec_on_all_hosts(ignite, cmd)

        server_host = ignite.config['environment'].get('server_hosts', [])[0]
        ignite.ssh.download_from_host(server_host, '%s/json_model.json' % remote_res,
                                      '%s/json_model.json' % ignite.config['rt']['test_resource_dir'])

    def util_get_host_with_file(self, file):
        commands = {}
        for host in self.config['environment']['server_hosts']:
            commands[host] = ['test -e %s && echo "File exist"' % file]
        results = self.ssh.exec(commands)
        for host, result in results.items():
            if result == ['File exist\n']:
                return host

    def check_read_only_cluster(self, role='master'):
        app = self.ignite_master_app if role == 'master' else self.ignite_replica_app
        read_only = True

        with PiClient(app, self.get_client_config(role), jvm_options=self.get_dr_jvm_options(role)) as piclient:
            cache = piclient.get_ignite().getOrCreateCache((piclient.get_ignite().cacheNames().toArray())[0])
            try:
                with IgniteTransaction() as tx:
                    cache.put(1, 1)
                    tx.commit()
                    read_only = False

            except Py4JJavaError:
                pass
            finally:
                if not read_only:
                    raise TidenException("assert {} read-only".format(role))
                log_print('Check cluster {} in read-only success'.format(role))

    def check_replica_value(self, role='master'):
        app = self.ignite_master_app if role == 'master' else self.ignite_replica_app
        with PiClient(app, self.get_client_config(role), jvm_options=self.get_dr_jvm_options(role=role),
                      cu=self.ignite_replica_app.cu) as piclient:
            cache = piclient.get_ignite().getOrCreateCache('com.sbt.bm.ucp.published.api.retail.PublishedIndividual')
            assert 1 == cache.get(10000)
            log_print('CHECK CLUSTER HAVE VALUE')

    def wait_for_another_cluster_apply_last_cut(self, from_cluster=None, to_cluster=None, timeout=180):
        cut_frequency = 35
        util_sleep_for_a_while(cut_frequency)
        already_wait = cut_frequency
        last_id = self.get_last_consistent_cut_from_logs(from_cluster)
        while True:
            to_cluster.ru.replication_utility('status')
            last_applied_id = to_cluster.ru.get_last_applied_cut_from_session()
            log_print('Found last applying id - {}, wait until value more that - {}'.format(last_applied_id, last_id))
            if int(last_applied_id) >= int(last_id):
                break
            already_wait += 10;
            util_sleep_for_a_while(10)
            if already_wait >= timeout:
                tiden_assert(already_wait < timeout, 'Check Limit of applying last cut reached')
        log_print('Found last cut applying cut on another cluster after {} s'.format(already_wait))

    def get_last_consistent_cut_from_logs(self, cluster) -> int:
        get_log = cluster.grep_all_data_from_log(
            'alive_server',
            'Finished FINISH stage of snapshot operation CONSISTENT_CUT FINISH',
            'snapshotId=(\d+),',
            'started_exchange_init',
        )
        min = 0
        for node in get_log.keys():
            if min > int(get_log[node][-1]) or min == 0:
                min = int(get_log[node][-1])
        return min

    def local_shared_dir(self, file_name, create=False):
        remote_test_module_directory = self.tiden.config['rt']['remote']['test_module_dir']
        server_hosts = self.tiden.config['environment']['server_hosts']
        commands = {}
        for host in server_hosts:
            if create:
                commands[host] = ['mkdir {}/{}'.format(remote_test_module_directory, file_name)]
            else:
                commands[host] = ['rm -r {}/{}'.format(remote_test_module_directory, file_name)]
        results = self.tiden.ssh.exec(commands)
        for host in server_hosts:
            if '' not in results[host]:
                raise TidenException('An error occurred while creating the directory')
        return '{}/{}'.format(remote_test_module_directory, file_name)

