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

import difflib
import random
from copy import copy
from pprint import pprint
from time import sleep

from tiden.apps.ignite import Ignite
from tiden import log_print, tiden_assert, util_sleep_for_a_while, is_enabled, attr, with_setup, repeated_test, search
from tiden.case.apptestcase import AppTestCase
from tiden_gridgain.piclient.helper.cache_utils import IgniteCache, IgniteCacheConfig
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_put_all_operation, create_distributed_checksum_operation
from tiden_gridgain.piclient.loading import TransactionalLoading
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from suites.dr.dr_cluster import Cluster, create_dr_relation
from suites.dr.util import wait_for_replication_is_up_for_clusters

START_DATA_SIZE = 500
LOAD_DATA_SIZE = 100
ITERATIONS = 10
START_DYNAMIC_CACHES = False
DR_STABILIZATION_TIMEOUT = 5
MINI_DR_STABILIZATION_TIMEOUT = 5
RECHECK_CHECKSUM_TIMEOUT = 5
FST_TIMEOUT = 15
PDS_ENABLED = True
SAVE_LFS = False
DO_FST_ON_DIFFERENCE = True
COLLECT_KEYS = True

SERVER_RESTART_TIMINGS = (0.5, 2.5)
SENDER_RESTART_TIMINGS = (0.5, 3.5)
RECEIVER_RESTART_TIMINGS = (0.5, 3.5)


class TestStressDataReplication(AppTestCase):
    ignite_app_names = {}
    data_model = ModelTypes.VALUE_ALL_TYPES_4_INDEX.value

    def __init__(self, *args):
        super().__init__(*args)

        self.clusters = {}
        self.fs_store_path = True

        for counter in range(1, 3):
            self.ignite_app_names[f'ignite{counter}'] = f'ignite{counter}'
            self.add_app(
                f'ignite{counter}',
                app_class_name='ignite',
                grid_name=f'ignite{counter}',
            )

    def setup_testcase(self):
        pass

    def teardown_testcase(self):
        for cluster in self.clusters:
            log_print('Teardown for cluster {}'.format(cluster))
            if cluster.grid:
                cluster.grid.kill_nodes()
                cluster.grid.remove_additional_nodes()

                if not SAVE_LFS:
                    cluster.grid.delete_lfs()

                cluster.grid = None

    @repeated_test(10)
    @attr('stress')
    @with_setup(setup_testcase, teardown_testcase)
    def test_master_master_restart_senders(self):
        self.prepare_clusters()
        self.run_stress_restarts(0, ITERATIONS, [7, 8], SENDER_RESTART_TIMINGS)

    @attr('stress')
    @with_setup(setup_testcase, teardown_testcase)
    def test_master_master_restart_receivers(self):
        self.prepare_clusters()
        self.run_stress_restarts(1, ITERATIONS, [7, 8], RECEIVER_RESTART_TIMINGS)

    @attr('stress')
    @with_setup(setup_testcase, teardown_testcase)
    def test_master_master_restart_main(self):
        self.prepare_clusters()
        self.run_stress_restarts(0, ITERATIONS, [1, 2, 3, 4, 5, 6], SERVER_RESTART_TIMINGS)

    @attr('stress')
    @with_setup(setup_testcase, teardown_testcase)
    def test_master_master_restart_replica(self):
        self.prepare_clusters()
        self.run_stress_restarts(1, ITERATIONS, [1, 2, 3, 4, 5, 6], SERVER_RESTART_TIMINGS)

    @attr('blt')
    @with_setup(setup_testcase, teardown_testcase)
    def test_master_master_master_blinking_blt(self):
        self.prepare_clusters()

        client_config = self.preconfigure_cluster_0()

        iterations = 10
        last_loaded_key = START_DATA_SIZE
        nodes_before = 6

        with PiClient(self.clusters[0].grid, client_config, jvm_options=['-ea']) as piclient:
            PiClientIgniteUtils.load_data_with_streamer(self.clusters[0].grid,
                                                        client_config,
                                                        end_key=last_loaded_key,
                                                        jvm_options=['-ea'],
                                                        check_clients=False
                                                        )

            sleep(60)

            with TransactionalLoading(self, ignite=self.clusters[0].grid, config_file=client_config,
                                      skip_consistency_check=True):
                for i in range(0, iterations):
                    log_print(f'Current iteration {i + 1} from {iterations}', color='debug')

                    self.clusters[0].grid.kill_node(2)

                    utility_baseline_log = 'control-utility-baseline.log'

                    self.clusters[0].grid.cu.set_current_topology_as_baseline(background=True, log=utility_baseline_log)

                    self.clusters[0].grid.start_node(2, skip_topology_check=True)

                    self.clusters[0].grid.wait_for_topology_snapshot(server_num=6)

                    self.clusters[0].grid.update_started_node_status(2)

                    self.clusters[0].grid.cu.set_current_topology_as_baseline(background=True, log=utility_baseline_log)

                    self.verify_cluster(0, nodes_before, last_loaded_key)

    def preconfigure_cluster_0(self):
        PiClient.read_timeout = 240
        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')
        self.clusters[0].grid.set_activation_timeout(240)
        self.clusters[0].grid.set_snapshot_timeout(240)
        self.clusters[0].grid.set_node_option('*', 'jvm_options', ['-ea'])
        self.clusters[0].grid.su.clear_snapshots_list()
        return client_config

    def start_clusters(self, clusters):
        for cluster in clusters:
            cluster.grid = self.start_ignite_grid(
                f'ignite{cluster.id}',
                cluster,
                activate=True
            )

    def generate_dr_topology(self, cluster_count, server_node_per_cluster, client_node_per_cluster):
        nodes_count_per_cluster = len(self.tiden.config['environment']['server_hosts']) * \
                                  self.tiden.config['environment']['servers_per_host']
        tiden_assert(server_node_per_cluster + client_node_per_cluster <= nodes_count_per_cluster,
                     '(server_node_per_cluster + client_node_per_cluster) <= (server_hosts * servers_per_host)')
        clusters = []
        for counter in range(1, cluster_count + 1):
            cluster = Cluster(counter, self.tiden.config)
            cluster.add_nodes(server_node_per_cluster, 'server')
            cluster.add_nodes(client_node_per_cluster, 'client')
            clusters.append(cluster)

        return clusters

    def generate_app_config(self, clusters):
        discovery_port_prefix = 4750
        communication_port_prefix = 4710

        create_dir = []
        for cluster in clusters:

            for node in cluster.nodes:
                fs_store_dir = None
                if self.fs_store_path:
                    fs_store_dir = '{}/fs_store_{}{}'.format(self.tiden.config['rt']['remote']['test_dir'],
                                                             cluster.id, node.id)
                    create_dir.append('mkdir {}'.format(fs_store_dir))

                config_name = f'cluster_{cluster.id}_node_{node.id}'
                node.config_name = config_name
                self.create_app_config_set(Ignite, config_name,
                                           config_type=node.node_type,
                                           deploy=True,
                                           consistent_id=True,
                                           caches='caches.xml',
                                           disabled_cache_configs=True,
                                           zookeeper_enabled=False,
                                           addresses=self.tiden.config['environment']['server_hosts'],
                                           discovery_port_prefix=discovery_port_prefix,
                                           communication_port_prefix=communication_port_prefix,
                                           node=node,
                                           ssl_enabled=is_enabled(self.tiden.config.get('ssl_enabled')),
                                           pds_enabled=PDS_ENABLED,
                                           fs_store_path=True,
                                           fs_store_path_value=fs_store_dir,
                                           logger=True,
                                           logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                               'test_module_dir'],
                                           custom_conflict_resolver=True,
                                           )

            # generate config without sender/receiver settings for piclient
            piclient_node = copy(cluster.nodes[1])
            piclient_node.sender_nodes = []
            piclient_node.receiver_nodes = []
            config_name = f'cluster_{cluster.id}_node_without_dr'
            self.create_app_config_set(Ignite, config_name,
                                       config_type='client',
                                       deploy=True,
                                       consistent_id=True,
                                       caches='caches.xml',
                                       disabled_cache_configs=True,
                                       zookeeper_enabled=False,
                                       addresses=self.tiden.config['environment']['server_hosts'],
                                       discovery_port_prefix=discovery_port_prefix,
                                       communication_port_prefix=communication_port_prefix,
                                       node=piclient_node,
                                       pds_enabled=PDS_ENABLED,
                                       ssl_enabled=is_enabled(self.tiden.config.get('ssl_enabled')),
                                       logger=True,
                                       logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                           'test_module_dir'],
                                       custom_conflict_resolver=True,
                                       )
            discovery_port_prefix += 1
            communication_port_prefix += 1

        if self.fs_store_path:
            self.tiden.ssh.exec(create_dir)

        return clusters

    def setup(self):
        super().setup()

    def start_ignite_grid(self, name, cluster, activate=False, already_nodes=0):
        app = self.get_app(self.ignite_app_names[name])
        for node in cluster.nodes:
            app.set_node_option(node.id, 'config',
                                Ignite.config_builder.get_config(node.node_type, config_set_name=node.config_name))

        artifact_cfg = self.tiden.config['artifacts'][app.name]

        app.reset()
        log_print("Ignite ver. %s, revision %s" % (
            artifact_cfg['ignite_version'],
            artifact_cfg['ignite_revision'],
        ))

        cmd = [
            'cp %s %s/libs/' % (app.config['artifacts']['piclient']['remote_path'],
                                app.config['artifacts'][self.ignite_app_names[name]]['remote_path'])
        ]
        self.util_exec_on_all_hosts(app, cmd)

        app.start_nodes(*cluster.get_server_node_ids(), already_nodes=already_nodes, other_nodes=already_nodes)
        app.start_additional_nodes(cluster.get_client_node_ids(), client_nodes=True)

        if activate:
            app.cu.activate(activate_on_particular_node=1)

        return app

    def prepare_clusters(self):
        self.clusters = self.generate_dr_topology(2, 6, 2)

        create_dr_relation(self.clusters[0].nodes[6], self.clusters[1].nodes[6])
        create_dr_relation(self.clusters[0].nodes[7], self.clusters[1].nodes[7])
        create_dr_relation(self.clusters[1].nodes[7], self.clusters[0].nodes[7])
        create_dr_relation(self.clusters[1].nodes[6], self.clusters[0].nodes[6])

        self.generate_app_config(self.clusters)
        self.start_clusters(self.clusters)
        wait_for_replication_is_up_for_clusters(self.clusters)

        self.put_data(self.clusters[0], 0, 'cluster_1_node_without_dr')

        sleep(MINI_DR_STABILIZATION_TIMEOUT)

    def run_stress_restarts(self, cluster_id_to_restart, iterations, nodes_to_restart, time_to_sleep_range):
        client_config = self.preconfigure_cluster_0()

        with PiClient(self.clusters[0].grid, client_config, jvm_options=['-ea']) as piclient:
            ignite = piclient.get_ignite()

            self.start_dynamic_caches_with_node_filter(client_config)

            last_loaded_key = START_DATA_SIZE
            PiClientIgniteUtils.load_data_with_putall(self.clusters[0].grid,
                                                      client_config,
                                                      end_key=last_loaded_key,
                                                      jvm_options=['-ea'],
                                                      check_clients=False
                                                      )

            util_sleep_for_a_while(MINI_DR_STABILIZATION_TIMEOUT)

            nodes_before = 6

            last_loaded_key += 1
            for i in range(0, iterations):
                log_print(f'Current iteration {i + 1} from {iterations}', color='debug')

                sleep_for_time = random.uniform(time_to_sleep_range[0], time_to_sleep_range[1])
                log_print(
                    f'In this run we are going to sleep for {sleep_for_time} seconds after each node restart',
                    color='green')

                log_print('Trying to load data into created/existing caches', color='yellow')

                self.start_dynamic_caches_with_node_filter(client_config)

                PiClientIgniteUtils.load_data_with_putall(self.clusters[0].grid,
                                                          client_config,
                                                          start_key=last_loaded_key,
                                                          end_key=last_loaded_key + LOAD_DATA_SIZE,
                                                          jvm_options=['-ea'],
                                                          check_clients=False)
                last_loaded_key += LOAD_DATA_SIZE

                self.increment_atomic(ignite)

                log_print("Round restart")
                for node_id in nodes_to_restart:
                    self.clusters[cluster_id_to_restart].grid.kill_node(node_id)
                    self.clusters[cluster_id_to_restart].grid.start_node(node_id, skip_topology_check=True)
                    sleep(sleep_for_time)

                log_print("Wait for topology messages")
                for node_id in nodes_to_restart:
                    self.clusters[cluster_id_to_restart].grid.update_started_node_status(node_id)

                util_sleep_for_a_while(MINI_DR_STABILIZATION_TIMEOUT)

                last_loaded_key = self.verify_cluster(0, nodes_before, last_loaded_key)

        util_sleep_for_a_while(DR_STABILIZATION_TIMEOUT)

        checksum_master1, checksum_slave1 = self.calculate_checksum_and_validate(last_loaded_key)
        tiden_assert(checksum_master1 == checksum_slave1, 'Hash sum master and slave not match')

        self.put_data(self.clusters[1], 1, 'cluster_2_node_without_dr')

        util_sleep_for_a_while(MINI_DR_STABILIZATION_TIMEOUT)

        checksum_master2, checksum_slave2 = self.calculate_checksum_and_validate(last_loaded_key)
        tiden_assert(checksum_master2 == checksum_slave2, 'Hash sum master and slave not match')

    def verify_cluster(self, cluster_to_verify_id, nodes_before, last_loaded_key=None):
        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')

        servers = 0
        ignite = self.clusters[cluster_to_verify_id].grid

        for i in range(3):
            for res in ignite.last_topology_snapshot():
                if res['servers'] > servers:
                    servers = res['servers']
                else:
                    break
            util_sleep_for_a_while(5)

        if nodes_before != servers:
            log_print(f"There are missing nodes on cluster: Nodes in cluster: {servers} expecting {nodes_before}",
                      color='yellow')

            self.verify_no_meaning_errors()

            log_print("Wait for topology messages again.", color='yellow')
            for node_id in ignite.get_all_default_nodes():
                ignite.update_started_node_status(node_id)

            log_print("Missing nodes case confirmed. Trying to restart node.", color='red')
            current_cluster_nodes = ignite.get_nodes_num('server')
            if nodes_before != current_cluster_nodes:
                log_print(f"Current nodes in cluster {current_cluster_nodes}")
                nodes_to_start = []

                for node_id in ignite.get_alive_default_nodes():
                    # assert that node is not dead otherwise kill/restart again
                    if not ignite.check_node_status(node_id):
                        log_print("Restarting node %s" % node_id, color='yellow')
                        nodes_to_start.append(node_id)

                log_print(f"Going to restart nodes: {nodes_to_start}", color='debug')
                for node_id in nodes_to_start:
                    ignite.start_node(node_id, skip_nodes_check=True, check_only_servers=True)

                current_cluster_nodes = ignite.get_nodes_num('server')
                if nodes_before != current_cluster_nodes:
                    log_print(f"Current amount of nodes in cluster: {current_cluster_nodes}, expecting {nodes_before}",
                              color='debug')

                    for node_id in ignite.get_alive_default_nodes():
                        self.util_get_threads_from_jstack(ignite, node_id, "FAILED")

                    assert False, "Failed to restart node"

        ignite.cu.control_utility('--activate')

        activate_failed = False
        log_print('Check that there is no Error in activate logs', color='yellow')
        if 'Error' in ignite.cu.latest_utility_output:
            activate_failed = True
            log_print('Failed!', color='red')
        sleep(5)

        ignite.cu.control_utility('--baseline')
        self.verify_no_meaning_errors()
        log_print('Check that there is no Error in control.sh --baseline logs', color='yellow')

        if 'Error' in ignite.cu.latest_utility_output:
            log_print('Failed! Second try after sleep 60 seconds', color='red')
            sleep(60)

            ignite.cu.control_utility('--baseline')

            if 'Error' in ignite.cu.latest_utility_output or activate_failed:
                log_print('Cluster looks hang.')

        log_print('Check that there is no AssertionError in logs', color='yellow')
        self.verify_no_meaning_errors()

        if last_loaded_key:
            try:
                new_last_key = last_loaded_key - int(random.uniform(0, 1) * LOAD_DATA_SIZE)
                log_print(f'Trying to remove data from survivor caches ({new_last_key}, {last_loaded_key})',
                          color='yellow')
                PiClientIgniteUtils.remove_data(ignite,
                                                client_config,
                                                start_key=new_last_key,
                                                end_key=last_loaded_key,
                                                check_clients=False,
                                                )

                last_loaded_key = new_last_key
            except Exception:
                for node_id in ignite.get_alive_default_nodes():
                    self.util_get_threads_from_jstack(ignite, node_id, "FAILED")

                assert False, "Unable to connect client"
            finally:
                self.verify_no_meaning_errors()

        util_sleep_for_a_while(MINI_DR_STABILIZATION_TIMEOUT)

        checksum_master, checksum_slave = self.calculate_checksum_and_validate(last_loaded_key)

        tiden_assert(checksum_master == checksum_slave, 'Hash sum master and slave should be equal')

        return last_loaded_key

    def verify_no_meaning_errors(self):
        for cluster in self.clusters:
            # self.verify_no_errors(cluster.grid, 'java.lang.NullPointerException')
            self.verify_no_errors(cluster.grid, 'java.lang.AssertionError')

    def verify_no_errors(self, ignite, error_text):
        log_print(f'Check that there is no {error_text} in logs', color='yellow')

        assertion_errors = ignite.find_exception_in_logs(".*%s.*" % error_text)

        # remove assertions from ignite.nodes to prevent massive output
        for node_id in ignite.nodes.keys():
            if 'exception' in ignite.nodes[node_id] and ignite.nodes[node_id]['exception'] != '':
                log_print(f"{error_text} found on node {node_id} on host {ignite.nodes[node_id]['host']}, "
                          f"text: {ignite.nodes[node_id]['exception'][:100]}",
                          color='red')
                ignite.nodes[node_id]['exception'] = ''

        # collect jstack from each node
        # assert that there is no errors
        if assertion_errors != 0:
            for node_id in ignite.nodes.keys():
                self.util_get_threads_from_jstack(ignite, node_id, "FAILED")

            assert False, f"{error_text} found in server logs! Count {assertion_errors}"

    def util_get_threads_from_jstack(self, ignite, node_id, iteration, to_find=''):
        """
        Run jstack and find thread id using thread name.
        """
        # Start grid to get thread names

        commands = []
        host = ignite.nodes[node_id]['host']
        pid = ignite.nodes[node_id]['PID']
        test_dir = self.tiden.config['rt']['remote']['test_dir']
        commands.append('jstack %s > %s/thread_dump_%s_%s.txt; cat %s/thread_dump.txt | grep "%s"' %
                        (pid, test_dir, node_id, iteration, test_dir, to_find))

        # log_print(commands)
        response = self.tiden.ssh.exec_on_host(host, commands)
        # log_print(response)
        out = response[host][0]

        # log_print('Node locked id = %s' % out)

        return out

    @staticmethod
    def increment_atomic(ignite):
        atomic_name = None
        atomic = None
        try:
            log_print("Incrementing atomics")
            for c in range(0, 10):
                atomic_name = "nodeId_%s" % c
                atomic = ignite.atomicLong(atomic_name, 0, True)
                for j in range(0, 50):
                    atomic.incrementAndGet()
        except Exception:
            log_print("Failed to increment atomics: assert that atomic.removed() and recreating it",
                      color='red')

            if atomic:
                ignite.atomicLong(atomic_name, 100, True)

    def calculate_checksum_and_validate(self, last_loaded_key, iteration=0):
        diff_keys = {}
        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')
        with PiClient(self.clusters[0].grid, client_config, new_instance=True) as piclient:
            checksum_master = create_distributed_checksum_operation().evaluate()

            if COLLECT_KEYS:
                cache_names = piclient.get_ignite().cacheNames().toArray()

                diff_keys['master'] = {}
                for cache in cache_names:
                    diff_keys['master'][cache] = []
                    iterator = piclient.get_ignite().cache(cache).iterator()
                    while iterator.hasNext():
                        diff_keys['master'][cache].append(iterator.next().getKey())

                    diff_keys['master'][cache].sort()

        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr')
        with PiClient(self.clusters[1].grid, client_config, new_instance=True) as piclient:
            checksum_slave = create_distributed_checksum_operation().evaluate()

            if COLLECT_KEYS:
                cache_names = piclient.get_ignite().cacheNames().toArray()

                diff_keys['replica'] = {}
                for cache in cache_names:
                    diff_keys['replica'][cache] = []
                    iterator = piclient.get_ignite().cache(cache).iterator()
                    while iterator.hasNext():
                        diff_keys['replica'][cache].append(iterator.next().getKey())

                    diff_keys['replica'][cache].sort()

        if checksum_master != checksum_slave:
            print(f"Checksum master\n{checksum_master}")
            print(f"Checksum slave\n{checksum_slave}")

            # if COLLECT_KEYS:
            #     pprint(diff_keys, width=240)

            comparison = ''.join(difflib.Differ()
                                 .compare(checksum_master.splitlines(True), checksum_slave.splitlines(True)))

            if iteration > 3:
                print(comparison)

                # log_print('Unable to get checksum equality on both clusters after FST', color='blue')
                #
                # return

                tiden_assert(False, 'Unable to get checksum equality on both clusters after FST')

            if iteration > 2:
                if DO_FST_ON_DIFFERENCE:
                    caches_with_diff = []
                    for line in comparison.split('\n'):
                        val = None
                        if line.startswith('-'):
                            m = search('Cache.*\'(.*)\'.rows', line)
                            if m:
                                val = m.group(1)

                        if val:
                            caches_with_diff.append(val)

                    log_print(f'Difference detected in some caches.',
                              color='red')

                    log_print(f'Starting clear() caches: {caches_with_diff}.',
                              color='red')

                    client_config = Ignite.config_builder.get_config('client',
                                                                     config_set_name='cluster_2_node_without_dr')
                    with PiClient(self.clusters[1].grid, client_config, new_instance=True) as piclient:
                        ignite = piclient.get_ignite()
                        for cache in caches_with_diff:
                            ignite.cache(cache).clear()

                        # cache_names = piclient.get_ignite().cacheNames().toArray()
                        #
                        # print(list(
                        #     create_checksum_operation(cache_name, 1, last_loaded_key).evaluate() for cache_name in
                        #     cache_names))

                    util_sleep_for_a_while(RECHECK_CHECKSUM_TIMEOUT)

                    log_print(f'Starting full state transfer on caches: {caches_with_diff}.',
                              color='red')

                    client_config = Ignite.config_builder.get_config('client',
                                                                     config_set_name='cluster_1_node_without_dr')
                    with PiClient(self.clusters[0].grid, client_config, new_instance=True) as piclient:
                        ignite = piclient.get_ignite()

                        try:
                            futs = []
                            for cache in caches_with_diff:
                                # TODO https://ggsystems.atlassian.net/browse/GG-22669
                                ignite.cache(cache)

                                futs.append(ignite.plugin('GridGain').dr().stateTransfer(cache, bytes([2])))

                            for fut in futs:
                                fut.get()
                        except Exception as e:
                            log_print('Exception caught on on FST\n{}'.format(e), color='red')
                            log_print('Going to restart replication with FST', color='yellow')
                            futs = []
                            for cache in caches_with_diff:
                                ignite.cache(cache)
                                ignite.plugin("GridGain").dr().startReplication(cache)
                                futs.append(ignite.plugin('GridGain').dr().stateTransfer(cache, bytes([2])))

                            for fut in futs:
                                fut.get()

                    util_sleep_for_a_while(FST_TIMEOUT)

            log_print(f'Going to collect checksum again after timeout - {RECHECK_CHECKSUM_TIMEOUT} seconds',
                      color='red')

            util_sleep_for_a_while(RECHECK_CHECKSUM_TIMEOUT)

            return self.calculate_checksum_and_validate(last_loaded_key, iteration + 1)

        return checksum_master, checksum_slave

    @staticmethod
    def put_data(cluster, iteration, config_name):
        client_config = Ignite.config_builder.get_config('client', config_set_name=config_name)
        with PiClient(cluster.grid, client_config):
            cache_names = ['cache_group_1_001']

            log_print(f'Put data into caches {cache_names}, from {iteration * 10 + 1} to {iteration * 10 + 11}')

            operations = [
                create_put_all_operation(cache_name, iteration * 10 + 1, iteration * 10 + 11, 100,
                                         value_type=ModelTypes.VALUE_ALL_TYPES_4_INDEX.value)
                for cache_name in cache_names]
            [operation.evaluate() for operation in operations]

    def start_dynamic_caches_with_node_filter(self, client_config):
        if START_DYNAMIC_CACHES:
            modifiers = ['', '_first_copy', '_second_copy', '_third_copy', '_forth_copy']

            for cluster in self.clusters.values():
                ignite = cluster.grid
                with PiClient(ignite, client_config, jvm_options=['-ea']) as piclient:
                    gateway = piclient.get_gateway()

                    for modifier in modifiers:
                        string_class = gateway.jvm.java.lang.String
                        string_array = gateway.new_array(string_class, 3)

                        for i, arg in enumerate([ignite.get_node_consistent_id(1), ignite.get_node_consistent_id(2)]):
                            string_array[i] = arg

                        node12_filter = gateway.jvm.org.apache.ignite.piclient.affinity.ConsistentIdNodeFilter(
                            string_array)

                        string_class = gateway.jvm.java.lang.String
                        string_array = gateway.new_array(string_class, 3)

                        for i, arg in enumerate([ignite.get_node_consistent_id(3), ignite.get_node_consistent_id(4)]):
                            string_array[i] = arg

                        node34_filter = gateway.jvm.org.apache.ignite.piclient.affinity.ConsistentIdNodeFilter(
                            string_array)

                        cfg = IgniteCacheConfig(gateway=gateway)
                        cfg.set_name('req' + modifier)
                        cfg.set_cache_mode('partitioned')
                        cfg.set_atomicity_mode('transactional')
                        cfg.set_backups(1)
                        cfg.set_rebalance_mode('async')
                        cfg.set_affinity(False, 64)
                        cfg.set_write_synchronization_mode('full_sync')
                        cfg.set_plugin_configurations('group1')

                        # transient?
                        cfg.cacheConfig.setLoadPreviousValue(True)
                        cfg.cacheConfig.setStoreKeepBinary(True)
                        cfg.cacheConfig.setNodeFilter(node12_filter)
                        cfg.cacheConfig.setDataRegionName('handled')
                        # cfg.cacheConfig.setReadThrough(True)
                        # cfg.cacheConfig.setWriteThrough(True)

                        IgniteCache('req' + modifier, cfg, gateway=gateway)

                        cfg = IgniteCacheConfig(gateway=gateway)
                        cfg.set_name('trans' + modifier)
                        cfg.set_atomicity_mode('transactional')
                        cfg.set_backups(1)
                        cfg.set_rebalance_mode('async')
                        cfg.set_affinity(False, 64)
                        cfg.set_plugin_configurations('group1')

                        # transient?
                        cfg.cacheConfig.setNodeFilter(node12_filter)
                        cfg.cacheConfig.setLoadPreviousValue(True)
                        cfg.cacheConfig.setStoreKeepBinary(True)
                        cfg.cacheConfig.setDataRegionName('handled')

                        IgniteCache('trans' + modifier, cfg, gateway=gateway)

                        cfg = IgniteCacheConfig(gateway=gateway)
                        cfg.set_name('id' + modifier)
                        cfg.set_atomicity_mode('atomic')
                        cfg.set_cache_mode('replicated')
                        cfg.set_rebalance_mode('async')
                        cfg.set_backups(0)
                        cfg.set_affinity(False, 256)
                        cfg.set_plugin_configurations('group1')

                        cfg.cacheConfig.setDataRegionName('no-evict')

                        IgniteCache('id' + modifier, cfg, gateway=gateway)

                        cfg = IgniteCacheConfig(gateway=gateway)
                        cfg.set_name('tid' + modifier)
                        cfg.set_atomicity_mode('transactional')
                        cfg.set_backups(1)
                        cfg.set_rebalance_mode('async')
                        cfg.set_affinity(False, 64)
                        cfg.set_write_synchronization_mode('full_sync')
                        cfg.set_plugin_configurations('group1')

                        cfg.cacheConfig.setDataRegionName('handled')
                        cfg.cacheConfig.setNodeFilter(node34_filter)

                        IgniteCache('tid' + modifier, cfg, gateway=gateway)

                        cfg = IgniteCacheConfig(gateway=gateway)
                        cfg.set_name('config' + modifier)
                        cfg.set_atomicity_mode('atomic')
                        cfg.set_cache_mode('replicated')
                        cfg.set_rebalance_mode('async')
                        cfg.set_backups(0)
                        cfg.set_affinity(False, 256)

                        cfg.cacheConfig.setDataRegionName('no-evict')

                        IgniteCache('config' + modifier, cfg, gateway=gateway)

