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

from os import mkdir
from os.path import join, exists
from time import time, sleep
from traceback import format_exc

from tiden.apps import NodeStatus
from tiden import log_print
from tiden.ignite import Ignite
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_put_all_operation, create_tables_operation, create_indexes_operation, \
    wait_snapshot_operation
from tiden_gridgain.piclient.piclient import PiClient
from suites.combine.framework.common import step


class SetupStateMixin:

    def __init__(self):
        self.cluster: Ignite = None
        self.context: dict = None
        self.server_config = None
        self.client_config = None

    def _set_key_iter(self, count):
        """
        Start counter for data to setup
        Needs to prevent put same values

        :param count:   expected count to put
        :return:        counter
        """
        if self.context.get('key_counter'):
            self.context['key_counter'] = count + self.context['key_counter']
        else:
            self.context['key_counter'] = count + 1
        return self.context['key_counter']

    def start_grid(self):
        """
        Start new grid
        """
        self.server_config = Ignite.config_builder.get_config('server', config_set_name='base')
        self.client_config = Ignite.config_builder.get_config('client', config_set_name='base')
        self.cluster.set_node_option('*', 'config', self.server_config)
        piclient_path = self.tiden.config['artifacts']['piclient']['remote_path']
        for node in self.cluster.nodes.values():
            self.cluster.ssh.exec_on_host(node['host'], [f"cp -r {piclient_path} {node['ignite_home']}/libs/"])
        self.cluster.start_nodes()
        self.cluster.cu.activate()
        self.context['activate'] = True

    @step(lambda args, kwargs: 'Clean cluster')
    def clean_cluster_step(self):
        self.clean_cluster()

    def clean_cluster(self):
        """
        Kill all Java processes
        Reset cluster state
        Clean history
        Set client flag
        """
        log_print('clean cluster')
        self.cluster.ssh.exec([
            f"for i in $(ps aux | grep java | grep -v bash | awk \'{{print $2}}\'); do kill -9 $i; done"
        ])
        keys = list(self.cluster.nodes.keys())

        for node_id in keys:
            try:
                try:
                    self.cluster.kill_node(node_id, ignore_exceptions=True)
                except:
                    pass
                self.cluster.cleanup_work_dir(node_id)
            except:
                log_print(format_exc(), color='red')

        try:
            self.cluster.reset(hard=True)
        except:
            log_print(format_exc(), color='red')
        self.context['history'] = []
        self.context['clean_cluster_was_here'] = True

    def clients_go_away(self):
        """
        Kill all client nodes java processes
        Delete from self.cluster.nodes
        """
        find_clients = lambda: [node_id for node_id in self.cluster.nodes.keys() if node_id >= 20000]
        if find_clients():
            end_time = 60 + time()
            while True:
                if not find_clients():
                    break
                if time() > end_time:
                    self.cluster.ssh.exec([
                        r"for i in $(ps aux | grep java | grep -v bash | "
                        r"grep Client | awk '{print $2}'); do kill -9 $i; done"
                    ])
                    to_delete = find_clients()
                    for node_id in to_delete:
                        if node_id in self.cluster.nodes.keys():
                            self.cluster.kill_node(node_id, ignore_exceptions=True)
                            del self.cluster.nodes[node_id]
                    break
                sleep(1)

    def cluster_state_empty(self):
        """
        Start new clean cluster
        """
        log_print('[cluster state] staring EMPTY', color='yellow')
        self.clean_cluster()
        self.start_grid()
        self.context['clean_cluster_was_here'] = False
        log_print('[cluster state] started EMPTY', color='yellow')

    def _data_changed(self):
        """
        Check history for any data changed tasks

        :return:    True - data in cluster might be changed
                    False - data in cluster not changed at all
        """
        data_changed_operations = [
            'cache_creation',
            'cache_destroy',
            'jdbc',
            'query',
            'put',
            'get',
            'remove',
            'streamer',
            'create_sql_index',
            'drop_sql_index'
        ]
        for item in self.context['history']:
            if all([
                [True for operation in data_changed_operations if operation in item['operations']],
                [a for a in item['action'] if 'leave' in a] or [a for a in item['action'] if 'existed' in a]
            ]):
                return True
        return False

    def was_snapshots(self):
        """
        Check previous run for any snapshot tasks
        Wait for snapshot creation or restore ends
        """
        end_time = time() + 60 * 4
        if len(self.context['history']) < 2:
            return
        if [o for o in self.context['history'][-2]['operations'] if 'snapshot' in o]:
            while True:
                try:
                    with PiClient(self.cluster, self.client_config, new_instance=True,
                                  name='snapshot_wait', exception_print=False,
                                  read_timeout=60 * 4) as piclient:
                        op = create_async_operation(wait_snapshot_operation,
                                                    100,
                                                    gateway=piclient.get_gateway())
                        op.evaluate()

                        while True:
                            if op.getStatus().toString() == "FINISHED":
                                if bool(op.getResult()):
                                    return

                            if time() > end_time:
                                log_print('failed to wait snapshot wait operation finished', color='red')
                                return
                except:
                    log_print('snapshot wait failed', color='red')
                    sleep(5)
                    if time() > end_time:
                        log_print('failed to wait piclient start to wait snapshot execution', color='red')
                        return

    def cluster_state_data(self, logs=True):
        """
        Save previous cluster state
        Restore if some nodes are killed
        Add some data
        Create tables

        :param logs:    log in console
        :return:        setup state
                            True  - cluster started as new
                            False - cluster restored
        """
        tables_count = 10
        tables_rows_count = 100
        put_count = 100
        batch_size = 100

        if logs:
            log_print('[cluster state] staring DATA', color='yellow')

        self.clients_go_away()
        self.was_snapshots()

        # for all alive nodes trying to restore topology
        if [node for node in self.cluster.nodes.values() if node['status'] != NodeStatus.NEW] and not self.context['clean_cluster_was_here']:
            data_changed = self._data_changed()
            try:
                # kill and delete all additional nodes
                nodes_to_kill = [node_id for node_id in self.cluster.nodes.keys() if node_id > 100]
                for node_id in nodes_to_kill:
                    self.cluster.kill_node(node_id, ignore_exceptions=True)
                    self.cluster.cleanup_work_dir(node_id)
                    del self.cluster.nodes[node_id]
                    log_print(f'delete {node_id}')
                if nodes_to_kill:
                    active_nodes = [node for node_id, node in self.cluster.nodes.items() if node_id < 100 and node['status'] == NodeStatus.STARTED]
                    self.cluster.wait_for_topology_snapshot(
                        server_num=len(active_nodes),
                        timeout=80,
                        check_only_servers=True
                    )
                nodes_started = 0
                for node_id, node in self.cluster.nodes.items():
                    # start all killed nodes
                    if node['status'] != NodeStatus.STARTED:
                        if data_changed:
                            # clean node data if data in cluster changed without node
                            self.cluster.cleanup_work_dir(node_id)
                        self.cluster.start_node(node_id, force=True)
                        nodes_started += 1
                if nodes_started == len(self.cluster.nodes):
                    # if all nodes killed already
                    self.cluster.cu.activate()
                    self.context['activate'] = True
                baseline_nodes = self.cluster.cu.get_current_baseline()
                # update BLT if changed
                if len(baseline_nodes) != len(self.cluster.nodes) or \
                        [stat for stat in baseline_nodes.values() if stat != 'ONLINE']:
                    self.cluster.cu.set_current_topology_as_baseline(strict=True, **self.control_util_ssh_options)

                if logs:
                    log_print('[cluster state] started DATA (restored)', color='yellow')

                # add tables
                with PiClient(self.cluster, self.client_config,
                              new_instance=True, name='setup',
                              read_timeout=60 * 10) as piclient:
                    assert create_tables_operation(tables_count, tables_rows_count,
                                                   gateway=piclient.get_gateway()).evaluate(), 'Restore tables data operation has failed'
                return False
            except:
                # clean cluster if something wrong and start from start
                stacktrace = format_exc()
                log_print(stacktrace, color='red')
                self.context['step_failed'] = stacktrace
                try:
                    self.save_fail('restore')
                except:
                    log_print('failed to save restore data')
                    self.context['step_failed'] += f'\n\nFailed to save restore data\n{format_exc()}'
                self.clean_cluster()
        self.start_grid()
        with PiClient(self.cluster, self.client_config,
                      new_instance=True, name='setup', read_timeout=60 * 10) as piclient:
            # put data
            start_counter = self._set_key_iter(put_count)
            operations = []
            caches_names = list(piclient.get_ignite().cacheNames().toArray())
            log_print(f'[setup data] put {put_count} keys in each of {len(caches_names)} caches')
            for cache_name in caches_names:
                op = create_async_operation(create_put_all_operation,
                                            cache_name=cache_name,
                                            start=start_counter,
                                            end=start_counter + put_count,
                                            batch_size=batch_size)
                operations.append(op)
                op.evaluate()
            for op in operations:
                op.getResult()
            # create tables
            log_print(f'[setup data] create {tables_count} tables with {tables_rows_count} rows in each')
            assert create_tables_operation(tables_count, tables_rows_count,
                                           gateway=piclient.get_gateway()).evaluate(), 'Create tables with operations has failed'

        if logs:
            log_print(f'[cluster state] started DATA (as new)', color='yellow')
        return True

    def cluster_state_data_index(self, logs=True):
        """
        Start cluster with data or restore
        Create indexes

        :param logs:    disable logging in console
        """
        if logs:
            log_print('[cluster state] staring DATA INDEX', color='yellow')
        if not self.cluster_state_data(logs=False):
            log_print('[setup data] create indexes')
            with PiClient(self.cluster, self.client_config,
                          new_instance=True, name='setup indexes', read_timeout=60 * 4) as piclient:
                assert create_indexes_operation(100, gateway=piclient.get_gateway()).evaluate(), 'Create index operation has failed'

        if logs:
            log_print(f'[cluster state] started DATA INDEX', color='yellow')

    def cluster_state_data_remove_index(self):
        """
        Start cluster with data or restore
        Create indexes
        Remove index.bin files in work directory
        """
        log_print('[cluster state] staring DATA REMOVED INDEX', color='yellow')
        self.cluster_state_data_index(logs=False)

        for node_id, node in self.cluster.nodes.items():
            log_print('[cluster state] remove indexes from disk')
            self.cluster.ssh.exec([f"rm -f $(find {node['ignite_home']} | grep db | grep index.bin)"])
        log_print(f'[cluster state] started DATA REMOVED INDEX', color='yellow')

    @step(lambda args, kwargs: f'Save fail {args[1]}')
    def step_save_fail(self, *args, **kwargs):
        self.save_fail(*args, **kwargs)

    def save_fail(self, name):
        """
        Save all files from remote host
        :param name:    unique name
        """
        run_dir = join(self.tiden.config['rt']['test_module_dir'], self.current_run_cycle_start.isoformat())
        case_name = f"{self.context['current_number']}_{self.case_name}"
        if not exists(run_dir):
            mkdir(run_dir)
        combine_run_dir = join(run_dir, case_name)

        if not exists(combine_run_dir):
            mkdir(combine_run_dir)
        setup_fail_dir = join(combine_run_dir, name)
        mkdir(setup_fail_dir)

        info_path = join(combine_run_dir, f'info_{name}.txt')
        with open(info_path, 'w') as f:
            f.write(f"code: '{self.context['retry']['all']}'")

        self.download_run_logs()

