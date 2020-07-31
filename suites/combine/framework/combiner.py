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

from copy import deepcopy
from datetime import datetime
from os import mkdir, environ
from os.path import join, exists, dirname, basename
from threading import Thread
from time import sleep, time
from traceback import format_exc
from uuid import uuid4

from requests import post, get

from tiden import log_print, TidenException
from tiden.case.apptestcase import AppTestCase
from tiden.ignite import Ignite

from suites.combine.framework.actions import ActionsMixin
from suites.combine.framework.assert_results import AssertResultsMixin
from suites.combine.framework.common import step, RunType
from suites.combine.framework.operations import OperationsMixin
from suites.combine.framework.setup_cluster import SetupStateMixin
from suites.combine.framework.setup_runs import SetupMixin


class CombineTestCase(AppTestCase, OperationsMixin, ActionsMixin, SetupStateMixin, SetupMixin, AssertResultsMixin):

    def __init__(self, *args):
        SetupMixin.__init__(self)
        OperationsMixin.__init__(self)
        ActionsMixin.__init__(self)
        SetupStateMixin.__init__(self)
        AssertResultsMixin.__init__(self)
        AppTestCase.__init__(self, *args)
        self.add_app('ignite')
        self.control_util_ssh_options = {'ssh_options': {'timeout': 60 * 3}}
        self.cluster: Ignite = None
        self.server_config = None
        self.client_config = None
        self.operations = {}
        self.expected_operations = []
        self.runs = []
        self.end_time = None
        self.current_run = None
        self.current_run_cycle_start = None
        self.current_code = None
        self.run_name = None
        join_common = [
            'Failed to wait for partition release future',
            'Failed to send message.+isClient\=true'
        ]
        leave_common = [
            'Connection reset by peer',
            'Failed to send message \(node left topology\)',
            'Failed to wait for partition release future',
            'Failed to wait for partition map exchange',
            "Failing client node due to not receiving metrics updates from client node within \'IgniteConfiguration.clientFailureDetectionTimeout\'",
            'Failed to map keys for cache \(all partition nodes left the grid\)',
            'Failed to acquire lock for keys \(primary node left grid',
            'Failed over job to a new node',
            'Failed to execute SQL query',
            'Failed to execute DML statement',
            'Joining node has caches with data which are not presented on cluster, it could mean that they were already destroyed, to add the node to cluster - remove directories with the caches',
            'Failed to find server node for cache \(all affinity nodes have left the grid or cache was stopped\)',
            'Failed to calculate set of changed pages for incremental snapshot, full page set will be written to snapshot instead',
            '\[diagnostic\] Failed to wait for partition map exchange'
            'Failed to send message \(node may have left the grid or TCP connection cannot be established due to firewall issues\)',
            'Failed to connect to node \(is node still alive\?\)',
            'The query was cancelled while executing',
            'Failed to communicate with Ignite cluster',
            'Transaction has been rolled back',
            'SQLException: Failed to connect to server',
            'Failed to send message to remote node',
            'Failed to complete snapshot operation \(\d+ nodes left topology during snapshot operation',
            'all partition owners have left the grid, partition data has been lost.+\|b(0|1)\|'
            "Table doesn't exist",
            'Failed to write to cache \(cache is moved to a read-only state\)',
            'Failed to execute cache operation \(all partition owners have left the grid, partition data has been lost\)',
            'Finished waiting for partition release future',
            'Failed to map keys to nodes \(partition is not mapped to any node\)',
            'Data streamer has been closed',
            'Failed to send message because node left grid',
            'Failed to acquire lock within provided timeout for transaction'
        ]
        baselines_common = [
            'Changing BaselineTopology on inactive cluster is not allowed',
            'Failed to start/stop cache, cluster state change is in progress'
        ]
        self.context = {
            'clean_cluster_was_here': False,
            'activate': True,
            'grid': {
                'tables': False,
                'indexes': False
            },
            'history': [],
            'options': {
                'actions': {
                    'nodes_leave': {'nodes_count': 1},
                    'nodes_leave_lp': {'nodes_count': 2},
                    'nodes_leave_baseline': {'nodes_count': 1},
                    'nodes_leave_baseline_lp': {'nodes_count': 2},
                    'nodes_join': {'nodes_count': 1},
                    'nodes_join_many': {'nodes_count': 2},
                    'nodes_join_baseline': {'nodes_count': 1},
                    'nodes_join_baseline_many': {'nodes_count': 2},
                    'nodes_join_existed': {'nodes_count': 1},
                    'nodes_join_existed_empty': {'nodes_count': 1},
                    'nodes_join_existed_removed_indexes': {'nodes_count': 1},
                    'nodes_join_existed_baseline': {'nodes_count': 1},
                    'nodes_join_existed_empty_baseline': {'nodes_count': 1},
                    'nodes_join_existed_remove_indexes_baseline': {'nodes_count': 1},
                },
                'allowed_exceptions': {
                    'verify': [],
                    'nodes_leave': [],
                    'nodes_join': [],
                    'common': [
                        'Failing client node due to not receiving metrics updates',
                        '\[diagnostic\] Failed to wait for partition map exchange',
                        'Failed to send message to remote node.+node_1_2000.+',
                        'Failed to send message to next node',
                        'Failed to calculate set of changed pages for incremental snapshot',
                        'Failed to write to cache \(cache is moved to a read-only state\)'
                    ],
                    'deactivation': [
                        'Failed to update keys',
                        'Can not perform the operation because the cluster is inactive',
                        'Failed to start/stop cache, cluster state change is in progress',
                        'Cluster is not active'
                    ],
                    'snapshot_creation': [
                        "Can't create incremental snapshot: last full",
                        'creation of incremental snapshot is not possible for this cache group'
                    ],
                    'snapshot_restore': [
                        'Cache is restarting:\w+, you could wait restart completion with restartFuture',
                        'Checking for the presence of all partitions failed',
                        '\[GridCacheProcessor\] Failed to wait proxy initialization'
                    ],
                    'activation': [
                        'Failed to perform cache operation \(cache is stopped\)',
                        'Failed to update keys',
                        'Can not perform the operation because the cluster is inactive',
                        'Failed to start/stop cache, cluster state change is in progress',
                        'Cluster is not active'
                    ],
                    'cache_destroy': [
                        'Failed to get page store for the given cache ID'
                    ],
                    'baseline': [
                        'Joining node during caches restart is not allowed',
                        'has to be merged which is impossible on active grid',
                    ],
                    'nodes_leave_baseline'
                    '+nodes_leave_baseline_lp'
                    '+nodes_join_baseline'
                    '+nodes_join_baseline_many'
                    '+baseline'
                    '+snapshot_creation'
                    '+snapshot_restore': [
                        'Concurrent snapshot operations are not allowed cluster-wide'
                    ],
                    'service+deactivation': [
                        'Failed to deploy service, cluster in active'
                    ]
                },
                'ignore_combinations': {
                    'snapshot_restore': ['nodes_join',
                                         'nodes_join_many',
                                         'nodes_join_baseline',
                                         'nodes_join_baseline_many',
                                         'nodes_join_existed',
                                         'nodes_join_existed_empty',
                                         'nodes_join_existed_removed_indexes',
                                         'nodes_join_existed_baseline',
                                         'nodes_join_existed_empty_baseline',
                                         'nodes_join_existed_remove_indexes_baseline',
                                         'deactivation',
                                         'activation'],
                    'activation': ['deactivation'],
                },
            }
        }

        existed_keys = ['nodes_join_existed',
                        'nodes_join_existed_empty',
                        'nodes_join_existed_removed_indexes',
                        'nodes_join_existed_baseline',
                        'nodes_join_existed_empty_baseline',
                        'nodes_join_existed_remove_indexes_baseline']

        for key in ['nodes_leave', 'nodes_leave_lp', 'nodes_leave_baseline', 'nodes_leave_baseline_lp'] + existed_keys:
            self.context['options']['allowed_exceptions'][key] = self.context['options']['allowed_exceptions'].get(key, []) + leave_common
        for key in ['nodes_join', 'nodes_join_many', 'nodes_join_baseline', 'nodes_join_baseline_many'] + existed_keys:
            self.context['options']['allowed_exceptions'][key] = self.context['options']['allowed_exceptions'].get(key, []) + join_common
        for key in [k for k in self.context['options']['allowed_exceptions'].keys() if 'baseline' in k]:
            self.context['options']['allowed_exceptions'][key] = self.context['options']['allowed_exceptions'].get(key, []) + baselines_common

        self.times = []

        self.operations_methods = {
            'none': self.operation_none,
            'compute': self.operation_compute,
            'cache_creation': self.operation_cache_creation,
            'create_sql_index': self.operation_create_sql_index,
            'cache_destroy': self.operation_destroy_caches,
            'drop_sql_index': self.operation_drop_sql_index,
            'get': self.operation_get,
            'jdbc': self.operation_jdbc,
            'put': self.operation_put,
            'query': self.operation_query,
            'remove': self.operation_remove,
            'service': self.operation_service,
            'snapshot_creation': self.operation_snapshot_creation,
            'snapshot_restore': self.operation_snapshot_restore,
            'streamer': self.operation_streamer,
            # ignore for a while to avoid false interfering
            # 'deactivation': self.operation_deactivation,
            # 'activation': self.operation_activation,
            'baseline': self.operation_baseline,
        }
        self.cluster_setup_methods = {
            'empty': self.cluster_state_empty,
            'data': self.cluster_state_data,
            'data_index': self.cluster_state_data_index,
            'data_remove_index': self.cluster_state_data_remove_index,
        }
        self.action_methods = {
            'stable': self.state_stable,
            'nodes_leave': self.state_nodes_leave,
            'nodes_leave_lp': self.state_nodes_leave_lp,
            'nodes_leave_baseline': self.state_nodes_leave_baseline,
            'nodes_leave_baseline_lp': self.state_nodes_leave_baseline_lp,
            'nodes_join': self.state_nodes_join,
            'nodes_join_many': self.state_nodes_join_many,
            'nodes_join_baseline': self.state_nodes_join_baseline,
            'nodes_join_baseline_many': self.state_nodes_join_baseline_many,
            'nodes_join_existed': self.state_nodes_join_existed,
            'nodes_join_existed_empty': self.state_nodes_join_existed_empty,
            'nodes_join_existed_removed_indexes': self.state_nodes_join_existed_removed_indexes,
        }

    def setup(self):
        log_print('create ignite app')
        self.create_app_config_set(Ignite, 'base',
                                   disabled_cache_configs=True,
                                   logger=True,
                                   logger_path=f"{self.tiden.config['rt']['remote']['test_module_dir']}/ignite-log4j2.xml",
                                   caches='caches.xml',
                                   snapshots_enabled=True,
                                   pitr_enabled=True,
                                   deploy=True)
        log_print('setup apps')
        AppTestCase.setup(self)
        self.cluster = self.get_app('ignite')

        self.run_name = self.tiden.config.get('run_name', self.current_time_pretty)
        self.runs = [{}]
        log_print('setup runs')
        self._setup_runs()
        self.context['suite_name'] = self.current_time_pretty
        log_print(f'Runs: {len(self.runs)}')

    @property
    def case_name(self):
        return '_'.join(['_'.join([i for i in self.current_run[item]]) for item in ['operations', 'setup_cluster', 'action']])

    def runner(self, run: dict, cycle_start: datetime, order_idx: int, run_type: RunType):
        """
        Run case
            1) setup data
            2) run operations
            3) execute action with cluster
            4) assert results
            5) stop operations
            6) send run report

        :param run:         case operations, action, cluster state
        :param cycle_start: time when last cluster clean was made
        :param order_idx:       order index
        """
        start = round(time())
        to_add = deepcopy(run)
        self.context['history'].append(to_add)
        run_config = deepcopy(run)

        if self.context['clean_cluster_was_here'] or "empty" in run_config["setup_cluster"]:
            # create new section if cluster was cleared
            self.context['suite_name'] = self.current_time_pretty

        run_config['start'] = datetime.now()
        self.current_run = run_config
        self.current_run_cycle_start = cycle_start
        self.print_info(to_add, cycle_start, order_idx, run_type)
        self.setup_report_data(run_config)
        self.clean_redundant_logs()
        operations_stopped = False
        try:
            try:
                try:
                    self.setup_cluster_state(run_config['setup_cluster'])
                    if len(self.context['history']) == 0:
                        self.context['history'].append(to_add)
                    run_config['start'] = datetime.now()
                    self.clean_redundant_logs()
                    self.setup_operations(run_config['operations'])
                    self.execute_action(run_config['action'])
                except:
                    self.context['report']['status'] = 'failed'
                    self.step_save_fail('execution')
                    raise
                finally:
                    self.assert_results(self.get_allowed_asserts(run), cycle_start, run_config['start'],
                                        f"{self.context['current_number']}_{self.case_name}")
            except:
                self.context['report']['status'] = 'failed'
                log_print(format_exc(), color='red')
                self.stop_operations()
                operations_stopped = True
                self.clean_cluster_step()
            finally:
                if not operations_stopped:
                    self.stop_operations()
            self.times.append(round(time()) - start)
            if self.context['history'] and not self.context['activate']:

                @step(lambda a, b: 'Activate cluster after')
                def activate(s):
                    self.cluster.cu.activate()

                try:
                    activate(self)
                    self.context['activate'] = True
                except:
                    log_print(format_exc(), color='red')
                    self.step_save_fail('activate')
                    self.clean_cluster_step()
            # if all([
            #     order_idx % 8 == 1,
            #     not self.context['clean_cluster_was_here'],
            #     not self.tiden.config['combinations'].get('code')
            # ]):
            #     run_dir = self.ensure_dir(join(self.tiden.config['rt']['test_module_dir'], cycle_start.isoformat()))
            #     verify_check_dirname = f'{self.context["current_number"]}_verify_check'
            #     verify_check_dir = self.ensure_dir(join(run_dir, verify_check_dirname))
            #     time_before_verity = datetime.now()
            #     try:
            #         self.execute_cache_check(verify_check_dir)
            #     finally:
            #         patterns = []
            #         for pattern in self.context['options']['allowed_exceptions']['verify']:
            #             patterns.append({'method': self.assert_pattern, 'args': [pattern]})
            #         self.assert_results(patterns, cycle_start, time_before_verity, verify_check_dirname)

        finally:
            if run_type == RunType.SERVER_BASE_WORKER:
                if not self.send_test_pass():
                    return
            self.send_data()

    def ensure_dir(self, path):
        if not exists(path):
            mkdir(path)
        return path

    def send_data(self):
        """
        Send run report to WARD
        """
        if not self.tiden.config['environment'].get('report_url'):
            return

        if not self.context['report'].get('status'):
            # check steps for any failed step
            if [step_item for step_item in self.context['report']['steps'] if step_item.get('status') == 'failed']:
                self.context['report']['status'] = 'failed'
            else:
                self.context['report']['status'] = 'passed'

        # set end time
        self.context['report']['time']['end'] = self.current_time
        self.context['report']['time']['end_pretty'] = self.current_time_pretty
        diff = self.context['report']['time']['end'] - self.context['report']['time']['start']
        diff_seconds = diff // 1000
        if diff_seconds > 60:
            self.context['report']['time']['diff'] = f'{diff_seconds // 60}m {diff_seconds % 60}s'
        else:
            self.context['report']['time']['diff'] = f'{diff_seconds}s'

        try:
            # send attachments
            base = self.tiden.config['environment']['report_url']
            if self.tiden.config['combinations'].get('code'):
                middle_suite_name = ['Code retry',
                                     self.tiden.config['artifacts']['ignite']['gridgain_version'],
                                     self.run_name]
            else:
                middle_suite_name = [self.tiden.config['combinations'].get('suite_name',
                                                                           self.tiden.config['artifacts']['ignite']['gridgain_version']),
                                     self.run_name,
                                     self.tiden.config['environment']['server_hosts'][0]]

            self.context['report']['suites'] = ['Combiner', *middle_suite_name,
                                                self.context['suite_name']]
            log_print(f'test sent')
            send_res = post(f'{base}/add_test', json=self.context['report'])
            assert send_res.status_code == 200, f'Failed to add: {self.context["report"]["name"]} from {send_res.content}'
        except:
            log_print(f'Failed to send report:\n{format_exc()}', color='red')

    @property
    def current_time_pretty(self):
        current_time = datetime.now().isoformat().replace('T', ' ')
        index = current_time.rindex('.') if '.' in current_time else len(current_time)
        return current_time[:index].replace(' ', '   ')

    @property
    def current_time_pretty_server(self):
        date_time = datetime.now().isoformat().replace('T', ' ')
        index = date_time.rindex('.') if '.' in date_time else len(date_time)
        date_time = date_time[:index]
        date_time = f'{date_time} {self.tiden.config["environment"]["server_hosts"][0]}'
        return date_time

    @property
    def current_time(self):
        return round(time() * 1000)

    @step(lambda args, kwargs: f'Setup {" ".join(args[1])}')
    def setup_cluster_state(self, states):
        for s in states:
            log_print(f'[cluster state] {s} starting')
            self.cluster_setup_methods[s]()
            log_print(f'[cluster state] {s} ended')
        self.context['clean_cluster_was_here'] = False

    @step(lambda args, kwargs: 'Setup operations')
    def setup_operations(self, operations: list):
        """
        Start all operations and wait when they being running
        """
        steps = []
        try:
            for operation in operations:
                steps.append({
                    'name': operation,
                    'time': {'start': self.current_time,
                             'start_pretty': self.current_time_pretty}
                })
                log_print(f'[operation] {operation.upper()} starting', color='yellow')
                # start
                t = Thread(target=self.operations_methods[operation])
                self.operations[operation] = {'thread': t}
                t.start()

            end_time = time() + 60
            while True:
                started_operations = [(name, op) for name, op in self.operations.items() if op.get('started', False)]
                for name, started_operation in started_operations:
                    idx = [idx for idx, _step in enumerate(steps) if _step['name'] == name][0]
                    # wait for operation being running
                    if steps[idx].get('status'):
                        continue
                    # step stuff
                    steps[idx]['status'] = 'passed'
                    steps[idx]['stacktrace'] = ''
                    steps[idx]['time']['end'] = self.current_time
                    steps[idx]['time']['end_pretty'] = self.current_time_pretty
                    diff = steps[idx]["time"]["end"] - steps[idx]["time"]["start"]
                    steps[idx]['time']['diff'] = f'{diff // 1000}s'

                # all operations started
                if len(started_operations) == len(operations):
                    return

                if [op for op in self.operations.values() if op.get('killed', False)] or end_time < time():
                    # if operations killed before start or long time not started
                    for idx, step in enumerate(deepcopy(steps)):
                        if not step.get('status'):
                            steps[idx]['status'] = 'failed'
                            steps[idx]['time']['end'] = self.current_time
                            steps[idx]['time']['end_pretty'] = self.current_time_pretty
                            diff = steps[idx]["time"]["end"] - steps[idx]["time"]["start"]
                            steps[idx]['time']['diff'] = f'{diff // 1000}s'
                            steps[idx]['stacktrace'] = f'Failed to wait for operations start: \n{format_exc()}\n' \
                                                       f'Operations: {operations}\n' \
                                                       f'Started: {started_operations}'
                    raise TidenException(f'Failed to wait for operations start'
                                         f'Operations: {operations}\n'
                                         f'Started: {started_operations}')
                sleep(0.5)
        finally:
            self.context['report']['children'] = steps

    @step(lambda args, kwargs: f'Action {" -> ".join(args[1])}')
    def execute_action(self, state):
        for s in state:
            log_print(f'[state] {s} starting', color='yellow')
            split_state = s.split('#')
            self.context['temp_action_options'] = split_state[1:] if len(split_state) > 1 else []
            self.action_methods[split_state[0]]()
            log_print(f'[state] {s} ended', color='yellow')

    @step(lambda args, kwargs: 'Stop operations')
    def stop_operations(self):
        for name, operation in self.operations.items():
            log_print(f'stop {name} operation')
            self.operations[name]['kill'] = True
        end_time = time() + 100
        while len([k for k, v in self.operations.items() if v.get('killed', False)]) != len(self.operations):
            sleep(1)
            if time() > end_time:
                log_print('kill operations', color='red')
                break
        for operation in self.operations.values():
            t: Thread = operation['thread']
            t.join()
        self.operations = {}
        log_print('operations stopped', color='yellow')

    @step(lambda a, b: 'Verify caches')
    def execute_cache_check(self, verify_check_dir):
        """
        Run control.sh idle verify command on existed cluster
        """
        name = 'verify.log'
        host = self.tiden.ssh.hosts[0]
        remote_log_path = join(self.tiden.config['rt']['remote']['test_dir'], name)
        local_log_path = join(verify_check_dir, name)
        skip_download = False
        try:
            self.cluster.cu.idle_verify_dump(key_dump=False, log=remote_log_path, output_limit=300)
        except Exception as e:
            stacktrace = format_exc()
            log_print(stacktrace, color='red')
            self.context['step_failed'] = f'Caches verification is failed\n{format_exc()}'
            if 'Not found running server nodes' in stacktrace:
                skip_download = True
        finally:
            # add logs in any case
            if skip_download:
                return
            try:
                self.cluster.ssh.download_from_host(host, remote_log_path, local_log_path)
                with open(local_log_path) as file:
                    fail_message = ''.join(file.readlines())
                    if 'no conflicts have been found' not in fail_message:
                        self.context['step_failed'] = self.context.get('step_failed', '') + f'\nFound conflicts!\n{fail_message}'
                self.context['attachments'].append({
                    'name': 'log',
                    'source': local_log_path,
                    'type': 'file'
                })
            except:
                self.context['step_failed'] = self.context.get('step_failed', '') + f'\nDownload failed!\n{format_exc()}'

    def download_run_logs(self):
        log_print('download logs')

        logs_dir = self.tiden.config['rt']['remote']['test_dir']
        files = []
        commands = dict([[host, [f"ls {logs_dir} | grep .log"]] for host in self.tiden.ssh.hosts])
        items_list = self.cluster.ssh.exec(commands)
        for host in self.tiden.ssh.hosts:
            files.append([host, [f'{logs_dir}/{file}' for file in items_list[host][0].split('\n') if file]])

        log_print('downloaded')
        files_receiver_url = self.tiden.config['environment'].get('report_files_url')
        if files_receiver_url:
            for host, paths in files:
                for file_path in paths:
                    base_filename = basename(file_path)
                    file_name = f'{uuid4()}_{base_filename}'
                    self.tiden.ssh.exec_on_host(
                        host,
                        [f'cd {dirname(file_path)}; '
                         f'curl -H "filename: {file_name}" '
                         f'-F "file=@{base_filename};filename={base_filename}" '
                         f'{files_receiver_url}/files/add']
                    )
                    self.context['attachments'].append({
                        'name': base_filename,
                        'source': file_name,
                        'type': 'file'
                    })

    def file_exist_in_steps(self, file_name):
        for step_item in self.context['report'].get('steps', []):
            for attachment in step_item.get('attachments', []):
                if attachment['name'] == file_name:
                    return True
        return False

    def clean_redundant_logs(self):
        """
        Clean all unused log files on remote host
        """

        logs = [node['log'] for node_id, node in self.cluster.nodes.items() if node.get('log') and node_id < 100]
        command_to_delete = {}
        remote_logs_dir = self.tiden.config['rt']['remote']['test_dir']
        for host, results in self.cluster.ssh.exec([f'ls {remote_logs_dir} | grep .log']).items():
            if len(results) == 0:
                continue
            files = results[0].split('\n')
            files = [f'{remote_logs_dir}/{file}' for file in files]
            files_to_delete = ' '.join([file for file in files if file not in logs and file.endswith('.log')])
            if files_to_delete:
                command_to_delete[host] = [f"rm -f {files_to_delete}"]
        self.cluster.ssh.exec(command_to_delete)

    def print_info(self, run, cycle_start, order_idx, run_type: RunType):
        """
        Print code, time, expected time to end, average time in console log

        :param run:         current case
        :param cycle_start: time of latest cluster clean
        """
        if run_type == RunType.BASIC_WORKER:
            current_number = self.runs.index(run) + 1
            count = len(self.runs)
        else:
            current_number = order_idx + 1
            count = 'inf'

        self.context['current_number'] = current_number

        retry_one_code = self.code([self.context["history"][-1]])
        retry_all_code = self.code(self.context["history"])
        self.context['retry'] = {
            'one': retry_one_code,
            'all': retry_all_code
        }

        sum_times = sum(self.times)
        sum_times = sum_times if sum_times else 3 * 60
        times_count = len(self.times) if len(self.times) else 1
        avg_sec = round(sum_times / times_count)
        # percent of passed cases
        if run_type == RunType.BASIC_WORKER:
            log_print(f"{current_number}/{count} {round(current_number * 100 / count, 2)}%", color='pink')
        else:
            log_print(f"{current_number}/{count}", color='pink')
        # code to retry from latest cluster clean
        log_print(f'from latest clean: {retry_all_code}', color='pink')
        # code to retry current case
        log_print(f'exact one: {retry_one_code}')

        if run_type == RunType.BASIC_WORKER:
            diff = round((datetime.now() - cycle_start).total_seconds())
            to_end = (len(self.runs) + 1 - current_number) * avg_sec
            log_print(f'avg: {avg_sec // 60} min {avg_sec % 60} sec '
                      f'| passed: {diff // 60 // 60} hours {diff // 60 % 60} min {diff % 60} sec '
                      f'| end after: {to_end // 60 // 60} hours {to_end // 60 % 60} min {to_end % 60} sec',
                      color='pink')

        # case pretty print
        log_print(f'setup cluster')
        for op in run["setup_cluster"]:
            log_print(f"  {op}")
        log_print('operations')
        for op in run["operations"]:
            log_print(f'  {op}')
        log_print(f'action')
        for op in run["action"]:
            log_print(f'  {op}')

    def setup_report_data(self, run_config):
        """
        Setup report structure for report framework
        """

        operations = ', '.join([op for op in run_config['operations']])
        action = ', '.join([op for op in run_config['action']])
        setup_cluster = ', '.join([op for op in run_config['setup_cluster']])

        case = {'operations': run_config['operations'],
                'cluster_setup': run_config['setup_cluster'],
                'action': run_config['action']}
        host_num = self.tiden.config['environment']["server_hosts"][0][self.tiden.config['environment']["server_hosts"][0].rindex('.') + 1:]
        self.context['report'] = {
            'title': f'{self.context["current_number"]}. ({host_num}) op: {operations} st: {setup_cluster} ac: {action}',
            'test_case_id': 0,
            'run_id': str(uuid4()),
            'time': {
                'start': round(run_config['start'].timestamp() * 1000),
                'start_pretty': run_config['start'].isoformat().replace('T', ' ')},
            'data': {
                'jenkins_build_url': environ.get("BUILD_URL")
            },
            'steps': [],
            'case': case,
            'description': f'Code from latest cluster restart: {self.context["retry"]["all"]}'
                           f'\n\nCase code: {self.context["retry"]["one"] if "empty" not in run_config["setup_cluster"] else ""}',
            'retry_options': [
                {'type': 'selector',
                 'label': 'Retry',
                 'name': 'code',
                 'options': [{'title': 'from latest full cluster restart',
                              'value': self.context["retry"]["all"]},
                             {'title': 'only this case',
                              'value': self.context["retry"]["one"]}]},
                {'type': 'text_field',
                 'label': 'Cluster version',
                 'name': 'version'},
                {'type': 'text_field',
                 'label': 'Host number',
                 'name': 'host_num'},
                {'type': 'text_field',
                 'label': 'Jenkins login',
                 'name': 'user'},
                {'type': 'text_field',
                 'label': 'Jenkins token',
                 'name': 'token'}
            ]
        }

    def get_new_test(self):
        seed = self.tiden.config['seed']
        res = get(f"{self.tiden.config['combinations']['seeds_server']}/get_test",
                  params={'seed': seed})
        assert res.status_code == 200, f'Failed to get new test with seed {seed}: {res.reason}'
        response = res.json()
        test_number = response['test_number']
        self.context['test_number'] = test_number
        if test_number is None:
            return None
        else:
            self.run_name = response['run_name']
            return self.runs[int(test_number)]

    def send_test_pass(self):
        seed = self.tiden.config['seed']
        res = post(f"{self.tiden.config['combinations']['seeds_server']}/test_passed",
                   json={
                       'seed': seed,
                       'test_number': self.context['test_number']
                   })
        assert res.status_code in [500, 200], f'Failed to send test result: {res.reason}'
        return res.status_code == 200

