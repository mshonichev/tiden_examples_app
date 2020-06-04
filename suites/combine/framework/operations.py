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

from time import sleep, time
from traceback import format_exc

from tiden import log_print, TidenException
from tiden.ignite import Ignite
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_combine_operation
from tiden_gridgain.piclient.piclient import PiClient


class OperationClasses:
    COMPUTE = 'org.apache.ignite.piclient.operations.impl.combination.ComputeOperation'
    CREATE_CACHES = 'org.apache.ignite.piclient.operations.impl.combination.CreateCachesOperation'
    CREATE_INDEXES = 'org.apache.ignite.piclient.operations.impl.combination.CreateIndexesOperation'
    DESTROY_CACHES = 'org.apache.ignite.piclient.operations.impl.combination.DestroyCachesOperation'
    DROP_INDEXES = 'org.apache.ignite.piclient.operations.impl.combination.DropIndexesOperation'
    GET = 'org.apache.ignite.piclient.operations.impl.combination.GetOperation'
    JDBC = 'org.apache.ignite.piclient.operations.impl.combination.JdbcQueryOperation'
    PUT = 'org.apache.ignite.piclient.operations.impl.combination.PutOperation'
    QUERY = 'org.apache.ignite.piclient.operations.impl.combination.QueryOperation'
    REMOVE = 'org.apache.ignite.piclient.operations.impl.combination.RemoveOperation'
    SERVICE = 'org.apache.ignite.piclient.operations.impl.combination.ServiceOperation'
    SNAPSHOT_CREATE = 'org.apache.ignite.piclient.operations.impl.combination.SnapshotCreateOperation'
    SNAPSHOT_RESTORE = 'org.apache.ignite.piclient.operations.impl.combination.SnapshotRestoreOperation'
    STREAMER = 'org.apache.ignite.piclient.operations.impl.combination.StreamerOperation'


class OperationName:
    COMPUTE = 'compute'
    CREATE_CACHES = 'cache_creation'
    CREATE_INDEXES = 'create_sql_index'
    SNAPSHOT_CREATE = 'snapshot_creation'
    SNAPSHOT_RESTORE = 'snapshot_restore'
    DESTROY_CACHES = 'cache_destroy'
    DROP_INDEXES = 'drop_sql_index'
    GET = 'get'
    JDBC = 'jdbc'
    PUT = 'put'
    QUERY = 'query'
    REMOVE = 'remove'
    SERVICE = 'service'
    STREAMER = 'streamer'


class OperationsMixin:

    def __init__(self):
        self.cluster: Ignite = None
        self.context: dict = None
        self.client_config = None
        self.operations = {}
        self.su = None
        self.max_operation_timeout = 60 * 5

    def _run_operation(self, property_name, class_path):
        """
        Run basic operations

        1) Start operation
        2) Wait till starting string appear in piclient logs
        3) Wait till kill command was coming
        4) Kill piclient

        :param property_name:   operation name
        :param class_path:      operation class name
        """
        # handle lock
        try:
            with PiClient(self.cluster,
                          self.client_config,
                          new_instance=True,
                          name=property_name,
                          read_timeout=60 * 10) as piclient:
                # free lock
                self.operations[property_name]['started'] = False
                self.operations[property_name]['kill'] = False
                try:
                    # start async
                    log_print(f'[{property_name}] piclient started, begin operation')
                    async_operation = create_async_operation(create_combine_operation,
                                                             class_path,
                                                             gateway=piclient.get_gateway())
                    async_operation.evaluate()
                    log_print(f'[{property_name}] operation started')

                    if not self.cluster.wait_for_messages_in_log(piclient.node_ids[0],
                                                                 f'{property_name} operation started',
                                                                 timeout=50,
                                                                 fail_pattern=f'(\\[{property_name}\\] Failed|\(err\) Failed)'):
                        # kill operation if not found
                        # or some exception thrown faster
                        log_print(f'Failed to wait {property_name} operation start in logs', color='red')
                        log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                        self.operations[property_name]['kill'] = True
                        self.operations[property_name]['killed'] = True
                        return
                    if not self.operations[property_name]['started']:
                        log_print(f'[{property_name}] failed logs not found')
                        self.operations[property_name]['started'] = True
                except:
                    log_print(format_exc(), color='red')
                    self.operations[property_name]['kill'] = True
                    self.operations[property_name]['killed'] = True
                    log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                    return

                end_time = time() + self.max_operation_timeout
                while True:
                    # wait for kill
                    if self.operations[property_name]['kill']:
                        self.operations[property_name]['killed'] = True
                        log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                        return
                    if time() > end_time:
                        if self.operations.get(property_name):
                            self.operations[property_name]['killed'] = True
                        log_print(f'[operation] {property_name.upper()} timeout kill', color='yellow')
                        return
                    sleep(0.5)
        except:
            log_print(f'[{property_name}] fail happened, killing operation')
            if self.operations.get(property_name):
                self.operations[property_name]['kill'] = True
                self.operations[property_name]['killed'] = True
            raise

    def operation_compute(self):
        self._run_operation(OperationName.COMPUTE, OperationClasses.COMPUTE)

    def operation_cache_creation(self):
        self._run_operation(OperationName.CREATE_CACHES, OperationClasses.CREATE_CACHES)

    def operation_create_sql_index(self):
        self._run_operation(OperationName.CREATE_INDEXES, OperationClasses.CREATE_INDEXES)

    def operation_destroy_caches(self):
        self._run_operation(OperationName.DESTROY_CACHES, OperationClasses.DESTROY_CACHES)

    def operation_drop_sql_index(self):
        self._run_operation(OperationName.DROP_INDEXES, OperationClasses.DROP_INDEXES)

    def operation_get(self):
        self._run_operation(OperationName.GET, OperationClasses.GET)

    def operation_jdbc(self):
        self._run_operation(OperationName.JDBC, OperationClasses.JDBC)

    def operation_put(self):
        self._run_operation(OperationName.PUT, OperationClasses.PUT)

    def operation_query(self):
        self._run_operation(OperationName.QUERY, OperationClasses.QUERY)

    def operation_remove(self):
        self._run_operation(OperationName.REMOVE, OperationClasses.REMOVE)

    def operation_service(self):
        self._run_operation(OperationName.SERVICE, OperationClasses.SERVICE)

    def operation_snapshot_creation(self):
        self._run_operation(OperationName.SNAPSHOT_CREATE, OperationClasses.SNAPSHOT_CREATE)

    def operation_snapshot_restore(self):
        """
        Restore data from random snapshot
        Start operations only after other exceptions was started
        """
        property_name = OperationName.SNAPSHOT_RESTORE
        self.operations[property_name]['started'] = False
        self.operations[property_name]['kill'] = False
        done = False
        while True:
            if done:
                break
            keys = list(self.operations.keys())
            for k in keys:
                if self.operations[property_name]['kill']:
                    return
                if k != property_name and self.operations[k].get('started'):
                    done = True
        self._run_operation(property_name, OperationClasses.SNAPSHOT_RESTORE)

    def operation_streamer(self):
        self._run_operation(OperationName.STREAMER, OperationClasses.STREAMER)

    def operation_deactivation(self):
        """
        deactivate cluster one time
        Start operations only after other exceptions was started
        """
        property_name = 'deactivation'
        self.operations[property_name]['started'] = False
        self.operations[property_name]['kill'] = False
        test_dir = self.tiden.config['rt']['remote']['test_dir']
        set_log_path = f"{test_dir}/deactivation_log.log"
        while True:
            keys = list(self.operations.keys())
            for k in keys:
                if not self.operations.get(property_name):
                    log_print(f'[operation] {property_name.upper()} kill already killed', color='yellow')
                    return
                if self.operations[property_name]['kill']:
                    return
                if len(keys) == 1 or (k != property_name and self.operations[k].get('started')):
                    self.operations[property_name]['started'] = True
                    self.cluster.cu.deactivate(log=set_log_path)
                    self.context['activate'] = False
                    self.operations[property_name]['killed'] = True
                    log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                    return
            sleep(1)

    def operation_activation(self):
        """
        Deactivate and activate grid
        Start operations only after other exceptions was started
        """
        property_name = 'activation'
        self.operations[property_name]['started'] = False
        self.operations[property_name]['kill'] = False
        test_dir = self.tiden.config['rt']['remote']['test_dir']
        set_log_path_activation = f"{test_dir}/activation_log.log"
        set_log_path_deactivation = f"{test_dir}/activation_deactivation_log.log"
        while True:
            keys = list(self.operations.keys())
            for k in keys:
                if not self.operations.get(property_name):
                    log_print(f'[operation] {property_name.upper()} kill already killed', color='yellow')
                    return
                if self.operations[property_name]['kill']:
                    return
                if len(keys) == 1 or (k != property_name and self.operations[k].get('started')):
                    self.cluster.cu.deactivate(log=set_log_path_deactivation)
                    self.operations[property_name]['started'] = True
                    self.context['activate'] = False
                    self.cluster.cu.activate(log=set_log_path_activation)
                    self.context['activate'] = True
                    if 20001 in list(self.cluster.nodes.keys()):
                        while not self.cluster.wait_for_messages_in_log(20001, 'Received activate request', timeout=2):
                            if self.operations[property_name]['kill']:
                                break
                    self.operations[property_name]['killed'] = True
                    log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                    return
            sleep(1)

    def operation_baseline(self):
        """
        Update baseline in cycle
        """
        property_name = 'baseline'
        self.operations[property_name]['started'] = False
        self.operations[property_name]['kill'] = False
        test_dir = self.tiden.config['rt']['remote']['test_dir']
        set_log_path = f"{test_dir}/set_baseline_join.log"
        get_log_path = f"{test_dir}/get_baseline_version.log"
        fail_counter = 0
        end_time = time() + self.max_operation_timeout
        while True:
            try:
                curr_top_version = self.cluster.cu.get_current_topology_version(
                    log=get_log_path,
                    show_output=False,
                    ssh_options={'timeout': 60 * 3}
                )

                if not curr_top_version:
                    raise TidenException('Failed to get current topology version')

                if not self.operations[property_name]['started']:
                    self.operations[property_name]['started'] = True
                    log_print(f'[operation] {property_name.upper()} started', color='yellow')

                self.cluster.cu.set_baseline(curr_top_version,
                                             log=set_log_path,
                                             show_output=False,
                                             output_limit=100,
                                             ssh_options={'timeout': 60 * 3})
            except:
                fail_counter += 1
                log_print(format_exc(), color='red')
                if fail_counter > 10:
                    self.operations[property_name]['kill'] = True
            finally:
                if time() > end_time:
                    log_print(f'[operation] {property_name.upper()} timeout kill', color='yellow')
                    if self.operations.get(property_name):
                        self.operations[property_name]['killed'] = True
                    return
                if not self.operations.get(property_name):
                    log_print(f'[operation] {property_name.upper()} kill already killed', color='yellow')
                    return
                if self.operations[property_name]['kill']:
                    log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                    self.operations[property_name]['killed'] = True
                    return
                sleep(1)

    def operation_none(self):
        property_name = 'none'
        self.operations[property_name]['started'] = True
        self.operations[property_name]['kill'] = False
        log_print(f'[operation] {property_name.upper()} started', color='yellow')
        while True:
            if self.operations[property_name]['kill']:
                self.operations[property_name]['killed'] = True
                log_print(f'[operation] {property_name.upper()} kill', color='yellow')
                return
            sleep(0.5)

