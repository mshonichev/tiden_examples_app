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

from tiden.apps import NodeStatus
from tiden.apps.ignite import Ignite
from tiden import log_put, log_print, with_setup, repeated_test, util_sleep_for_a_while
from tiden.case.apptestcase import AppTestCase
from tiden_gridgain.piclient.helper.cache_utils import IgniteCacheConfig
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_put_all_operation, TxDescriptor
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils


class TestCacheDestroy(AppTestCase):
    main_utils = None

    client_config = None
    server_config = None

    def __init__(self, *args):
        super().__init__(*args)

        for name in args[0]['artifacts'].keys():
            if args[0]['artifacts'][name].get('type', '') == 'ignite':
                self.add_app(
                    name,
                    app_class_name='ignite',
                )

                self.ignite_name = name
                break

    def setup(self):
        self.create_app_config_set(Ignite, 'base',
                                   snapshots_enabled=False,
                                   caches_list_file='caches.xml',
                                   logger=True,
                                   logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                       'test_module_dir'],
                                   disabled_cache_configs=False,
                                   zookeeper_enabled=False)

        super().setup()

        self.server_config = Ignite.config_builder.get_config('server', config_set_name='base')
        self.client_config = Ignite.config_builder.get_config('client', config_set_name='base')

    def setup_testcase(self):
        pass

    def teardown_testcase(self):
        for ignite in self.get_app_by_type('ignite'):
            ignite.kill_nodes()
            ignite.delete_lfs()

            log_put("Cleanup Ignite LFS ... ")
            commands = {}
            for node_idx in ignite.nodes.keys():
                host = ignite.nodes[node_idx]['host']
                if commands.get(host) is None:
                    commands[host] = [
                        'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                    ]
                else:
                    commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
            results = self.tiden.ssh.exec(commands)
            print(results)
            log_put("Ignite LFS deleted.")
            log_print()

    def start_ignite_grid(self, name, activate=False, already_nodes=0, config_set='base', jvm_options=None):
        app = self.get_app(name)
        app.set_node_option('*', 'config',
                            Ignite.config_builder.get_config('server', config_set_name=config_set))

        if jvm_options:
            app.set_node_option('*', 'jvm_options', jvm_options)

        artifact_cfg = self.tiden.config['artifacts'][app.name]

        app.reset()
        log_print("Ignite ver. %s, revision %s" % (
            artifact_cfg['ignite_version'],
            artifact_cfg['ignite_revision'],
        ))

        app.set_activation_timeout(240)
        app.set_snapshot_timeout(240)

        app.start_nodes(already_nodes=already_nodes)

        if activate:
            app.cu.activate(activate_on_particular_node=1)

        return app

    @repeated_test(20)
    @with_setup(setup_testcase, teardown_testcase)
    def test_during_rebalance(self):
        ignite = self.start_ignite_grid(self.ignite_name)

        ignite.cu.activate(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_putall(ignite, self.client_config, )

        util_sleep_for_a_while(30)
        with PiClient(ignite, self.client_config) as piclient:
            cache_to_test = 'test_cache_with_index'
            # self.create_cache_with_indexed_data(cache_to_test)
            client_ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()

            cache_config = IgniteCacheConfig(gateway)
            cache_config.set_name('cache_1')
            cache_config.set_cache_mode('replicated')
            cache_config.set_atomicity_mode('transactional')
            cache_config.set_write_synchronization_mode('full_sync')
            cache_config.set_affinity(False, 32)
            cache_config.set_group_name('some_new_group')

            cache_config1 = IgniteCacheConfig(gateway)
            cache_config1.set_name(cache_to_test)
            cache_config1.set_cache_mode('replicated')
            cache_config1.set_atomicity_mode('transactional')
            cache_config1.set_write_synchronization_mode('full_sync')
            cache_config1.set_affinity(False, 32)
            cache_config1.set_group_name('some_new_group')

            # set query entities

            caches = gateway.jvm.java.util.ArrayList()
            caches.add(cache_config.get_config_object())
            caches.add(cache_config1.get_config_object())

            log_print("Creating caches", color='green')
            client_ignite.getOrCreateCaches(caches)

            cache_names = piclient.get_ignite().cacheNames().toArray()
            if cache_to_test not in cache_names:
                log_print("Could not find cache in %s" % cache_names, color='red')

            util_sleep_for_a_while(10)

            ignite.kill_node(2)

            log_print("Overwrite values in cache %s" % cache_to_test, color='green')

            operation = create_put_all_operation(cache_to_test, 1, 1001, 100,
                                                 value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value)
            operation.evaluate()

            util_sleep_for_a_while(15)
            ignite.start_node(2, skip_topology_check=True)

            util_sleep_for_a_while(5)
            client_ignite.cache(cache_to_test).destroy()

            ignite.update_starting_node_attrs()
            ignite.nodes[3]['status'] = NodeStatus.STARTED
            client_ignite.cache('cache_1').destroy()

    @repeated_test(20)
    @with_setup(setup_testcase, teardown_testcase)
    def test_during_loading(self):
        """
        Should be fully fixed in 8.5.8-p1

        Scenario:

            1. Start 3 server nodes
            2. Load 1000 keys into 120 TX caches
            3. Start 3 client node and start TX loading (PESSIMISTIC/REPEATABLE_READ, OPTIMISTIC/SERIALIZABLE)
                    (12 transfer operations, 10 caches in each operation,
                        1000ms between each transaction i.e. ~ 4 tx per second from each client))
            4. In clients try to destroy caches
            5. Interesting things happens

        Fixed in 8.5.8-p1
        https://ggsystems.atlassian.net/browse/GG-19179

        Issues that was found during this test:
        https://ggsystems.atlassian.net/browse/GG-19411
        https://ggsystems.atlassian.net/browse/GG-19383

        :return:
        """
        PiClient.read_timeout = 600

        ignite = self.start_ignite_grid(self.ignite_name)

        ignite.cu.activate(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_putall(ignite, self.client_config, )

        def get_dumps():
            for node_id in ignite.nodes.keys():
                self.util_get_threads_from_jstack(ignite, node_id, 'END')

        try:
            with PiClient(ignite, self.client_config) as piclient:
                with TransactionalLoading(self,
                                          ignite=ignite,
                                          config_file=self.client_config,
                                          on_exit_action=get_dumps,
                                          kill_transactions_on_exit=True,
                                          with_exception=False,  # do interrupt loading operation if smth happens?
                                          skip_consistency_check=True,  # we are destroying caches here if you notice
                                          loading_profile=LoadingProfile(
                                              delay=1000,
                                              allowed_transactions=(
                                                      TxDescriptor(concurrency='OPTIMISTIC',
                                                                   isolation='SERIALIZABLE', ),)
                                          )):
                    # allowed_transactions=(TxDescriptor(), ))):
                    # )):
                    node_id = piclient.get_node_id()
                    client_ignite = piclient.get_ignite(node_id)

                    cache_names = client_ignite.cacheNames().toArray()

                    caches_to_kill_num = 50
                    frags = 0

                    for cache in cache_names:
                        node_id = piclient.get_node_id()

                        log_print('Destroying cache %s on node %s' % (cache, node_id), color='red')

                        piclient.get_ignite(node_id).cache(cache).destroy()

                        frags += 1

                        if frags >= caches_to_kill_num:
                            break
        finally:
            npe_errors = ignite.find_exception_in_logs(".*java.lang.NullPointerException.*")

            assertion_errors = ignite.find_exception_in_logs(".*java.lang.AssertionError.*")

            if npe_errors != 0 or assertion_errors != 0:
                assert False, "There are errors in logs: NPE - %s, AE - %s" % (npe_errors, assertion_errors)

    # TODO code duplication (need to make TestStressGrid AppTestcase)
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

