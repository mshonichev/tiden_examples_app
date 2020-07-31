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

from tiden.apps.ignite import Ignite
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_streamer_operation, create_async_operation
from tiden_gridgain.piclient.piclient import PiClient
from suites.consumption.framework.scenarios.abstract import AbstractScenario
from suites.dr.dr_cluster import Cluster, create_dr_relation
from tiden import TidenException, is_enabled, util_sleep_for_a_while
from copy import copy
from datetime import datetime
from tiden.util import log_print, version_num


class DRScenario(AbstractScenario):

    def __init__(self, test_class, fst_scenario=False):
        super().__init__(test_class)

        # default values for instance variables
        self.master: Ignite = None
        self.replica: Ignite = None
        self.master_client_config = None
        self.replica_client_config = None
        self.fst_scenario = fst_scenario

        self.metrics_idle = 30
        self.with_loading = False
        self.idle_verify = False
        self.jfr_settings = False
        self.clusters = None

        self.test_class.config['environment']['dr_1_hosts'] = self.test_class.dc1_hosts
        self.test_class.config['environment']['dr_2_hosts'] = self.test_class.dc2_hosts

        self.min_version = {
            '2.5': '2.5.9',
            '8.7': '8.7.8'
        }
        self.new_behaviour = None

    def _validate_config(self):
        super()._validate_config()

        assert self.config.get('times_to_run'), 'There is no "times_to_run" variable in config'
        assert self.config.get('keys_to_load'), 'There is no "keys_to_load" variable in config'

    def run(self, artifact_name):
        super().run(artifact_name)

    def initialize_clusters(self, **kwargs):
        """
        For now we use simple configuration with 1 server node and 1 sender/receiver on client node.
        :param kwargs:
        :return:
        """
        dr_config = [
            'cluster.0.client.1<->cluster.1.client.1'
            # ,'cluster.0.client.2<->cluster.1.client.2'
        ]
        clusters_count, server_nodes_per_cluster, client_nodes_per_cluster = 2, 1, 1

        self.clusters = self.generate_dr_topology(clusters_count,
                                                  server_nodes_per_cluster,
                                                  client_nodes_per_cluster)

        for dr_rule in dr_config:
            sender_node, receiver_node, active_active_replication = self.process_dr_rule(dr_rule)
            create_dr_relation(sender_node, receiver_node, active_active_replication)

        self.generate_app_config(self.clusters, configuration_ruler=None, **kwargs)

    def _wait_for_same_caches_size(self, piclient_master, piclient_replica, how_long=300, predicate=None):
        from datetime import datetime
        start = datetime.now()
        iteration = 0
        delay = 5
        while True:
            cache_mask = lambda x: '' in x
            if predicate:
                cache_mask = predicate
            master_sizes = self.get_caches_size(cache_mask, piclient=piclient_master, debug=True)
            replica_sizes = self.get_caches_size(cache_mask, piclient=piclient_replica, debug=True)

            if master_sizes == replica_sizes:
                break

            self._compare_dicts(master_sizes, replica_sizes, debug=False)
            util_sleep_for_a_while(delay)
            iteration += 1
            log_print('Waiting for {} seconds. Master size={}, replica size={}'.format(iteration * delay, master_sizes,
                                                                                       replica_sizes))
            if (datetime.now() - start).seconds > how_long:
                self._compare_dicts(master_sizes, replica_sizes)
                raise TidenException('Caches size were not sync for {} seconds.'.format(how_long))

        execution_time = (datetime.now() - start).seconds
        log_print('Caches size have had sync for {} seconds.'.format(execution_time))

    def start_ignite_grid(self, name, cluster, activate=True, already_nodes=0):
        app = self.test_class.get_app(name)
        for node in cluster.nodes:
            node_config = Ignite.config_builder.get_config(node.node_type, config_set_name=node.config_name)
            if node.node_type == 'client':
                # we start client nodes as additional nodes.
                # NB: this overrides cluster.node.id at first run(!)
                node.id = app.add_additional_nodes(node_config, num_nodes=1, server_hosts=[node.host])[0]
            else:
                app.set_node_option(node.id, 'config', node_config)
                app.set_node_option(node.id, 'host', node.host)

        artifact_name = self.test_class.get_app_artifact_name(app.name)

        artifact_cfg = self.test_class.config['artifacts'][artifact_name]

        app.reset()
        log_print("Ignite ver. {}, revision {}".format(artifact_cfg['ignite_version'], artifact_cfg['ignite_revision']))

        app.start_nodes(*cluster.get_server_node_ids(), already_nodes=already_nodes, other_nodes=already_nodes)

        additional_nodes = cluster.get_client_node_ids()
        if additional_nodes:
            app.start_additional_nodes(additional_nodes, client_nodes=True)

        if activate:
            app.cu.activate(activate_on_particular_node=1)

        if not app.jmx.is_started():
            app.jmx.start_utility()

        return app

    def start_clusters(self, clusters):
        log_print('Starting clusters', color='debug')
        clusters[0].grid = self.start_ignite_grid(self.artifact_name + '_dc1', clusters[0])
        clusters[1].grid = self.start_ignite_grid(self.artifact_name + '_dc2', clusters[1])

        self.master = self.clusters[0].grid
        self.replica = self.clusters[1].grid
        self.master_client_config = \
            Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')
        self.replica_client_config = \
            Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr')

    def generate_app_config(self, clusters, events_enabled=True, configuration_ruler=None, **kwargs):

        discovery_port_prefix = 4750
        communication_port_prefix = 4710
        fs_store_path = True

        create_dir = []
        for cluster in clusters:

            for node in cluster.nodes:
                fs_store_dir = None
                if fs_store_path and node.is_sender():
                    fs_store_dir = '{}/fs_store_{}{}'.format(self.test_class.config['rt']['remote']['test_dir'],
                                                             cluster.id, node.id)
                    # fs_store_dir = fs_store_dir.replace('ssd', 'hdd')
                    create_dir.append('mkdir -p {}'.format(fs_store_dir))

                config_name = f'cluster_{cluster.id}_node_{node.id}'
                node.config_name = config_name

                additional_configs = configuration_ruler(cluster, node) if configuration_ruler is not None else {
                    'group_names': ['group1', 'dr']}

                self.test_class.create_app_config_set(Ignite, config_name,
                                                      deploy=True,
                                                      config_type=[node.node_type, 'caches'],
                                                      consistent_id=True,
                                                      caches=f'caches_{config_name}.xml',
                                                      disabled_cache_configs=True,
                                                      zookeeper_enabled=False,
                                                      addresses=cluster.get_server_hosts(),
                                                      discovery_port_prefix=discovery_port_prefix,
                                                      communication_port_prefix=communication_port_prefix,
                                                      node=node,
                                                      ssl_enabled=is_enabled(self.test_class.config.get('ssl_enabled')),
                                                      fs_store_path=True,
                                                      fs_store_path_value=fs_store_dir,
                                                      # snapshots_enabled=self.snapshot_storage,
                                                      events_enabled=events_enabled,
                                                      additional_configs=['caches.tmpl.xml'],
                                                      **additional_configs)

            # generate config without sender/receiver settings for piclient
            piclient_node = copy(cluster.nodes[1])
            piclient_node.sender_nodes = []
            piclient_node.receiver_nodes = []
            config_name = f'cluster_{cluster.id}_node_without_dr'
            self.test_class.create_app_config_set(Ignite, config_name,
                                                  config_type='client',
                                                  deploy=True,
                                                  consistent_id=True,
                                                  caches=f'caches_{config_name}.xml',
                                                  # caches='caches.xml',
                                                  disabled_cache_configs=True,
                                                  zookeeper_enabled=False,
                                                  addresses=cluster.get_server_hosts(),
                                                  discovery_port_prefix=discovery_port_prefix,
                                                  communication_port_prefix=communication_port_prefix,
                                                  node=piclient_node,
                                                  ssl_enabled=is_enabled(self.test_class.config.get('ssl_enabled')),
                                                  events_enabled=events_enabled,
                                                  group_names=['group1'],
                                                  additional_configs=['caches.tmpl.xml'])
            cluster.piclient_config = Ignite.config_builder.get_config('client', config_set_name=config_name)
            discovery_port_prefix += 1
            communication_port_prefix += 1

        if fs_store_path:
            self.test_class.ssh.exec(create_dir)

        return clusters

    def process_dr_rule(self, dr_rule):
        if '<->' in dr_rule:
            active_active_replication = True
        elif '->' in dr_rule:
            active_active_replication = False
        else:
            raise TidenException('Could not find DR direction in DR rule {}'.format(dr_rule))

        subrule = dr_rule.split('<->' if active_active_replication else '->')

        sender_cluster_id, sender_node_type, sender_node_id = \
            int(subrule[0].split('.')[1]), subrule[0].split('.')[2], int(subrule[0].split('.')[3])
        receiver_cluster_id, receiver_node_type, receiver_node_id = \
            int(subrule[1].split('.')[1]), subrule[1].split('.')[2], int(subrule[1].split('.')[3])

        sender_nodes = self.clusters[sender_cluster_id].get_nodes_by_type(sender_node_type)

        # this is HACK for now:
        if sender_node_type == 'client':
            last_sender_servers_node_id = max(self.clusters[sender_cluster_id].get_nodes_by_type('server'))
            sender_node_id += last_sender_servers_node_id

        if receiver_node_type == 'client':
            last_receiver_servers_node_id = max(self.clusters[receiver_cluster_id].get_nodes_by_type('server'))
            receiver_node_id += last_receiver_servers_node_id

        if sender_node_id not in sender_nodes:
            raise ValueError('Node with id {} could not be found in nodes {}'.format(sender_node_id, sender_nodes))

        # sender_node = sender_nodes[sender_node_id]
        sender_node = self.clusters[sender_cluster_id].get_node_by_id(sender_node_id)

        receiver_nodes = self.clusters[receiver_cluster_id].get_nodes_by_type(receiver_node_type)
        if receiver_node_id not in receiver_nodes:
            raise ValueError('Node with id {} could not be found in nodes {}'.format(receiver_node_id, receiver_nodes))

        # receiver_node = receiver_nodes[receiver_node_id]
        receiver_node = self.clusters[receiver_cluster_id].get_node_by_id(receiver_node_id)

        return sender_node, receiver_node, active_active_replication

    def generate_dr_topology(self, cluster_count, server_node_per_cluster, client_node_per_cluster):
        clusters = []
        for counter in range(1, cluster_count + 1):
            cluster = Cluster(counter, self.test_class.config)
            cluster.add_nodes(server_node_per_cluster, 'server')
            cluster.add_nodes(client_node_per_cluster, 'client')
            clusters.append(cluster)

        return clusters

    def initialize_config(self):
        self.metrics_idle = self.config.get('metrics_idle', 30)
        self.with_loading = self.config.get('with_loading', False)
        self.idle_verify = is_enabled(self.config.get('idle_verify'))
        self.jfr_settings = self.config.get('jfr_settings', None)

    def kill_cluster(self, ignite):
        ignite.kill_nodes()
        ignite.delete_lfs()
        log_print("Cleanup Ignite LFS ... ", color='debug')
        commands = {}
        for node_idx in ignite.nodes.keys():
            host = ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
        self.test_class.tiden.ssh.exec(commands)
        log_print("Ignite LFS deleted.", color='debug')

    def _cleanup_lfs(self, ignite, node_id=None):
        log_print('Cleanup Ignite LFS ... ')
        commands = {}

        nodes_to_clean = [node_id] if node_id else ignite.nodes.keys()

        for node_idx in nodes_to_clean:
            host = ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
        results = self.test_class.ssh.exec(commands)
        log_print(results, color='debug')
        log_print('Ignite LFS deleted.')
        log_print()

    @staticmethod
    def if_new_behaviour(version: str, min_version: dict):
        ignite_version = version_num(version)

        for base_ver, min_bs_version in min_version.items():
            if base_ver == version[:3]:
                return ignite_version >= version_num(min_bs_version)
        return False

    @staticmethod
    def _compare_dicts(dict_1: dict, dict_2: dict, debug=True):
        equals = True
        for key, value in dict_1.items():
            if not key in dict_2:
                log_print(f'Cache {key} is not found on replica \n{dict_2}')
                equals = False
            else:
                if not value == dict_2.get(key):
                    if debug:
                        log_print(f'Values for cache {key} are not equal:\n master={value}\nreplica={dict_2.get(key)}',
                                  color='debug')
                    equals = False
        return equals

    @staticmethod
    def get_caches_size(cache_mask=lambda x: '' in x, piclient=None, debug=True):
        from pt.piclient.helper.cache_utils import IgniteCache
        cache_size = {}
        cache_names = piclient.get_ignite().cacheNames().toArray()
        cache_names = [cache_name for cache_name in cache_names if cache_mask(cache_name)]

        for cache_name in cache_names:
            if debug:
                log_print('Getting size for cache {}'.format(cache_name), color='blue')
            cache = IgniteCache(cache_name, gateway=piclient.get_gateway())
            cache_size[cache_name] = cache.size()
            if debug:
                log_print('Size for cache {} is {}'.format(cache_name, cache_size[cache_name]), color='blue')
        return cache_size

    @staticmethod
    def util_exec_on_all_hosts(ignite, commands_to_exec):
        commands = {}
        hosts = ignite.config['environment'].get('server_hosts', []) + \
                ignite.config['environment'].get('client_hosts', [])

        for host in hosts:
            if commands.get(host) is None:
                commands[host] = commands_to_exec

        ignite.ssh.exec(commands)


class DataReplicationScenario(DRScenario):
    def run(self, artifact_name):
        """
        Run Data Replication scenario for defined artifact

        Scenario is very simple
        1. start 2 clusters (master and replica) with replication on clients up and running.
        2. load data to first N caches (check caches_amount variable in the test) with streamer on master.
        3. check the time used for caches to sync.

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running Data Replication benchmark with config: %s" % self.config, color='green')

        caches_amount = 4
        try:
            # collect properties from config
            self.initialize_clusters()
            self.start_clusters(self.clusters)

            # run ignite app
            keys_to_load = int(self.config.get('keys_to_load'))

            with PiClient(self.master, self.master_client_config, nodes_num=1) as piclient_master:
                cache_names = piclient_master.get_ignite().cacheNames().toArray()
                cache_names = [cache_name for cache_name in cache_names]
                with PiClient(self.replica, self.replica_client_config, nodes_num=1,
                              new_instance=True) as piclient_replica:
                    time_results = list()

                    self.start_probes(artifact_name, self.master.name)
                    async_operations = []
                    start_key, end_key, remove_probability = 0, keys_to_load, 0.0
                    for cache_name in cache_names[:caches_amount]:
                        log_print(f'Uploading data into cache {cache_name}')
                        start_time = datetime.now()
                        async_operation = create_async_operation(
                            create_streamer_operation,
                            cache_name, start_key, end_key,
                            value_type=ModelTypes.VALUE_ALL_TYPES.value,
                            gateway=piclient_master.get_gateway(),
                        )

                        async_operations.append(async_operation)
                        async_operation.evaluate()

                        for async_op in async_operations:
                            async_op.getResult()
                        log_print('Uploading is done', color='green')

                        self._wait_for_same_caches_size(piclient_master, piclient_replica,
                                                        predicate=lambda x: cache_name in x)
                        replication_time = (datetime.now() - start_time).seconds
                        log_print(f'Replication time {replication_time}')
                        time_results.append(replication_time)
                    self.stop_probes(time_results=time_results, seconds=True)
                    log_print()
            self.results['evaluated'] = True

        finally:
            if self.clusters:
                for cluster in self.clusters:
                    log_print('Teardown for cluster {}'.format(cluster))
                    if cluster.grid:
                        cluster.grid.jmx.kill_utility()
                        cluster.grid.remove_additional_nodes()
                        cluster.grid.kill_nodes()
                        cluster.grid.delete_lfs()
                        cluster.grid = None


class DataReplicationFSTScenario(DRScenario):
    def run(self, artifact_name):
        """
        Run FST Data Replication scenario for defined artifact

        1. start 2 clusters (master and replica) with replication on clients up and running.
        2. stop senders to prevent replication.
        3. load data to first N caches (check caches_amount variable in the test) with streamer on master.
        4. start senders, start replication and make FST operation.
        3. check the time used for caches to sync.

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running Data Replication benchmark with config: %s" % self.config, color='green')

        caches_amount = 4
        try:
            # collect properties from config
            self.initialize_clusters()
            self.start_clusters(self.clusters)
            version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
            self.new_behaviour = self.if_new_behaviour(version, self.min_version)

            # run ignite app
            keys_to_load = int(self.config.get('keys_to_load'))

            with PiClient(self.master, self.master_client_config, nodes_num=1) as piclient_master:
                cache_names = piclient_master.get_ignite().cacheNames().toArray()
                cache_names = [cache_name for cache_name in cache_names]
                with PiClient(self.replica, self.replica_client_config, nodes_num=1,
                              new_instance=True) as piclient_replica:
                    time_results = list()
                    async_operations = []
                    start_key, end_key, remove_probability = 0, keys_to_load, 0.0
                    for cache_name in cache_names[:caches_amount]:

                        # kill senders to prevent replication
                        senders = self.clusters[0].get_sender_nodes()
                        for node in senders:
                            self.master.kill_node(node.id)

                        log_print(f'Uploading data into cache {cache_name}')

                        async_operation = create_async_operation(
                            create_streamer_operation,
                            cache_name, start_key, end_key,
                            value_type=ModelTypes.VALUE_ALL_TYPES.value,
                            gateway=piclient_master.get_gateway(),
                        )

                        async_operations.append(async_operation)
                        async_operation.evaluate()

                        for async_op in async_operations:
                            async_op.getResult()

                        log_print('Uploading is done', color='green')

                        master_sizes = self.get_caches_size(cache_mask=lambda x: cache_name in x,
                                                            piclient=piclient_master, debug=False)
                        replica_sizes = self.get_caches_size(cache_mask=lambda x: cache_name in x,
                                                             piclient=piclient_replica, debug=False)
                        log_print('Master size={}, replica size={}'.format(master_sizes, replica_sizes))

                        # start senders
                        senders_ids = [node.id for node in senders]
                        self.master.start_additional_nodes(senders_ids, client_nodes=True, already_started=1,
                                                           other_nodes=1)

                        log_print(f'Running State transfer for cache {cache_name}', color='blue')

                        status = piclient_master.get_ignite().plugin("GridGain").dr().senderCacheStatus(cache_name)
                        log_print(f'DR status {status} for cache {cache_name}')

                        # start probes
                        self.start_probes(artifact_name, self.master.name)
                        start_time = datetime.now()

                        # make FST (new behaviour - use resume operation instead of start replication)
                        if self.new_behaviour:
                            piclient_master.get_ignite().plugin("GridGain").dr().startReplication(cache_name)
                        else:
                            piclient_master.get_ignite().plugin("GridGain").dr().resume(cache_name)
                        piclient_master.get_ignite().plugin("GridGain").dr().stateTransfer(cache_name, bytes([2]))

                        self._wait_for_same_caches_size(piclient_master, piclient_replica,
                                                        predicate=lambda x: cache_name in x)
                        replication_time = (datetime.now() - start_time).seconds
                        log_print(f'Replication time {replication_time}')
                        time_results.append(replication_time)

                    self.stop_probes(time_results=time_results, seconds=True)
                    log_print()
            self.results['evaluated'] = True

        finally:
            if self.clusters:
                for cluster in self.clusters:
                    log_print('Teardown for cluster {}'.format(cluster))
                    if cluster.grid:
                        cluster.grid.jmx.kill_utility()
                        cluster.grid.remove_additional_nodes()
                        cluster.grid.kill_nodes()
                        cluster.grid.delete_lfs()
                        cluster.grid = None

