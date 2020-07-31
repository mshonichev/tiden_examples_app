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

from collections import namedtuple
from concurrent.futures.thread import ThreadPoolExecutor
from copy import copy
from os.path import join, basename
from time import sleep, time

from tiden.apps.ignite import Ignite
from tiden import TidenException, log_print, tiden_assert, with_setup, attr, is_enabled, util_sleep_for_a_while, \
    known_issue, test_case_id, if_applicable_ignite_version
from tiden.case.apptestcase import AppTestCase
from tiden.assertions import tiden_assert_equal
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_put_all_operation, \
    create_checksum_operation, create_streamer_operation, create_put_with_optional_remove_operation, \
    create_event_collect_operation
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden.tidenfabric import TidenFabric
from tiden.utilities import Sqlline
from tiden.report.steps import step, Step, add_attachment, AttachmentType, suites, test_name
from suites.dr.dr_cluster import Cluster, create_dr_relation
from suites.dr.util import wait_for_replication_is_up_for_clusters, dr_state_transfer, dr_start_replication, \
    calculate_and_compare_checksums, create_piclient, \
    calc_checksums_on_client, dr_is_stopped, \
    create_cache_config, dr_stop_replication
from suites.snapshots.ult_utils import UltimateUtils


class TestDataReplication(AppTestCase):
    ignite_app_names = {}
    data_model = ModelTypes.VALUE_ACCOUNT.value

    def __init__(self, *args):
        super().__init__(*args)
        self.config = self.tiden.config
        self.clusters_amount = None
        self.clusters = None
        self.snapshot_storage = False
        self.run_client_nodes_on_start = True
        self.preloading_size = 100
        self.iteration = 10
        min_version = {
            '2.5': '2.5.9',
            '8.7': '8.7.8'
        }
        self.new_behaviour = if_applicable_ignite_version(self.tiden.config, min_version)
        artifacts = self.tiden.config['artifacts']
        self.max_clusters_amount = len(
            [artifact for artifact in artifacts.values() if artifact.get('type') == 'ignite'])
        for counter in range(1, self.max_clusters_amount + 1):
            self.ignite_app_names[f'ignite{counter}'] = f'ignite{counter}'
            self.add_app(
                f'ignite{counter}',
                app_class_name='ignite',
                grid_name=f'ignite{counter}',
            )

    def setup(self):
        super().setup()

    def setup_empty(self):
        pass

    def setup_testcase_master_master_on_servers(self, **kwargs):
        """
        This is the common DR setup where sender and receiver hubs are server nodes:
        1. Started data nodes and sender/receiver hubs for both clusters.
        2. Upload data into master cluster.
        :param kwargs:
        :return:
        """
        dr_config = [
            'cluster.0.server.3<->cluster.1.server.3',
            'cluster.0.server.4<->cluster.1.server.4',
        ]
        self.setup_testcase_master_master(**kwargs, dr_config=dr_config)

    def setup_testcase_master_master_on_servers_separated_caches(self, **kwargs):
        """
        This is the common DR setup where sender and receiver hubs are server nodes:
        1. Started data nodes and sender/receiver hubs for both clusters.
        1.1 Sender node #3 is sending caches from group1 sender group
        1.2 Sender node #4 is sending caches from group2 sender group
        2. Upload data into master cluster.
        :param kwargs:
        :return:
        """
        dr_config = [
            'cluster.0.server.3<->cluster.1.server.3',
            'cluster.0.server.4<->cluster.1.server.4',
        ]

        def configurator(cluster, node):
            """
            Additional config properties supplier
            Add second group of caches for second sender group
            Also separate sender groups for each sender node

            :param cluster: dr_cluster.Cluster
            :param node:    dr_cluster.Node
            :return:        additional parameters for config generation
            """
            parameters = {
                'second_sender_group': True
            }
            client_nodes = [c_node for c_node in cluster.nodes if c_node.node_type == 'server' and c_node.sender_nodes]
            if client_nodes[0].id == node.id:
                parameters['group_names'] = ['group1', 'dr']
            elif client_nodes[1].id == node.id:
                parameters['group_names'] = ['group2', 'dr']
            return parameters

        self.setup_testcase_master_master(dr_config=dr_config, configuration_ruler=configurator, **kwargs)

    def setup_testcase_master_master_on_clients(self, **kwargs):
        """
        This is the common DR setup:
        1. Started data nodes for both clusters.
        2. Upload data into master cluster.
        3. Start 2 client nodes in both clusters as sender/receiver hubs.
        Note: in this case replication is stopped (case 'no sender hub').
        :param kwargs:
        :return:
        """
        dr_config = [
            'cluster.0.client.1<->cluster.1.client.1',
            'cluster.0.client.2<->cluster.1.client.2',
        ]
        self.setup_testcase_master_master(**kwargs, dr_config=dr_config)

    def setup_testcase_master_master_on_clients_separated_caches(self, **kwargs):
        """
        This is the common DR setup:
        1. Started data nodes for both clusters.
        2. Upload data into master cluster.
        3. Start 2 client nodes in both clusters as sender/receiver hubs.
        3.1 Sender client #1 is sending caches from group1 sender group
        3.2 Sender client #2 is sending caches from group2 sender group
        Note: in this case replication is stopped (case 'no sender hub').
        :param kwargs:
        :return:
        """
        dr_config = [
            'cluster.0.client.1<->cluster.1.client.1',
            'cluster.0.client.2<->cluster.1.client.2',
        ]

        def configurator(cluster, node):
            """
            Additional config properties supplier
            Add second group of caches for second sender group
            Also separate sender groups for each sender client node

            :param cluster: dr_cluster.Cluster
            :param node:    dr_cluster.Node
            :return:        additional parameters for config generation
            """
            parameters = {
                'second_sender_group': True
            }
            client_nodes = [c_node for c_node in cluster.nodes if c_node.node_type == 'client' and c_node.sender_nodes]
            if client_nodes[0].id == node.id:
                parameters['group_names'] = ['group1', 'dr']
            elif client_nodes[1].id == node.id:
                parameters['group_names'] = ['group2', 'dr']
            return parameters

        self.setup_testcase_master_master(dr_config=dr_config, configuration_ruler=configurator, **kwargs)

    def setup_testcase_master_master_on_clients_ttl_caches(self, **kwargs):
        dr_config = [
            'cluster.0.client.1<->cluster.1.client.1',
            'cluster.0.client.2<->cluster.1.client.2',
        ]

        def configurator(cluster, node):
            return {
                'ttl_caches': True,
                'period_type': 'MINUTES',
                'period_count': '2',
                'group_names': ['group1', 'dr']
            }

        self.setup_testcase_master_master(dr_config=dr_config, configuration_ruler=configurator, **kwargs)
        with PiClient(self.clusters[0].grid, self.master_client_config, nodes_num=1) as piclient_master:
            with PiClient(self.clusters[1].grid, self.replica_client_config, new_instance=True,
                          nodes_num=1) as piclient_replica:

                cache_names_master = list(piclient_master.get_ignite().cacheNames().toArray())
                cache_name: str
                for cache_name in cache_names_master:
                    if cache_name.startswith('ttl'):
                        create_put_all_operation(cache_name, 1, 100, 100,
                                                 key_type='java.lang.Long',
                                                 value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                 gateway=piclient_master.get_gateway()).evaluate()
                self.make_state_transfer(piclient=piclient_master)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                self.assert_checksums()

    def setup_testcase_master_master_on_servers_ttl_caches(self, **kwargs):
        dr_config = [
            'cluster.0.server.3<->cluster.1.server.3',
            'cluster.0.server.4<->cluster.1.server.4',
        ]

        def configurator(cluster, node):
            return {
                'ttl_caches': True,
                'period_type': 'MINUTES',
                'period_count': '2',
                'group_names': ['group1', 'dr']
            }

        self.setup_testcase_master_master(dr_config=dr_config, configuration_ruler=configurator, **kwargs)
        with PiClient(self.clusters[0].grid, self.master_client_config, nodes_num=1) as piclient_master:
            with PiClient(self.clusters[1].grid, self.replica_client_config, new_instance=True,
                          nodes_num=1) as piclient_replica:

                cache_names_master = list(piclient_master.get_ignite().cacheNames().toArray())
                cache_name: str
                for cache_name in cache_names_master:
                    if cache_name.startswith('ttl'):
                        create_put_all_operation(cache_name, 1, 100, 100,
                                                 key_type='java.lang.Long',
                                                 value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                 gateway=piclient_master.get_gateway()).evaluate()
                self.make_state_transfer(piclient=piclient_master)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                self.assert_checksums()

    def setup_testcase_master_master(self, configuration_ruler=None, **kwargs):

        dr_config = kwargs.get('dr_config')
        self.clusters_amount = kwargs.get('clusters_count')
        self.run_client_nodes_on_start = False
        if kwargs.get('snapshot_storage'):
            from time import time
            snapshot_storage = 'setup_dr_test_{}_{}'.format(self.tiden.config['environment']['username'], int(time()))
            nas_manager = TidenFabric().getNasManager()
            self.snapshot_storage = nas_manager.create_shared_folder(snapshot_storage, cleanup=True)
            log_print('Created shared storage {}'.format(self.snapshot_storage), color='debug')

        self.clusters = self.generate_dr_topology(kwargs.get('clusters_count'),
                                                  kwargs.get('server_nodes_per_cluster'),
                                                  kwargs.get('client_nodes_per_cluster'))

        for dr_rule in dr_config:
            sender_node, receiver_node, active_active_replication = self.process_dr_rule(dr_rule)
            create_dr_relation(sender_node, receiver_node)

            if active_active_replication:
                create_dr_relation(receiver_node, sender_node)

        self.generate_app_config(self.clusters, configuration_ruler=configuration_ruler, **kwargs)
        self.start_clusters(self.clusters)

        self.master: Ignite = self.clusters[0].grid
        self.replica: Ignite = self.clusters[1].grid

        self.master_client_config = \
            Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')
        self.replica_client_config = \
            Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr')

        if kwargs.get('preload', True):
            log_print(f'Make preloading ({self.preloading_size} keys per cache)', color='debug')
            PiClientIgniteUtils.load_data_with_streamer_batch(self.master,
                                                              self.master_client_config,
                                                              start_key=0,
                                                              end_key=self.preloading_size,
                                                              parallel_operations=8,
                                                              check_clients=True,
                                                              silent=False,
                                                              allow_overwrite=True)
        if kwargs.get('client_nodes_per_cluster') > 0:
            log_print('Starting client nodes', color='debug')
            futures = []
            with ThreadPoolExecutor(max_workers=2) as executor:
                for cluster in self.clusters:
                    additional_nodes = cluster.get_client_node_ids()
                    futures.append(executor.submit(cluster.grid.start_additional_nodes,
                                                   additional_nodes, client_nodes=True))

        wait_for_replication_is_up_for_clusters(self.clusters)
        log_print('Clusters are up and running!', color='green')
        log_print(repr(self.master), color='debug')

    def setup_testcase_servers_only(self, **kwargs):

        dr_config = [
            'cluster.0.server.1->cluster.1.server.1',
            'cluster.0.server.2->cluster.1.server.2',
        ]

        self.clusters_amount = kwargs.get('clusters_count')

        clusters = self.generate_dr_topology(kwargs.get('clusters_count'),
                                             kwargs.get('server_nodes_per_cluster'),
                                             kwargs.get('client_nodes_per_cluster'))

        for dr_rule in dr_config:
            if '<->' in dr_rule:
                active_active_replication = True
            elif '->' in dr_rule:
                active_active_replication = False
            else:
                raise TidenException('Could not find DR direction in DR rule {}'.format(dr_rule))

            subrule = dr_rule.split('<->' if active_active_replication else '->')

            sender_cluster_id, sender_node_id = int(subrule[0].split('.')[1]), int(subrule[0].split('.')[3]) - 1
            receiver_cluster_id, receiver_node_id = int(subrule[1].split('.')[1]), int(subrule[1].split('.')[3]) - 1
            sender_node = clusters[sender_cluster_id].nodes[sender_node_id]
            receiver_node = clusters[receiver_cluster_id].nodes[receiver_node_id]

            create_dr_relation(sender_node, receiver_node)

            if active_active_replication:
                create_dr_relation(receiver_node, sender_node)

        self.generate_app_config(clusters)
        self.start_clusters(clusters)
        wait_for_replication_is_up_for_clusters(clusters)
        self.clusters = clusters

    def setup_basic_dr_topology(self, clients=2, **kwargs):
        self.run_client_nodes_on_start = True
        self.iteration = 10

        if kwargs.get('rolling_upgrade'):
            clusters_count, server_node_count, client_node_count = self.max_clusters_amount, 4, clients
            self.clusters = self.generate_dr_topology(clusters_count, server_node_count, client_node_count)
            self.new_master: Cluster = self.clusters[0]
            self.new_replica: Cluster = self.clusters[1]
            self.master: Cluster = self.clusters[2]
            self.replica: Cluster = self.clusters[3]
        else:
            clusters_count, server_node_count, client_node_count = 2, 4, clients
            self.clusters = self.generate_dr_topology(clusters_count, server_node_count, client_node_count)
            self.master: Cluster = self.clusters[0]
            self.replica: Cluster = self.clusters[1]

        if clients == 0:
            create_dr_relation(self.master.server3, self.replica.server3, bidirectional=True)
            create_dr_relation(self.master.server4, self.replica.server4, bidirectional=True)
            self.run_client_nodes_on_start = False
        else:
            create_dr_relation(self.master.client1, self.replica.client1, bidirectional=True)
            create_dr_relation(self.master.client2, self.replica.client2, bidirectional=True)
            self.run_client_nodes_on_start = True
        if kwargs.get('rolling_upgrade'):
            self.generate_app_config(self.clusters, events_enabled=False)
        else:
            self.generate_app_config(self.clusters)

        if kwargs.get('data_preload'):
            self.start_clusters([self.master, self.replica])
            wait_for_replication_is_up_for_clusters(self.clusters)
            with create_piclient(self.master) as piclient_master:
                with create_piclient(self.replica) as piclient_replica:
                    with Step(self, 'Preload different data into both clusters', color='debug'):
                        static_cache_names_master = [cache_name for cache_name in
                                                     piclient_master.get_ignite().cacheNames().toArray()]
                        static_cache_names_replica = [cache_name for cache_name in
                                                      piclient_replica.get_ignite().cacheNames().toArray()]
                        async_operations = []
                        with Step(self, 'Load data in master cluster'):
                            for cache_name in static_cache_names_master:
                                async_operation = create_async_operation(create_streamer_operation, cache_name, 1, 51,
                                                                         value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                                         gateway=piclient_master.get_gateway())
                                async_operations.append((async_operation))
                                async_operation.evaluate()
                        with Step(self, 'Load data in replica cluster'):
                            for cache_name in static_cache_names_replica:
                                async_operation = create_async_operation(create_streamer_operation, cache_name, 51, 101,
                                                                         value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                                         gateway=piclient_replica.get_gateway())
                                async_operations.append((async_operation))
                                async_operation.evaluate()
                            for async_op in async_operations:
                                async_op.getResult()
                        self._wait_for_same_caches_size(piclient_master, piclient_replica)
                        util_sleep_for_a_while(60)
                        checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master,
                                                                                            piclient_replica,
                                                                                            start_key=1, end_key=301)
                        with Step(self, 'Compare checksum on master and replica'):
                            tmp_dir = self.tiden.config['tmp_dir']
                            master_checksum_path = join(tmp_dir, 'master_cluster_checksum.txt')
                            replica_checksum_path = join(tmp_dir, 'replica_cluster_checksum.txt')
                            open(master_checksum_path, 'w').write(str(checksum_master))
                            open(replica_checksum_path, 'w').write(str(checksum_replica))
                            add_attachment(self, basename(master_checksum_path), master_checksum_path, AttachmentType.FILE)
                            add_attachment(self, basename(replica_checksum_path), replica_checksum_path, AttachmentType.FILE)
                            tiden_assert(checksum_master == checksum_replica,
                                         f'Checksums must be equals:\n'
                                         f'master:\n{checksum_master}\n'
                                         f'replica:\n{checksum_replica}')

    def setup_basic_dr_topology_on_clients(self, **kwargs):
        self.setup_basic_dr_topology(**kwargs)

    def setup_basic_dr_topology_on_servers(self, **kwargs):
        self.setup_basic_dr_topology(clients=0, **kwargs)

    def teardown_testcase(self):
        for cluster in self.clusters:
            log_print('Teardown for cluster {}'.format(cluster))
            if cluster.grid:
                cluster.grid.jmx.kill_utility()
                cluster.grid.kill_nodes()
                cluster.grid.delete_lfs()
                cluster.grid.remove_additional_nodes()
                try:
                    if self.snapshot_storage:
                        cluster.grid.su.remove_shared_snapshot_storage(self.snapshot_storage)
                        self.snapshot_storage = False
                except Exception as e:
                    log_print('Got exception while deleting snapshot directory.\n{}'.format(e), color='red')
                    self.snapshot_storage = False
                cluster.grid = None

    @step()
    def start_clusters(self, clusters):
        futures = []
        log_print('Starting clusters', color='debug')
        with ThreadPoolExecutor(max_workers=2) as executor:
            for cluster in clusters:
                futures.append(executor.submit(self.start_ignite_grid, f'ignite{cluster.id}', cluster, activate=True))

        for i, cluster in enumerate(clusters):
            cluster.grid = futures[i].result()

    def generate_dr_topology(self, cluster_count, server_node_per_cluster, client_node_per_cluster):
        nodes_count_per_cluster = len(self.config['environment']['server_hosts']) * self.config['environment'][
            'servers_per_host']
        assert server_node_per_cluster + client_node_per_cluster <= nodes_count_per_cluster, \
            'sum of the number of server nodes and the number of client nodes should be no more then ' \
            'total number of nodes'
        clusters = []
        for counter in range(1, cluster_count + 1):
            cluster = Cluster(counter, self.config)
            cluster.add_nodes(server_node_per_cluster, 'server')
            cluster.add_nodes(client_node_per_cluster, 'client')
            clusters.append(cluster)

        return clusters

    def generate_app_config(self, clusters, events_enabled=True, configuration_ruler=None, **kwargs):
        discovery_port_prefix = 4750
        communication_port_prefix = 4710

        self.fs_store_path = True
        events_enabled = events_enabled and self.new_behaviour

        create_dir = []
        for cluster in clusters:

            for node in cluster.nodes:
                fs_store_dir = None
                if self.fs_store_path and node.is_sender():
                    fs_store_dir = '{}/fs_store_{}{}'.format(self.tiden.config['rt']['remote']['test_dir'],
                                                             cluster.id, node.id)
                    # fs_store_dir = fs_store_dir.replace('ssd', 'hdd')
                    create_dir.append('mkdir -p {}'.format(fs_store_dir))

                config_name = f'cluster_{cluster.id}_node_{node.id}'
                node.config_name = config_name

                additional_configs = configuration_ruler(cluster, node) if configuration_ruler is not None else {
                    'group_names': ['group1', 'dr']}

                self.create_app_config_set(Ignite, config_name,
                                           deploy=True,
                                           config_type=[node.node_type, 'caches'],
                                           consistent_id=True,
                                           caches=f'caches_{config_name}.xml',
                                           disabled_cache_configs=True,
                                           zookeeper_enabled=False,
                                           addresses=self.tiden.config['environment']['server_hosts'],
                                           discovery_port_prefix=discovery_port_prefix,
                                           communication_port_prefix=communication_port_prefix,
                                           node=node,
                                           ssl_enabled=is_enabled(self.config.get('ssl_enabled')),
                                           fs_store_path=True,
                                           fs_store_path_value=fs_store_dir,
                                           snapshots_enabled=self.snapshot_storage,
                                           events_enabled=events_enabled,
                                           additional_configs=['caches.tmpl.xml'],
                                           **additional_configs)

            # generate config without sender/receiver settings for piclient
            piclient_node = copy(cluster.nodes[1])
            piclient_node.sender_nodes = []
            piclient_node.receiver_nodes = []
            config_name = f'cluster_{cluster.id}_node_without_dr'
            self.create_app_config_set(Ignite, config_name,
                                       config_type='client',
                                       deploy=True,
                                       consistent_id=True,
                                       caches=f'caches_{config_name}.xml',
                                       # caches='caches.xml',
                                       disabled_cache_configs=True,
                                       zookeeper_enabled=False,
                                       addresses=self.tiden.config['environment']['server_hosts'],
                                       discovery_port_prefix=discovery_port_prefix,
                                       communication_port_prefix=communication_port_prefix,
                                       node=piclient_node,
                                       ssl_enabled=is_enabled(self.config.get('ssl_enabled')),
                                       events_enabled=events_enabled,
                                       group_names=['group1'],
                                       additional_configs=['caches.tmpl.xml'])
            cluster.piclient_config = Ignite.config_builder.get_config('client', config_set_name=config_name)
            discovery_port_prefix += 1
            communication_port_prefix += 1

        if self.fs_store_path:
            self.tiden.ssh.exec(create_dir)

        return clusters

    def start_ignite_grid(self, name, cluster, activate=False, already_nodes=0):
        app = self.get_app(self.ignite_app_names[name])
        for node in cluster.nodes:
            app.set_node_option(node.id, 'config',
                                Ignite.config_builder.get_config(node.node_type, config_set_name=node.config_name))
            app.set_node_option(node.id, 'host', node.host)

        artifact_cfg = self.tiden.config['artifacts'][app.name]

        app.reset()
        log_print("Ignite ver. %s, revision %s" % (
            artifact_cfg['ignite_version'],
            artifact_cfg['ignite_revision'],
        ))

        self.enable_ssl_on_grid(app)

        app.start_nodes(*cluster.get_server_node_ids(), already_nodes=already_nodes, other_nodes=already_nodes)

        if self.run_client_nodes_on_start:
            additional_nodes = cluster.get_client_node_ids()
            if additional_nodes:
                app.start_additional_nodes(additional_nodes, client_nodes=True)

        if activate:
            app.cu.activate(activate_on_particular_node=1)

        if not app.jmx.is_started():
            app.jmx.start_utility()

        return app

    @step()
    def start_ignite_nodes(self, cluster, server_nodes, client_nodes, already_nodes=0, cluster_for_config_name=None):
        for node in server_nodes + client_nodes:
            if cluster_for_config_name is None:
                config_name = node.config_name
            else:
                config_name = cluster_for_config_name.nodes[node.id - 1].config_name
            cluster.grid.set_node_option(node.id, 'config',
                                         Ignite.config_builder.get_config(node.node_type, config_set_name=config_name))

        artifact_cfg = self.tiden.config['artifacts'][cluster.grid.name]

        log_print("Ignite ver. %s, revision %s" % (
            artifact_cfg['ignite_version'],
            artifact_cfg['ignite_revision'],
        ))

        server_nodes_ids = [server_node.id for server_node in server_nodes]
        if server_nodes_ids:
            cluster.grid.start_nodes(*server_nodes_ids, already_nodes=already_nodes, other_nodes=already_nodes)
        client_nodes_ids = [client_node.id for client_node in client_nodes]
        if client_nodes_ids:
            cluster.grid.start_additional_nodes(client_nodes_ids, client_nodes=True, skip_topology_check=True)

        if not cluster.grid.jmx.is_started():
            cluster.grid.jmx.start_utility()

        return cluster.grid

    @staticmethod
    def prepare_cluster_for_rolling_upgrade(old_cluster: Cluster, new_cluster: Cluster):
        for old_node, new_node in old_cluster.grid.nodes, new_cluster.grid.nodes:
            new_node.config_name = old_node.config_name

    @test_case_id(257204)
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False)
    def test_1_full_state_transfer_on_servers(self):
        self.case_1_full_state_transfer()

    @test_case_id(259494)
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False)
    def test_1_full_state_transfer_all_api_on_servers(self):
        self.case_1_full_state_transfer_all_api()

    @test_case_id(259494)
    # @known_issue('GG-24421')
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_clients, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                start_clients=True, wait_for_replication_online=False)
    def test_1_full_state_transfer_all_api_on_clients(self):
        self.case_1_full_state_transfer_all_api()

    @test_case_id(257204)
    @attr('common', 'on_clients')
    @with_setup(setup_testcase_master_master_on_clients, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                start_clients=False, wait_for_replication_online=False)
    def test_1_full_state_transfer_on_clients(self):
        self.case_1_full_state_transfer()

    @test_case_id(257205)
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False)
    def test_2_full_state_transfer_with_load_on_servers(self):
        self.case_2_full_state_transfer_with_load()

    @test_case_id(257205)
    @attr('common', 'on_clients')
    @with_setup(setup_testcase_master_master_on_clients, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                start_clients=False, wait_for_replication_online=False)
    def test_2_full_state_transfer_with_load_on_clients(self):
        self.case_2_full_state_transfer_with_load()

    @attr('not_added')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False)
    def test_2_full_state_transfer_with_transactional_loading_on_servers(self):
        self.case_2_full_state_transfer_with_transactional_loading()

    @test_case_id(257206)
    @attr('common', 'on_servers')
    @with_setup(setup_basic_dr_topology_on_servers, teardown_testcase)
    def test_3_pause_resume_replication_on_servers(self):
        self.case_3_pause_resume_replication(replication_on_clients=False)

    @test_case_id(257206)
    @attr('common', 'on_clients')
    @with_setup(setup_basic_dr_topology_on_clients, teardown_testcase)
    def test_3_pause_resume_replication_on_clients(self):
        self.case_3_pause_resume_replication()

    @test_case_id(257207)
    @known_issue('GG-23095')
    @attr('common', 'on_servers')
    @with_setup(setup_basic_dr_topology_on_servers, teardown_testcase)
    def test_4_pause_resume_replication_under_load_on_servers(self):
        self.case_4_pause_resume_replication_under_load()

    @test_case_id(257207)
    @known_issue('GG-23095')
    @attr('common', 'on_clients')
    @with_setup(setup_basic_dr_topology_on_clients, teardown_testcase)
    def test_4_pause_resume_replication_under_load_on_clients(self):
        self.case_4_pause_resume_replication_under_load()

    @test_case_id(257208)
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False, snapshot_storage=True)
    def test_5_snapshot_replace_fst_on_servers(self):
        self.case_5_snapshot_replace_fst()

    @test_case_id(257208)
    @attr('common', 'on_clients')
    @with_setup(setup_testcase_master_master_on_clients, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=2, client_nodes_per_cluster=2,
                start_clients=False, wait_for_replication_online=False, snapshot_storage=True)
    def test_5_snapshot_replace_fst_on_clients(self):
        self.case_5_snapshot_replace_fst()

    @test_case_id(259495)
    @attr('rolling_upgrade', 'on_servers')
    @suites(["Rolling upgrade"])
    @test_name('In memory replication on servers')
    @with_setup(setup_basic_dr_topology_on_servers, teardown_testcase, rolling_upgrade=True, data_preload=True)
    def test_6_rolling_enable_replication_on_servers(self):
        self.case_6_rolling_enable_replication()

    @test_case_id(259495)
    @attr('rolling_upgrade', 'on_clients')
    @suites(["Rolling upgrade"])
    @test_name('In memory replication on clients')
    @with_setup(setup_basic_dr_topology_on_clients, teardown_testcase, rolling_upgrade=True, data_preload=True)
    def test_6_rolling_enable_replication_on_clients(self):
        self.case_6_rolling_enable_replication()

    @attr('rolling_upgrade', 'on_servers')
    @suites(["Rolling upgrade"])
    @test_name('PDS replication on servers')
    @with_setup(setup_basic_dr_topology_on_servers, teardown_testcase, rolling_upgrade=True, data_preload=True)
    def test_7_rolling_upgrade_with_pds_cleanup_on_servers(self):
        self.case_7_rolling_upgrade_with_pds_cleanup()

    @attr('rolling_upgrade', 'on_clients')
    @suites(["Rolling upgrade"])
    @test_name('PDS replication on clients')
    @with_setup(setup_basic_dr_topology_on_clients, teardown_testcase, rolling_upgrade=True, data_preload=True)
    def test_7_rolling_upgrade_with_pds_cleanup_on_clients(self):
        self.case_7_rolling_upgrade_with_pds_cleanup()

    @test_case_id(257209)
    @attr('common', 'on_servers')
    @with_setup(setup_basic_dr_topology_on_servers, teardown_testcase)
    def test_9_add_dynamic_caches_to_replication_on_servers(self):
        self.case_9_add_dynamic_caches_to_replication()

    @test_case_id(257209)
    @attr('common', 'on_clients')
    @with_setup(setup_basic_dr_topology_on_clients, teardown_testcase)
    def test_9_add_dynamic_caches_to_replication_on_clients(self):
        self.case_9_add_dynamic_caches_to_replication()

    @test_case_id(111)
    @attr('common', 'on_servers')
    @with_setup(setup_basic_dr_topology_on_servers, teardown_testcase)
    def test_13_new_pause_resume_replication_on_servers(self):
        self.case_13_new_pause_resume_replication()

    @test_case_id(111)
    @attr('common', 'on_clients')
    @with_setup(setup_basic_dr_topology_on_clients, teardown_testcase)
    def test_13_new_pause_resume_replication_on_clients(self):
        self.case_13_new_pause_resume_replication()

    @known_issue('GG-23986')
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers_separated_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False)
    def test_one_sender_down_with_second_sender_group_on_servers(self):
        self.one_sender_down_with_second_sender_group('server')

    @known_issue('GG-23986')
    @attr('common', 'on_clients')
    @with_setup(setup_testcase_master_master_on_clients_separated_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                start_clients=False, wait_for_replication_online=False)
    def test_one_sender_down_with_second_sender_group_on_clients(self):
        self.one_sender_down_with_second_sender_group('client')

    @test_case_id(259491)
    @attr('common', 'on_servers', 'ttl')
    @with_setup(setup_testcase_master_master_on_servers_ttl_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                preload=False, start_clients=False, wait_for_replication_online=False)
    def test_ttl_caches_servers(self):
        self.ttl_caches(0)

    @test_case_id(259491)
    @attr('common', 'on_clients', 'ttl')
    @with_setup(setup_testcase_master_master_on_clients_ttl_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                preload=False, start_clients=False, wait_for_replication_online=False)
    def test_ttl_caches_clients(self):
        self.ttl_caches(0)

    @attr('common', 'on_servers', 'ttl')
    @with_setup(setup_testcase_master_master_on_servers_ttl_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                preload=False, start_clients=False, wait_for_replication_online=False)
    def test_ttl_caches_clear_servers(self):
        self.ttl_caches(1)

    @attr('common', 'on_clients', 'ttl')
    @with_setup(setup_testcase_master_master_on_clients_ttl_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                preload=False, start_clients=False, wait_for_replication_online=False)
    def test_ttl_caches_clear_clients(self):
        self.ttl_caches(1)

    @attr('common', 'on_servers', 'ttl')
    @with_setup(setup_testcase_master_master_on_servers_ttl_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                preload=False, start_clients=False, wait_for_replication_online=False)
    def test_ttl_caches_double_clear_servers(self):
        self.ttl_caches(2)

    @attr('common', 'on_clients', 'ttl')
    @with_setup(setup_testcase_master_master_on_clients_ttl_caches, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                preload=False, start_clients=False, wait_for_replication_online=False)
    def test_ttl_caches_double_clear_clients(self):
        self.ttl_caches(2)

    @test_case_id(259492)
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False, enable_dr_events=False)
    def test_recreate_dynamic_caches_on_replica(self):
        self.case_recreate_dynamic_caches_on_replica()

    @test_case_id(259493)
    @attr('common', 'on_servers')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False, enable_dr_events=False)
    def test_add_server_nodes(self):
        self.case_add_server_nodes()

    def case_1_full_state_transfer(self):
        with PiClient(self.master, self.master_client_config, nodes_num=1) as piclient_master:
            with PiClient(self.replica, self.replica_client_config, nodes_num=1, new_instance=True) as piclient_replica:
                self.make_state_transfer(piclient=piclient_master)
                # util_sleep_for_a_while(20)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                checksum_sender_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                    self.master,
                    self.master_client_config)

                checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                    self.replica,
                    self.replica_client_config,
                    new_instance=True
                )

                self.assert_check_sums(checksum_sender_cluster, checksum_receiver_cluster)

                self._some_additional_loading(piclient_master, 1200, 1300)

                # util_sleep_for_a_while(20)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                checksum_sender_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                    self.master,
                    self.master_client_config)

                checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                    self.replica,
                    self.replica_client_config,
                    new_instance=True
                )

                self.assert_check_sums(checksum_sender_cluster, checksum_receiver_cluster)
                self._make_idle_verify()

    def case_1_full_state_transfer_all_api(self):
        master: Cluster = self.clusters[0]
        replica: Cluster = self.clusters[1]

        def upload_some_data(piclient, max_key):
            log_print('Start to load data', color='debug')
            async_operations = []
            remove_probability = 0.3
            for cache_name in cache_names_master:
                async_operation = create_async_operation(create_put_with_optional_remove_operation,
                                                         cache_name, 1, max_key, remove_probability,
                                                         gateway=piclient.get_gateway(),
                                                         use_monotonic_value=True)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        def compare_checksums(piclient1, piclient2, end_key, equal=True):
            checksum_master, checksum_replica = calculate_and_compare_checksums(piclient1, piclient2,
                                                                                start_key=1, end_key=end_key)
            if equal:
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')
            else:
                tiden_assert(not self._compare_dicts(checksum_master, checksum_replica),
                             'Expecting checksums are NOT equal')

        with create_piclient(master) as piclient_master:
            with create_piclient(replica) as piclient_replica:
                cache_names_master = [cache_name for cache_name in piclient_master.get_ignite().cacheNames().toArray()]
                upload_some_data(piclient_master, 300)

                for cache_name in cache_names_master:
                    if self.dr_is_stopped_over_jmx(cache_name, master.grid):
                        log_print(f"Replication for cache {cache_name} is stopped. Starting...")
                        dr_start_replication(piclient_master, cache_name)

                log_print("Make FST over JMX", color='green')
                self._fst_over_jmx(cache_names_master, master.grid)

                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                compare_checksums(piclient_master, piclient_replica, 300)

                log_print("Stop replication over Java API", color='green')
                for cache_name in [cache_name for cache_name in piclient_master.get_ignite().cacheNames().toArray()]:
                    dr_stop_replication(piclient_master, cache_name)

                for cache_name in [cache_name for cache_name in piclient_master.get_ignite().cacheNames().toArray()]:
                    piclient_replica.get_ignite().cache(cache_name).clear()

                compare_checksums(piclient_master, piclient_replica, 300, equal=False)

                upload_some_data(piclient_master, 500)
                log_print("Start replication over Java API", color='green')
                for cache_name in [cache_name for cache_name in piclient_master.get_ignite().cacheNames().toArray()]:
                    dr_start_replication(piclient_master, cache_name)

                log_print("Make FST over control utility", color='green')
                master.grid.cu.control_utility('--dr', 'cache .+ --action full-state-transfer --yes', node=1)

                self._wait_for_same_caches_size(piclient_master, piclient_replica, how_long=100)
                compare_checksums(piclient_master, piclient_replica, 500)

    def case_2_full_state_transfer_with_transactional_loading(self):
        with PiClient(self.master, self.master_client_config, nodes_num=1) as piclient_master:
            cache_names = piclient_master.get_ignite().cacheNames().toArray()
            cache_names = [cache_name for cache_name in cache_names]

            for i in range(1, 2):
                log_print(f'Iteration {i}', color='green')
                with TransactionalLoading(self,
                                          cross_cache_batch=2,
                                          skip_atomic=True,
                                          ignite=self.master,
                                          config_file=self.master_client_config):

                    util_sleep_for_a_while(10)
                    self.make_state_transfer(piclient=piclient_master)
                    util_sleep_for_a_while(10)

                with PiClient(self.replica, self.replica_client_config, nodes_num=1, new_instance=True) as piclient_replica:
                    self._wait_for_same_caches_size(piclient_master, piclient_replica)

                    log_print('Loading is done', color='debug')
                    util_sleep_for_a_while(10)

                    checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master,
                                                                                        piclient_replica)

                    tiden_assert(self._compare_dicts(checksum_master, checksum_replica),
                                 'Expecting checksums are equal')

                    self._some_additional_loading(piclient_master, 200, 300)

                    self._wait_for_same_caches_size(piclient_master, piclient_replica)

                    checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master,
                                                                                        piclient_replica)

                    tiden_assert(self._compare_dicts(checksum_master, checksum_replica),
                                 'Expecting checksums are equal')
                self._make_idle_verify()

    def case_2_full_state_transfer_with_load(self):
        with PiClient(self.master, self.master_client_config, nodes_num=1) as piclient_master:
            cache_names = piclient_master.get_ignite().cacheNames().toArray()
            cache_names = [cache_name for cache_name in cache_names]
            with PiClient(self.replica, self.replica_client_config, nodes_num=1, new_instance=True) as piclient_replica:

                async_operations = []
                start_key, end_key, remove_probability = 1001, 1300, 0.5
                for cache_name in cache_names:
                    async_operation = create_async_operation(
                        create_put_with_optional_remove_operation,
                        cache_name, start_key, end_key, remove_probability,
                        gateway=piclient_master.get_gateway(),
                        use_monotonic_value=True,
                    )

                    async_operations.append(async_operation)
                    async_operation.evaluate()

                util_sleep_for_a_while(10)
                self.make_state_transfer(piclient=piclient_master)

                for async_op in async_operations:
                    async_op.getResult()

                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                log_print('Loading is done', color='debug')
                util_sleep_for_a_while(10)

                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica)

                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')

                self._some_additional_loading(piclient_master, 200, 300)

                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica)

                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')
                self._make_idle_verify()

    def case_3_pause_resume_replication(self, replication_on_clients=True):
        self.run_client_nodes_on_start = False
        self.start_clusters(self.clusters)

        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.replica) as piclient_replica:
                log_print('Preload different data into both clusters', color='debug')
                static_cache_names_master = [cache_name for cache_name in
                                             piclient_master.get_ignite().cacheNames().toArray()]
                static_cache_names_replica = [cache_name for cache_name in
                                              piclient_replica.get_ignite().cacheNames().toArray()]

                def load_data(master_start_key, master_end_key, replica_start_key, replica_end_key):
                    async_operations = []
                    for cache_name in static_cache_names_master:
                        async_operation = create_async_operation(create_put_with_optional_remove_operation, cache_name,
                                                                 master_start_key, master_end_key, 0.5,
                                                                 gateway=piclient_master.get_gateway(),
                                                                 use_monotonic_value=True)
                        async_operation.evaluate()
                        async_operations.append(async_operation)
                    for cache_name in static_cache_names_replica:
                        async_operation = create_async_operation(create_put_with_optional_remove_operation, cache_name,
                                                                 replica_start_key, replica_end_key, 0.5,
                                                                 gateway=piclient_master.get_gateway(),
                                                                 use_monotonic_value=True)
                        async_operation.evaluate()
                        async_operations.append(async_operation)
                    for async_op in async_operations:
                        async_op.getResult()

                load_data(master_start_key=1, master_end_key=51, replica_start_key=51, replica_end_key=101)
                util_sleep_for_a_while(10)

                if replication_on_clients:
                    log_print('Check that replication on pause', color='debug')
                    not_stopped_caches_in_master = []
                    for cache_name in static_cache_names_master:
                        if not self.dr_is_stopped_over_jmx(cache_name, self.master.grid):
                            not_stopped_caches_in_master.append(cache_name)
                        # if not dr_is_stopped(piclient_master, cache_name):
                        #     not_stopped_caches_in_master.append(cache_name)
                    not_stopped_caches_in_replica = []
                    for cache_name in static_cache_names_replica:
                        if not self.dr_is_stopped_over_jmx(cache_name, self.replica.grid):
                            not_stopped_caches_in_replica.append(cache_name)
                        # if not dr_is_stopped(piclient_replica, cache_name):
                        #     not_stopped_caches_in_replica.append(cache_name)
                    tiden_assert((len(not_stopped_caches_in_master) == 0 and len(not_stopped_caches_in_replica) == 0),
                                 f'Expecting all caches are stopped. But these are not:'
                                 f'\nmaster:\n{not_stopped_caches_in_master}'
                                 f'\nreplica:\n{not_stopped_caches_in_replica}')

                    checksum_master = calc_checksums_on_client(piclient_master, start_key=1, end_key=101,
                                                               dict_mode=True)
                    checksum_replica = calc_checksums_on_client(piclient_replica, start_key=1, end_key=101,
                                                                dict_mode=True)
                    tiden_assert(checksum_master != checksum_replica,
                                 f'Expecting checksums are not equals:'
                                 f'\nmaster:\n{checksum_master}'
                                 f'\nreplica:\n{checksum_replica}')

        if replication_on_clients:
            log_print('Start client sender and receiver hubs', color='debug')
            for cluster in self.clusters:
                additional_nodes = cluster.get_client_node_ids()
                if additional_nodes:
                    cluster.grid.start_additional_nodes(additional_nodes, client_nodes=True)
            wait_for_replication_is_up_for_clusters(self.clusters)

        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.replica) as piclient_replica:
                for cache_name in static_cache_names_master:
                    self.master.grid.jmx.dr_start(cache_name)
                    self.replica.grid.jmx.dr_start(cache_name)
                # self._fst_over_jmx(static_cache_names_master, self.master.grid)
                for cache_name in static_cache_names_master:
                    dr_state_transfer(piclient_master, cache_name, 2)
                # self._fst_over_jmx(static_cache_names_replica, self.replica.grid, dc_id=1)
                for cache_name in static_cache_names_replica:
                    dr_state_transfer(piclient_replica, cache_name, 1)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                log_print('Check that data a equals after start and state transfer', color='debug')
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=101)

                tiden_assert(self._compare_dicts(checksum_master, checksum_replica),
                             'Expecting checksums are equal')

                log_print('Stop replication', color='debug')
                for cache_name in static_cache_names_master:
                    self.master.grid.jmx.dr_stop(cache_name)
                    self.replica.grid.jmx.dr_stop(cache_name)

                log_print('Load new data', color='debug')
                load_data(master_start_key=101, master_end_key=151, replica_start_key=151, replica_end_key=201)
                for cache_name in static_cache_names_master:
                    dr_start_replication(piclient_master, cache_name)
                    dr_start_replication(piclient_replica, cache_name)
                    # self.master.grid.jmx.dr_start(cache_name)
                    # self.replica.grid.jmx.dr_start(cache_name)
                # self._fst_over_jmx(static_cache_names_master, self.master.grid)
                # self._fst_over_jmx(static_cache_names_replica, self.replica.grid, dc_id=1)
                for cache_name in static_cache_names_master:
                    dr_state_transfer(piclient_master, cache_name, 2)
                for cache_name in static_cache_names_replica:
                    dr_state_transfer(piclient_replica, cache_name, 1)

                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                log_print('Check that data a equals after start and state transfer', color='debug')
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=201)
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica),
                             'Expecting checksums are equal')

                log_print('Load new data', color='debug')
                load_data(master_start_key=201, master_end_key=251, replica_start_key=251, replica_end_key=301)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=301)
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica),
                             'Expecting checksums are equal')

        self._make_idle_verify()

    def case_4_pause_resume_replication_under_load(self):
        self.start_clusters(self.clusters)
        wait_for_replication_is_up_for_clusters(self.clusters)

        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.replica) as piclient_replica:
                cache_names_master = [cache_name for cache_name in piclient_master.get_ignite().cacheNames().toArray()]
                log_print('Start to load data', color='debug')
                async_operations = []
                remove_probability = 0.3
                for cache_name in cache_names_master:
                    async_operation = create_async_operation(create_put_with_optional_remove_operation,
                                                             cache_name, 1, 401, remove_probability,
                                                             gateway=piclient_master.get_gateway(),
                                                             use_monotonic_value=True)
                    async_operations.append(async_operation)
                    async_operation.evaluate()

                util_sleep_for_a_while(4)

                log_print('Pause replication on senders', color='debug')
                for node in self.master.get_sender_nodes():
                    self.master.grid.jmx.dr_pause_for_all_receivers(node.id)

                util_sleep_for_a_while(40)

                try:
                    self._wait_for_same_caches_size(piclient_master, piclient_replica, how_long=60)
                    raise TidenException('Expecting caches are not sync.')
                except TidenException as e:
                    log_print('Caches are not sync.', color='debug')

                log_print('Resume replication on senders', color='debug')
                for node in self.master.get_sender_nodes():
                    self.master.grid.jmx.dr_resume_for_all_receivers(node.id)

                log_print('Waiting until loading finished', color='debug')
                for async_op in async_operations:
                    async_op.getResult()

                log_print('Finished', color='debug')

                self._wait_for_same_caches_size(piclient_master, piclient_replica, how_long=100)

                log_print('Check data after load', color='debug')
                checksum_master, checksum_replica = \
                    calculate_and_compare_checksums(piclient_master, piclient_replica, start_key=1, end_key=401)
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')
                self._make_idle_verify()
        self._make_idle_verify()

    def case_5_snapshot_replace_fst(self):
        master: Ignite = self.clusters[0].grid
        replica: Ignite = self.clusters[1].grid

        snapshot_id = master.su.snapshot_utility('snapshot', '-type=full')
        master.su.snapshot_utility('move', '-id={} -dest={}'.format(snapshot_id, self.snapshot_storage))
        replica.su.snapshot_utility('restore', '-id={} -src={}'.format(snapshot_id, self.snapshot_storage))

        with PiClient(self.clusters[0].grid, self.master_client_config, nodes_num=1) as piclient:
            self._start_replication(piclient=piclient)
            util_sleep_for_a_while(20)

            checksum_sender_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[0].grid,
                self.master_client_config)

            checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[1].grid,
                Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr'),
                new_instance=True
            )

            self.assert_check_sums(checksum_sender_cluster, checksum_receiver_cluster)

            self._some_additional_loading(piclient, 200, 300)

            util_sleep_for_a_while(20)

            checksum_sender_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[0].grid,
                self.master_client_config)

            checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[1].grid,
                Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr'),
                new_instance=True
            )

            self.assert_check_sums(checksum_sender_cluster, checksum_receiver_cluster)
            self._make_idle_verify()

    def case_6_rolling_enable_replication(self):
        def compaire_checksums(piclient1, piclient2):
            with Step(self, 'Compare checksums'):
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient1, piclient2,
                                                                                    start_key=1, end_key=301)
                tiden_assert(checksum_master == checksum_replica,
                             f'Checksums must be equals:\nmaster:\n{checksum_master}\nreplica:\n{checksum_replica}')

        log_print('Rolling upgrade of replica', color='debug')
        self.new_replica.grid = self.get_app(self.ignite_app_names['ignite2'])
        self.new_replica.grid.reset(True)
        self.new_replica.grid.set_additional_grid_name('ignite4')
        self.rolling_upgrade_cluster(self.replica, self.new_replica)

        self.new_replica.piclient_config = self.replica.piclient_config
        log_print('Load data after rolling upgrade of replica', color='debug')
        with Step(self, 'Load data after rolling upgrade of replica'):
            self.put_data(self.master, self.iteration, 'cluster_3_node_without_dr')
        self.iteration += 1

        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.new_replica) as piclient_replica:
                self._wait_for_same_caches_size(piclient_master, piclient_replica, how_long=30)
                compaire_checksums(piclient_master, piclient_replica)

        log_print('Rolling upgrade of master', color='debug')
        self.new_master.grid = self.get_app(self.ignite_app_names['ignite1'])
        self.new_master.grid.reset(True)
        self.new_master.grid.set_additional_grid_name('ignite3')
        self.rolling_upgrade_cluster(self.master, self.new_master)
        self.new_master.piclient_config = self.master.piclient_config

        log_print('Load data after rolling upgrade of master', color='debug')
        self.put_data(self.new_master, self.iteration, 'cluster_3_node_without_dr')
        self.iteration += 1
        with create_piclient(self.new_master) as piclient_master:
            with create_piclient(self.new_replica) as piclient_replica:
                self._wait_for_same_caches_size(piclient_master, piclient_replica, how_long=30)
                compaire_checksums(piclient_master, piclient_replica)

        log_print('Load data to replica', color='debug')
        self.put_data(self.new_replica, self.iteration, 'cluster_4_node_without_dr')
        self.iteration += 1
        log_print('Checksums after load data to replica', color='debug')
        with create_piclient(self.new_master) as piclient_master:
            with create_piclient(self.new_replica) as piclient_replica:
                self._wait_for_same_caches_size(piclient_master, piclient_replica, how_long=30)
                compaire_checksums(piclient_master, piclient_replica)
        self._make_idle_verify()

    def case_7_rolling_upgrade_with_pds_cleanup(self):
        def compaire_checksums(piclient1, piclient2):
            checksum_master, checksum_replica = calculate_and_compare_checksums(piclient1, piclient2,
                                                                                start_key=1, end_key=301)
            tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')

        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.replica) as piclient_replica:
                log_print('Rolling upgrade of replica', color='debug')
                self.new_replica.grid = self.get_app(self.ignite_app_names['ignite2'])
                self.new_replica.grid.reset(True)
                self.new_replica.grid.set_additional_grid_name('ignite4')
                self.upgrade_cluster(self.replica, self.new_replica)

                app = self.get_app(self.ignite_app_names['ignite2'])
                app.cu.activate(activate_on_particular_node=1)

                self.new_replica.piclient_config = self.replica.piclient_config

                with Step(self, 'State transfer'):
                    static_cache_names_master = [cache_name for cache_name in
                                                 piclient_master.get_ignite().cacheNames().toArray()]
                    for cache_name in static_cache_names_master:
                        dr_state_transfer(piclient_master, cache_name, 4)
                    self._wait_for_same_caches_size(piclient_master, piclient_replica)

                with Step(self, 'Load data to master'):
                    self.put_data(self.master, self.iteration, 'cluster_3_node_without_dr')
                    self.iteration += 1
                    self._wait_for_same_caches_size(piclient_master, piclient_replica)
                    log_print('Checksums', color='debug')
                    compaire_checksums(piclient_master, piclient_replica)

                with Step(self, 'Load data to replica'):
                    self.put_data(self.new_replica, self.iteration, 'cluster_2_node_without_dr')
                    self.iteration += 1
                    self._wait_for_same_caches_size(piclient_master, piclient_replica)
                    log_print('Checksums', color='debug')
                    compaire_checksums(piclient_master, piclient_replica)

                with Step(self, 'Execute idle_verify and validate_indexes on replica cluster'):
                    self.new_replica.grid.cu.control_utility('--cache', 'idle_verify')
                    self.new_replica.grid.cu.control_utility('--cache', 'validate_indexes')
                with Step(self, 'Execute idle_verify and validate_indexes on master cluster'):
                    self.master.grid.cu.control_utility('--cache', 'idle_verify')
                    self.master.grid.cu.control_utility('--cache', 'validate_indexes')

    @attr('common')
    @with_setup(setup_basic_dr_topology, teardown_testcase)
    def case_9_add_dynamic_caches_to_replication(self):
        self.start_clusters(self.clusters)
        wait_for_replication_is_up_for_clusters(self.clusters)
        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.replica) as piclient_replica:
                log_print('Create dynamic caches', color='debug')
                cache_config = create_cache_config(piclient_replica, 'dynamic_cache_1', 'group1')
                piclient_replica.get_ignite().createCache(cache_config.get_config_object())

                cache_config = create_cache_config(piclient_master, 'dynamic_cache_1', 'group1')
                piclient_master.get_ignite().createCache(cache_config.get_config_object())
                cache_config = create_cache_config(piclient_master, 'dynamic_cache_2', 'group1')
                piclient_master.get_ignite().createCache(cache_config.get_config_object())
                cache_config = create_cache_config(piclient_master, 'dynamic_cache_3', None)
                piclient_master.get_ignite().createCache(cache_config.get_config_object())

                all_dynamic_cache_names = ['dynamic_cache_1', 'dynamic_cache_2', 'dynamic_cache_3']

                def load_data(piclient, start_key, end_key):
                    async_operations = []
                    for cache_name in all_dynamic_cache_names:
                        async_operation = create_async_operation(create_streamer_operation,
                                                                 cache_name, start_key, end_key,
                                                                 value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                                 gateway=piclient.get_gateway())
                        async_operations.append(async_operation)
                        async_operation.evaluate()
                    for async_op in async_operations:
                        async_op.getResult()

                util_sleep_for_a_while(10)
                log_print('Load data into dynamic caches on master', color='debug')
                load_data(piclient_master, 1, 51)
                util_sleep_for_a_while(10)

                log_print('Check data after load', color='debug')
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=51)

                cache_names_replica = [cache_name for cache_name in
                                       piclient_replica.get_ignite().cacheNames().toArray()]

                tiden_assert(checksum_master['dynamic_cache_1'] == checksum_replica['dynamic_cache_1'],
                             'Checksums are not equals for cache {}'.format('dynamic_cache_1'))
                tiden_assert('dynamic_cache_2' not in cache_names_replica,
                             'Cache dynamic_cache_2 must not be exist in replica')
                tiden_assert('dynamic_cache_3' not in cache_names_replica,
                             'Cache dynamic_cache_3 must not be exist in replica')

                log_print('Create cache dynamic_cache_2 on receiver cluster', color='debug')
                cache_config = create_cache_config(piclient_replica, 'dynamic_cache_2', 'group1')
                piclient_replica.get_ignite().createCache(cache_config.get_config_object())
                # dr are not is stopped, so don't assert it (GG-22800)
                master_stopped = dr_is_stopped(piclient_master, 'dynamic_cache_2')
                replica_stopped = dr_is_stopped(piclient_replica, 'dynamic_cache_2')

                log_print("Start replication for cache dynamic_cache_2", color='green')
                dr_start_replication(piclient_master, 'dynamic_cache_2')
                log_print("Make full state transfer for cache dynamic_cache_2", color='green')
                dr_state_transfer(piclient_master, 'dynamic_cache_2', 2)

                util_sleep_for_a_while(10)
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=51)
                tiden_assert(checksum_master['dynamic_cache_1'] == checksum_replica['dynamic_cache_1'],
                             'Checksums must be equals')
                tiden_assert(checksum_master['dynamic_cache_2'] == checksum_replica['dynamic_cache_2'],
                             'Checksums must be equals')
                tiden_assert('dynamic_cache_3' not in cache_names_replica,
                             'Cache dynamic_cache_3 must not be exist in replica')

                log_print('Load data into dynamic caches on master', color='debug')
                load_data(piclient_replica, 51, 101)
                util_sleep_for_a_while(10)

                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=101)
                tiden_assert(checksum_master['dynamic_cache_1'] == checksum_replica['dynamic_cache_1'],
                             'Checksums must be equals:\n{}\n{}'.format(checksum_master['dynamic_cache_1'],
                                                                        checksum_replica['dynamic_cache_1']))
                tiden_assert(checksum_master['dynamic_cache_2'] == checksum_replica['dynamic_cache_2'],
                             'Checksums must be equals:\n{}\n{}'.format(checksum_master['dynamic_cache_2'],
                                                                        checksum_replica['dynamic_cache_2']))
        self._make_idle_verify()

    @attr('common')
    @with_setup(setup_testcase_servers_only, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=2, client_nodes_per_cluster=0)
    def test_10_add_sql_tables_to_replication(self):

        def get_balance_data(table_name, start_index, count):
            data = []
            starting_balance = 10000
            for i in range(start_index, start_index + count):
                data.append(f'({i}, {int(starting_balance) + i})')

            insert_statements = [
                '!tables',
                '\'insert into {} values {};\''.format(table_name, ','.join(data)),
            ]
            return insert_statements

        initial_setup_replica = [
            '!tables',
            '\'create table if not exists Balance_1(id int, balance bigint, PRIMARY KEY(id)) '
            'WITH "template=Balance, value_type=Balance";\'']

        initial_setup_master = [
                                   '!tables',
                                   '\'create table if not exists Balance_2(id int, balance bigint, PRIMARY KEY(id)) '
                                   'WITH "template=Balance, value_type=Balance";\'',
                                   '\'create table if not exists Balance_3(id int, balance bigint, PRIMARY KEY(id));\'',
                               ] + initial_setup_replica

        sql_tool_replica = \
            Sqlline(self.clusters[1].grid, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        sql_tool_master = \
            Sqlline(self.clusters[0].grid, ssl_connection=None if not self.ssl_enabled else self.ssl_conn_tuple)

        sql_tool_replica.run_sqlline(initial_setup_replica)
        sql_tool_master.run_sqlline(initial_setup_master)

        checks = [
            '!tables',
            '\'select count(1), sum(balance) from Balance_1;\'',
            '\'select count(1), sum(balance) from Balance_2;\'',
            '\'select count(1), sum(balance) from Balance_3;\''
        ]
        for table_name, starting_index in [('Balance_1', 1), ('Balance_2', 1), ('Balance_3', 1)]:
            insert_some_data = get_balance_data(table_name, starting_index, 2000)
            sql_tool_master.run_sqlline(insert_some_data + checks)

        util_sleep_for_a_while(5)
        output = sql_tool_replica.run_sqlline(checks)
        tiden_assert('select count(1), sum(balance) from Balance_1;'
                     '\n\'COUNT(1)\',\'SUM(BALANCE)\''
                     '\n\'2000\',\'22001000\'' in output, 'Expecting data for Balance_1 is replicated')

        create_cache_on_replica = [
            '!tables',
            '\'create table if not exists Balance_2(id int, balance bigint, PRIMARY KEY(id)) '
            'WITH "template=Balance, value_type=Balance";\''
        ]

        sql_tool_replica.run_sqlline(create_cache_on_replica)

        util_sleep_for_a_while(3, 'Wait for cache creation')

        self.make_state_transfer(cache_mask='BALANCE_2')

        util_sleep_for_a_while(30)
        sql_tool_replica.run_sqlline(checks)

        for table_name, starting_index in [('Balance_2', 2001)]:
            insert_some_data = get_balance_data(table_name, starting_index, 1000)
            sql_tool_master.run_sqlline(insert_some_data + checks)

        util_sleep_for_a_while(5)
        sql_tool_replica.run_sqlline(checks)

        remove_some_data = [
            '\'delete from Balance_1 where id > 1500;\'',
            '\'delete from Balance_2 where id < 100;\''
        ]
        sql_tool_master.run_sqlline(remove_some_data + checks)
        util_sleep_for_a_while(5)
        output = sql_tool_replica.run_sqlline(checks)
        tiden_assert('select count(1), sum(balance) from Balance_2;'
                     '\n\'COUNT(1)\',\'SUM(BALANCE)\''
                     '\n\'2901\',\'33506550\'' in output, 'Expecting data for Balance_2 is replicated')
        self._make_idle_verify()

    def case_13_new_pause_resume_replication(self):
        self.start_clusters(self.clusters)
        wait_for_replication_is_up_for_clusters(self.clusters)

        with create_piclient(self.master) as piclient_master:
            with create_piclient(self.replica) as piclient_replica:
                cache_names_master = [cache_name for cache_name in piclient_master.get_ignite().cacheNames().toArray()]
                log_print('Start to load data', color='debug')
                iteration = 0

                def load_data(iteration):
                    async_operations = []
                    for cache_name in cache_names_master:
                        async_operation = create_async_operation(create_streamer_operation, cache_name,
                                                                 iteration * 50 + 1, iteration * 50 + 51,
                                                                 value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                                 gateway=piclient_master.get_gateway())
                        async_operations.append(async_operation)
                        async_operation.evaluate()
                    for async_op in async_operations:
                        async_op.getResult()
                    iteration += 1

                load_data(iteration)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                log_print('Check data after load', color='debug')
                checksum_master, checksum_replica = \
                    calculate_and_compare_checksums(piclient_master, piclient_replica, start_key=1, end_key=151)
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')

                for node in self.replica.get_sender_nodes():
                    self.master.grid.jmx.dr_pause_for_all_receivers(node.id)

                load_data(iteration)

                util_sleep_for_a_while(20)
                log_print('Verify thant data has not replacated after pause', color='debug')
                caches_size = self.get_caches_size(piclient=piclient_replica, debug=False)
                for size in caches_size.values():
                    if size != int((iteration + 1) * 50):
                        tiden_assert(False, f'Replica has more entries ({size}) than expected:\n{caches_size}')

                for node in self.replica.get_sender_nodes():
                    self.master.grid.jmx.dr_resume_for_all_receivers(node.id)

                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                log_print('Check data after resume', color='debug')
                checksum_master, checksum_replica = \
                    calculate_and_compare_checksums(piclient_master, piclient_replica, start_key=1, end_key=151)
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')

                load_data(iteration)

                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                log_print('Check data after additional data load', color='debug')
                checksum_master, checksum_replica = \
                    calculate_and_compare_checksums(piclient_master, piclient_replica, start_key=1, end_key=151)
                tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')
        self._make_idle_verify()

    def case_add_server_nodes(self):
        """
        Pre-condition: default DR setup on servers:
            4 server nodes in each cluster
            2 nodes are senders/receivers
            replication is up and running
            some data uploaded into master cluster and replicated into replica cluster

        Steps:
            1. Start additional server node in master cluster (node is not in baseline).
            2. Start additional server node in replica cluster (node is not in baseline).
            3. Upload some data into caches on master.
            4. Check data is successfully replicated.
            5. Add additional nodes to baseline on master and replica clusters.
            6. Upload some data into caches on master.
            7. Check data is successfully replicated.
        :return:
        """
        master: Ignite = self.clusters[0].grid
        replica: Ignite = self.clusters[1].grid

        def compaire_checksums(piclient1, piclient2, end_key):
            checksum_master, checksum_replica = calculate_and_compare_checksums(piclient1, piclient2,
                                                                                start_key=1, end_key=end_key)
            tiden_assert(self._compare_dicts(checksum_master, checksum_replica), 'Expecting checksums are equal')

        def upload_data_and_wait_for_sync(start_key, end_key):
            async_operations = []
            remove_probability = 0.5
            for cache_name in cache_names:
                async_operation = create_async_operation(
                    create_put_with_optional_remove_operation,
                    cache_name, start_key, end_key, remove_probability,
                    gateway=piclient_master.get_gateway(),
                    use_monotonic_value=True,
                )

                async_operations.append(async_operation)
                async_operation.evaluate()

            util_sleep_for_a_while(10)

            for async_op in async_operations:
                async_op.getResult()

            self._wait_for_same_caches_size(piclient_master, piclient_replica)

        with PiClient(master, self.master_client_config, nodes_num=1) as piclient_master:
            cache_names = piclient_master.get_ignite().cacheNames().toArray()
            cache_names = [cache_name for cache_name in cache_names]
            with PiClient(replica, self.replica_client_config, new_instance=True, nodes_num=1) as piclient_replica:
                dynamic_caches = ['dynamic_cache_1', 'dynamic_cache_2', 'dynamic_cache_3']
                for cache_name in dynamic_caches:
                    log_print('Create dynamic caches', color='debug')
                    cache_config = create_cache_config(piclient_replica, cache_name, 'group1')
                    piclient_replica.get_ignite().createCache(cache_config.get_config_object())

                    cache_config = create_cache_config(piclient_master, cache_name, 'group1')
                    piclient_master.get_ignite().createCache(cache_config.get_config_object())

                cache_names += dynamic_caches
                upload_data_and_wait_for_sync(1001, 1300)
                compaire_checksums(piclient_master, piclient_replica, 1300)

                server_config = Ignite.config_builder.get_config('server', config_set_name='cluster_1_node_1')
                log_print('Starting additional node on master cluster')
                master.start_additional_nodes(
                    master.add_additional_nodes(config=server_config, num_nodes=1, name='ignite1'),
                    name='ignite1')

                server_config = Ignite.config_builder.get_config('server', config_set_name='cluster_2_node_1')
                log_print('Starting additional node on replica cluster')
                replica.start_additional_nodes(
                    replica.add_additional_nodes(config=server_config, num_nodes=1, name='ignite2'),
                    name='ignite2'
                )

                upload_data_and_wait_for_sync(1301, 3500)
                compaire_checksums(piclient_master, piclient_replica, 3500)

                master.cu.set_current_topology_as_baseline()
                replica.cu.set_current_topology_as_baseline()
                upload_data_and_wait_for_sync(3501, 4000)
                compaire_checksums(piclient_master, piclient_replica, 4000)

                replica.cu.deactivate()
                master.cu.deactivate()

    def case_recreate_dynamic_caches_on_replica(self):
        """
        This test is ported from i2test: master-master-pds-with-recreate-caches
        Pre-condition: default DR setup on servers:
            4 server nodes in each cluster
            2 nodes are senders/receivers
            replication is up and running
            some data uploaded into master cluster and replicated into replica cluster

        Steps:
            1. Create caches on master and replica clusters.
            2. Upload data into master cluster and check data replicated into replica cluster.
            3. Recreate some caches (destroy and create) on replica cluster.
            4. Upload data into master cluster and check new data for recreated caches replicated into replica cluster.
        :return:
        """
        master: Ignite = self.clusters[0].grid
        replica: Ignite = self.clusters[1].grid

        def upload_data_and_wait_for_sync(start_key, end_key, wait_for_sync=True, on_master=True):
            self.make_put_and_remove_operations(piclient_master if on_master else piclient_replica, start_key, end_key)

            if wait_for_sync:
                self._wait_for_same_caches_size(piclient_master, piclient_replica)

        with PiClient(master, self.master_client_config, nodes_num=1) as piclient_master:
            cache_names = piclient_master.get_ignite().cacheNames().toArray()
            cache_names = [cache_name for cache_name in cache_names]
            with PiClient(replica, self.replica_client_config, new_instance=True, nodes_num=1) as piclient_replica:

                # Create caches on master and replica clusters
                dynamic_caches = ['dynamic_cache_1', 'dynamic_cache_2', 'dynamic_cache_3']
                for cache_name in dynamic_caches:
                    log_print('Create dynamic caches on replica', color='debug')
                    cache_config = create_cache_config(piclient_replica, cache_name, 'group1')
                    piclient_replica.get_ignite().createCache(cache_config.get_config_object())

                    log_print('Create dynamic caches on master', color='debug')
                    cache_config = create_cache_config(piclient_master, cache_name, 'group1')
                    piclient_master.get_ignite().createCache(cache_config.get_config_object())

                cache_names += dynamic_caches
                # Upload data into master cluster and check data replicated into replica cluster
                upload_data_and_wait_for_sync(1001, 1300)

                # Recreate some caches (destroy and create) on replica cluster
                for cache_name in dynamic_caches:
                    log_print('Destroy dynamic caches on replica', color='debug')
                    piclient_replica.get_ignite().cache(cache_name).destroy()

                for cache_name in dynamic_caches:
                    log_print('Create dynamic caches on replica', color='debug')
                    cache_config = create_cache_config(piclient_replica, cache_name, 'group1')
                    piclient_replica.get_ignite().createCache(cache_config.get_config_object())

                # Upload data into master cluster and check new data for recreated caches
                # replicated into replica cluster
                upload_data_and_wait_for_sync(1301, 3000, wait_for_sync=False)
                util_sleep_for_a_while(20)
                log_print('Upload data to replica', color='debug')
                upload_data_and_wait_for_sync(3001, 3500, wait_for_sync=False, on_master=False)
                util_sleep_for_a_while(20)

                checksum_master, checksum_replica = \
                    calculate_and_compare_checksums(piclient_master, piclient_replica, start_key=1301, end_key=3500)
                tiden_assert(self.compare_checksums_only(checksum_master, checksum_replica),
                             'Expecting checksums are equal')

                replica.cu.deactivate()
                master.cu.deactivate()

    @attr('excluded')
    @with_setup(setup_testcase_master_master_on_servers, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                start_clients=False, wait_for_replication_online=False, snapshot_storage=False)
    def test_active_active_load(self):
        events_async_operations = []
        with PiClient(self.master, self.master_client_config) as piclient_master:
            events_async_operation = create_async_operation(
                create_event_collect_operation,
                [1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028],
                gateway=piclient_master.get_gateway()
            )

            events_async_operations.append(events_async_operation)
            events_async_operation.evaluate()

            with PiClient(self.replica, self.replica_client_config, new_instance=True) as piclient_replica:
                cache_mask = ''
                cache_names = piclient_master.get_ignite().cacheNames().toArray()
                cache_names = [cache_name for cache_name in cache_names if cache_mask in cache_name]
                self._start_replication_over_jmx(cache_names, self.master)
                util_sleep_for_a_while(10)
                self._fst_over_jmx(cache_names, self.master)

                # self._state_transfer(cache_mask='', piclient=piclient_master)
                # self._state_transfer(cache_mask='', piclient=piclient_replica)
                util_sleep_for_a_while(20)

                futures = []
                with ThreadPoolExecutor(max_workers=2) as executor:
                    for piclient in [piclient_master, piclient_replica]:
                        # for piclient in [piclient_replica]:
                        futures.append(executor.submit(self.make_put_and_remove_operations, piclient, 500, 600))

                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica,
                                                                                    start_key=1, end_key=101)
                tiden_assert(checksum_master == checksum_replica,
                             'Checksums must be equals')

            for async_op in events_async_operations:
                async_op.getOperation().interrupt()
                log_print(async_op.getResult(), color='blue')

    @attr('test')
    @with_setup(setup_testcase_master_master_on_clients, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=2, client_nodes_per_cluster=2,
                start_clients=False, wait_for_replication_online=False, snapshot_storage=False)
    def test_active_active_sender_restart(self):
        events_async_operations = []
        with PiClient(self.master, self.master_client_config) as piclient_master:
            events_async_operation = create_async_operation(
                create_event_collect_operation,
                [1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028],
                gateway=piclient_master.get_gateway()
            )

            events_async_operations.append(events_async_operation)
            events_async_operation.evaluate()

            with PiClient(self.replica, self.replica_client_config, new_instance=True) as piclient_replica:
                cache_mask = ''
                cache_names = piclient_master.get_ignite().cacheNames().toArray()
                cache_names = [cache_name for cache_name in cache_names if cache_mask in cache_name]
                self._start_replication_over_jmx(cache_names, self.master)
                util_sleep_for_a_while(10)
                self._fst_over_jmx(cache_names, self.master)

                # self._state_transfer(cache_mask='', piclient=piclient_master)
                # self._state_transfer(cache_mask='', piclient=piclient_replica)
                util_sleep_for_a_while(20)

                async_operations = []
                start_key, end_key, remove_probability = 500, 1300, 0.5
                for cache_name in cache_names:
                    async_operation = create_async_operation(
                        create_put_with_optional_remove_operation,
                        cache_name, start_key, end_key, remove_probability,
                        gateway=piclient_master.get_gateway(),
                        use_monotonic_value=True,
                    )

                    async_operations.append(async_operation)
                    async_operation.evaluate()

                util_sleep_for_a_while(20)
                sender_id = 3
                self.master.kill_node(sender_id)

                for async_op in async_operations:
                    async_op.getResult()

                self._wait_for_same_caches_size(piclient_master, piclient_replica)

                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica)
                tiden_assert(checksum_master == checksum_replica,
                             'Checksums must be equals')

                self.master.start_additional_nodes(sender_id, skip_topology_check=True)
                util_sleep_for_a_while(30)
                checksum_master, checksum_replica = calculate_and_compare_checksums(piclient_master, piclient_replica)
                tiden_assert(checksum_master == checksum_replica,
                             'Checksums must be equals')

            for async_op in events_async_operations:
                async_op.getOperation().interrupt()
                log_print(async_op.getResult(), color='blue')

    @with_setup(setup_empty, teardown_testcase)
    def test_draft_master_master_slave(self):
        self.clusters = self.generate_dr_topology(3, 4, 2)

        create_dr_relation(self.clusters[0].nodes[4], self.clusters[2].nodes[4])
        create_dr_relation(self.clusters[0].nodes[5], self.clusters[2].nodes[5])

        create_dr_relation(self.clusters[1].nodes[4], self.clusters[2].nodes[4])
        create_dr_relation(self.clusters[1].nodes[5], self.clusters[2].nodes[5])

        self.generate_app_config(self.clusters)
        self.start_clusters(self.clusters)
        wait_for_replication_is_up_for_clusters(self.clusters)

        self.put_data(self.clusters[0], 0, 'cluster_1_node_without_dr')
        self.put_data(self.clusters[1], 1, 'cluster_2_node_without_dr')

        sleep(10)

        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')
        with PiClient(self.clusters[0].grid, client_config) as piclient:
            # cache_names = piclient.get_ignite().cacheNames().toArray()
            cache_names = ['cache_group_1_001']
            checksum_master1 = [create_checksum_operation(cache_name, 1, 200).evaluate() for cache_name in cache_names]
            print('checksum of master cluster:\n' + str(checksum_master1))

        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr')
        with PiClient(self.clusters[1].grid, client_config) as piclient:
            # cache_names = piclient.get_ignite().cacheNames().toArray()
            cache_names = ['cache_group_1_001']
            checksum_slave1 = [create_checksum_operation(cache_name, 1, 200).evaluate() for cache_name in cache_names]
            print('checksum of slave cluster:\n' + str(checksum_slave1))

        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_3_node_without_dr')
        with PiClient(self.clusters[2].grid, client_config) as piclient:
            # cache_names = piclient.get_ignite().cacheNames().toArray()
            cache_names = ['cache_group_1_001']
            checksum_slave2 = [create_checksum_operation(cache_name, 1, 200).evaluate() for cache_name in cache_names]
            print('checksum of slave cluster:\n' + str(checksum_slave2))

    @step(attach_parameters=True)
    def put_data(self, cluster, iteration, config_name):
        client_config = Ignite.config_builder.get_config('client', config_set_name=config_name)
        with PiClient(cluster.grid, client_config) as piclient:
            cache_names = piclient.get_ignite().cacheNames().toArray()
            operations = [
                create_put_all_operation(cache_name, iteration * 10 + 1, iteration * 10 + 11, 100,
                                         value_type=ModelTypes.VALUE_ALL_TYPES.value)
                for cache_name in cache_names]
            [operation.evaluate() for operation in operations]

    @with_setup(setup_testcase_servers_only, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=2, client_nodes_per_cluster=0)
    def test_configurations(self):
        cmd = [
            'rm {}/libs/{}'.format(
                self.clusters[0].grid.config['artifacts'][self.ignite_app_names['ignite2']]['remote_path'],
                self.clusters[0].grid.config['artifacts']['piclient']['remote_path'].split('/')[-1])
        ]

        self.clusters[0].grid.ssh.exec(cmd)

        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')

        with PiClient(self.clusters[0].grid, client_config) as piclient:
            cache_names = piclient.get_ignite().cacheNames().toArray()
            cache_names = [cache_name for cache_name in cache_names]

        for i in range(0, 1):
            log_print(f'Iteration {i + 1}', color='green')
            PiClientIgniteUtils.load_data_with_streamer_batch(self.clusters[0].grid,
                                                              client_config,
                                                              start_key=1000 * i,
                                                              end_key=1000 * i + 1000,
                                                              parallel_operations=8,
                                                              check_clients=False,
                                                              silent=True,
                                                              allow_overwrite=True)

            self.clusters[0].grid.jmx.get_dr_metrics(90, cache_names)
            checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[1].grid,
                Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr'))
            checksum_sender_cluster = PiClientIgniteUtils.calc_checksums_distributed(self.clusters[0].grid,
                                                                                     client_config)
            tiden_assert(checksum_sender_cluster == checksum_receiver_cluster, 'Expecting checksums are equal')

    @with_setup(setup_testcase_servers_only, teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=2, client_nodes_per_cluster=0)
    def test_configurations_with_loading(self):

        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')
        PiClientIgniteUtils.load_data_with_streamer_batch(self.clusters[0].grid,
                                                          client_config,
                                                          start_key=0,
                                                          end_key=1000,
                                                          parallel_operations=8,
                                                          check_clients=True,
                                                          silent=True,
                                                          allow_overwrite=True)
        for i in range(0, 5):
            log_print(f'Iteration {i + 1}', color='green')
            with PiClient(self.clusters[0].grid, client_config):
                with TransactionalLoading(self,
                                          ignite=self.clusters[0].grid,
                                          config_file=client_config,
                                          loading_profile=LoadingProfile(delay=1, run_for_seconds=40)):
                    util_sleep_for_a_while(20)
                    # self.clusters[0].grid.jmx.get_dr_metrics(90, cache_names)

            checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[1].grid,
                Ignite.config_builder.get_config('client', config_set_name='cluster_2_node_without_dr')
            )
            checksum_serner_cluster = PiClientIgniteUtils.calc_checksums_distributed(
                self.clusters[0].grid,
                client_config)

            tiden_assert(checksum_serner_cluster == checksum_receiver_cluster, 'Expecting checksums are equal')

    def one_sender_down_with_second_sender_group(self, node_type):
        """
        DR cluster #1
            2 data nodes
            1 sender node sending "group1" caches group (all caches)
            1 sender node sending "group2" caches group (all caches)
        DR cluster #2
            2 data nodes
            1 sender node sending "group1" caches group
                (all caches, but cache name and group name started with "sender_group_2_")
            1 sender node sending "group2" caches group
                (all caches, but cache name and group name started with "sender_group_2_")

        1. Start load
        2. Kill node which is sending cache "group2"
        3. Wait until caches from sender "group2" will be Stopped and others will be Running as they should be
        4. Check caches size in both clusters, some caches from group2 should not have all values

        :param node_type:    sender node type with sender group "group2" which should be killed
        """
        with PiClient(self.clusters[0].grid, self.master_client_config, nodes_num=1) as piclient_master:
            with PiClient(self.clusters[1].grid, self.replica_client_config, new_instance=True, nodes_num=1) as piclient_replica:
                cluster_1: Ignite = self.clusters[0].grid
                self.make_state_transfer(piclient=piclient_master)
                # util_sleep_for_a_while(30)
                self._wait_for_same_caches_size(piclient_master, piclient_replica)
                master_counts = self.get_caches_size(piclient=piclient_master)
                replica_counts = self.get_caches_size(piclient=piclient_replica)
                tiden_assert(master_counts == replica_counts, 'Caches are the same after FST.')
                cache_names_master = list(piclient_master.get_ignite().cacheNames().toArray())

                log_print('Start to load data', color='debug')
                async_operations = []
                log_print()
                for cache_name in cache_names_master:
                    async_operation = create_async_operation(create_put_all_operation,
                                                             cache_name,
                                                             self.preloading_size,
                                                             # 100,
                                                             self.preloading_size + 300,
                                                             # 300,
                                                             100,
                                                             key_type='java.lang.Long',
                                                             value_type=ModelTypes.VALUE_ALL_TYPES.value,
                                                             gateway=piclient_master.get_gateway())
                    async_operations.append(async_operation)
                    async_operation.evaluate()

                util_sleep_for_a_while(1)

                found_nodes = [c_node.id for c_node in self.clusters[0].nodes if
                               c_node.node_type == node_type and c_node.sender_nodes]
                log_print('Going to kill node {}'.format(found_nodes[1]), color='debug')
                cluster_1.kill_node(found_nodes[1])
                util_sleep_for_a_while(2)

                def get_wrong_states() -> dict:
                    """
                    Find caches with wrong status
                    Caches from "group1" should be Active
                    Caches from "group2" should be Stopped
                    :return: {'cache_name_1': {'expected': 'Active', 'actulal': 'Stopped...'}}
                    """
                    wrong_states = {}
                    for cache_name in cache_names_master:
                        attr_result = cluster_1.jmx.get_attributes(1, cache_name, "Cache data replication", "DrStatus")
                        actual = attr_result['DrStatus']
                        expected = 'Active' if 'sender_group_2' not in cache_name else 'Stop'
                        if expected not in actual:
                            wrong_states[cache_name] = {'expected': expected, 'actual': actual}
                    return wrong_states

                end_time = time() + 60
                log_print('Waiting for right statuses')
                while True:
                    statuses = get_wrong_states()
                    if not statuses:
                        break
                    if time() > end_time:
                        for cache_name, status in statuses.items():
                            log_print(
                                f'status {cache_name} | Expected: {status["expected"]} | Actual: {status["actual"]}',
                                color='red')
                        tiden_assert(False, 'Found caches with different statuses')
                        break
                    sleep(5)
                log_print('Statuses are good')

                for async_op in async_operations:
                    async_op.getResult()
                # util_sleep_for_a_while(10)
                self._wait_for_same_caches_size(piclient_master, piclient_replica,
                                                predicate=lambda x: 'sender_group_2' not in x)

                master_counts = self.get_caches_size(piclient=piclient_master)
                replica_counts = self.get_caches_size(piclient=piclient_replica)

                wrong_counts = []
                for cache_name, count in master_counts.items():
                    if count != (self.preloading_size + 300):
                    # if count != 300:
                        # all caches should be filled
                        wrong_counts.append((cache_name, count))
                for cache_name, count in wrong_counts:
                    log_print(f'count {cache_name} | Expected: {self.preloading_size + 300} | Actual: {count}', color='red')
                    # log_print(f'count {cache_name} | Expected: {300} | Actual: {count}', color='red')
                tiden_assert(not wrong_counts, 'Sender cluster has caches with wrong sizes')

                not_completed_caches = []
                for cache_name, count in replica_counts.items():
                    if cache_name.startswith('sender_group_2'):
                        # caches from group2 should not be found filled fully
                        # if self.preloading_size + 100 <= count < self.preloading_size + 300:
                        if self.preloading_size <= count < (self.preloading_size + 300):
                            log_print(f'Not completed cache {cache_name} {count}', color='green')
                            not_completed_caches.append(cache_name)
                tiden_assert(not_completed_caches, 'All caches replicated successfully')

    def ttl_caches(self, clear_count):
        with PiClient(self.clusters[0].grid, self.master_client_config, nodes_num=1) as piclient_master:
            with PiClient(self.clusters[1].grid, self.replica_client_config, new_instance=True, nodes_num=1) as piclient_replica:

                if clear_count > 0:
                    cache_names_master = list(piclient_master.get_ignite().cacheNames().toArray())
                    for cache_name in cache_names_master:
                        piclient_replica.get_ignite().cache(cache_name).clear()
                    self.make_state_transfer(cache_mask='ttl', piclient=piclient_master)
                    self._wait_for_same_caches_size(piclient_master, piclient_replica)

                if clear_count == 2:
                    for cache_name in cache_names_master:
                        piclient_master.get_ignite().cache(cache_name).clear()
                    self.make_state_transfer(cache_mask='ttl', piclient=piclient_replica)
                    self._wait_for_same_caches_size(piclient_master, piclient_replica)

                util_sleep_for_a_while(60 * 2)
                for piclient, name in [(piclient_master, 'master'), (piclient_replica, 'replica')]:
                    sizes = self.get_caches_size(piclient=piclient_master)
                    assert_sizes = [f'{cache_name}={cache_size}' for cache_name, cache_size in sizes.items()
                                    if cache_name.startswith('ttl') and cache_size > 0]
                    tiden_assert(not assert_sizes,
                                 'Entries found in {} caches: \n{}'.format(name, '\n'.join(assert_sizes)))

    def assert_checksums(self):
        checksum_sender_cluster = PiClientIgniteUtils.calc_checksums_distributed(
            self.master,
            self.master_client_config)

        checksum_receiver_cluster = PiClientIgniteUtils.calc_checksums_distributed(
            self.replica,
            self.replica_client_config,
            new_instance=True)
        self.assert_check_sums(checksum_sender_cluster, checksum_receiver_cluster)

    @step('Rolling upgrade')
    def rolling_upgrade_cluster(self, old_cluster: Cluster, new_cluster: Cluster):
        for node_id in old_cluster.grid.nodes:
            with Step(self, f'Upgrade node #{node_id}'):
                if node_id <= len(old_cluster.nodes):
                    old_node = old_cluster.grid.nodes[node_id]
                    new_node = new_cluster.grid.nodes[node_id]

                    log_print('Stop node {} in grid {}'.format(node_id, old_cluster.id))
                    with Step(self, f'Kill node #{node_id}'):
                        old_cluster.grid.kill_nodes(node_id)
                    self.copy_work_directory(old_node, new_node)
                    if old_cluster.nodes[node_id - 1].node_type == 'server':
                        with Step(self, f'Start node {node_id - 1} in grid {new_cluster.id}'):
                            self.start_ignite_nodes(new_cluster,
                                                    [new_cluster.nodes[node_id - 1]], [],
                                                    len(new_cluster.get_server_node_ids()) - node_id,
                                                    cluster_for_config_name=old_cluster)
                    else:
                        log_print('Start client node {} in grid {}'.format(node_id - 1, new_cluster.id))
                        with Step(self, f'Start client node {node_id - 1} in grid {new_cluster.id}'):
                            self.start_ignite_nodes(new_cluster,
                                                    [], [new_cluster.nodes[node_id - 1]],
                                                    0, cluster_for_config_name=old_cluster)
                            new_cluster.grid.wait_for_topology_snapshot(len(new_cluster.get_server_node_ids()),
                                                                        len(new_cluster.get_client_node_ids()),
                                                                        other_nodes=len(
                                                                            new_cluster.get_server_node_ids()) + len(
                                                                            new_cluster.get_client_node_ids()) - node_id)
                    if new_cluster == self.new_master:
                        self.put_data(self.new_master, self.iteration, 'cluster_3_node_without_dr')
                    else:
                        self.put_data(self.master, self.iteration, 'cluster_3_node_without_dr')
                    self.iteration += 1

    @step()
    def upgrade_cluster(self, old_cluster: Cluster, new_cluster: Cluster):
        for node in old_cluster.nodes:
            old_cluster.grid.kill_nodes(node.id)
        self.start_ignite_nodes(new_cluster,
                                new_cluster.get_server_nodes(), new_cluster.get_client_nodes(),
                                0, cluster_for_config_name=old_cluster)

    @step(attach_parameters=True)
    def copy_work_directory(self, source_node, dest_node):
        cmd = dict()
        command = 'cp -a %s/%s/. %s/%s/' % (
            source_node['ignite_home'],
            'work',
            dest_node['ignite_home'],
            'work',
        )
        host = source_node['host']
        cmd[host] = [command]
        self.tiden.ssh.exec(cmd)

    @step('Make idle verify')
    def _make_idle_verify(self):
        master: Ignite = self.clusters[0].grid
        replica: Ignite = self.clusters[1].grid
        log_print('Execute idle_verify and validate_indexes on REPLICA cluster')
        replica.cu.control_utility('--cache', 'idle_verify')
        replica.cu.control_utility('--cache', 'validate_indexes')
        log_print('Execute idle_verify and validate_indexes on MASTER cluster')
        master.cu.control_utility('--cache', 'idle_verify')
        master.cu.control_utility('--cache', 'validate_indexes')

    @staticmethod
    def _some_additional_loading(piclient, start_key, end_key):
        cache_names = piclient.get_ignite().cacheNames()
        log_print("Loading %s values per cache into %s caches" % (end_key - start_key, cache_names.size()))

        batch_size = 100
        key_type = 'java.lang.Long'
        key_map = None
        value_type = ModelTypes.VALUE_ALL_TYPES_INDEXED.value
        async_operations = []
        for cache_name in cache_names.toArray():
            async_operation = create_async_operation(create_put_all_operation,
                                                     cache_name,
                                                     start_key,
                                                     end_key,
                                                     batch_size,
                                                     key_type=key_map[cache_name] if key_map else key_type,
                                                     value_type=value_type,
                                                     with_exception=None,
                                                     gateway=piclient.get_gateway())
            async_operations.append(async_operation)
            async_operation.evaluate()

        for async_op in async_operations:
            async_op.getResult()

        log_print("Data loading is done")

    def make_state_transfer(self, cache_mask='', piclient=None):
        client_config = Ignite.config_builder.get_config('client', config_set_name='cluster_1_node_without_dr')

        if piclient:
            self._state_transfer(cache_mask, piclient)
        else:
            with PiClient(self.clusters[0].grid, client_config, nodes_num=1) as piclient:
                self._state_transfer(cache_mask, piclient)

    @staticmethod
    def get_caches_size(cache_mask=lambda x: '' in x, piclient=None, debug=True):
        from pt.piclient.helper.cache_utils import IgniteCache
        cache_size = {}
        cache_names = piclient.get_ignite().cacheNames().toArray()
        # cache_names = [cache_name for cache_name in cache_names if cache_mask in cache_name]
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
    def _start_replication(cache_mask='', piclient=None):
        cache_names = piclient.get_ignite().cacheNames().toArray()
        cache_names = [cache_name for cache_name in cache_names if cache_mask in cache_name]

        for cache_name in cache_names:
            log_print('Running State transfer for cache {}'.format(cache_name), color='blue')
            piclient.get_ignite().cache(cache_name)
            status = piclient.get_ignite().plugin("GridGain").dr().senderCacheStatus(cache_name)
            log_print('DR status for cache {} - {}'.format(cache_name, status))
            piclient.get_ignite().plugin("GridGain").dr().resume(cache_name)

    @staticmethod
    def dr_is_stopped_over_jmx(cache, grid: Ignite):
        replication_state = grid.jmx.dr_get_status(cache, metric_name='DrStatus')
        log_print('Got replication status {} for cache {}'.format(replication_state, cache), color='blue')
        return 'Stopped' in replication_state

    @staticmethod
    def _get_dr_status_over_jmx(cache, grid: Ignite):
        replication_state = grid.jmx.dr_get_status(cache, metric_name='DrStatus')
        log_print('Got replication status {} for cache {}'.format(replication_state, cache), color='blue')
        return replication_state

    @staticmethod
    def _start_replication_over_jmx(caches, grid: Ignite):
        for cache_name in caches:
            log_print('Running Resume for cache {}'.format(cache_name), color='blue')
            grid.jmx.dr_resume(cache_name)
            grid.jmx.wait_for_dr_attributes(90, cache_name, metric_name='DrStatus', expected_value='Active')

    @staticmethod
    def _fst_over_jmx(caches, grid: Ignite, dc_id=2):
        for cache_name in caches:
            log_print('Running State transfer for cache {}'.format(cache_name), color='blue')
            grid.jmx.wait_for_dr_attributes(90, cache_name, metric_name='DrStatus', expected_value='Active')
            grid.jmx.dr_state_transfer(cache_name, dcr_id=dc_id)

    def _state_transfer(self, cache_mask, piclient, dc_id=2):
        cache_names = piclient.get_ignite().cacheNames().toArray()
        cache_names = [cache_name for cache_name in cache_names if cache_mask in cache_name]

        for cache_name in cache_names:
            log_print('Running State transfer for cache {}'.format(cache_name), color='blue')
            piclient.get_ignite().cache(cache_name)
            status = piclient.get_ignite().plugin("GridGain").dr().senderCacheStatus(cache_name)
            if self.new_behaviour:
                # piclient.get_ignite().plugin("GridGain").dr().stateTransfer(cache_name, piclient.get_gateway().jvm.Boolean(False), bytes([2])).get()
                piclient.get_ignite().plugin("GridGain").dr().startReplication(cache_name)
            else:
                piclient.get_ignite().plugin("GridGain").dr().resume(cache_name)
            piclient.get_ignite().plugin("GridGain").dr().stateTransfer(cache_name, bytes([dc_id]))

    def process_dr_rule(self, dr_rule):
        if '<->' in dr_rule:
            active_active_replication = True
        elif '->' in dr_rule:
            active_active_replication = False
        else:
            TidenException('Could not find DR direction in DR rule {}'.format(dr_rule))

        subrule = dr_rule.split('<->' if active_active_replication else '->')

        sender_cluster_id, sender_node_type, sender_node_id = \
            int(subrule[0].split('.')[1]), subrule[0].split('.')[2], int(subrule[0].split('.')[3])
        # int(subrule[0].split('.')[1]), subrule[0].split('.')[2], int(subrule[0].split('.')[3]) - 1
        receiver_cluster_id, receiver_node_type, receiver_node_id = \
            int(subrule[1].split('.')[1]), subrule[1].split('.')[2], int(subrule[1].split('.')[3])
        # int(subrule[1].split('.')[1]), subrule[1].split('.')[2], int(subrule[1].split('.')[3]) - 1

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

    @staticmethod
    def make_put_and_remove_operations(piclient, start_key, end_key):
        cache_names = piclient.get_ignite().cacheNames().toArray()
        cache_names = [cache_name for cache_name in cache_names]
        async_operations = []
        remove_probability = 0.3
        value_type = ModelTypes.VALUE_ACCOUNT.value
        for cache_name in cache_names:
            async_operation = create_async_operation(
                create_put_with_optional_remove_operation,
                cache_name, start_key, end_key, remove_probability,
                gateway=piclient.get_gateway(),
                value_type=value_type,
                use_monotonic_value=True,
            )

            async_operations.append(async_operation)
            async_operation.evaluate()

        for async_op in async_operations:
            async_op.getResult()

    def enable_ssl_on_grid(self, app):
        self.ssl_enabled = is_enabled(self.config.get('ssl_enabled'))

        if self.ssl_enabled:
            keystore_pass = truststore_pass = '123456'
            ssl_config_path = self.config['rt']['remote']['test_module_dir']
            ssl_params = namedtuple('ssl_conn', 'keystore_path keystore_pass truststore_path truststore_pass')

            self.ssl_conn_tuple = ssl_params(keystore_path='{}/{}'.format(ssl_config_path, 'server.jks'),
                                             keystore_pass=keystore_pass,
                                             truststore_path='{}/{}'.format(ssl_config_path, 'trust.jks'),
                                             truststore_pass=truststore_pass)

            app.cu.enable_ssl_connection(self.ssl_conn_tuple)
            app.su.enable_ssl_connection(self.ssl_conn_tuple)

    @step('Wait for same caches size')
    def _wait_for_same_caches_size(self, piclient_master, piclient_replica, how_long=300, predicate=None):
        from datetime import datetime
        start = datetime.now()
        iteration = 0
        delay = 5
        while True:
            cache_mask = lambda x: '' in x
            if predicate:
                cache_mask = predicate
            master_sizes = self.get_caches_size(cache_mask, piclient=piclient_master, debug=False)
            replica_sizes = self.get_caches_size(cache_mask, piclient=piclient_replica, debug=False)

            if master_sizes == replica_sizes:
                break
            else:
                self._compare_dicts(master_sizes, replica_sizes, debug=False)
                util_sleep_for_a_while(delay)
                iteration += 1
            log_print('Waiting for {} seconds'.format(iteration * delay))
            if (datetime.now() - start).seconds > how_long:
                self._compare_dicts(master_sizes, replica_sizes)
                raise TidenException('Caches size were not sync for {} seconds.'.format(how_long))

        execution_time = (datetime.now() - start).seconds
        log_print('Caches size have had sync for {} seconds.'.format(execution_time))

    @staticmethod
    def assert_check_sums(checksum_1, checksum_2):
        UltimateUtils.util_compare_check_sums(checksum_1, checksum_2)
        tiden_assert_equal(checksum_1, checksum_2, 'Check sums assertion')

    @staticmethod
    def compare_checksums_only(dict_1: dict, dict_2: dict, debug=True):
        equals = True
        for key, value in dict_1.items():
            if key not in dict_2:
                log_print(f'Cache {key} is not found on replica \n{dict_2}')
                equals = False
            else:
                if not value.split()[-1] == dict_2.get(key).split()[-1]:
                    if debug:
                        log_print(f'Checksums for cache {key} are not equal:'
                                  f'\n master={value}\nreplica={dict_2.get(key)}',
                                  color='debug')
                    equals = False
        return equals

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

