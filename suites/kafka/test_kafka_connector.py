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

import os
import re
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep, time
from tiden.apps.ignite import Ignite
from tiden import SshPool, log_print, util_sleep_for_a_while, tiden_assert, with_setup, disable_connections_between_hosts, \
    enable_connections_between_hosts, known_issue
from tiden.case.apptestcase import AppTestCase
from tiden.dockermanager import DockerManager, TidenException
from tiden_gridgain.piclient.helper.cache_utils import IgniteCacheConfig
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_put_with_optional_remove_operation, \
    create_checksum_operation
from tiden_gridgain.piclient.piclient import PiClient


class Boolean:
    def __init__(self):
        self.is_cancelled = True

    def cancel(self):
        self.is_cancelled = False


class Integer:
    def __init__(self):
        self.value = 0


class TestKafkaConnector(AppTestCase):
    ignite_app_names = {}

    def __init__(self, *args):
        super().__init__(*args)
        self.config = args[0]
        self.ssh_pool: SshPool = args[1]
        self.docker_manager = DockerManager(self.config, self.ssh_pool)
        self.docker_manager.remove_all_containers()

        self.config = self.tiden.config
        self.clusters_amount = None
        self.static_caches = []
        # self.max_clusters_amount = len(
        #     [artifact for artifact in artifacts.values() if artifact.get('type') == 'ignite'])
        # self.config['environment']['server_hosts']
        # servers_per_host
        self.node_count = 3
        self.clusters = {'source':
                             {'node_count': self.node_count,
                              'first_node_number_to_start': 1,
                              'last_node_number_to_start': self.node_count},
                         'sink':
                             {'node_count': self.node_count,
                              'first_node_number_to_start': self.node_count + 1,
                              'last_node_number_to_start': self.node_count * 2}
                         }

        for cluster_type in self.clusters.keys():
            self.ignite_app_names[f'ignite_{cluster_type}'] = f'ignite_{cluster_type}'
            self.add_app(
                f'ignite_{cluster_type}',
                app_class_name='ignite',
                grid_name=f'ignite_{cluster_type}',
            )

    def setup(self):
        super().setup()

    def setup_testcase(self):
        self.clusters = {'source': {'node_count': 2}, 'sink': {'node_count': 2}}
        self.docker_manager.remove_all_containers()

    def setup_testcase(self):
        pass

    def teardown_testcase(self):
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [
            f"docker logs kafka-connector-tiden-test > {self.config['rt']['test_dir']}/kafka-connector-tiden-test_log.txt"])
        self.docker_manager.remove_all_containers()
        enable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                         self.kafka_connectors_host)
        enable_connections_between_hosts(self.ssh_pool, self.clusters['sink']['host'],
                                         self.kafka_connectors_host)
        for cluster_name in self.clusters.keys():
            log_print('Teardown for cluster {}'.format(cluster_name))
            if self.clusters[cluster_name]['grid']:
                # self.clusters[cluster_name]['grid'].jmx.kill_utility()
                self.clusters[cluster_name]['grid'].kill_nodes()
                self.clusters[cluster_name]['grid'].delete_lfs()
                self.clusters[cluster_name]['grid'].remove_additional_nodes()

                self.clusters[cluster_name]['grid'] = None

    def setup(self):
        super().setup()

    def prepare_environment(self):
        self.generate_host_config()
        self.generate_app_config()

        log_print('Start clusters', color='debug')
        self.start_clusters()

        if not self.static_caches:
            with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'],
                          new_instance=True,
                          nodes_num=1) as piclient_source:
                self.static_caches = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]

        log_print('Start kafka connectors', color='debug')
        self.start_kafka_connectors()

        log_print('Wait topology with kafka connectors', color='debug')
        self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 1, timeout=30,
                                                                   skip_nodes_check=True)
        self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 1, timeout=30,
                                                                 skip_nodes_check=True)

    @with_setup(setup_testcase, teardown_testcase)
    def test_base_data_replication(self):
        self.prepare_environment()

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                cache_names_source = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]
                self.upload_some_data(piclient_source, cache_names_source, 3001)

                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=120)
                self.compare_checksums(piclient_source, piclient_sink, 3001)

    @with_setup(setup_testcase, teardown_testcase)
    def test_data_replication_with_nodes_blinking(self):
        self.prepare_environment()

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                entry_counter = Integer()
                entry_counter.value = 1

                first_node = self.clusters['source']['first_node_number_to_start']
                last_node = self.clusters['source']['last_node_number_to_start']

                for i in range(first_node, last_node + 1):
                    log_print(f'Restart source cluster node {i}', color='debug')
                    self.restart_node_while_data_are_loading(piclient_source, 'source', i, self.static_caches, entry_counter)
                    self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=120)

                self.compare_checksums(piclient_source, piclient_sink, entry_counter.value)

                first_node = self.clusters['sink']['first_node_number_to_start']
                last_node = self.clusters['sink']['last_node_number_to_start']

                for i in range(first_node, last_node + 1):
                    log_print(f'Restart sink cluster node {i}', color='debug')
                    self.restart_node_while_data_are_loading(piclient_source, 'sink', i, self.static_caches, entry_counter)
                    self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=120)

                self.compare_checksums(piclient_source, piclient_sink, entry_counter.value)

    @known_issue()
    @with_setup(setup_testcase, teardown_testcase)
    def test_recreate_cache_in_sink_cluster(self):
        self.generate_host_config()

        self.generate_app_config()

        log_print('Start clusters', color='debug')
        self.start_clusters()

        dynamic_cache_name_1 = 'dynamic_cache_1'
        dynamic_cache_name_2 = 'dynamic_cache_2'

        if not self.static_caches:
            with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True,
                          nodes_num=1) as piclient_source:
                self.static_caches = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]

        log_print('Start kafka connectors', color='debug')
        self.start_kafka_connectors(additional_sink_caches=[dynamic_cache_name_1, dynamic_cache_name_2])

        log_print('Wait topology with kafka connectors', color='debug')
        self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 1, timeout=30, skip_nodes_check=True)
        self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 1, timeout=30, skip_nodes_check=True)

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                cache_config_source_1 = create_cache_config(piclient_source, dynamic_cache_name_1)
                cache_config_sink_1 = create_cache_config(piclient_sink, dynamic_cache_name_1)

                cache_config_source_2 = create_cache_config(piclient_source, dynamic_cache_name_2)
                cache_config_sink_2 = create_cache_config(piclient_sink, dynamic_cache_name_2)

                log_print('Create cache', color='debug')
                piclient_source.get_ignite().createCache(cache_config_source_1.get_config_object())
                piclient_sink.get_ignite().createCache(cache_config_sink_1.get_config_object())
                piclient_source.get_ignite().createCache(cache_config_source_2.get_config_object())
                piclient_sink.get_ignite().createCache(cache_config_sink_2.get_config_object())

                self.upload_data(piclient_source, [dynamic_cache_name_1, dynamic_cache_name_2], 1, 101)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 101)

                log_print('Destroy cache on sink cluster', color='debug')
                piclient_sink.get_ignite().cache(dynamic_cache_name_1).destroy()

                # self.ssh_pool.exec_on_host(self.kafka_connectors_host,
                #                            [f'curl http://{self.kafka_connectors_host}:8083/connectors/gridgain-sink-tiden-test/status'])

                # self.upload_data(piclient_source, [dynamic_cache_name_2], 201, 301) если раскомментить то позже тест упадет
                # self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=25, predicate=lambda x: 'dynamic_cache_2' in x)

                log_print('Recreate cache on sink cluster', color='debug')
                piclient_sink.get_ignite().createCache(cache_config_source_1.get_config_object())
                self.docker_manager.restart_container(self.kafka_connectors_host, 'kafka-connector-tiden-test')
                # self.ssh_pool.exec_on_host(self.kafka_connectors_host,
                #                            [f'curl -X POST http://{self.kafka_connectors_host}:8083/connectors/gridgain-source-tiden-test/restart'])
                # self.ssh_pool.exec_on_host(self.kafka_connectors_host,
                #                            [f'curl -X POST http://{self.kafka_connectors_host}:8083/connectors/gridgain-sink-tiden-test/restart'])
                self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 2,
                                                                         timeout=40, skip_nodes_check=True)
                # sleep(3)
                self.upload_data(piclient_source, [dynamic_cache_name_1, dynamic_cache_name_2], 1, 201)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 201)

    @with_setup(setup_testcase, teardown_testcase)
    def test_recreate_cache_in_source_cluster(self):
        self.generate_host_config()

        self.generate_app_config()

        log_print('Start clusters', color='debug')
        self.start_clusters()

        dynamic_cache_name_1 = 'dynamic_cache_1'
        dynamic_cache_name_2 = 'dynamic_cache_2'
        
        if not self.static_caches:
            with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True,
                          nodes_num=1) as piclient_source:
                self.static_caches = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]

        log_print('Start kafka connectors', color='debug')
        self.start_kafka_connectors(additional_sink_caches=[dynamic_cache_name_1, dynamic_cache_name_2])

        log_print('Wait topology with kafka connectors', color='debug')
        self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 1, timeout=30,
                                                                   skip_nodes_check=True)
        self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 1, timeout=30,
                                                                 skip_nodes_check=True)

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True,
                      nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True,
                          nodes_num=1) as piclient_sink:
                cache_config_source_1 = create_cache_config(piclient_source, dynamic_cache_name_1)
                cache_config_sink_1 = create_cache_config(piclient_sink, dynamic_cache_name_1)

                cache_config_source_2 = create_cache_config(piclient_source, dynamic_cache_name_2)
                cache_config_sink_2 = create_cache_config(piclient_sink, dynamic_cache_name_2)

                log_print('Create cache', color='debug')
                piclient_source.get_ignite().createCache(cache_config_source_1.get_config_object())
                piclient_sink.get_ignite().createCache(cache_config_sink_1.get_config_object())
                piclient_source.get_ignite().createCache(cache_config_source_2.get_config_object())
                piclient_sink.get_ignite().createCache(cache_config_sink_2.get_config_object())

                self.upload_data(piclient_source, [*self.static_caches, dynamic_cache_name_1, dynamic_cache_name_2], 1, 21)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 21)

                log_print('Destroy cache on source cluster', color='debug')
                piclient_source.get_ignite().cache(dynamic_cache_name_1).destroy()
                sleep(10) #time for kafka conector reconfiguration
                self.upload_data(piclient_source, [*self.static_caches, dynamic_cache_name_2], 21, 41)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60, predicate=lambda x: dynamic_cache_name_1 not in x)

                log_print('Recreate cache', color='debug')
                piclient_source.get_ignite().createCache(cache_config_source_1.get_config_object())
                sleep(10) #time for kafka conector reconfiguration
                # self.docker_manager.restart_container(self.kafka_connectors_host, 'kafka-connector-tiden-test')
                # self.ssh_pool.exec_on_host(self.kafka_connectors_host,
                #                            [f'curl -X POST http://{self.kafka_connectors_host}:8083/connectors/gridgain-source-tiden-test/restart'])
                # self.ssh_pool.exec_on_host(self.kafka_connectors_host,
                #                            [f'curl -X POST http://{self.kafka_connectors_host}:8083/connectors/gridgain-sink-tiden-test/restart'])
                # self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 2,
                #                                                          timeout=20, skip_nodes_check=True)
                self.upload_data(piclient_source, [*self.static_caches, dynamic_cache_name_1, dynamic_cache_name_2], 41, 61)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60,
                                                predicate=lambda x: dynamic_cache_name_1 not in x)
                # self.compare_checksums_some_entries(piclient_source, piclient_sink, 201, 301)
                # self.compare_checksums_some_entries(piclient_source, piclient_sink, 201, 301)
                checksum_source, checksum_sink = \
                    self.calculate_and_compaire_checksums(piclient_source, piclient_sink, start_key=41, end_key=61)
                tiden_assert(self.compare_checksums_only(checksum_source, checksum_sink),
                             'Expecting checksums are equal')

    @with_setup(setup_testcase, teardown_testcase)
    def test_reconnect_sink_connector(self):
        self.prepare_environment()

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                cache_names_source = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]
                self.upload_data(piclient_source, cache_names_source, 1, 51)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 51)

                disable_connections_between_hosts(self.ssh_pool, self.clusters['sink']['host'],
                                                  self.kafka_connectors_host)

                self.upload_data(piclient_source, cache_names_source, 51, 101)

                enable_connections_between_hosts(self.ssh_pool, self.clusters['sink']['host'],
                                                  self.kafka_connectors_host)

                # sleep(10) #time for kafka conector reconfiguration
                # self.docker_manager.restart_container(self.kafka_connectors_host, 'kafka-connector-tiden-test')
                self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 2,
                                                                         timeout=40, skip_nodes_check=True)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 101)

                self.upload_data(piclient_source, cache_names_source, 101, 151)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 151)

    @known_issue()
    @with_setup(setup_testcase, teardown_testcase)
    def test_reconnect_source_connector_failover_policy_full_snapshot(self):
        self.generate_host_config()

        self.generate_app_config(kafka_data_region=True)

        log_print('Start clusters', color='debug')
        self.start_clusters()
        
        if not self.static_caches:
            with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True,
                          nodes_num=1) as piclient_source:
                self.static_caches = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]

        log_print('Start kafka connectors', color='debug')
        self.start_kafka_connectors(additional_env='-e FAILOVER_POLICY=FULL_SNAPSHOT')

        log_print('Wait topology with kafka connectors', color='debug')
        self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 1, timeout=30, skip_nodes_check=True)
        self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 1, timeout=30, skip_nodes_check=True)

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                cache_names_source = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]
                self.upload_data(piclient_source, cache_names_source, 1, 51)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 51)

                disable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                                  self.kafka_connectors_host)

                self.upload_data(piclient_source, cache_names_source, 51, 101)

                enable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                                  self.kafka_connectors_host)

                # self.docker_manager.restart_container(self.kafka_connectors_host, 'kafka-connector-tiden-test')
                self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 2,
                                                                           timeout=40, skip_nodes_check=True)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 101)

                self.upload_data(piclient_source, cache_names_source, 101, 151)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 151)

    @known_issue()
    @with_setup(setup_testcase, teardown_testcase)
    def test_reconnect_source_connector_failover_policy_backlog(self):
        self.generate_host_config()

        self.generate_app_config(kafka_data_region=True)

        log_print('Start clusters', color='debug')
        self.start_clusters()
        
        if not self.static_caches:
            with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True,
                          nodes_num=1) as piclient_source:
                self.static_caches = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]

        log_print('Start kafka connectors', color='debug')
        self.start_kafka_connectors(additional_env='-e FAILOVER_POLICY=BACKLOG')

        log_print('Wait topology with kafka connectors', color='debug')
        self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 1, timeout=30, skip_nodes_check=True)
        self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 1, timeout=30, skip_nodes_check=True)

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                cache_names_source = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]
                self.upload_data(piclient_source, cache_names_source, 1, 51)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)

                self.compare_checksums(piclient_source, piclient_sink, 51)

                disable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                                  self.kafka_connectors_host)

                self.upload_data(piclient_source, cache_names_source, 51, 101)

                enable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                                  self.kafka_connectors_host)

                # self.docker_manager.restart_container(self.kafka_connectors_host, 'kafka-connector-tiden-test')
                self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 2,
                                                                           timeout=40, skip_nodes_check=True)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 101)

                self.upload_data(piclient_source, cache_names_source, 101, 151)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 151)

    @known_issue()
    @with_setup(setup_testcase, teardown_testcase)
    def test_reconnect_source_connector_failover_policy_none(self):
        self.generate_host_config()

        self.generate_app_config(kafka_data_region=True)

        log_print('Start clusters', color='debug')
        self.start_clusters()
        
        if not self.static_caches:
            with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True,
                          nodes_num=1) as piclient_source:
                self.static_caches = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]

        log_print('Start kafka connectors', color='debug')
        self.start_kafka_connectors(additional_env='-e FAILOVER_POLICY=NONE')

        log_print('Wait topology with kafka connectors', color='debug')
        self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 1, timeout=30, skip_nodes_check=True)
        self.clusters['sink']['grid'].wait_for_topology_snapshot(self.clusters['sink']['node_count'], 1, timeout=30, skip_nodes_check=True)

        with PiClient(self.clusters['source']['grid'], self.clusters['source']['piclient_config'], new_instance=True, nodes_num=1) as piclient_source:
            with PiClient(self.clusters['sink']['grid'], self.clusters['sink']['piclient_config'], new_instance=True, nodes_num=1) as piclient_sink:
                cache_names_source = [cache_name for cache_name in piclient_source.get_ignite().cacheNames().toArray()]
                self.upload_data(piclient_source, cache_names_source, 1, 51)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 51)

                disable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                                  self.kafka_connectors_host)

                self.upload_data(piclient_source, cache_names_source, 51, 101)

                enable_connections_between_hosts(self.ssh_pool, self.clusters['source']['host'],
                                                  self.kafka_connectors_host)

                # self.docker_manager.restart_container(self.kafka_connectors_host, 'kafka-connector-tiden-test')
                self.clusters['source']['grid'].wait_for_topology_snapshot(self.clusters['source']['node_count'], 2,
                                                                           timeout=40, skip_nodes_check=True)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 101)

                self.upload_data(piclient_source, cache_names_source, 101, 151)
                self._wait_for_same_caches_size(piclient_source, piclient_sink, how_long=60)
                self.compare_checksums(piclient_source, piclient_sink, 151)

    def restart_node_while_data_are_loading(self, piclient, cluster_name, node_id, cache_names, entry_counter):
        need_to_load_data = Boolean()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.load_some_data_for_a_while, piclient,
                                               cache_names, entry_counter, need_to_load_data)

            self.restart_node(cluster_name, node_id)
            need_to_load_data.cancel()
            future.result()

    def restart_node(self, cluster_name, node_id):
        util_sleep_for_a_while(5)
        self.clusters[cluster_name]['grid'].kill_nodes(node_id)
        util_sleep_for_a_while(5)
        self.clusters[cluster_name]['grid'].start_nodes(node_id)

    def load_some_data_for_a_while(self, piclient, cache_names, entry_counter, need_to_load_data):
        end_time = round(time()) + 120
        while need_to_load_data.is_cancelled or end_time > round(time()):
            self.upload_data(piclient, cache_names, entry_counter.value, entry_counter.value + 10)
            entry_counter.value = entry_counter.value + 10

    def generate_host_config(self):
        self.number_of_hosts = len(self.config['environment']['server_hosts'])
        self.kafka_connector_image_remote_dir = self.config['rt']['remote'][
                                                    'test_module_dir'] + '/kafka-connector-image'

        # if number_of_hosts == 1:
        self.clusters['source']['host'] = self.config['environment']['server_hosts'][0]
        self.clusters['sink']['host'] = self.config['environment']['server_hosts'][0]
        self.kafka_connectors_host = self.config['environment']['server_hosts'][0]
        if self.number_of_hosts >= 2:
            self.clusters['sink']['host'] = self.config['environment']['server_hosts'][1]
            self.kafka_connectors_host = self.config['environment']['server_hosts'][1]
        if self.number_of_hosts >= 3:
            self.kafka_connectors_host = self.config['environment']['server_hosts'][2]
            self.ssh_pool.exec(['mkdir -p %s' % self.kafka_connector_image_remote_dir])

    def start_kafka_connectors(self, additional_env='-e FAILOVER_POLICY=NONE', additional_sink_caches=[]):
        kafka_conector_image_local_dir = f"{self.config['suite_dir']}/image/kafka/kafka-connector/"
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [f'rm -r {self.kafka_connector_image_remote_dir}'])
        # self.ssh_pool.exec_on_host(host, [f'mkdir {remote_path}/image3'])
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [f'mkdir {self.kafka_connector_image_remote_dir}'])
        file_list = list(kafka_conector_image_local_dir + file for file in os.listdir(kafka_conector_image_local_dir))
        self.ssh_pool.upload_on_host(self.kafka_connectors_host, file_list, f'{self.kafka_connector_image_remote_dir}')
        gridgain_version = self.config['artifacts']['ignite_source']['gridgain_version']
        kafka_clients_jar_name = self.ssh_pool.exec_on_host(self.clusters['source']['host'], [
            f"ls {self.config['artifacts']['ignite_source']['remote_path']}/libs/ignite-kafka | grep kafka-clients"])[
            self.clusters['source']['host']][0]
        re_kafka_version = re.search('\d*\.\d*\.\d*', kafka_clients_jar_name)
        kafka_version = re_kafka_version.group(0)
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [
            "sed -i -e 's/{{gridgain_version}}/" + f"{gridgain_version}/g' {self.kafka_connector_image_remote_dir}/Dockerfile"])
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [
            "sed -i -e 's/{{kafka_version}}/" + f"{kafka_version}/g' {self.kafka_connector_image_remote_dir}/Dockerfile"])
        self.docker_manager.build_image(self.kafka_connectors_host, f'{self.kafka_connector_image_remote_dir}',
                                        tag='kafka-connector-tiden-test')

        # sink_topics = additional_sink_topics.append('prefperson')
        sink_topics = ''
        for sink_topic in [*self.static_caches, *additional_sink_caches]:
            sink_topics += 'pref' + sink_topic + ','
        sink_topics = sink_topics[:-1]

        self.ssh_pool.exec_on_host(self.kafka_connectors_host,
                                   [f'docker run --network host --name zookeeper-tiden-test -d wurstmeister/zookeeper'])
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [
            f'docker run --network host --name kafka-tiden-test -d -e KAFKA_ADVERTISED_HOST_NAME={self.kafka_connectors_host} -e KAFKA_ADVERTISED_PORT=9092 -e KAFKA_ZOOKEEPER_CONNECT={self.kafka_connectors_host}:2181 -v /var/run/docker.sock:/var/run/docker.sock wurstmeister/kafka:2.12-2.4.0'])
        self.ssh_pool.exec_on_host(self.kafka_connectors_host, [
            f"docker run --network host --name kafka-connector-tiden-test -d "\
            f"-e SOURCE_TASKS_MAX=1 -e SOURCE_TOPIC_PREFIX=pref -e SOURCE_SHALL_LOAD_INITIAL_DATA=true -e SOURCE_IP_FINDER_ADDRESSES={self.clusters['source']['host']}:{self.clusters['source']['discovery_port_prefix']}0..{self.clusters['source']['discovery_port_prefix']}9 "\
            f"-e SINK_TOPICS={sink_topics} -e SINK_TASKS_MAX=4 -e SINK_TOPIC_PREFIX=pref -e SINK_PUSH_INTERVAL=1000 -e SINK_IP_FINDER_ADDRESSES={self.clusters['sink']['host']}:{self.clusters['sink']['discovery_port_prefix']}0..{self.clusters['sink']['discovery_port_prefix']}9 "\
            f"-e KAFKA_CONNECT_BOOTSTRAP_SERVERS={self.kafka_connectors_host}:9092 {additional_env} kafka-connector-tiden-test"])

    def upload_some_data(self, piclient, cache_names, max_key):
        self.upload_data(piclient, cache_names, 1, max_key)

    def upload_data(self, piclient, cache_names, start_key, end_key):
        log_print(f'Start to load data: start_key {start_key}, end_key {end_key}', color='debug')
        async_operations = []
        remove_probability = 0
        for cache_name in cache_names:
            async_operation = create_async_operation(create_put_with_optional_remove_operation,
                                                         cache_name, start_key, end_key, remove_probability,
                                                         gateway=piclient.get_gateway(),
                                                         use_monotonic_value=True)
                                                     # value_type=ModelTypes.VALUE_ORGANIZATION.value)
            async_operations.append(async_operation)
            async_operation.evaluate()

        for async_op in async_operations:
            async_op.getResult()

    def _wait_for_same_caches_size(self, piclient_source, piclient_sink, how_long=300, predicate=None):
        from datetime import datetime
        start = datetime.now()
        iteration = 0
        delay = 5
        while True:
            cache_mask = lambda x: '' in x
            if predicate:
                cache_mask = predicate
            source_sizes = self.get_caches_size(cache_mask, piclient=piclient_source, debug=False)
            sink_sizes = self.get_caches_size(cache_mask, piclient=piclient_sink, debug=False)

            if source_sizes == sink_sizes:
                break
            else:
                self._compare_dicts(source_sizes, sink_sizes, debug=False)
                util_sleep_for_a_while(delay)
                iteration += 1
            log_print('Waiting for {} seconds'.format(iteration * delay))
            if (datetime.now() - start).seconds > how_long:
                self._compare_dicts(source_sizes, sink_sizes)
                raise TidenException(f'Caches size were not sync for {how_long} seconds. source_sizes: {source_sizes}, sink_sizes: {sink_sizes}')

        execution_time = (datetime.now() - start).seconds
        log_print(f'Caches size have had sync for {execution_time} seconds. {source_sizes}')

    def compare_checksums(self, piclient1, piclient2, end_key, equal=True):
        self.compare_checksums_some_entries(piclient1, piclient2, 1, end_key, equal)

    def compare_checksums_some_entries(self, piclient1, piclient2, start_key, end_key, equal=True):
        checksum_source, checksum_sink = self.calculate_and_compaire_checksums(piclient1, piclient2,
                                                                                 start_key=start_key, end_key=end_key)
        if equal:
            tiden_assert(self._compare_dicts(checksum_source, checksum_sink), 'Expecting checksums are equal')
        else:
            tiden_assert(not self._compare_dicts(checksum_source, checksum_sink),
                             'Expecting checksums are NOT equal')

    # start_key = 1
    # end_key = 201
    # checksum_source, checksum_sink = self.calculate_and_compaire_checksums(piclient_source, piclient_sink, 1,
    #                                                                           end_key)
    # print(checksum_source)

    # tiden_assert(self._compare_dicts(checksum_source, checksum_sink), 'Expecting checksums are equal')
    # tiden_assert(not self._compare_dicts(checksum_source, checksum_sink), 'Expecting checksums are NOT equal')

    def calculate_and_compaire_checksums(self, piclient1, piclient2, start_key=None, end_key=None):
        checksum1 = self.calc_checksums_on_client(piclient1, start_key=start_key, end_key=end_key, dict_mode=True)
        checksum2 = self.calc_checksums_on_client(piclient2, start_key=start_key, end_key=end_key, dict_mode=True)
        return checksum1, checksum2

    def calc_checksums_on_client(
            self,
            piclient,
            start_key=0,
            end_key=1000,
            dict_mode=False
    ):
        """
        Calculate checksum based on piclient
        :param start_key: start key
        :param end_key: end key
        :param dict_mode:
        :return:
            """
        log_print("Calculating checksums using cache.get() from client")
        cache_operation = {}
        cache_checksum = {}

        sorted_cache_names = []
        for cache_name in piclient.get_ignite().cacheNames().toArray():
            sorted_cache_names.append(cache_name)

        sorted_cache_names.sort()

        async_operations = []
        for cache_name in sorted_cache_names:
            async_operation = create_async_operation(create_checksum_operation,
                                                     cache_name,
                                                     start_key,
                                                     end_key,
                                                     gateway=piclient.get_gateway())
            async_operations.append(async_operation)
            cache_operation[async_operation] = cache_name
            async_operation.evaluate()

        checksums = ''

        for async_operation in async_operations:
            result = str(async_operation.getResult())
            cache_checksum[cache_operation.get(async_operation)] = result
            checksums += result

        log_print('Calculating checksums done')

        if dict_mode:
            return cache_checksum
        else:
            return checksums

    @staticmethod
    def get_caches_size(cache_mask=lambda x: '' in x, piclient=None, debug=True):
        from pt.piclient.helper.cache_utils import IgniteCache
        cache_size = {}
        cache_names = piclient.get_ignite().cacheNames().toArray()
        # cache_names = [cache_name for cache_name in cache_names if cache_mask in cache_name]
        cache_names = [cache_name for cache_name in cache_names if cache_mask(cache_name) and cache_name != 'kafka-connect-backlog']

        for cache_name in cache_names:
            if debug:
                log_print('Getting size for cache {}'.format(cache_name), color='blue')
            cache = IgniteCache(cache_name, gateway=piclient.get_gateway())
            cache_size[cache_name] = cache.size()
            if debug:
                log_print('Size for cache {} is {}'.format(cache_name, cache_size[cache_name]), color='blue')
        return cache_size

    @staticmethod
    def _compare_dicts(dict_1: dict, dict_2: dict, debug=True):
        equals = True
        for key, value in dict_1.items():
            if not key in dict_2:
                log_print(f'Cache {key} is not found on sink \n{dict_2}')
                equals = False
            else:
                if not value == dict_2.get(key):
                    if debug:
                        log_print(f'Values for cache {key} are not equal:\n source={value}\nsink={dict_2.get(key)}',
                                  color='debug')
                    equals = False
        return equals

    @staticmethod
    def compare_checksums_only(dict_1: dict, dict_2: dict, debug=True):
        equals = True
        for key, value in dict_1.items():
            if key not in dict_2:
                log_print(f'Cache {key} is not found on sink \n{dict_2}')
                equals = False
            else:
                if not value.split()[-1] == dict_2.get(key).split()[-1]:
                    if debug:
                        log_print(f'Checksums for cache {key} are not equal:'
                                  f'\n source={value}\nsink={dict_2.get(key)}',
                                  color='debug')
                    equals = False
        return equals

    def generate_app_config(self, **kwargs):
        discovery_port_prefix = 4750
        communication_port_prefix = 4710

        for cluster_type in self.clusters.keys():

            config_name = f'cluster_{cluster_type}'
            self.clusters[cluster_type]['config_name'] = config_name

            self.clusters[cluster_type]['discovery_port_prefix'] = discovery_port_prefix

            kafka_data_region = kwargs.get('kafka_data_region') if cluster_type == 'source' else False

            self.create_app_config_set(Ignite, config_name,
                                           deploy=True,
                                           config_type='server',
                                           consistent_id=True,
                                           addresses=[self.clusters[cluster_type]['host']],
                                           discovery_port_prefix=discovery_port_prefix,
                                           communication_port_prefix=communication_port_prefix,
                                            kafka_data_region=kafka_data_region
                                       )

            # generate config without sender/receiver settings for piclient
            config_name = f'cluster_{cluster_type}_piclient'
            self.create_app_config_set(Ignite, config_name,
                                       config_type='client',
                                       deploy=True,
                                       consistent_id=True,
                                       addresses=[self.clusters[cluster_type]['host']],
                                       discovery_port_prefix=discovery_port_prefix
                                       )
            self.clusters[cluster_type]['piclient_config'] = Ignite.config_builder.get_config('client', config_set_name=config_name)
            discovery_port_prefix += 1
            communication_port_prefix += 1

    def start_clusters(self):
        futures = []
        log_print('Starting clusters', color='debug')
        with ThreadPoolExecutor(max_workers=2) as executor:
            for cluster_type in self.clusters.keys():
                futures.append(executor.submit(self.start_ignite_grid, f'ignite_{cluster_type}', self.clusters[cluster_type], activate=True))

        for i, cluster in enumerate(self.clusters.values()):
            cluster['grid'] = futures[i].result()

    def start_ignite_grid(self, name, cluster, activate=False, already_nodes=0):
        first_node = cluster['first_node_number_to_start']
        last_node = cluster['last_node_number_to_start']

        app = self.get_app(self.ignite_app_names[name])
        for i in range(first_node, last_node + 1):
            app.set_node_option(i, 'config',
                                Ignite.config_builder.get_config('server', config_set_name=cluster['config_name']))
            app.set_node_option(i, 'host', cluster['host'])

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

        app.start_nodes(*list(range(first_node, last_node + 1)), already_nodes=already_nodes, other_nodes=already_nodes)

        if activate:
            app.cu.activate(activate_on_particular_node=first_node)

        # if not app.jmx.is_started():
        #     app.jmx.start_utility()

        return app


def create_cache_config(piclient, cache_name):
    gateway = piclient.get_gateway()
    cache_config = IgniteCacheConfig(gateway)
    cache_config.set_name(cache_name)
    cache_config.set_cache_mode('replicated')
    cache_config.set_atomicity_mode('ATOMIC')
    cache_config.set_write_synchronization_mode('full_sync')
    return cache_config

# [15:01:20,465][INFO][main][IgniteKernal] IGNITE_HOME=/storage/ssd/sutsel/tiden/dr-200428-145903/test_control_utils/ignite1.server.1
# [15:01:20,465][INFO][main][IgniteKernal] VM arguments: [-XX:+AggressiveOpts, -Dfile.encoding=UTF-8, -DIGNITE_QUIET=false, -DIGNITE_SUCCESS_FILE=/storage/ssd/sutsel/tiden/dr-200428-145903/test_control_utils/ignite1.server.1/work/ignite_success_b4e218dc-7de4-4656-a39e-e699dff2fe3e, -Dcom.sun.management.jmxremote, -Dcom.sun.management.jmxremote.port=54437, -Dcom.sun.management.jmxremote.authenticate=false, -Dcom.sun.management.jmxremote.ssl=false, -DIGNITE_HOME=/storage/ssd/sutsel/tiden/dr-200428-145903/test_control_utils/ignite1.server.1, -DIGNITE_PROG_NAME=bin/ignite.sh, -ea, -Xmx8g, -Xms8g, -DIGNITE_DISABLE_WAL_DURING_REBALANCING=true, -DNODE_IP=172.25.1.33, -DNODE_COMMUNICATION_PORT=47100, -DCONSISTENT_ID=node_ignite1_1]

