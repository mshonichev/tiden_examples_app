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

from re import search
from traceback import format_exc

from tiden.apps import NodeStatus
from tiden_gridgain.piclient.helper.operation_utils import create_put_all_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden import with_setup, attr, tiden_assert_equal, log_print, tiden_assert, TidenException
from suites.dr.test_data_replication import TestDataReplication
from tiden.utilities import ControlUtility
from tiden_gridgain.utilities.gridgain_control_utility import DRControlUtilityException


class TestControlUtils(TestDataReplication):

    @attr('common', 'on_clients', 'control')
    @with_setup(TestDataReplication.setup_testcase_master_master_on_clients,
                TestDataReplication.teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=2,
                preload=True, start_clients=False, wait_for_replication_online=False)
    def test_control_utility_clients(self):
        self.smoke()

    @attr('common', 'on_servers', 'control')
    @with_setup(TestDataReplication.setup_testcase_master_master_on_servers,
                TestDataReplication.teardown_testcase,
                clusters_count=2, server_nodes_per_cluster=4, client_nodes_per_cluster=0,
                preload=True, start_clients=False, wait_for_replication_online=False)
    def test_control_utility_servers(self):
        self.smoke()

    def smoke(self):
        cu_master = ControlUtility(self.clusters[0].grid)
        cu_replica = ControlUtility(self.clusters[1].grid)
        master = self.clusters[0].grid
        replica = self.clusters[1].grid
        master.jmx.start_utility()
        replica.jmx.start_utility()

        known_issues = []

        with PiClient(master, self.master_client_config) as piclient_master:
            caches = list(piclient_master.get_ignite().cacheNames().toArray())

        # start stopped caches
        for cache in caches:
            if master.jmx.dr_status(cache, node_id=1)['DrStatus'] != 'Active':
                master.jmx.dr_start(cache, node_id=1)
            if replica.jmx.dr_status(cache, node_id=1)['DrStatus'] != 'Active':
                replica.jmx.dr_start(cache, node_id=1)

        try:
            # topology
            all_topology_commands = '--dr', 'topology', '--sender-hubs', '--receiver-hubs', '--data-nodes', '--other-nodes'
            actual_topology_master = cu_master.control_utility(*all_topology_commands).dr().parse()
            actual_topology_replica = cu_replica.control_utility(*all_topology_commands).dr().parse()
            expected_topology = self.get_topology_data()
            tiden_assert_equal(expected_topology[1], actual_topology_master, 'master topology')
            tiden_assert_equal(expected_topology[2], actual_topology_replica, 'replica topology')
        except DRControlUtilityException:
            message = 'known issue: GG-24679 - DR: control.sh --dr topology throw exception during topology change'
            known_issues.append(message)
            log_print(message, color='red')

        try:
            # state
            actual_state_master = cu_master.control_utility('--dr', 'state').dr().parse()
            actual_state_replica = cu_replica.control_utility('--dr', 'state').dr().parse()
            tiden_assert_equal({'dc_id': '1', 'receiver_caches': '120'}, actual_state_master, 'master state')
            tiden_assert_equal({'dc_id': '2', 'receiver_caches': '120'}, actual_state_replica, 'replica state')

            # sender groups
            actual_state_verb_master = cu_master.control_utility('--dr', 'state', '--verbose').dr().parse()
            actual_state_verb_replica = cu_replica.control_utility('--dr', 'state', '--verbose').dr().parse()
            tiden_assert_equal({'dc_id': '1', 'sender_groups': 'dr, group1'}, actual_state_verb_master, 'master state verbose')
            tiden_assert_equal({'dc_id': '2', 'sender_groups': 'dr, group1'}, actual_state_verb_replica, 'replica state verbose')
        except AssertionError:
            message = 'Known issue: GG-24460 - DR: control.sh --dr state is throw exceptions on clients senders/receivers'
            known_issues.append(message)
            log_print(message, color='red')

        # node
        try:
            for cluster in self.clusters:
                for node_id, node in cluster.grid.nodes.items():
                    if node['status'] == NodeStatus.STARTED and node_id < 100:
                        actual_node = ControlUtility(cluster.grid).control_utility('--dr', 'node', node['id'], '--config', '--metrics').dr().parse()
                        tiden_assert_equal({'addresses': node['host'],
                                            'mode': 'Server, Baseline node',
                                            'streamer_pool_size': '16',
                                            'thread_pool_size': '4',
                                            'dc_id': str(cluster.id)}, actual_node,
                                           f'cluster {cluster.id} node {node_id} info')
        except DRControlUtilityException:
            message = 'Known issue: GG-24463 - DR: control.sh --dr node failed to execute on client sender/receiver node'
            known_issues.append(message)
            log_print(message, color='red')

        # cache config
        actual_cache_metrics = cu_master.control_utility('--dr', 'cache', '.+', '--config').dr().parse()
        tiden_assert_equal(120, len(actual_cache_metrics['sender_configuration']), 'configs sender count')
        tiden_assert_equal(120, len(actual_cache_metrics['receiver_configuration']), 'configs receiver count')
        actual_cache_metrics = cu_replica.control_utility('--dr', 'cache', '.+', '--config').dr().parse()
        tiden_assert_equal(120, len(actual_cache_metrics['sender_configuration']), 'configs sender count')
        tiden_assert_equal(120, len(actual_cache_metrics['receiver_configuration']), 'configs receiver count')

        try:
            # cache metrics
            actual_cache_metrics = cu_master.control_utility('--dr', 'cache', '.+', '--metrics').dr().parse()
            tiden_assert_equal(120, len(actual_cache_metrics.get('sender_metrics')), 'metrics sender count')
            tiden_assert_equal(120, len(actual_cache_metrics.get('receiver_metrics')), 'metrics receiver count')
            actual_cache_metrics = cu_replica.control_utility('--dr', 'cache', '.+', '--metrics').dr().parse()
            tiden_assert_equal(120, len(actual_cache_metrics.get('sender_metrics')), 'metrics sender count')
            tiden_assert_equal(120, len(actual_cache_metrics.get('receiver_metrics')), 'metrics receiver count')
        except:
            message = 'Known issue: GG-24725 - DR: control.sh --dr cache .+ --metrics not show metrics before replication'
            known_issues.append(message)
            log_print(message, color='red')

        started_cache_counter = 101
        with PiClient(master, self.master_client_config) as piclient_master:
            with PiClient(replica, self.replica_client_config, new_instance=True) as piclient_replica:
                for cluster, piclient, cu, compare_piclient, transfer, cluster_id in [
                    (master, piclient_master, cu_master, piclient_replica, 'fst', 1),
                    (replica, piclient_replica, cu_replica, piclient_master, 'action_fst', 2)
                ]:

                    # action stop
                    actual_stop = cu.control_utility('--dr', 'cache', '.+', '--action', 'stop', '--yes').dr().parse()
                    tiden_assert_equal('120', actual_stop['caches_affected'][0], 'affected caches')

                    for cache_name in caches:
                        # all should be stopped
                        cache_status = cluster.jmx.dr_status(cache_name, node_id=1)
                        tiden_assert_equal({'DrStatus': 'Stopped [reason=USER_REQUEST]'}, cache_status, f'{cache_name} dr status')

                        # put data for FST
                        create_put_all_operation(cache_name,
                                                 started_cache_counter, started_cache_counter + 100,
                                                 100, key_type='java.lang.Long', value_type='java.lang.Long',
                                                 gateway=piclient.get_gateway()).evaluate()
                    started_cache_counter = started_cache_counter + 101

                    # action start
                    actual_start = cu.control_utility('--dr', 'cache', '.+', '--action', 'start', '--yes').dr().parse()
                    tiden_assert_equal('120', actual_start['caches_affected'][0], 'affected caches')

                    for cache_name in caches:
                        # all should be active
                        cache_status = cluster.jmx.dr_status(cache_name, node_id=1)
                        tiden_assert_equal({'DrStatus': 'Active'}, cache_status, f'{cache_name} dr status')

                    if transfer == 'fst':
                        # fst
                        actual_fst = cu.control_utility('--dr', 'full-state-transfer', '--yes').dr().parse()
                        tiden_assert_equal(120, len(actual_fst['transferred_caches'].split(',')), 'transferred caches')
                    else:
                        # action fst
                        actual_fst = cu.control_utility('--dr', 'cache', '.+', '--action', 'full-state-transfer', '--yes').dr().parse()
                        tiden_assert_equal('120', actual_fst['caches_affected'][0], 'transferred caches with action')

                    try:
                        # data should be consistent after fst
                        self._wait_for_same_caches_size(piclient, compare_piclient, how_long=60)
                    except TidenException:
                        if getattr(self.clusters[0], 'client1', False):
                            message = 'Known issue GG-24760 - DR: control.sh --dr full-state-transfer does ...'
                            known_issues.append(message)
                            log_print(message, color='red')
                        else:
                            raise

                    self.assert_checksums()

                    # pause
                    actual_pause = cu.control_utility('--dr', 'pause', str(cluster_id), '--yes').dr().parse()
                    tiden_assert_equal(str(cluster_id), actual_pause['dc_id'], 'pause data center id')
                    try:
                        for cache_name in caches:
                            cache_status = cluster.jmx.dr_status(cache_name, node_id=1)
                            tiden_assert_equal({'DrStatus': 'Stopped [reason=USER_REQUEST]'}, cache_status, f'{cache_name} dr status')
                    except AssertionError:
                        # todo: Remove when https://ggsystems.atlassian.net/browse/GG-24383 is fixed
                        message = 'Known issue GG-24383 - DR control.sh --dr pause is not pause replication'
                        known_issues.append(message)
                        log_print(message, color='red')

                    # resume
                    actual_resume = cu.control_utility('--dr', 'resume', str(cluster_id), '--yes').dr().parse()
                    tiden_assert_equal(str(cluster_id), actual_resume['dc_id'], 'resume data center id')
                    try:
                        for cache_name in caches:
                            cache_status = cluster.jmx.dr_status(cache_name, node_id=1)
                            tiden_assert_equal({'DrStatus': 'Active'}, cache_status, f'{cache_name} dr status')
                    except AssertionError:
                        # todo: Remove when https://ggsystems.atlassian.net/browse/GG-24383 is fixed
                        message = 'Known issue GG-24383 - DR control.sh --dr pause is not pause replication'
                        known_issues.append(message)
                        log_print(message, color='red')
        tiden_assert(not bool(known_issues), '\n'.join(known_issues))

    def get_topology_data(self):
        """
        Get current topology dict from self.clusters
        """
        result = {}
        for cluster_id in range(2):
            expected_nodes = []
            sender_nodes = []
            receiver_nodes = []
            clients_count = 0
            servers_count = 0
            server_nodes_count = len([key for key in self.clusters[cluster_id].__dict__.keys() if key.startswith('server')])
            for node_name, dr_node in self.clusters[cluster_id].__dict__.items():
                found = search('(client|server)(\d+)', node_name)
                if found:

                    node_type = found.group(1)
                    node_id = int(found.group(2))

                    # clients/servers count
                    if node_type == 'client':
                        clients_count += 1
                        node = self.clusters[cluster_id].grid.nodes[server_nodes_count + node_id]
                    else:
                        servers_count += 1
                        node = self.clusters[cluster_id].grid.nodes[node_id]
                        # store servers ids
                        expected_nodes.append([node['id'].lower(), node['host']])

                    # store sender/receivers ids
                    type_pretty = f'{node_type[:1].upper()}{node_type[1:]}'
                    if dr_node.sender_nodes:
                        sender_nodes.append([node['id'].lower(), node['host'], type_pretty])
                    if dr_node.receiver_nodes:
                        receiver_nodes.append([node['id'].lower(), f'null:{dr_node.dr_port}', type_pretty])

            result[cluster_id + 1] = dict(node_info=sorted(expected_nodes),
                                          sender_hubs_info=sorted(sender_nodes),
                                          receiver_hubs_info=sorted(receiver_nodes),
                                          data_nodes_count='4', sender_count='2', receiver_count='2',
                                          other_nodes='not found', topology=[str(servers_count), str(clients_count)],
                                          dc_id=str(cluster_id + 1))
        return result

