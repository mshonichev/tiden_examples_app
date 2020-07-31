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

from tiden import with_setup, tiden_assert
from tiden_gridgain.piclient.loading import TransactionalLoading
from tiden_gridgain.piclient.piclient import PiClient
from suites.snapshots.ult_utils import UltimateUtils
from tiden.util import *
from tiden.tidenexception import TidenException

from enum import Enum


class ZkScenarios(Enum):
    stop_leader = 'stop leader node'
    stop_follower = 'stop follower node'
    stop_leader_follower = 'stop leader and follower nodes'
    split_leader_follower = 'split leader and follower nodes'
    split_all_nodes = 'split all ZK nodes'


class TestZookeeperPmi(UltimateUtils):
    no_idle_verify_conflicts_msg = 'idle_verify check has finished, no conflicts have been found'

    def setup(self):
        super().setup()
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestZookeeperPmi]')

    def setup_testcase(self):
        self.iptables_clear()
        super().setup_testcase()

    def teardown_testcase(self):
        super().teardown_testcase()
        self.iptables_clear()

    def assert_no_errors_in_utility_output(self, tx_check=False, reverse=False):

        if tx_check:
            self.cu.control_utility('--tx', reverse=reverse)
            tiden_assert('Error' not in self.cu.latest_utility_output, 'Error found in control.sh utility output')

        self.cu.control_utility('--cache', 'idle_verify', reverse=reverse)
        tiden_assert(self.no_idle_verify_conflicts_msg in self.cu.latest_utility_output, 'No conflicts have been found')

    def split_nodes(self, first_hosts_list, second_hosts_list):
        log_print('Drop connection between groups:\n1: {}\n2: {}'.format(first_hosts_list, second_hosts_list))
        output = self.ignite.ssh.exec(self.make_server_disconnect(first_hosts_list, second_hosts_list))
        log_print('Result - {}'.format(output))

    def kill_zk_node_with_role(self, roles):
        """
        Kill ZK node with particular role (leader or follower)
        :param roles:
        :return:
        """
        if not isinstance(roles, list):
            roles = [roles]

        for role in roles:
            log_print('Going to kill ZK node with role {}'.format(role), color='debug')
            self.zoo.kill_node(self.zoo.get_zookeeper_specific_role(role))

    def split_zookeeper_nodes(self, scenario):
        """
        Emulate network problem on zookeeper cluster
        :param scenario:
            leader - disconnect leader from other node
            all - disconnect each node from other
        """
        if scenario == 'leader':
            self.make_zookeeper_disconnect_from_other_node(self.zoo.get_zookeeper_specific_role('leader'))
        elif scenario == 'all':
            for node in self.zoo.nodes.keys():
                self.make_zookeeper_disconnect_from_other_node(node)
        else:
            TidenException('Unexpected value {} passed to scenario split_zookeepers_nodes'.format(scenario))

    def make_zookeeper_disconnect_from_other_node(self, node_id):
        def zookeeper_iptables_rule(node_to, node_from):
            zoo_node_to = self.zoo.nodes[node_to]
            zoo_node_from = self.zoo.nodes[node_from]
            return self.iptables_rule(host_from=zoo_node_from['host'], host_to=zoo_node_to['host'],
                                      port_to='2881:3888', multiple=True)

        log_print('Going to disconnect node {} from other nodes with iptable rule'.format(node_id), color='green')
        command = 'date'
        host = self.zoo.nodes[node_id]['host']
        for node in self.zoo.nodes.keys():
            if node != node_id:
                command += ';' + zookeeper_iptables_rule(node_id, node)
        log_print('host: {} exec command: {}'.format(host, command), color='debug')
        log_print(self.ssh.exec_on_host(self.zoo.nodes[node_id]['host'], [command]))

    def get_server_connections(self):
        connection = {}
        alive_nodes = self.ignite.get_alive_default_nodes()
        for node, node_value in self.ignite.nodes.items():
            if node in alive_nodes:
                connection[node] = [node_value['host'], node_value['communication_port']]
        return connection

    def make_server_disconnect(self, first, second):
        """
        Func for generate network rules between first and second group
        for each node in first
            for each node in second
                make rule for first nodes
        for each node in second
            for each node in first
                make rule for second node
        :param first: dict nodes in first group
        :param second: dict nodes in second group
        :return: network rules
        """
        split_command = {}
        for node_first in first.values():
            if split_command.get(node_first[0]) is None:
                split_command[node_first[0]] = []
            for node_second in second.values():
                split_command[node_first[0]] = [
                    '{};{}'.format(self.iptables_rule(node_second[0], node_first[0], node_first[1]),
                                   ''.join(split_command[node_first[0]]))]
        for node_second in second.values():
            if split_command.get(node_second[0]) is None:
                split_command[node_second[0]] = []
            for node_first in first.values():
                split_command[node_second[0]] = [
                    '{};{}'.format(self.iptables_rule(node_first[0], node_second[0], node_second[1]),
                                   ''.join(split_command[node_second[0]]))]
        log_print('iptables rules to split nodes:\n{}'.format(split_command), color='debug')
        return split_command

    def iptables_rule(self, host_from, host_to, port_to, multiple=False):
        if not multiple:
            cmd = 'sudo iptables -I INPUT 1 -p tcp -s {} -d {} --dport {} -j DROP'.format(host_from, host_to, port_to)
        else:
            cmd = 'sudo iptables -I INPUT 1 -p tcp -s {} -d {} --match multiport --dports {} -j DROP'.format(
                host_from, host_to, port_to)
        return cmd

    def iptables_clear(self):
        command_clear = "sudo iptables -L INPUT -n --line-number | awk '{ print $1 \" \" $5 }' | grep 172.25.1 | " \
                        "awk '{print $1}' | sort -n -r | xargs -I'{}' sudo iptables -D INPUT {}"
        log_print(command_clear, color='debug')
        for host in list(set(self.config['environment']['server_hosts'] +
                             self.config['environment']['client_hosts'] +
                             self.config['environment']['zookeeper_hosts'])):
            output = self.ssh.exec_on_host(host, [command_clear])
            log_print(output, color='debug')

    @test_case_id('51663')
    @attr('zookeeper_failover')
    @require(min_zookeeper_node=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_kill_leader_zookeeper_node(self):
        """
        Test based on scenario when leader zookeeper node stopped.
        Expected result: Nothing will change in cluster working process.
        """
        self.zookeeper_fail_test(scenario=ZkScenarios.stop_leader)

    @test_case_id('51664')
    @attr('zookeeper_failover')
    @require(min_zookeeper_node=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_kill_follower_zookeeper_node(self):
        """
        Test based on scenario when follower zookeeper node stopped.
        Expected result: Nothing will change in cluster working process.
        """
        self.zookeeper_fail_test(scenario=ZkScenarios.stop_follower)

    @test_case_id('51665')
    @attr('zookeeper_failover')
    @require(min_zookeeper_node=3)
    @require(min_zookeeper_hosts=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_split_leader_and_followers(self):
        """
        Test based on scenario when leader zookeeper node lost connection to other zookeeper nodes.
        Expected result: Nothing will change in cluster working process.
        """
        self.zookeeper_fail_test(scenario=ZkScenarios.split_leader_follower)

    @test_case_id('51666')
    @attr('zookeeper_failover')
    @require(min_zookeeper_node=3)
    @require(min_zookeeper_hosts=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_split_all_zookeeper_node(self):
        """
        Test based on scenario when each zookeeper node lost connect to other zookeeper node (there will be no quorum).
        Expected result: All cluster nodes will get segmentation.
        """
        self.zookeeper_fail_test(scenario=ZkScenarios.split_all_nodes, expecting_broken_cluster=True)

    @test_case_id('201855')
    @attr('zookeeper_failover')
    @require(min_zookeeper_node=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_kill_leader_and_follower_zookeeper_node(self):
        """
        Test based on scenario when leader and follower zookeeper node stopped (there will be no quorum).
        Expected result: All cluster nodes will get segmentation.
        """
        self.zookeeper_fail_test(scenario=ZkScenarios.stop_leader_follower, expecting_broken_cluster=True)

    def zookeeper_fail_test(self, scenario, expecting_broken_cluster=False):
        node_connection = self.get_server_connections()
        try:
            with PiClient(self.ignite, self.get_client_config()):
                with TransactionalLoading(self, skip_consistency_check=True):
                    util_sleep_for_a_while(10, msg='Wait until load started')
                    self.zookeeper_fail_scenario(scenario)
                    self.su.snapshot_utility('SNAPSHOT', '-type=full')
                    self.ignite.kill_node(2)
                    util_sleep_for_a_while(60, msg='Wait after zookeeper issue')

            self.ignite.start_node(2)

            for node_id in node_connection.keys():
                tiden_assert(self.ignite.check_node_is_alive(node_id),
                             "Node {} is expected to be alive".format(node_id))
            if expecting_broken_cluster:
                tiden_assert(False, 'split up all zookeeper host expected to broke cluster')

        except Exception as e:
            if expecting_broken_cluster:
                util_sleep_for_a_while(60, msg='Wait all node segmented')
                for node_id in node_connection.keys():
                    tiden_assert(not self.ignite.check_node_is_alive(node_id),
                                 "Node {} is expected to be dead".format(node_id))
            else:
                raise e

    def zookeeper_fail_scenario(self, scenario):
        zk_scenario_to_action_mapper = {
            ZkScenarios.stop_leader: {
                    'action': self.kill_zk_node_with_role,
                    'node': 'leader'
                },
            ZkScenarios.stop_follower: {
                    'action': self.kill_zk_node_with_role,
                    'node': 'follower'
                },
            ZkScenarios.stop_leader_follower: {
                    'action': self.kill_zk_node_with_role,
                    'node': ['follower', 'leader']
                },
            ZkScenarios.split_leader_follower: {
                    'action': self.split_zookeeper_nodes,
                    'node': 'leader'
                },
            ZkScenarios.split_all_nodes: {
                    'action': self.split_zookeeper_nodes,
                    'node': 'all'
                }
        }

        zk_scenario_action = zk_scenario_to_action_mapper.get(ZkScenarios(scenario))

        if zk_scenario_action:
            zk_scenario_action.get('action')(zk_scenario_action.get('node'))
        else:
            raise TidenException('Could not find scenario-action mapping for scenario {}'.format(scenario))

    @test_case_id('51660')
    @attr('server_network_split')
    @require(min_server_hosts=4)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_topology_split_50_50(self):
        """
        Test based on network scenario with split cluster on 50/50 proportions
        Expecting result: all nodes in the segment without coordinator should be failed with communication error
                          resolver cause of SEGMENTED reason.
        """
        connection = self.get_server_connections()
        first_hosts_list = dict(list(connection.items())[:len(connection.keys()) // 2])
        second_hosts_list = dict(list(connection.items())[len(connection.keys()) // 2:])
        self.server_segmentation_emulate(first_hosts_list, second_hosts_list)
        self.server_check_cluster_behaviour(second_hosts_list)

    @test_case_id('51661')
    @attr('server_network_split')
    @require(min_server_hosts=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_topology_split_small_big(self):
        """
        Test based on network scenario with split cluster on 40/60 proportions
        Expecting result: all nodes in the small segment should be failed with communication error
                          resolver cause of SEGMENTED reason.
        """
        connection = self.get_server_connections()
        host_count = int(len(self.config['environment']['server_hosts']) * 0.4)
        if host_count < 1:
            host_count = 1
        node_count = host_count * self.config['environment']['servers_per_host']
        first_hosts_list = dict(list(connection.items())[:node_count])
        second_hosts_list = dict(list(connection.items())[node_count:])
        self.server_segmentation_emulate(first_hosts_list, second_hosts_list, reverse=True)
        self.server_check_cluster_behaviour(first_hosts_list)

    @test_case_id('51662')
    @attr('server_network_split')
    @require(min_server_hosts=3)
    @with_setup(setup_testcase, teardown_testcase)
    def test_zookeeper_topology_split_custom(self):
        """
        Test based on network scenario with split cluster 25% part with other 25% part, but both of them safe
        connection to other 50% of the cluster.
        Expecting result:
        """
        connection = self.get_server_connections()
        host_count = int(len(self.config['environment']['server_hosts']) * 0.25)
        if host_count < 1:
            host_count = 1
        node_count = host_count * self.config['environment']['servers_per_host']
        half_hosts_list = dict(list(connection.items())[:(node_count * 2)])
        first_hosts_list = dict(list(half_hosts_list.items())[:node_count])
        second_hosts_list = dict(list(half_hosts_list.items())[node_count:])
        self.server_segmentation_emulate(first_hosts_list, second_hosts_list, reverse=True)
        self.server_check_cluster_behaviour(first_hosts_list)

    def server_segmentation_emulate(self, first_hosts_list, second_hosts_list, reverse=False):
        try:
            self.iptables_clear()

            self.assert_no_errors_in_utility_output()

            with PiClient(self.ignite, self.get_client_config()):
                with TransactionalLoading(self, skip_consistency_check=True):
                    util_sleep_for_a_while(10, msg='Wait until load started')
                    self.split_nodes(first_hosts_list, second_hosts_list)
                    util_sleep_for_a_while(90, msg='Wait after network issue')
            util_sleep_for_a_while(5, msg='Wait after load')

            self.assert_no_errors_in_utility_output(tx_check=True, reverse=reverse)

        finally:
            self.iptables_clear()

    def server_check_cluster_behaviour(self, node_segmented_group):
        """
        Func got expected segmented group:
            1. check that all nodes in this group are dead
            2. start all these nodes
            3. wait rebalance timeout
            4. check there was no data corruption:
                - call idle_verify
                - try to do some loading
                - call idle_verify again and check transactions
        :param node_segmented_group: group of nodes expected to be dead
        """
        # check all nodes are dead
        for node_id in node_segmented_group.keys():
            tiden_assert(not self.ignite.check_node_is_alive(node_id),
                         "Node {} is expected to be dead".format(node_segmented_group.get(node_id)))

        second_hosts_node_ids = [int(node) for node in node_segmented_group.keys()]

        # start all nodes and wait for rebalance completed
        self.ignite.start_nodes(*second_hosts_node_ids, force=True)
        util_sleep_for_a_while(90, msg='Wait rebalance timeout')

        # check idle verify does not return any errors
        self.assert_no_errors_in_utility_output()

        # check with some loading
        with PiClient(self.ignite, self.get_client_config()):
            with TransactionalLoading(self, skip_consistency_check=True):
                util_sleep_for_a_while(15, msg='Little load')

        util_sleep_for_a_while(5, msg='Wait after load')

        self.assert_no_errors_in_utility_output(tx_check=True)

