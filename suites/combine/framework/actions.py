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

from os.path import join
from random import choice

from apps import NodeStatus
from tiden import util_sleep_for_a_while
from tiden.ignite import Ignite


class ActionsMixin:

    def __init__(self):
        self.cluster: Ignite = None
        self.context: dict = None
        self.server_config = None

    def state_stable(self):
        pass

    @property
    def get_cluster_state(self):
        return self.context['history'][-1]['setup_cluster']

    def state_nodes_leave(self, options='nodes_leave'):
        """
        Kill nodes
        """
        default_options = self.context['options']['actions'][options]
        nodes_to_leave = int(default_options['nodes_count'])
        killed_nodes = []
        for i in range(nodes_to_leave):
            nodes_to_delete = self.cluster.get_alive_default_nodes()
            if self.context.get('temp_action_options'):
                node_to_kill = int(self.context.get('temp_action_options')[0])
            else:
                node_to_kill = choice(nodes_to_delete)
            self.context['attachments'].append({
                'name': 'Kill info',
                'source': f'Kill node: {node_to_kill}\nKill time: {self.current_time_pretty}',
                'type': 'text/plain'
            })
            killed_nodes.append(node_to_kill)
            self.cluster.kill_node(node_to_kill)
            server_nodes = len(self.cluster.get_alive_default_nodes()) + len(self.cluster.get_alive_additional_nodes())
            self.cluster.wait_for_topology_snapshot(server_num=server_nodes,
                                                    check_only_servers=True,
                                                    timeout=60)
            util_sleep_for_a_while(5)
        return killed_nodes

    def state_nodes_leave_lp(self):
        """
        Kill nodes with guaranteed lost partitions
        """
        self.state_nodes_leave('nodes_leave_lp')

    def state_nodes_leave_baseline(self, options='nodes_leave_baseline'):
        """
        Kill nodes and update BLT
        """
        self.state_nodes_leave(options)
        log_path = join(self.tiden.config['rt']['remote']['test_dir'], 'set_baseline_leave.log')
        baseline_nodes = self.cluster.cu.set_current_topology_as_baseline(strict=True, log=log_path,
                                                                          show_output=False,
                                                                          **self.control_util_ssh_options)
        assert len(set(baseline_nodes.values())) == 1, 'Not all nodes are in baseline'

    def state_nodes_leave_baseline_lp(self):
        """
        Kill nodes with guaranteed lost partitions and update BLT
        """
        self.state_nodes_leave_baseline('nodes_leave_baseline_lp')

    def state_nodes_join(self, options='nodes_join'):
        """
        Add node in topology
        """
        default_options = self.context['options']['actions'][options]
        nodes_to_join = int(default_options['nodes_count'])
        self.context['attachments'].append({
            'name': 'Join info',
            'source': f'Join time: {self.current_time_pretty}',
            'type': 'text/plain'
        })
        if self.context.get('temp_action_options'):
            node_to_start = int(self.context['temp_action_options'][0])
            self.cluster.start_node(node_to_start, force=True)
        else:
            nodes_added = self.cluster.add_additional_nodes(self.server_config, nodes_to_join)
            self.cluster.start_additional_nodes(nodes_added)

    def state_nodes_join_many(self):
        """
        Add several nodes in topology
        """
        self.state_nodes_join('nodes_join_many')

    def state_nodes_join_baseline(self, options='nodes_join_baseline'):
        """
        Add node in topology and update BLT
        """
        self.state_nodes_join(options)
        log_path = join(self.tiden.config['rt']['remote']['test_dir'], 'set_baseline_join.log')
        baseline_nodes = self.cluster.cu.set_current_topology_as_baseline(strict=True, log=log_path,
                                                                          show_output=False,
                                                                          **self.control_util_ssh_options)
        assert len(set(baseline_nodes.values())) == 1, 'Not all nodes are in baseline'

    def state_nodes_join_baseline_many(self):
        """
        Add several nodes in topology and update baseline
        """
        self.state_nodes_join_baseline('nodes_join_baseline_many')

    def state_nodes_join_existed(self, options='nodes_join_existed', action=None):
        """
        Kill node and start again
        """
        if action is None:
            action = lambda node_id: None
        self.context['attachments'].append({
            'name': 'Join info',
            'source': f'Join time: {self.current_time_pretty}',
            'type': 'text/plain'
        })
        killed_nodes = self.state_nodes_leave(options)
        util_sleep_for_a_while(5)
        hosts = len(self.tiden.config['environment']["server_hosts"])
        nodes_per_host = self.tiden.config['environment']["servers_per_host"]
        for idx, killed_node in enumerate(killed_nodes):
            action(killed_node)
            self.cluster.start_node(killed_node, force=True)
            self.cluster.wait_for_topology_snapshot(
                server_num=hosts * nodes_per_host - len(killed_nodes) + idx + 1,
                timeout=80,
                check_only_servers=True
            )

    def state_nodes_join_existed_empty(self):
        """
        Kill node, clean work directory and start again
        """
        self.state_nodes_join_existed(options='nodes_join_existed_empty',
                                      action=lambda node_id: self.cluster.cleanup_work_dir(node_id))

    def state_nodes_join_existed_removed_indexes(self):
        """
        Kill node, delete all index.bin files and start again
        """
        action = lambda node_id: self.cluster.ssh.exec([f"rm -f $(find {self.cluster.nodes[node_id]['ignite_home']} | grep db | grep index.bin)"])
        self.state_nodes_join_existed(options='nodes_join_existed_removed_indexes', action=action)

