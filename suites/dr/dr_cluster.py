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


class Cluster:
    last_dr_port = 50000

    def __init__(self, id, config):
        self.config = config
        self.id = id
        self.nodes = []
        self.last_node_id = 0
        self.grid: Ignite = None
        self.piclient_config = None

    def add_nodes(self, nodes_count, node_type):
        already_nodes = self.last_node_id
        for counter in range(self.last_node_id + 1, self.last_node_id + nodes_count + 1):
            node = Node(self)
            self.last_node_id += 1
            node.id = self.last_node_id
            node.node_type = node_type
            Cluster.last_dr_port += 1
            node.dr_port = Cluster.last_dr_port
            node.cluster = self
            self.nodes.append(node)
            node.host = self.get_host(node.id - already_nodes, node_type)
            if node_type == 'server':
                setattr(self, f'{node_type}{len(self.get_server_nodes())}', node)
            if node_type == 'client':
                setattr(self, f'{node_type}{len(self.get_client_nodes())}', node)

    def get_host(self, node_id, node_type):
        if node_type == 'client':
            return self.config['environment']['client_hosts'][
                (node_id - 1) // self.config['environment']['clients_per_host']]
        else:
            if self.config['environment'].get('dr_{}_hosts'.format(self.id)):
                return self.config['environment']['dr_{}_hosts'.format(self.id)][
                    (node_id - 1) // self.config['environment']['servers_per_host']]
            else:
                return self.config['environment']['server_hosts'][
                    (node_id - 1) // self.config['environment']['servers_per_host']]

    def get_node_by_id(self, id):
        nodes = [node for node in self.nodes if node.id == id]
        if not nodes:
            raise ValueError('Can not find node with id {} in nodes {}'.format(id, self.nodes))
        return nodes[-1]

    def get_nodes_by_type(self, node_type):
        if node_type not in ('client', 'server'):
            raise ValueError('Incorrect type for node. Expecting \'server\' or \'client\'. Got {}'.format(node_type))
        return [node.id for node in self.nodes if node.node_type == node_type]

    def get_server_node_ids(self):
        return [node.id for node in self.nodes if node.node_type == 'server']

    def get_client_node_ids(self):
        return [node.id for node in self.nodes if node.node_type == 'client']

    def get_server_nodes(self):
        return [node for node in self.nodes if node.node_type == 'server']

    def get_server_hosts(self):
        return list(set([node.host for node in self.nodes if node.node_type == 'server']))

    def get_client_nodes(self):
        return [node for node in self.nodes if node.node_type == 'client']

    def get_sender_nodes(self):
        return [node for node in self.nodes if node.is_sender()]


class Node:
    def __init__(self, cluster):
        self.id = None
        self.dr_port = None
        self.sender_nodes = []
        self.receiver_nodes = []
        self.config_name = None
        self.cluster = cluster
        self.node_type = None

    def is_sender(self):
        return len(self.receiver_nodes) > 0

    def is_receiver(self):
        return len(self.sender_nodes) > 0


def create_dr_relation(sender_node, receiver_node, bidirectional=False):
    sender_node.receiver_nodes.append(receiver_node)
    receiver_node.sender_nodes.append(sender_node)
    if bidirectional:
        sender_node.sender_nodes.append(receiver_node)
        receiver_node.receiver_nodes.append(sender_node)

