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

from tiden.ignite import Ignite
from tiden.zookeeper import Zookeeper


class IgniteBenchAdapter:
    ignite_srvs = None
    ignite_cli_load = None
    ignite_cli_flaky = None

    config = None
    ssh = None

    nodes = {}

    def __init__(self, config, ssh):
        self.config = config
        self.ssh = ssh

        self.ignite_srvs = Ignite(config, ssh, grid_name='srv')
        self.ignite_srvs.set_snapshot_timeout(240)

        client_hosts = config['environment']['client_hosts']

        assert len(client_hosts) > 0, "At least one client host is required!"
        if len(client_hosts) > 1:
            client_hosts_load = client_hosts[1:]
            client_hosts_flaky = [client_hosts[0]]
        else:
            client_hosts_load = [client_hosts[0]]
            client_hosts_flaky = [client_hosts[0]]

        config_cli_load = deepcopy(config)
        load_env = config_cli_load['environment']
        load_env['client_hosts'] = client_hosts_load
        load_env['clients_per_host'] = 1
        num_load_clients = (
            load_env['clients_per_host'] * len(set(load_env['client_hosts']))
        )
        self.ignite_cli_load = Ignite(config_cli_load, ssh, grid_name='load')

        config_cli_flaky = deepcopy(config)
        config_cli_flaky['environment']['client_hosts'] = client_hosts_flaky
        config_cli_flaky['environment']['clients_per_host'] = 5
        self.ignite_cli_flaky = Ignite(config_cli_flaky, ssh, grid_name='flicker',
                                       start_client_idx=num_load_clients + 1)

        self.zoo = Zookeeper(config_cli_flaky, ssh)

    def setup(self):
        self.ignite_srvs.setup()
        self.ignite_cli_load.setup()
        self.ignite_cli_flaky.setup()
        self._update_nodes()

    def _update_nodes(self):
        for i in self.ignite_srvs.nodes.keys():
            self.nodes[i] = self.ignite_srvs.nodes[i]
            self.ignite_cli_load.nodes[i] = self.ignite_srvs.nodes[i]
            self.ignite_cli_flaky.nodes[i] = self.ignite_srvs.nodes[i]
        for i in self.ignite_cli_load.nodes.keys():
            self.nodes[i] = self.ignite_cli_load.nodes[i]
        for i in self.ignite_cli_flaky.nodes.keys():
            self.nodes[i] = self.ignite_cli_flaky.nodes[i]

    def set_node_option(self, filter, option, value):
        if filter in ['*', 'server']:
            self.ignite_srvs.set_node_option(filter, option, value)
        if filter in ['*', 'client']:
            self.ignite_cli_load.set_node_option(filter, option, value)
            self.ignite_cli_flaky.set_node_option(filter, option, value)
        else:
            self.ignite_srvs.set_node_option(filter, option, value)
        self._update_nodes()

    def start_nodes(self, **kwargs):
        self.ignite_srvs.start_nodes(**kwargs)
        self._update_nodes()

    def get_control_utility(self):
        return self.ignite_srvs.get_control_utility()

    cu = property(get_control_utility, None)

    def get_jmx_utility(self):
        return self.ignite_srvs.get_jmx_utility()

    jmx = property(get_jmx_utility, None)

    def get_snapshot_utility(self):
        return self.ignite_srvs.get_snapshot_utility()

    su = property(get_snapshot_utility, None)

    def get_all_default_nodes(self):
        return self.ignite_srvs.get_all_default_nodes()

    def get_alive_default_nodes(self):
        return self.ignite_srvs.get_alive_default_nodes()

    def get_all_additional_nodes(self):
        return self.ignite_srvs.get_all_additional_nodes()

    def get_alive_additional_nodes(self):
        return self.ignite_srvs.get_alive_additional_nodes()

    def get_all_client_nodes(self):
        return self.ignite_cli_load.get_all_client_nodes() + self.ignite_cli_flaky.get_all_client_nodes()

    def get_all_common_nodes(self):
        return self.ignite_cli_load.get_all_common_nodes() + self.ignite_cli_flaky.get_all_common_nodes()

    def get_cache_names(self, *args):
        return self.ignite_srvs.get_cache_names(*args)

    def get_entries_num(self, *args):
        return self.ignite_srvs.get_entries_num(*args)

    def get_nodes_num(self, filter):
        if filter == 'client':
            return self.ignite_cli_load.get_nodes_num('client') + self.ignite_cli_flaky.get_nodes_num('client')
        if filter == 'server':
            return self.ignite_srvs.get_nodes_num('server')

    def stop_nodes(self, *args):
        for i in self.ignite_srvs.nodes.keys():
            if i in self.ignite_cli_load.nodes.keys():
                del self.ignite_cli_load.nodes[i]
            if i in self.ignite_cli_flaky.nodes.keys():
                del self.ignite_cli_flaky.nodes[i]
        self.ignite_srvs.stop_nodes(*args)
        if len(self.ignite_cli_flaky.get_all_alive_nodes()) > 0:
            self.ignite_cli_flaky.kill_nodes()
        if len(self.ignite_cli_load.get_all_alive_nodes()) > 0:
            self.ignite_cli_load.kill_nodes()

    def reset(self):
        self.ignite_srvs.reset(hard=True)
        self.ignite_cli_flaky.reset(hard=True)
        self.ignite_cli_load.reset(hard=True)
        self.ssh.killall('java')

    def create_jfr(self):
        self.ignite_srvs.make_cluster_jfr(20)

