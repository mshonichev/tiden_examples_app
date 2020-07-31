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

from tiden.util import log_print, prettydict, util_sleep_for_a_while, is_enabled

from suites.consumption.framework.scenarios.abstract import AbstractScenario


class PMEAbstractScenario(AbstractScenario):

    # skip preloading, dump more debug info
    debug = False

    run_id = 0

    def _validate_config(self):
        super()._validate_config()
        self._validate_suite_config()
        self._validate_preloading_config()
        self._validate_tx_load_config()
        self._validate_test_config()

    def _validate_preloading_config(self):
        assert 'load_factor' in self.config, 'There is no "load_factor" variable in config'
        assert 'num_preloading_nodes' in self.config, 'There is no "num_preloading_nodes" variable in config'

    def _get_environment_config(self):
        return self.test_class.tiden.config['environment']

    def _validate_suite_config(self):
        assert 'iterations' in self.config, 'There is no "iterations" variable in config'

        # by default, servers and clients must be started at different hosts, so total number of hosts
        # should be enough for num_clients + num_servers
        if self.config.get('distinct_servers_and_clients', True):
            environment_config = self._get_environment_config()

            total = self.config['num_client_hosts'] + \
                    self.config['num_loading_client_hosts'] + \
                    self.config['num_server_hosts']

            assert len(environment_config['server_hosts']) >= total, \
                "Total number of hosts should be at least {} (number of servers + number of clients)".format(total)

    def _validate_environment_config(self):
        assert 'num_server_hosts' in self.config, 'There is no "num_server_hosts" variable in config'
        assert 'servers_per_host' in self.config, 'There is no "servers_per_host" variable in config'

        assert 'num_client_hosts' in self.config, 'There is no "num_client_hosts" variable in config'
        self.config['clients_per_host'] = self.config.get('clients_per_host', 2)

        environment_config = self._get_environment_config()

        assert set(environment_config['server_hosts']) == set(environment_config['client_hosts']), \
            "PME benchmark should be started with the same set client and server hosts specified"

        assert self.config['servers_per_host'] == environment_config.get('servers_per_host', 1), \
            "Environment `servers_per_host` is expected to be {} ".format(self.config['servers_per_host'])

        num_server_hosts = self.config['num_server_hosts']
        assert len(environment_config['server_hosts']) >= num_server_hosts, \
            "Not enough servers to run benchmark: should give at least {} hosts".format(num_server_hosts)

        total_num_client_hosts = self.config['num_client_hosts'] + self.config['num_loading_client_hosts']
        assert len(environment_config['client_hosts']) >= total_num_client_hosts, \
            "Not enough clients to run benchmark: should give at least {} hosts".format(total_num_client_hosts)

    def _validate_tx_load_config(self):
        assert 'num_loading_client_hosts' in self.config, 'There is no "num_loading_client_hosts" variable in config'
        self.config['loading_clients_per_host'] = self.config.get('loading_clients_per_host', 1)

        self.config['consistency_check_enabled'] = self.config.get('consistency_check_enabled', False)
        assert 'transaction_timeout' in self.config, "There is no 'transaction_timeout' variable in config"
        assert 'cross_cache_batch' in self.config, "There is no 'cross_cache_batch' variable in config"
        assert 'tx_delay' in self.config, "There is no 'tx_delay' variable in config"
        assert 'commit_possibility' in self.config, "There is no 'commit_possibility' variable in config"
        assert 'cross_cache_batch' in self.config, "There is no 'cross_cache_batch' variable in config"
        assert 'skip_atomic' in self.config, "There is no 'skip_atomic' variable in config"
        assert 'kill_transactions_on_exit' in self.config, "There is no 'kill_transactions_on_exit' variable in config"

    def _get_server_hosts(self):
        environment_config = self.test_class.tiden.config['environment']
        all_hosts = set(environment_config['server_hosts']) and set(environment_config['client_hosts'])

        server_hosts = []
        for i in range(0, self.config['num_server_hosts']):
            server_hosts.append(all_hosts.pop())

        return server_hosts

    def _get_num_server_nodes(self):
        return len(self._get_server_hosts()) * self.config.get('servers_per_host', 1)

    def _get_num_client_nodes(self):
        return len(self._get_client_hosts(self._get_loading_client_hosts())) * self.config.get('clients_per_host', 1)

    def _get_loading_client_hosts(self):
        """
        PME benchmark use aggregated set of server and client hosts.
        loading clients are started after servers.
        first num_server_hosts from server_hosts are reserved for servers.
        next num_loading_client_hosts are used for loading clients.
        :return: list of hosts for use with loading clients
        """
        environment_config = self.test_class.tiden.config['environment']

        all_hosts = set(environment_config['server_hosts']) and set(environment_config['client_hosts'])

        for i in range(0, self.config['num_server_hosts']):
            all_hosts = all_hosts - {environment_config['server_hosts'][i]}

        loading_client_hosts = []
        for i in range(0, self.config['num_loading_client_hosts']):
            loading_client_hosts.append(all_hosts.pop())

        return loading_client_hosts

    def _get_client_hosts(self, loading_client_hosts):
        """
        PME benchmark use aggregated set of server and client hosts.
        flacky clients are started after servers and loading clients.
        first num_server_hosts from server_hosts are reserved for servers.
        next num_loading_client_hosts are used for loading clients.
        flacky clients use all other hosts.
        :return: list of hosts for use with flacky clients
        """
        environment_config = self.test_class.tiden.config['environment']

        all_hosts = set(environment_config['server_hosts']) and set(environment_config['client_hosts'])

        for i in range(0, self.config['num_server_hosts']):
            all_hosts = all_hosts - {environment_config['server_hosts'][i]}

        for host in loading_client_hosts:
            all_hosts = all_hosts - {host}

        return list(all_hosts)

    def run(self, artifact_name):
        """
        Run generic PME scenario for defined artifact

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)
        self.run_id += 1

        log_print("Running %s for '%s' with config: \n%s" %
                  (self._get_scenario_name(), artifact_name, prettydict(self.config)), color='green')

        if self.config['num_server_hosts'] > 0:
            log_print("{num_servers} server(s) will be started at {server_hosts}".format(
                num_servers=self.config['num_server_hosts'] * self.config.get('servers_per_host', 1),
                server_hosts=self._get_server_hosts())
            )
        if self.config['num_loading_client_hosts'] > 0:
            log_print("{num_clients} loading client(s) will be started at {client_hosts}".format(
                num_clients=self.config['num_loading_client_hosts'] * self.config.get('loading_clients_per_host', 1),
                client_hosts=self._get_loading_client_hosts()
            ))
        if self.config['num_client_hosts'] > 0:
            log_print("{num_clients} flacky client(s) will be started at {client_hosts}".format(
                num_clients=self.config['num_client_hosts'] * self.config.get('clients_per_host', 1),
                client_hosts=self._get_client_hosts(self._get_loading_client_hosts())
            ))

        version, ignite = self.test_class.start_ignite_grid(
            artifact_name,
            num_servers=self.config['num_server_hosts'] * self.config.get('servers_per_host', 1),
            run_id=self.run_id,
            pin_coordinator=self._need_pin_coordinator(),
        )

        if not self.debug:
            log_print("Data preloading", color='green')
            self.test_class.load_data_with_streamer(
                ignite=ignite,
                config=self.test_class.client_config,
                start_key=0,
                end_key=1 * self.config['load_factor'],
                nodes_num=self.config['num_preloading_nodes']
            )

        run_data = self._init_run_data()
        self.start_probes(artifact_name)

        for i in range(0, self.config['iterations']):
            iteration_data = self._run_iteration(ignite, i + 1)
            self._update_run_data(run_data, iteration_data)

        self.stop_probes(**self._finalize_run_data(run_data))

        self.results['evaluated'] = True

        ignite.kill_nodes()
        ignite.cleanup_work_dir()

    # ---
    # Overridables
    # ---

    def _validate_test_config(self):
        pass

    def _need_pin_coordinator(self):
        return False

    def _get_scenario_name(self):
        return "PME abstract"

    def _init_run_data(self):
        """
        initializes scenario run collected data
        :return:
        """
        return {}

    def _run_iteration(self, ignite, i):
        """
        performs i-th iteration of scenario run and return iteration data
        :param ignite:
        :param i:
        :return:
        """
        return {}

    def _finalize_run_data(self, run_data):
        """
        post-process scenario run data before passing up to probes
        :param run_data:
        :return:
        """
        return run_data

    def _update_run_data(self, run_data, iteration_data):
        """
        updates scenario run data with run iteration data
        :param run_data:
        :param iteration_data:
        :return:
        """
        for key in iteration_data:
            if key not in run_data.keys():
                run_data[key] = []
            run_data[key].append(iteration_data[key])

