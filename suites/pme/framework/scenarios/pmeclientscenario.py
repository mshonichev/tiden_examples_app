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

from tiden import log_print, log_put
from tiden.util import prettydict, util_sleep_for_a_while, is_enabled
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.piclient import PiClient
from suites.pme.framework.scenarios.pmeabstractscenario import PMEAbstractScenario


class PMEClientScenario(PMEAbstractScenario):

    x1_leave_time = None
    x1_join_time = None
    version = None
    ignite = None

    def _validate_test_config(self):
        assert 'num_clients_to_kill' in self.config, 'There is no "num_clients_to_kill" variable in config'

        num_client_nodes = self._get_num_client_nodes()

        assert num_client_nodes >= int(self.config['num_clients_to_kill']), \
            "You can't kill {} out of {} clients!".format(self.config['num_clients_to_kill'], num_client_nodes)

    def _get_scenario_name(self):
        return "%d client(s) leave-join PME benchmark" % self.config.get('num_clients_to_kill', 1)

    def _run_iteration(self, ignite, iteration):
        """
        One iteration of clients PME benchmark is as follows:

            1. start transactional loading at `loading_clients_hosts`, sleep `warmup_clients_delay` so load stabilize
            2.    start `num_clients_to_kill` clients at `clients_hosts` (different from `loading_clients_hosts`)
                  measure JOIN exchange time, sleep `stabilization_delay`
            3.    stop started additional clients, measure LEAVE exchange time, sleep `cooldown_delay`
        :param ignite:
        :param iteration:
        :return:
        """
        log_print("===> PME {} Clients(s) Left-Join Benchmark iteration {}/{} artifact started ".format(
            self.config['num_clients_to_kill'],
            iteration,
            self.config['iterations']
        ), color='green')

        loading_client_hosts = self._get_loading_client_hosts()
        client_hosts = self._get_client_hosts(loading_client_hosts)
        num_clients = self.config['num_clients_to_kill']

        metrics = None
        ex = None
        x1_join_time = None
        x1_leave_time = None

        try:
            # start loading clients
            with PiClient(
                    ignite,
                    self.test_class.client_config,
                    client_hosts=loading_client_hosts,
                    clients_per_host=self.config.get('loading_clients_per_host', 1)
            ):
                # initiate transactional loading
                with TransactionalLoading(
                        self.test_class,
                        ignite=ignite,
                        kill_transactions_on_exit=self.config['kill_transactions_on_exit'],
                        cross_cache_batch=self.config['cross_cache_batch'],
                        skip_atomic=self.config['skip_atomic'],
                        skip_consistency_check=not self.config['consistency_check_enabled'],
                        loading_profile=LoadingProfile(
                            delay=self.config['tx_delay'],
                            commit_possibility=self.config['commit_possibility'],
                            start_key=1,
                            end_key=self.config['load_factor'] - 1,
                            transaction_timeout=self.config['transaction_timeout']
                        ),
                        tx_metrics=['txCreated', 'txCommit', 'txFailed', 'txRollback']
                ) as tx_loading:
                    metrics = tx_loading.metrics

                    util_sleep_for_a_while(self.config['warmup_clients_delay'], "Before JOIN")

                    current_clients_num = ignite.get_nodes_num('client')
                    expected_total_clients_num = current_clients_num + num_clients

                    self.test_class._prepare_before_test(ignite, tx_loading, 'JOIN %d client(s)' % num_clients)

                    # start num_clients client nodes on 'flaky' hosts
                    with PiClient(
                            ignite,
                            self.test_class.client_config,
                            client_hosts=client_hosts,
                            clients_per_host=self.config.get('clients_per_host', 1),
                            nodes_num=num_clients,
                            new_instance=True,
                    ):

                        ignite.wait_for_topology_snapshot(client_num=expected_total_clients_num, timeout=600, check_only_servers=True, exclude_nodes_from_check=[])
                        tx_loading.metrics_thread.add_custom_event('%d client(s) joined' % num_clients)
                        new_topVer = self.test_class._get_new_top_after_test(ignite)
                        self.test_class._wait_exchange_finished(ignite, new_topVer)

                        x1_join_time, x2_time = self.test_class._measurements_after_test('JOIN %d client(s)' % num_clients, skip_exch=1)

                        util_sleep_for_a_while(self.config['stabilization_delay'])

                        # upon exit from with block, num_clients client nodes will be killed
                        self.test_class._prepare_before_test(ignite, tx_loading, 'LEAVE %d client(s)' % num_clients)

                    ignite.wait_for_topology_snapshot(client_num=current_clients_num, timeout=600, check_only_servers=True, exclude_nodes_from_check=[])
                    tx_loading.metrics_thread.add_custom_event('%d client(s) left' % num_clients)
                    new_topVer = self.test_class._get_new_top_after_test(ignite)
                    self.test_class._wait_exchange_finished(ignite, new_topVer)

                    x1_leave_time, x2_time = self.test_class._measurements_after_test('LEAVE %d client(s)' % num_clients, skip_exch=1)
                    util_sleep_for_a_while(self.config['cooldown_delay'])

            ignite.wait_for_topology_snapshot(client_num=0)
        except Exception as e:
            ex = e
        if metrics:
            self.test_class.create_loading_metrics_graph(
                'pme_%d_clients_left_join_%s_%d' % (num_clients, self.run_id, iteration),
                metrics,
                dpi_factor=0.75
            )
        if ex:
            raise ex

        return {
            'Exchange Client Join': x1_join_time,
            'Exchange Client Leave': x1_leave_time,
        }

    def _finalize_run_data(self, run_data):
        return {
            'x1_time': run_data
        }

