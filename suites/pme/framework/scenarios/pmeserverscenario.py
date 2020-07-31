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

from tiden import log_print
from tiden.util import prettydict, util_sleep_for_a_while, is_enabled
from tiden_gridgain.piclient.loading import TransactionalLoading, LoadingProfile
from tiden_gridgain.piclient.piclient import PiClient
from suites.pme.framework.scenarios.pmeabstractscenario import PMEAbstractScenario


class PMEServerScenario(PMEAbstractScenario):

    def _get_scenario_name(self):
        return "%d server(s) leave-join PME benchmark" % self.config.get('num_servers_to_kill', 1)

    def _validate_test_config(self):
        assert 'num_servers_to_kill' in self.config, 'There is no "num_servers_to_kill" variable in config'

        num_server_hosts = self.config['num_server_hosts']

        assert num_server_hosts > self.config['num_servers_to_kill'], \
            "You can't kill {} out of {} servers!".format(self.config['num_servers_to_kill'], num_server_hosts)

        self.config['kill_coordinator'] = self.config.get('kill_coordinator', False)

        self.config['measure_restart_nodes'] = self.config.get('measure_restart_nodes', True)

    def _need_pin_coordinator(self):
        return True
        # return not self.config.get('kill_coordinator')

    def _run_iteration(self, ignite, iteration):
        """
        One iteration of server PME benchmark is as follows:

            1. start transactional loading, sleep `warmup_servers_delay` so that load stabilize
            2.   kill random N nodes, measure LEAVE exchange time, sleep `stabilization_delay`
            3.   restart killed nodes, measure JOIN exchange time, sleep `cooldown_delay`
            4. stop load

        :param ignite:
        :param iteration:
        :return:
        """
        log_print("===> PME {} Server(s) Left-Join Benchmark iteration {}/{} started ".format(
            self.config['num_servers_to_kill'],
            iteration,
            self.config['iterations']
        ), color='green')

        # if debug:
        #     from pt.util import read_yaml_file
        #     from os.path import join
        #     base_path = 'pt/tests/res/exchanges'
        #     exch_test = iteration
        #     start_exch = read_yaml_file(join(base_path, 'start_exch.%d.yaml' % exch_test))
        #     finish_exch = read_yaml_file(join(base_path, 'finish_exch.%d.yaml' % exch_test))
        #     merge_exch = read_yaml_file(join(base_path, 'merge_exch.%d.yaml' % exch_test))
        #     self.test_class.exchanges = ExchangesCollection.create_from_log_data(start_exch, finish_exch, merge_exch)
        #     self.test_class.new_topVer = 5
        #     x1_leave_time, x2_time = self.test_class._measurements_after_test('test_leave', skip_exch=1)
        #     self.test_class.new_topVer = 6
        #     x1_join_time, x2_time = self.test_class._measurements_after_test('test_join', skip_exch=1)
        #
        #     return x1_leave_time, x1_join_time

        loading_client_hosts = self._get_loading_client_hosts()
        num_servers = self._get_num_server_nodes()
        num_servers_to_kill = self.config['num_servers_to_kill']
        kill_coordinator = self.config['kill_coordinator']

        metrics = None
        ex = None
        x1_join_time = None
        x1_leave_time = None

        try:
            # start loading clients ...
            with PiClient(
                    ignite,
                    self.test_class.client_config,
                    client_hosts=loading_client_hosts,
                    clients_per_host=self.config.get('loading_clients_per_host', 1)
            ):
                # ... and initiate transactional load
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

                    # pick random server nodes
                    node_ids = ignite.get_random_server_nodes(
                        num_servers_to_kill,
                        use_coordinator=kill_coordinator,
                        node_ids=self.test_class.server_node_ids,
                    )

                    expected_total_server_num = num_servers - len(node_ids)

                    # ... wait load stabilize
                    util_sleep_for_a_while(self.config['warmup_servers_delay'], "Before LEAVE")

                    if is_enabled(self.config.get('jfr_enabled', False)):
                        ignite.make_cluster_jfr(60)

                    util_sleep_for_a_while(2)
                    self.test_class._prepare_before_test(ignite, tx_loading, 'LEAVE %d server(s)' % len(node_ids))

                    # ... kill selected random nodes
                    ignite.kill_nodes(*node_ids)
                    ignite.wait_for_topology_snapshot(server_num=expected_total_server_num)
                    tx_loading.metrics_thread.add_custom_event('%d server(s) left' % len(node_ids))

                    new_topVer = self.test_class._get_new_top_after_test(ignite)
                    self.test_class._wait_exchange_finished(ignite, new_topVer)

                    x1_leave_time, x2_time = self.test_class._measurements_after_test(
                        'LEAVE %d server(s)' % len(node_ids), skip_exch=1)

                    if is_enabled(self.config.get('heapdump_enabled', False)):
                        ignite.make_cluster_heapdump([1], 'after_%d_server_leave' % len(node_ids))

                    # ... wait exchange stabilize
                    util_sleep_for_a_while(self.config['stabilization_delay'], "After LEAVE, before JOIN")

                    if self.config['measure_restart_nodes']:
                        self.test_class._prepare_before_test(ignite, tx_loading, 'JOIN %d server(s)' % len(node_ids))

                    # ... restart killed nodes
                    ignite.start_nodes(*node_ids)
                    ignite.wait_for_topology_snapshot(server_num=expected_total_server_num + len(node_ids))

                    if self.config['measure_restart_nodes']:
                        tx_loading.metrics_thread.add_custom_event('%d server(s) joined' % len(node_ids))

                        new_topVer = self.test_class._get_new_top_after_test(ignite)
                        self.test_class._wait_exchange_finished(ignite, new_topVer)
                        x1_join_time, x2_time = self.test_class._measurements_after_test(
                            'JOIN %d server(s)' % len(node_ids), skip_exch=1)
                        # if is_enabled(self.config.get('heapdump_enabled', False)):
                        #     ignite.make_cluster_heapdump([1], 'after_%d_server_join' % len(node_ids))

                    # ... wait exchange cooldown
                    util_sleep_for_a_while(self.config['cooldown_delay'], "After JOIN")

            ignite.wait_for_topology_snapshot(client_num=0)
        except Exception as e:
            ex = e
        if metrics:
            self.test_class.create_loading_metrics_graph(
                'pme_%d_servers_left_join_%s_%d' % (num_servers_to_kill, self.run_id, iteration),
                metrics,
                dpi_factor=0.75
            )
        if ex:
            raise ex

        return {
            'Exchange Server Join': x1_join_time,
            'Exchange Server Leave': x1_leave_time,
        }

    def _finalize_run_data(self, run_data):
        """
        post-process scenario run data before passing up to probes
        :param run_data:
        :return:
        """
        return {
            'x1_time': run_data
        }

