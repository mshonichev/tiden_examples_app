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
from tiden.case.apptestcase import AppTestCase
from tiden.util import log_print, attr, test_case_id, print_blue, print_red, is_enabled, with_setup, log_put
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden_gridgain.piclient.loading import LoadingUtils

from suites.consumption.framework.runner import ScenarioRunner
from suites.pme.framework.scenarios import *
from tiden.apps.ignite.exchange_info import ExchangesCollection

from time import sleep
from sys import stdout


class TestPmeAppBench(AppTestCase):
    client_config = None
    server_config = None

    scenarios = {}
    artifacts = {}
    pme_config = {}

    server_node_ids = []
    max_servers_per_host = 0

    def __init__(self, *args):
        super().__init__(*args)

        self.pme_config, self.artifacts = ScenarioRunner.initialize(self, args[0], 'pme.test_pme')

        self.scenarios = {
            'PME 1 server non-coordinator': (
                self.pme_config['pme_1_server_non_crd'],
                PMEServerScenario(self),
            ),
            'PME 1 server coordinator': (
                self.pme_config['pme_1_server_crd'],
                PMEServerScenario(self),
            ),
            'PME 5 server': (
                self.pme_config['pme_5_server'],
                PMEServerScenario(self),
            ),
            'PME 1 client': (
                self.pme_config['pme_1_client'],
                PMEClientScenario(self),
            ),
            'PME 5 client': (
                self.pme_config['pme_5_client'],
                PMEClientScenario(self),
            ),
            'PME activation': (
                self.pme_config['pme_activation'],
                PMEActivationScenario(self),
            ),
            'PME dynamic caches from client': (
                self.pme_config['pme_dynamic_caches_client'],
                PMEStartCachesScenario(self),
            ),
            'PME dynamic caches from server': (
                self.pme_config['pme_dynamic_caches_server'],
                PMEStartCachesScenario(self),
            ),
            'PME 64 client': (
                self.pme_config['pme_64_client'],
                PMEClientScenario(self),
            ),
        }

    def setup(self):
        zookeeper_enabled = is_enabled(self.tiden.config.get('zookeeper_enabled'))

        self.create_app_config_set(
            Ignite, 'base',
            additional_configs=['caches.tmpl.xml'],
            snapshots_enabled=True,
            logger=False,
            caches_list_file='caches_base.xml',
            logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote']['test_module_dir'],
            disabled_cache_configs=False,
            zookeeper_enabled=zookeeper_enabled,
            num_backups=self.pme_config['num_backups'],
            num_partitions=self.pme_config['num_partitions'],
        )

        # self.create_app_config_set(Ignite, 'snapshot',
        #                            snapshots_enabled=False,
        #                            logger=False,
        #                            logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
        #                                'test_module_dir'],
        #                            disabled_cache_configs=False,
        #                            zookeeper_enabled=False)

        self._fix_max_servers()
        super().setup()

    def _fix_max_servers(self):
        _ = self.__class__.__name__ + "._fix_max_servers(): "
        self.max_servers_per_host = int(self.tiden.config['environment'].get('servers_per_host', 1))
        log_print(_ + "environment servers_per_host: %d" % self.max_servers_per_host)
        for scenario_name, scenario_def in self.scenarios.items():
            scenario_config = scenario_def[0]
            scenario_servers_per_host = scenario_config.get('servers_per_host', 1)
            if scenario_servers_per_host > self.max_servers_per_host:
                log_print(_ + "scenario '%s' servers_per_host: %d" % (scenario_name, scenario_servers_per_host))
                self.max_servers_per_host = scenario_servers_per_host
        self.tiden.config['environment']['servers_per_host'] = self.max_servers_per_host

    def setup_testcase(self):
        pass

    def teardown_testcase(self):
        for ignite in self.get_app_by_type('ignite'):
            ignite.kill_nodes()
            ignite.cleanup_work_dir()

    def start_ignite_grid(self, name, num_servers=None, activate=True, pin_baseline=True, pin_coordinator=False, config_set='base', run_id=0):

        def _calc_server_node_ids(ignite):
            """
            Due to current tiden AppTestCase limitations we
                - can't change servers_per_host during test session
                - can't change number of servers to larger values than we already have
            because that implies re-running Ignite.setup().

            So, for the suite we set num_server_hosts to maximum of all tests, and during test we start only subset of
            server nodes starting from first.
            :return:
            """

            return [ignite.get_start_server_idx() + i * self.max_servers_per_host for i in range(num_servers)]

        app = self.get_app(name)
        from apps.ignite import Ignite
        ignite = Ignite(app)
        ignite.set_grid_name('run%d' % run_id)

        self.server_config = Ignite.config_builder.get_config('server', config_set_name=config_set)
        self.client_config = Ignite.config_builder.get_config('client', config_set_name=config_set)
        ignite.set_node_option('*', 'config', self.server_config)
        ignite.reset()
        # ignite.activate_default_modules()

        log_print("Ignite ver. %s, revision %s" % (
            self.artifacts[ignite.name]['ignite_version'],
            self.artifacts[ignite.name]['ignite_revision'],
        ))

        if not num_servers:
            ignite.start_nodes()
        else:
            self.server_node_ids = _calc_server_node_ids(ignite)
            if pin_coordinator:
                coordinator_node = self.server_node_ids.pop(0)
                ignite.start_nodes(coordinator_node)
            ignite.start_nodes(*self.server_node_ids)

        if activate:
            ignite.cu.activate(activate_on_particular_node=1)

        if pin_baseline:
            ignite.cu.set_current_topology_as_baseline()

        return self.artifacts[ignite.name]['ignite_version'], app

    def load_data_with_streamer(self, *args, **kwargs):
        PiClientIgniteUtils.load_data_with_streamer(*args, **kwargs)

    def create_loading_metrics_graph(self, file_name, metrics, **kwargs):
        LoadingUtils.create_loading_metrics_graph(self.tiden.config['suite_var_dir'], file_name, metrics, **kwargs)

    def get_client_config(self):
        return self.client_config

    #     # # dump results to x1.csv
    #     # makedirs(self.config['rt']['test_dir'], exist_ok=True)
    #     # with open(join(self.config['rt']['test_dir'], 'exchange-times-x1.csv'), 'w') as dump_exch_file:
    #     #     for topVer, node_data in exch_by_topVer.items():
    #     #         for node_idx, exch_data in node_data.items():
    #     #             dump_exch_file.write(
    #     #                 '{topVer},{minor_topVer},{node_idx},{node_ip},{start_time},{end_time},{duration}\n'.format(
    #     #                     topVer=int(topVer / 10000),
    #     #                     minor_topVer=topVer - int(topVer / 10000) * 10000,
    #     #                     node_idx=node_idx,
    #     #                     node_ip=self.ignite.nodes[node_idx]['host'],
    #     #                     start_time=exch_data.get('start_time', '?'),
    #     #                     end_time=exch_data.get('end_time', '?'),
    #     #                     duration=self.agg_exch_x1[topVer]['nodes'][node_idx],
    #     #                 ))
    #     #
    #     # with open(join(self.config['rt']['test_dir'], 'exchange-times-x2.csv'), 'w') as dump_exch_file:
    #     #     for topVer, node_data in exch_by_topVer.items():
    #     #         dump_exch_file.write(
    #     #             '{topVer},{minor_topVer},{min_start_time},{max_end_time},{duration},{duration2}\n'.format(
    #     #                 topVer=int(topVer / 10000),
    #     #                 minor_topVer=topVer - int(topVer / 10000) * 10000,
    #     #                 min_start_time=self.agg_exch_x2[topVer]['min_start_time'],
    #     #                 max_end_time=self.agg_exch_x2[topVer]['max_end_time'],
    #     #                 duration=self.agg_exch_x2[topVer]['max_duration'],
    #     #                 duration2=self.agg_exch_x1[topVer]['max_duration'],
    #     #             ))
    #     #
    #

    # def _dump_exchange_time(self, max_time_x1, max_time_x2, msg, num_caches=None, db_size=None, num_partitions=1024):
    #     pass
    #     # run_id = basename(self.config['suite_var_dir'])
        # method_id = self.config['rt']['test_method']
        # result_file_name = join(
        #     self.config['var_dir'],
        #     run_id,
        #     run_id + '-' + method_id + '.csv'
        # )
        #
        # if num_caches is None or db_size is None:
        #     cache_names = self.ignite.get_cache_names('')
        #     if num_caches is None:
        #         num_caches = len(cache_names)
        #     if db_size is None:
        #         db_size = self.ignite.get_entries_num(cache_names)
        #
        # with open(result_file_name, 'a') as f:
        #     f.write(
        #         "{gg_version},{method_id},\"{msg}\",{run_id},{result1},{result2},{disco},{num_nodes},{num_caches},{num_partitions},{db_size}\n".format(
        #             run_id=run_id,
        #             msg=msg,
        #             disco='ZookeeperDiscoverySpi' if self.get_context_variable(
        #                 'zookeeper_enabled') else 'TcpDiscoverySpi',
        #             method_id=method_id,
        #             num_nodes=len(self.ignite.get_all_default_nodes()),
        #             num_caches=num_caches,
        #             num_partitions=num_partitions,
        #             result1=max_time_x1,
        #             result2=max_time_x2,
        #             db_size=db_size,
        #             gg_version=self.config['artifacts']['ignite']['gridgain_version'],
        #         )
        #     )
        #     f.flush()

    def _prepare_before_test(self, ignite, tx_loading, custom_event_name='test'):
        self.last_top = ignite.last_topology_snapshot()
        self.last_topVer = max([_['ver'] for _ in self.last_top])
        print_blue("Last topology version before %s: %d" % (custom_event_name, self.last_topVer))
        tx_loading.metrics_thread.add_custom_event(custom_event_name)

    def _wait_exchange_finished(self, ignite, major_topVer, minor_topVer=0, max_tries=100, sleep_between_tries=5):
        n_tries = 0
        exchange_finished = False
        n_servers = None
        n_expected_servers = len(ignite.get_alive_default_nodes())
        while not exchange_finished and n_tries < max_tries:
            if n_tries > 0:
                sleep(sleep_between_tries)
            log_put(
                "Waiting for exchange topVer=%s to complete on: %s/%s servers, timeout %s/%s sec" %
                (
                    major_topVer,
                    '?' if n_servers is None else str(n_servers),
                    n_expected_servers,
                    str(n_tries * sleep_between_tries),
                    str(max_tries * sleep_between_tries),
                )
            )
            stdout.flush()
            self.exchanges = self._get_exchanges(ignite)
            exchange_finished, n_servers = self.exchanges.is_exchange_finished(
                major_topVer, minor_topVer, n_expected_servers)
            n_tries += 1
        log_print('')

    def _get_exchanges(self, ignite):
        return ExchangesCollection.get_exchanges_from_logs(ignite)

    def _get_new_top_after_test(self, ignite, custom_event_name=None):
        self.new_top = ignite.last_topology_snapshot()
        self.new_topVer = max([_['ver'] for _ in self.new_top])
        if custom_event_name is not None:
            print_blue("New topology version after %s: %d" % (custom_event_name, self.new_topVer))
        return self.new_topVer

    def _measurements_after_test(self, custom_event_name='test', skip_exch=0, skip_minor_exch=-1, max_tries=100,
                                 sleep_between_tries=10, num_partitions=1024):

        exch_maj_topVer = self.new_topVer
        exch_min_topVer = 0
        x1_time = self.exchanges.get_exchange_x1_time(exch_maj_topVer, exch_min_topVer)
        x2_time = self.exchanges.get_exchange_x2_time(exch_maj_topVer, exch_min_topVer)
        print_red(
            "Exchange [%d, %d] during %s: %d msec, %d msec" % (
                exch_maj_topVer, exch_min_topVer, custom_event_name, x1_time, x2_time
            )
        )

        return x1_time, x2_time

    @test_case_id(124591)
    @attr('server')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_1_server_leave(self):
        res = ScenarioRunner(self, *self.scenarios['PME 1 server non-coordinator']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Server Leave')

    @test_case_id(124592)
    @attr('server')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_1_server_join(self):
        res = ScenarioRunner(self, *self.scenarios['PME 1 server non-coordinator']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Server Join')

    @test_case_id(156956)
    # @attr('server')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_1_server_coordinator_leave(self):
        res = ScenarioRunner(self, *self.scenarios['PME 1 server coordinator']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Server Leave')

    @test_case_id(124595)
    @attr('server')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_5_server_leave(self):
        res = ScenarioRunner(self, *self.scenarios['PME 5 server']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Server Leave')

    @test_case_id(124596)
    @attr('server')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_5_server_join(self):
        res = ScenarioRunner(self, *self.scenarios['PME 5 server']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Server Join')

    @test_case_id(124589)
    @attr('client', '1client')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_1_client_leave(self):
        res = ScenarioRunner(self, *self.scenarios['PME 1 client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Client Leave')

    @test_case_id(124590)
    @attr('client', '1client')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_1_client_join(self):
        res = ScenarioRunner(self, *self.scenarios['PME 1 client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Client Join')

    @test_case_id(124593)
    @attr('client', '5client')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_5_client_leave(self):
        res = ScenarioRunner(self, *self.scenarios['PME 5 client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Client Leave')

    @test_case_id(124594)
    @attr('client', '5client')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_5_client_join(self):
        res = ScenarioRunner(self, *self.scenarios['PME 5 client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Client Join')

    @attr('regress', '64client')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_64_client_leave(self):
        res = ScenarioRunner(self, *self.scenarios['PME 64 client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Client Leave')

    @attr('regress', '64client')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_64_client_join(self):
        res = ScenarioRunner(self, *self.scenarios['PME 64 client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Client Join')

    @test_case_id(124586)
    @attr('activation')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_activate(self):
        res = ScenarioRunner(self, *self.scenarios['PME activate']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Activate')

    @test_case_id(124587)
    @attr('activation')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_deactivate(self):
        res = ScenarioRunner(self, *self.scenarios['PME activate']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Deactivate')

    @test_case_id(124597)
    @attr('caches')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_dynamic_caches_server(self):
        res = ScenarioRunner(self, *self.scenarios['PME dynamic caches from server']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Dynamic Cache Start')

    @test_case_id(124598)
    @attr('caches')
    @with_setup(setup_testcase, teardown_testcase)
    def test_pme_bench_dynamic_caches_client(self):
        res = ScenarioRunner(self, *self.scenarios['PME dynamic caches from client']).run()

        ScenarioRunner.validate_probe(res, 'Exchange Dynamic Cache Start')

