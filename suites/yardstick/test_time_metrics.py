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

from time import sleep, time
from tiden_gridgain.case.yardsticktestcase import YardstickTestCase
from tiden.util import log_print, attr, human_size
from tiden.apps.netstat import Netstat


class TestTimeMetrics (YardstickTestCase):

    attempts = 3
    preloading_size = 100000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.netstat = Netstat()

    def setup(self):
        super().setup()

    def teardown(self):
        pass

    def load_10gb(self, ignite_name, **kwargs):
        start_key = 1
        load_size = 100000
        expected_size = 10737418240
        expected_size = 3000000000
        current_size = 0
        caches = self.ignite[ignite_name].get_cache_names('cache')
        if kwargs.get('clear'):
            self.ignite[ignite_name].set_snapshot_timeout(600)
            self.start_simple_clients(
                ignite_name,
                '%s/%s' % (self.config['rt']['remote']['test_module_dir'], self.get_client_config()),
                '-operation=clear',
                jvm_options=['-DCACHE_CONFIG=rebalance_time-cache.xml', '-Xmx8g', '-Xms8g']
            )
            self.ignite[ignite_name].wait_for_topology_snapshot(
                None,
                0,
                ''
            )
        self.ignite[ignite_name].set_snapshot_timeout(180)
        while True:
            operations = "-cache=cache_ -operation=streamer:%s,%s" % (start_key, load_size + start_key)
            self.start_simple_clients(
                ignite_name,
                '%s/%s' % (self.config['rt']['remote']['test_module_dir'], self.get_client_config()),
                operations,
                jvm_options=['-DCACHE_CONFIG=rebalance_time-cache.xml', '-Xmx8g',  '-Xms8g']
            )
            self.ignite[ignite_name].wait_for_topology_snapshot(
                None,
                0,
                ''
            )
            current_size = self.ssh.dirsize(
                "%s/%s.server.1/work/db/node1/cacheGroup*" % (
                    self.config['rt']['remote']['test_module_dir'], ignite_name),
                [self.config['environment']['server_hosts'][0]]
            )
            entries_num = self.ignite[ignite_name].get_entries_num(caches)
            if current_size >= expected_size or entries_num >= 11200000:
                log_print("Cache directory size reached %s >= %s (%s entries)" % (
                    human_size(current_size),
                    human_size(expected_size),
                    entries_num),
                )
                break
            else:
                log_print("Cache directory size %s, (%s entries)" % (
                    human_size(current_size),
                    entries_num
                    )
                )
                start_key += load_size
        return current_size

    @attr('rebalance_time')
    def test_rebalance_time(self):
        for attempt in range(1, self.attempts+1):
            for ignite_name in self.ignite.keys():
                self.ignite[ignite_name].set_node_option(
                    '*', 'jvm_options',
                    ['-DCACHE_CONFIG=rebalance_time-cache.xml', '-DWAL_MODE=LOG_ONLY']
                )
                self.ignite[ignite_name].set_node_option('*', 'config', self.get_server_config())
                log_print("Artifact %s. Ignite ver. %s, revision %s: attempt %s/%s" % (
                    ignite_name,
                    self.config['artifacts'][ignite_name]['ignite_version'],
                    self.config['artifacts'][ignite_name]['ignite_revision'],
                    attempt,
                    self.attempts)
                )
                # Kill java process on all hosts
                self.ssh.killall('java')
                # Set grid name, it will be used for node logs
                self.ignite[ignite_name].set_grid_name("%s-%s" % (ignite_name, attempt))
                # Start grid
                self.start_grid(ignite_name)
                nodes_cnt = self.ignite[ignite_name].get_nodes_num('server')
                node_idx = self.ignite[ignite_name].get_last_node_id('server')
                current_size = self.load_10gb(ignite_name)
                self.ignite[ignite_name].kill_node(node_idx)
                self.ignite[ignite_name].wait_for_topology_snapshot(
                    nodes_cnt-1,
                    0,
                    ''
                )
                self.save_lfs(ignite_name, 'before')
                host = self.ignite[ignite_name].nodes[node_idx]['host']
                start_rcvd_bytes = self.netstat.network()[host]['total']['RX']['bytes']
                start_sent_bytes = self.netstat.network()[host]['total']['TX']['bytes']
                #self.ignite[ignite_name].kill_node(3)
                #self.ignite[ignite_name].wait_for_topology_snapshot(server_num-1, 0, 90)
                cmd = dict()
                cmd[host] = [
                    'cd %s; rm -rf %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        ('%s.server.%s/work/db/* %s.server.%s/work/wal/* '
                         '%s.server.%s/work/binary_meta/* %s.server.%s/work/marshaller/*' % (
                             ignite_name,
                             node_idx,
                             ignite_name,
                             node_idx,
                             ignite_name,
                             node_idx,
                             ignite_name,
                             node_idx)),
                    )
                ]
                log_print(cmd[host])
                sleep(5)
                log_print('Remove LFS for node %s' % node_idx)
                node_restarted = time()
                self.ssh.exec(cmd)
                self.save_lfs(ignite_name, 'deleted')
                # self.hoststat.start(
                #     'dstat',
                #     'mpstat',
                #     dir=self.config['rt']['remote']['test_dir'],
                #     tag=ignite_name
                # )
                self.ignite[ignite_name].start_node(node_idx)
                self.ignite[ignite_name].wait_for_finished_rebalance(
                    nodes_cnt,
                    'cache_group_1',
                    1200
                )
                finish_rcvd_bytes = self.netstat.network()[host]['total']['RX']['bytes']
                finish_sent_bytes = self.netstat.network()[host]['total']['TX']['bytes']
                log_print('Rebalanced %s in %s sec, TX/RX %s/%s, %s/sec' % (
                        human_size(current_size),
                        int(time()-node_restarted),
                        human_size(finish_sent_bytes-start_sent_bytes),
                        human_size(finish_rcvd_bytes-start_rcvd_bytes),
                        human_size(int(current_size/int(time()-node_restarted)))
                    )
                )
                self.stop_grid(ignite_name)
                # self.hoststat.stop()
                self.save_lfs(ignite_name, 'after')
            break

    @attr('load')
    def test_load_with_rebalance(self):
        for attempt in range(1, self.attempts+1):
            for ignite_name in self.ignite.keys():
                self.ignite[ignite_name].set_node_option(
                    '*', 'jvm_options',
                    ['-DCACHE_CONFIG=load_with_rebalance-cache.xml', '-DWAL_MODE=LOG_ONLY']
                )
                self.ignite[ignite_name].set_node_option('*', 'config', self.get_server_config())
                log_print("Artifact %s. Ignite ver. %s, revision %s: attempt %s/%s" % (
                    ignite_name,
                    self.config['artifacts'][ignite_name]['ignite_version'],
                    self.config['artifacts'][ignite_name]['ignite_revision'],
                    attempt,
                    self.attempts)
                )
                # Kill java process on all hosts
                self.ssh.killall('java')
                # Set grid name, it will be used for node logs
                self.ignite[ignite_name].set_grid_name("%s-%s" % (ignite_name, attempt))
                # Start grid
                self.start_grid(ignite_name)
                nodes_cnt = self.ignite[ignite_name].get_nodes_num('server')
                node_idx = self.ignite[ignite_name].get_last_node_id('server')
                self.ignite[ignite_name].kill_node(node_idx)
                self.ignite[ignite_name].wait_for_topology_snapshot(
                    nodes_cnt-1,
                    0,
                    ''
                )
                current_size = self.load_10gb(ignite_name)
                # Start benchmark drivers in background
                self.start_benchmark_drivers(
                    len(self.config['environment']['client_hosts']),
                    ignite_name,
                    'IgnitePutAllBenchmark',
                    'atomic-put-all-bs-100-full_sync',
                    1,
                    ['-DCACHE_CONFIG=load_with_rebalance-cache.xml'],
                    {
                        '-nn': 1,
                        '-w': 60,
                        '-d': 60,
                        '-t': 64,
                        '-b': 2,
                        '-bs': 100,
                        '-sm': 'FULL_SYNC',
                        '--client': ''
                    }
                )
                self.wait_for_drivers(0, 150)
                # Store default performance
                base_result = self.collect_per_run()
                base_result_dir = list(base_result[ignite_name].keys())[0]
                base_client_throughput = base_result[ignite_name][base_result_dir]['client_throughput']
                # log_print("Kill node 3")
                # server_num = self.ignite[ignite_name].get_nodes_num('server')
                host = self.ignite[ignite_name].nodes[node_idx]['host']
                start_rcvd_bytes = self.netstat.network()[host]['total']['RX']['bytes']
                start_sent_bytes = self.netstat.network()[host]['total']['TX']['bytes']
                # self.ignite[ignite_name].kill_node(3)
                # self.ignite[ignite_name].wait_for_topology_snapshot(server_num-1, 0, 90)
                cmd = dict()
                cmd[host] = [
                    'cd %s; rm -rf %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        ('%s.server.%s/work/db/* %s.server.%s/work/wal/* '
                         '%s.server.%s/work/binary_meta/* %s.server.%s/work/marshaller/*' % (
                            ignite_name,
                            node_idx,
                            ignite_name,
                            node_idx,
                            ignite_name,
                            node_idx,
                            ignite_name,
                            node_idx)),
                    )
                ]
                sleep(5)
                log_print('Remove LFS for node %s' % node_idx)
                node_restarted = time()
                self.ssh.exec(cmd)
                # self.hoststat.start(
                #     'dstat',
                #     'mpstat',
                #     dir=self.config['rt']['remote']['test_dir'],
                #     tag=ignite_name
                # )
                self.ignite[ignite_name].start_node(node_idx)
                sleep(10)
                # Start benchmark drivers in background
                self.start_benchmark_drivers(
                    len(self.config['environment']['client_hosts']),
                    ignite_name,
                    'IgnitePutAllBenchmark',
                    'atomic-put-all-bs-100-full_sync',
                    1,
                    ['-DCACHE_CONFIG=load_with_rebalance-cache.xml'],
                    {
                        '-nn': 1,
                        '-w': 60,
                        '-d': 120,
                        '-t': 64,
                        '-b': 2,
                        '-bs': 100,
                        '-sm': 'FULL_SYNC',
                        '--client': ''
                    }
                )
                self.ignite[ignite_name].wait_for_finished_rebalance(
                    nodes_cnt,
                    'cache_group_1',
                    1200
                )
                self.wait_for_drivers(0, 180)
                finish_rcvd_bytes = self.netstat.network()[host]['total']['RX']['bytes']
                finish_sent_bytes = self.netstat.network()[host]['total']['TX']['bytes']
                log_print('Rebalanced %s in %s sec, TX/RX %s/%s, %s/sec' % (
                        human_size(current_size),
                        int(time()-node_restarted),
                        human_size(finish_sent_bytes-start_sent_bytes),
                        human_size(finish_rcvd_bytes-start_rcvd_bytes),
                        human_size(int(current_size/int(time()-node_restarted)))
                    )
                )
                test_results = self.collect_per_run()
                # remove base results
                del test_results[ignite_name][base_result_dir]
                test_result_dir = list(test_results[ignite_name].keys())[0]
                test_client_throughput = test_results[ignite_name][test_result_dir]['client_throughput']
                log_print("%s: base per-client throughput: %.2f op/sec" % (ignite_name, base_client_throughput))
                log_print("%s: rebalance per-client throughput: %.2f op/sec" % (ignite_name, test_client_throughput))
                self.stop_grid(ignite_name)
                # self.hoststat.stop()
            break

    @attr('rebalance_time')
    def test_rebalance_time_with_evict(self):
        for attempt in range(1, self.attempts+1):
            for ignite_name in self.ignite.keys():
                self.ignite[ignite_name].set_node_option(
                    '*', 'jvm_options',
                    ['-DCACHE_CONFIG=rebalance_time-cache.xml', '-DWAL_MODE=LOG_ONLY']
                )
                self.ignite[ignite_name].set_node_option('*', 'config', self.get_server_config())
                log_print("Artifact %s. Ignite ver. %s, revision %s: attempt %s/%s" % (
                    ignite_name,
                    self.config['artifacts'][ignite_name]['ignite_version'],
                    self.config['artifacts'][ignite_name]['ignite_revision'],
                    attempt,
                    self.attempts)
                )
                # Kill java process on all hosts
                self.ssh.killall('java')
                # Set grid name, it will be used for node logs
                self.ignite[ignite_name].set_grid_name("%s-%s" % (ignite_name, attempt))
                # Start grid
                self.start_grid(ignite_name)
                nodes_cnt = self.ignite[ignite_name].get_nodes_num('server')
                node_idx = self.ignite[ignite_name].get_last_node_id('server')
                current_size = self.load_10gb(ignite_name)
                self.ignite[ignite_name].kill_node(node_idx)
                self.ignite[ignite_name].wait_for_topology_snapshot(
                    nodes_cnt-1,
                    0,
                    ''
                )
                #self.save_lfs(ignite_name, 'load_1')
                current_size = self.load_10gb(ignite_name, clear=True)
                host = self.ignite[ignite_name].nodes[node_idx]['host']
                start_rcvd_bytes = self.netstat.network()[host]['total']['RX']['bytes']
                start_sent_bytes = self.netstat.network()[host]['total']['TX']['bytes']
                #self.ignite[ignite_name].kill_node(3)
                #self.ignite[ignite_name].wait_for_topology_snapshot(server_num-1, 0, 90)
                cmd = dict()
                node_restarted = time()
                self.ssh.exec(cmd)
                #self.save_lfs(ignite_name, 'load_2')
                # self.hoststat.start(
                #     'dstat',
                #     'mpstat',
                #     dir=self.config['rt']['remote']['test_dir'],
                #     tag=ignite_name
                # )
                self.ignite[ignite_name].start_node(node_idx)
                self.ignite[ignite_name].wait_for_finished_rebalance(
                    nodes_cnt,
                    'cache_group_1',
                    1200
                )
                finish_rcvd_bytes = self.netstat.network()[host]['total']['RX']['bytes']
                finish_sent_bytes = self.netstat.network()[host]['total']['TX']['bytes']
                log_print('Rebalanced %s in %s sec, TX/RX %s/%s, %s/sec' % (
                        human_size(current_size),
                        int(time()-node_restarted),
                        human_size(finish_sent_bytes-start_sent_bytes),
                        human_size(finish_rcvd_bytes-start_rcvd_bytes),
                        human_size(int(current_size/int(time()-node_restarted)))
                    )
                )
                self.stop_grid(ignite_name)
                # self.hoststat.stop()
                #self.save_lfs(ignite_name, 'after')
            break

