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

from os.path import basename, join
from os import makedirs
from re import findall, match

from tiden import print_red, util_sleep_for_a_while, print_blue, is_enabled
from tiden_gridgain.case.regresstestcase import RegressTestCase
from tiden.apps.zookeeper import Zookeeper


class PmeUtils(RegressTestCase):
    warmup_time = 120

    stabilization_time = 90
    warmup_servers_time = 60
    warmup_clients_time = 60

    last_top = None
    last_topVer = None
    new_top = None
    new_topVer = None
    exchange_finished = False
    exchange_by_topVer = None

    zoo: Zookeeper = None

    def setup(self):
        self.warmup_servers_time = self.config.get('warmup_servers_time', 60)
        self.warmup_clients_time = self.config.get('warmup_clients_time', 60)
        self.warmup_time = self.warmup_clients_time + self.warmup_servers_time
        super().setup()

    @staticmethod
    def _parse_top_ver(s):
        m = findall('[0-9]+', s)
        if m:
            return int(m[0]) * 10000 + int(m[1])
        return 0

    @staticmethod
    def _parse_ignite_log_time(s):
        m = match('([0-9]+):([0-9]+):([0-9]+),([0-9]+)', s)
        if m:
            hours = int(m.group(1))
            mins = int(m.group(2))
            secs = int(m.group(3))
            msecs = int(m.group(4))
            return (((hours * 60) + mins) * 60 + secs) * 1000 + msecs
        return 0

    def _collect_msg(self, msg_key, host_group='client'):
        res = {}
        nodes = self.ignite.get_all_default_nodes() if 'server' in host_group else self.ignite.get_all_common_nodes()
        for node_id in nodes:
            if msg_key in self.ignite.nodes[node_id] and self.ignite.nodes[node_id][msg_key]:
                res[node_id] = self.ignite.nodes[node_id][msg_key]
        return res

    def grep_all_data_from_log(self, host_group, grep_text, regex_match, node_option_name, **kwargs):
        """
        Get data for node logs
        :return:
        """
        commands = {}
        result_order = {}

        if 'default_value' in kwargs:
            default_value = kwargs['default_value']

            for node_idx in self.ignite.nodes.keys():
                self.ignite.nodes[node_idx][node_option_name] = default_value

        if 'server' == host_group:
            node_idx_filter = (
                    self.ignite.get_all_additional_nodes() +
                    self.ignite.get_all_default_nodes()
            )
        elif 'client' == host_group:
            node_idx_filter = (
                    self.ignite.get_all_client_nodes() +
                    self.ignite.get_all_common_nodes()
            )
        elif 'alive_server' == host_group:
            node_idx_filter = (
                    self.ignite.get_alive_additional_nodes() +
                    self.ignite.get_alive_default_nodes()
            )
        elif 'alive_client' == host_group:
            node_idx_filter = (
                    self.ignite.get_alive_client_nodes() +
                    self.ignite.get_alive_common_nodes()
            )
        elif 'alive' == host_group:
            node_idx_filter = (
                self.ignite.get_all_alive_nodes()
            )
        elif '*' == host_group:
            node_idx_filter = (
                self.ignite.get_all_nodes()
            )
        else:
            assert False, "Unknown host group!"

        for node_idx in node_idx_filter:
            if 'log' in self.ignite.nodes[node_idx]:
                node_idx_host = self.ignite.nodes[node_idx]['host']
                if commands.get(node_idx_host) is None:
                    commands[node_idx_host] = []
                    result_order[node_idx_host] = []
                commands[node_idx_host].append('grep -E "%s" %s' % (grep_text, self.ignite.nodes[node_idx]['log']))
                result_order[node_idx_host].append(node_idx)
            else:
                print_red('There is no log for node %s' % node_idx)
        results = self.ssh.exec(commands)

        for host in results.keys():
            for res_node_idx in range(0, len(results[host])):
                m = findall(regex_match, results[host][res_node_idx])
                if m:
                    # val = m
                    # if kwargs.get('force_type') == 'int':
                    #     val = int(val)
                    self.ignite.nodes[result_order[host][res_node_idx]][node_option_name] = m

        return self._collect_msg(node_option_name, host_group)

    def _get_last_exchange_time(self):
        exchange_finished = True
        start_exch = self.grep_all_data_from_log(
            'alive_server',
            'Started exchange init',
            '\[([0-9,:]+)\]\[INFO\].*\[topVer=AffinityTopologyVersion \[(topVer=[0-9]+, minorTopVer=[0-9]+\])',
            'started_exchange_init',
        )
        finish_exch = self.grep_all_data_from_log(
            'alive_server',
            'Finish exchange future',
            '\[([0-9,:]+)\]\[INFO\].*resVer=AffinityTopologyVersion \[(topVer=[0-9]+, minorTopVer=[0-9]+\])',
            'finish_exchange_future',
        )

        max_time = None

        exch_by_topVer = {}

        lastTopVer = 0

        # collect 'Started exchange init' messages
        for node_idx, node_result in start_exch.items():
            for result in node_result:
                topVer = self._parse_top_ver(result[1])
                start_time = self._parse_ignite_log_time(result[0])
                if topVer not in exch_by_topVer.keys():
                    exch_by_topVer[topVer] = {}
                exch_by_topVer[topVer][node_idx] = {
                    'start_time': start_time,
                }
                if topVer > lastTopVer:
                    lastTopVer = topVer

        # collect 'Finished exchange init messages'
        for node_idx, node_result in finish_exch.items():
            for result in node_result:
                topVer = self._parse_top_ver(result[1])
                end_time = self._parse_ignite_log_time(result[0])
                if topVer not in exch_by_topVer.keys():
                    exch_by_topVer[topVer] = {}
                if node_idx not in exch_by_topVer[topVer].keys():
                    exch_by_topVer[topVer][node_idx] = {}
                exch_by_topVer[topVer][node_idx]['end_time'] = end_time

        # calculate aggregated 'x1' times
        self.agg_exch_x1 = {}
        for topVer, node_data in exch_by_topVer.items():
            self.agg_exch_x1[topVer] = {
                'nodes': {},
                'max_duration': -1
            }
            for node_idx, exch_data in node_data.items():
                duration = -1
                if exch_data.get('start_time', 0) != 0 and exch_data.get('end_time', 0) != 0:
                    duration = exch_data['end_time'] - exch_data['start_time']

                if duration < 0:
                    exchange_finished = False

                self.agg_exch_x1[topVer]['nodes'][node_idx] = duration

                if duration > self.agg_exch_x1[topVer]['max_duration']:
                    self.agg_exch_x1[topVer]['max_duration'] = duration

        # calculate aggregated 'x2' times
        self.agg_exch_x2 = {}
        for topVer, node_data in exch_by_topVer.items():
            self.agg_exch_x2[topVer] = {}

            min_start_time = None
            max_end_time = None
            duration = -1
            for node_idx, exch_data in node_data.items():
                if exch_data.get('start_time', 0) != 0:
                    if min_start_time is None or exch_data['start_time'] < min_start_time:
                        min_start_time = exch_data['start_time']
                if exch_data.get('end_time', 0) != 0:
                    if max_end_time is None or exch_data['end_time'] > max_end_time:
                        max_end_time = exch_data['end_time']
            if min_start_time is not None and max_end_time is not None:
                duration = max_end_time - min_start_time

            if duration < 0:
                exchange_finished = False
            self.agg_exch_x2[topVer]['min_start_time'] = min_start_time if min_start_time is not None else '?'
            self.agg_exch_x2[topVer]['max_end_time'] = max_end_time if max_end_time is not None else '?'
            self.agg_exch_x2[topVer]['max_duration'] = duration

        # dump results to x1.csv
        makedirs(self.config['rt']['test_dir'], exist_ok=True)
        with open(join(self.config['rt']['test_dir'], 'exchange-times-x1.csv'), 'w') as dump_exch_file:
            for topVer, node_data in exch_by_topVer.items():
                for node_idx, exch_data in node_data.items():
                    dump_exch_file.write(
                        '{topVer},{minor_topVer},{node_idx},{node_ip},{start_time},{end_time},{duration}\n'.format(
                            topVer=int(topVer / 10000),
                            minor_topVer=topVer - int(topVer / 10000) * 10000,
                            node_idx=node_idx,
                            node_ip=self.ignite.nodes[node_idx]['host'],
                            start_time=exch_data.get('start_time', '?'),
                            end_time=exch_data.get('end_time', '?'),
                            duration=self.agg_exch_x1[topVer]['nodes'][node_idx],
                        ))

        with open(join(self.config['rt']['test_dir'], 'exchange-times-x2.csv'), 'w') as dump_exch_file:
            for topVer, node_data in exch_by_topVer.items():
                dump_exch_file.write(
                    '{topVer},{minor_topVer},{min_start_time},{max_end_time},{duration},{duration2}\n'.format(
                        topVer=int(topVer / 10000),
                        minor_topVer=topVer - int(topVer / 10000) * 10000,
                        min_start_time=self.agg_exch_x2[topVer]['min_start_time'],
                        max_end_time=self.agg_exch_x2[topVer]['max_end_time'],
                        duration=self.agg_exch_x2[topVer]['max_duration'],
                        duration2=self.agg_exch_x1[topVer]['max_duration'],
                    ))

        # now
        for node_idx, result in start_exch.items():
            if node_idx not in finish_exch.keys():
                print_red('Not found "Finish exchange future" on node %s' % node_idx)
                continue

            start_time = 0
            end_time = 0
            for result in start_exch[node_idx]:
                topVer = self._parse_top_ver(result[1])
                if topVer == lastTopVer:
                    start_time = self._parse_ignite_log_time(result[0])
                    break

            for result in finish_exch[node_idx]:
                topVer = self._parse_top_ver(result[1])
                if topVer == lastTopVer:
                    end_time = self._parse_ignite_log_time(result[0])
                    break

            time_diff = end_time - start_time

            if max_time is None or time_diff > max_time:
                max_time = time_diff

        self.exchange_finished = exchange_finished
        self.exchange_by_topVer = exch_by_topVer
        return max_time

    def _dump_exchange_time(self, max_time_x1, max_time_x2, msg, num_caches=None, db_size=None, num_partitions=1024):
        run_id = basename(self.config['suite_var_dir'])
        method_id = self.config['rt']['test_method']
        result_file_name = join(
            self.config['var_dir'],
            run_id,
            run_id + '-' + method_id + '.csv'
        )

        if num_caches is None or db_size is None:
            cache_names = self.ignite.get_cache_names('')
            if num_caches is None:
                num_caches = len(cache_names)
            if db_size is None:
                db_size = self.ignite.get_entries_num(cache_names)

        with open(result_file_name, 'a') as f:
            f.write(
                "{gg_version},{method_id},\"{msg}\",{run_id},{result1},{result2},{disco},{num_nodes},{num_caches},{num_partitions},{db_size}\n".format(
                    run_id=run_id,
                    msg=msg,
                    disco='ZookeeperDiscoverySpi' if self.get_context_variable(
                        'zookeeper_enabled') else 'TcpDiscoverySpi',
                    method_id=method_id,
                    num_nodes=len(self.ignite.get_all_default_nodes()),
                    num_caches=num_caches,
                    num_partitions=num_partitions,
                    result1=max_time_x1,
                    result2=max_time_x2,
                    db_size=db_size,
                    gg_version=self.config['artifacts']['ignite']['gridgain_version'],
                )
            )
            f.flush()

    def setup_test(self, **kwargs):
        self.ignite.set_node_option('*', 'config', self.get_server_config())

        extended_jvm_options = []
        if 'exchange_history_size' in kwargs:
            extended_jvm_options.extend([
                '-DIGNITE_EXCHANGE_HISTORY_SIZE=%d' % int(kwargs['exchange_history_size'])
            ])

        if extended_jvm_options:
            self.jvm_options = self.get_extended_jvm_options(extended_jvm_options)
        else:
            self.jvm_options = self.get_default_jvm_options()

        if is_enabled(self.config.get('enable_jfr_and_gc_logs', False)):
            for node_id in self.ignite.nodes.keys():
                add_jfr = [
                        "-Xloggc:%s/gc-%s.log" % (self.config['rt']['remote']['test_dir'], node_id),
                        "-XX:+PrintGCDetails",
                        "-verbose:gc",
                        "-XX:+UseParNewGC",
                        "-XX:+UseConcMarkSweepGC",
                        "-XX:+PrintGCDateStamps"
                ]
                current_jvm_options = self.jvm_options + add_jfr
                self.ignite.set_node_option(node_id, 'jvm_options', current_jvm_options)
        else:
            self.ignite.set_node_option('*', 'jvm_options', self.jvm_options)
        self.util_copy_piclient_and_test_tools_models_to_libs()

        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.start()

        self.ignite.jmx.start_utility()

    def teardown_test(self):
        super().teardown_test()
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop()
        self.ignite.jmx.kill_utility()

    def teardown_test_hard(self):
        super().teardown_test_hard()
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop()
        self.ignite.jmx.kill_utility()

    def _prepare_before_test(self, tx_loading, custom_event_name='test'):
        util_sleep_for_a_while(self.warmup_time)
        self.last_top = self.ignite.ignite_srvs.last_topology_snapshot()
        self.last_topVer = max([_['ver'] for _ in self.last_top])
        print_blue("Last topology version before %s: %d" % (custom_event_name, self.last_topVer))
        tx_loading.metrics_thread.add_custom_event(custom_event_name)

    def _measurements_after_test(self, custom_event_name='test', skip_exch=0, skip_minor_exch=-1, max_tries=100,
                                 sleep_between_tries=10, num_partitions=1024):
        util_sleep_for_a_while(self.stabilization_time)
        n_tries = 0
        max_time = -1
        while n_tries < max_tries:
            n_tries += 1
            max_time = self._get_last_exchange_time()
            if self.exchange_finished:
                break
            util_sleep_for_a_while(sleep_between_tries)

        self.new_top = self.ignite.ignite_srvs.last_topology_snapshot()
        self.new_topVer = max([_['ver'] for _ in self.new_top])
        print_blue("New topology version after %s: %d" % (custom_event_name, self.new_topVer))

        if max_time > 0:
            for topVer in range(self.last_topVer + skip_exch, self.new_topVer + 1):
                for exch_topVer, exch_data in self.agg_exch_x1.items():
                    exch_maj_topVer = int(exch_topVer / 10000)
                    exch_min_topVer = exch_topVer - exch_maj_topVer * 10000
                    if exch_maj_topVer == topVer and exch_min_topVer > skip_minor_exch:
                        x1_time = self.agg_exch_x1[exch_topVer]['max_duration']
                        x2_time = self.agg_exch_x2[exch_topVer]['max_duration']
                        self._dump_exchange_time(x1_time, x2_time, "%s [%d, %d]" % (
                            custom_event_name, exch_maj_topVer, exch_min_topVer), num_partitions=num_partitions)
                        print_red(
                            "Exchange [%d, %d] during %s: %d msec, %d msec" % (
                                exch_maj_topVer, exch_min_topVer, custom_event_name, x1_time, x2_time
                            )
                        )

