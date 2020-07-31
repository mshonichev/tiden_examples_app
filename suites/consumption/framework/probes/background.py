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

import os
import re
import traceback
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor

from pt import log_print, sleep, SshPool, TidenException
from pt.utilities import JmxUtility
from suites.consumption.framework.utils import get_current_time
from suites.consumption.framework.probes.abstract import AbstractProbe


class BackgroundResourceProbe(AbstractProbe):
    """
    Background probe: cpu, memory, heap and JMX metrics

    TODO Refactor this
    """

    jmx = None
    executor = None
    futures = {}
    interrupt = False

    def __init__(self, probe_config):
        super().__init__(probe_config)

        self.jmx = None

    def validate_config(self):
        pass

    def start(self):
        """
        Start background probe (Separate process created)
        """
        super().start()

        # create result for current version
        self.results[self.version] = {}

        # remove previous lock file
        if os.path.isfile('lock'):
            os.remove('lock')

        # create lock file
        with open('lock', 'w') as f:
            pass

        # Create PoolExecutor and submit bs_probe in it
        # Can be launched in debug mode - with local ThreadPool. In default separate process will be launched.
        #
        # Using in default mode (with ProcessPoolExecutor):
        # Only default types can be passed into submit() as args (dict, list, set, int, string)
        # But there is no chance to pass class instance into separate process (e.g. current SshPool cannot be passed)
        #
        # Also self.watch_thread() method should be static (because self arg is passed by default in instance method)

        if 'debug_background_probe' in self.test_class.config:
            self.executor = ThreadPoolExecutor(max_workers=len(self.probe_config['probes']))
        else:
            self.executor = ProcessPoolExecutor(max_workers=len(self.probe_config['probes']))

        for probe_name in self.probe_config['probes']:
            if probe_name == 'cpu_mem':
                self.futures['cpu_mem'] = self.executor.submit(
                    self.cpu_mem_thread_method,
                    self.ignite.get_alive_default_nodes(),
                    self.ignite.nodes,
                    self.test_class.config['ssh']
                )
            elif probe_name == 'heap':
                self.futures['heap'] = self.executor.submit(
                    self.heap_thread_method,
                    self.ignite.get_alive_default_nodes(),
                    self.ignite.nodes,
                    self.test_class.config['ssh']
                )
            elif probe_name == 'jmx':
                assert 'jmx_metrics' in self.probe_config, 'jmx_metrics should be defined if jmx background probe used'

                # start jmx utility wo gateway
                self.jmx = JmxUtility(self.ignite, start_gateway=False)
                self.jmx.start_utility()
                jmx_node_id = int(self.jmx.node_id)

                self.futures['jmx'] = self.executor.submit(
                    self.jmx_thread_method,
                    self.ignite.get_alive_default_nodes(),
                    self.ignite.nodes,
                    jmx_node_id,
                    self.probe_config['jmx_metrics']
                )
            else:
                raise TidenException('Unknown probe name %s' % probe_name)

    def stop(self, **kwargs):
        """
        Stop background probe

        NB! Do not move fut.results vs jmx node killing. This may lead to py4j errors in log.
        """
        super().stop()

        # write 1 into lock file to stop all probes
        with open('lock', 'w') as f:
            f.write('1')

        # get result from process threads (this code guarantee that jmx is not usable anymore)
        sleep(10)

        for name, fut in self.futures.items():
            if fut.exception():
                log_print("Failed to evaluate probe %s, %s" % (name, fut.exception()),
                          color='red')
            else:
                self.results[self.version][name] = fut.result()

        # kill utility using ignite api (jmx gateway already down in probe thread)
        if self.jmx:
            self.jmx.kill_utility()
            # self.ignite.kill_node(self.jmx.node_id)

        # shutdown executor
        self.executor.shutdown(wait=True)

        # remove lock file
        os.remove('lock')

    def is_passed(self, **kwargs):
        """
        Just print results

        :param kwargs: None
        :return: always True
        """
        return True

    @staticmethod
    def cpu_mem_thread_method(nodes_to_monitor, ignite_nodes, ssh_config, timeout=5):
        """
        probe thread that collects cpu,mem from nodes_to_monitor

        Command to collect: "ps -p PID -o pid,%%cpu,%%mem"

        :param nodes_to_monitor: nodes that we want to monitor (server nodes in this example)
        :param ignite_nodes: nodes from current Ignite app (need to get PID)
        :param ssh_config: config['ssh_config'] from tiden config (Need to initialize SshPool)
        :param timeout: timeout between data collect
        :return: collected results ('default' python type)
        """
        ssh = SshPool(ssh_config)
        ssh.connect()

        cpu_mem_result = {}

        with open('lock', 'r') as f:
            while True:
                if f.read(1) == '1':
                    log_print("Background probe CPU has been interrupted")
                    break

                commands = {}
                node_ids_to_pid = {}

                for node_ids in nodes_to_monitor:
                    node_ids_to_pid[node_ids] = ignite_nodes[node_ids]['PID']

                for node_idx in nodes_to_monitor:
                    host = ignite_nodes[node_idx]['host']
                    if commands.get(host) is None:
                        commands[host] = [
                            'ps -p %s -o pid,%%cpu,%%mem' % ignite_nodes[node_idx]['PID']
                        ]
                    else:
                        commands[host].append('ps -p %s -o pid,%%cpu,%%mem' % ignite_nodes[node_idx]['PID'])

                results = ssh.exec(commands)

                results_parsed = {}
                for host in results.keys():
                    result = results[host][0]

                    search = re.search('(\d+)\s+?(\d+.?\d?)\s+?(\d+.?\d?)', result)

                    if search:
                        node_id = 0
                        for node_id, pid in node_ids_to_pid.items():
                            if pid == int(search.group(1)):
                                node_id = node_id
                                break

                        results_parsed[node_id] = (float(search.group(2)), float(search.group(3)))
                    else:
                        continue

                cpu_mem_result[get_current_time()] = results_parsed

                sleep(timeout)

        return cpu_mem_result

    @staticmethod
    def heap_thread_method(nodes_to_monitor, ignite_nodes, ssh_config, timeout=5):
        """
        probe thread that collects JVM Heap usage from nodes_to_monitor

        Command to collect: "jcmd PID GC.class_histogram"
        This command prints following text:

        "PID:
        1. JAVA_OBJECT_NUM JAVA_OBJECT_SIZE JAVA_OBJECT_NAME
        ...
        N.
        Total TOTAL_OBJECTS TOTAL_OBJECTS_SIZE"

        So we need to collect PID (to match it to node) and TOTAL_OBJECTS_SIZE from that output.

        :param nodes_to_monitor: nodes that we want to monitor (server nodes in this example)
        :param ignite_nodes: nodes from current Ignite app (need to get PID)
        :param ssh_config: config['ssh_config'] from tiden config (Need to initialize SshPool)
        :return: collected results ('default' python type)
        """
        ssh = SshPool(ssh_config)
        ssh.connect()

        heap_result = {}

        try:
            with open('lock', 'r') as f:
                while True:
                    if f.read(1) == '1':
                        log_print("Background probe HEAP has been interrupted")
                        break

                    commands = {}
                    node_ids_to_pid = {}

                    for node_ids in nodes_to_monitor:
                        node_ids_to_pid[node_ids] = ignite_nodes[node_ids]['PID']

                    for node_idx in nodes_to_monitor:
                        host = ignite_nodes[node_idx]['host']
                        if commands.get(host) is None:
                            commands[host] = [
                                'jcmd %s GC.class_histogram' % ignite_nodes[node_idx]['PID']
                            ]
                        else:
                            commands[host].append(
                                'jcmd %s GC.class_histogram' % ignite_nodes[node_idx]['PID'])

                    results = ssh.exec(commands)

                    results_parsed = {}
                    for host in results.keys():
                        result = results[host][0]

                        findall = re.compile('(\d+):\n|Total\s+\d+\s+(\d+)').findall(result)

                        # findall will return 2d array: [['PID', ''], [''] ['TOTAL_HEAP_USAGE']]
                        # todo maybe there is a better way to get this
                        if findall:
                            node_id = 0
                            for node_id, pid in node_ids_to_pid.items():
                                if pid == int(findall[0][0]):
                                    node_id = node_id
                                    break

                            try:
                                results_parsed[node_id] = (int(findall[1][1]))
                            except Exception:
                                results_parsed[node_id] = 0
                        else:
                            continue

                    heap_result[get_current_time()] = results_parsed

                    sleep(timeout)
        except Exception:
            log_print(traceback.format_exc())

        return heap_result

    @staticmethod
    def jmx_thread_method(nodes_to_monitor, ignite_nodes, jmx_node_id, metrics_to_collect, timeout=5):
        """
        probe thread that collects JMX metrics from specified nodes

        Uses mocked JMXUtility (does not start new instance, just use existing methods)
        We need to pass nothing to create this instance just override nodes, gateway and service

        :param nodes_to_monitor: nodes that we want to monitor (server nodes in this example)
        :param ignite_nodes: nodes from current Ignite app (need to get PID)
        :param jmx_node_id: jmx node id from tiden.ignite.nodes
        :param metrics_to_collect: {'attr': {'grp': 'Group', 'bean': 'Bean', 'attribute': 'Attr'}, ...}
        :param timeout: timeout to collect metrics
        :return: collected results ('default' python type)
        """
        # Close connections and shutdown gateway properly
        jmx_metric = {}

        jmx = None
        try:
            jmx = JmxUtility()
            jmx.initialize_manually(jmx_node_id, ignite_nodes)

            with open('lock', 'r') as f:
                while True:
                    if f.read(1) == '1':
                        log_print("Background probe JMX has been interrupted")
                        break

                    current_time = get_current_time()
                    for node_idx in nodes_to_monitor:
                        if current_time not in jmx_metric:
                            jmx_metric[current_time] = {}

                        if node_idx not in jmx_metric[current_time]:
                            jmx_metric[current_time][node_idx] = {}

                        for name, metric in metrics_to_collect.items():
                            try:
                                string_value = \
                                    jmx.get_attributes(node_idx,
                                                       metric['grp'],
                                                       metric['bean'],
                                                       metric['attribute'],
                                                       )[metric['attribute']]

                                if metric['type'] == 'int':
                                    jmx_metric[current_time][node_idx][name] = int(string_value)
                                else:
                                    jmx_metric[current_time][node_idx][name] = string_value
                            except Exception:
                                jmx_metric[current_time][node_idx][name] = None

                        sleep(timeout)
        except Exception:
            log_print(traceback.format_exc())
        finally:
            # Close connections and shutdown gateway properly
            if jmx:
                jmx.kill_manually()

        return jmx_metric

