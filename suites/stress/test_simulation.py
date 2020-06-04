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

import random
from re import findall

from tiden.apps.ignite import Ignite
from tiden import log_print, sleep
from tiden.case.apptestcase import AppTestCase
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from suites.stress.simulation.pigeon import PigeonSimulation, ClusterEvents, NodeEvents, FailNodeEvent

DEFAULT_ITERATIONS = 100
DEFAULT_NODES = 12 # !!!! number of backups are hardcoded


class TestSimulation(AppTestCase):

    def __init__(self, *args):
        super().__init__(*args)

        self.add_app('ignite')

    def setup(self):
        self.create_app_config_set(Ignite, 'base',
                                   snapshots_enabled=True,
                                   caches_list_file='caches.xml',
                                   logger=False,
                                   logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                       'test_module_dir'],
                                   wal_segment_size=64 * 1024 * 1024,
                                   disabled_cache_configs=False,
                                   zookeeper_enabled=False
                                   )

        super().setup()

    def teardown(self):
        super().teardown()

    def start_ignite_grid(self, activate=False, already_nodes=0, config_set='base', jvm_options=None):
        app = Ignite(self.get_app_by_type('ignite')[0])
        app.set_node_option('*', 'config',
                            Ignite.config_builder.get_config('server', config_set_name=config_set))

        if jvm_options:
            app.set_node_option('*', 'jvm_options', jvm_options)

        artifact_cfg = self.tiden.config['artifacts'][app.name]

        app.reset()
        version = artifact_cfg['ignite_version']
        log_print("Ignite ver. %s, revision %s" % (
            version,
            artifact_cfg['ignite_revision'],
        ))

        app.start_nodes(already_nodes=already_nodes)

        if activate:
            app.cu.activate(activate_on_particular_node=1)

        return version, app

    def get_pigeon(self, ignite, node):
        return {
            ClusterEvents.ROUND_RESTART: lambda: self.round_restart(ignite),
            ClusterEvents.PARALLEL_RESTART: lambda: self.parallel_restart(ignite),
            ClusterEvents.SET_CURRENT_BASELINE: lambda: self.nothing(ignite),
            NodeEvents.START_NODE: lambda: ignite.start_node(node),
            NodeEvents.RESTART_NODE: lambda: self.restart_node(ignite, node),
            FailNodeEvent.KILL_NODE: lambda: self.stop_node(ignite, node),
            FailNodeEvent.DROP_PDS: lambda: self.drop_pds(ignite, node),
            FailNodeEvent.DROP_NETWORK: lambda: self.nothing(ignite),
            FailNodeEvent.REMOVE_FROM_BASELINE: lambda: self.remove_from_baseline(ignite, node),
            None: lambda: self.nothing(ignite)
        }

    def test_sim(self):
        version, ignite = self.start_ignite_grid(True)

        ignite.jmx.start_utility()

        client_config = Ignite.config_builder.get_config('client', config_set_name='base')
        group_names = PiClientIgniteUtils.collect_cache_group_names(
            ignite,
            client_config
        )

        PiClientIgniteUtils.load_data_with_streamer(ignite, client_config, end_key=50)

        server_nodes_num = ignite.get_nodes_num('server')
        sim_engine = PigeonSimulation(server_nodes_num)

        for running_iteration in range(1, DEFAULT_ITERATIONS + 1):
            log_print("Running iteration %s" % running_iteration)

            ev, node = sim_engine.next_event()
            log_print("Evaluating event %s on node %s" % (ev, node))

            pigeon = self.get_pigeon(ignite, node)

            pigeon[ev]()

            ignite.jmx.wait_for_finish_rebalance(120, group_names)

            self.verify_cluster(ignite)

        ignite.jmx.kill_utility()

    def drop_pds(self, ignite, node):
        ignite.kill_node(node)
        ignite.cleanup_work_dir(node)

    def remove_from_baseline(self, ignite, node):
        ignite.kill_node(node)
        ignite.cu.remove_node_from_baseline(ignite.get_node_consistent_id(node))
        ignite.cleanup_work_dir(node)

    def restart_node(self, ignite, node):
        ignite.kill_node(node)
        sleep(round(random.uniform(0.5, 2.5), 1))
        ignite.start_node(node)

    def parallel_restart(self, ignite):
        # cleanup dead nodes LFS to avoid BLAT errors
        alive_node_id = ignite.get_alive_default_nodes()[0]

        ignite.cu.get_current_topology_version()
        all_nodes = set(findall('ConsistentID=([^,;\n]+)', ignite.cu.latest_utility_output))
        blt_nodes = set(findall('ConsistentID=(.*?), STATE=', ignite.cu.latest_utility_output))
        nblt_nodes = all_nodes - blt_nodes
        log_print(nblt_nodes)

        for node_id in ignite.get_all_default_nodes():
            if node_id not in ignite.get_alive_default_nodes() or ignite.get_node_consistent_id(node_id) in nblt_nodes:
                ignite.cleanup_work_dir(node_id)

        ignite.stop_nodes()
        # start first previously alive node to set BLAT correctly
        ignite.start_node(alive_node_id)

        for node_id in ignite.get_all_default_nodes():
            if node_id == alive_node_id:
                continue

            ignite.start_node(node_id)

        ignite.cu.set_current_topology_as_baseline()

    def round_restart(self, ignite):
        timeout = round(random.uniform(0.5, 1.5), 1)

        for node_id in ignite.get_all_default_nodes():
            ignite.kill_node(node_id)
            ignite.start_node(node_id, skip_topology_check=True)
            sleep(timeout)

        for node_id in ignite.get_all_default_nodes():
            ignite.update_started_node_status(node_id)

        ignite.cu.set_current_topology_as_baseline()

    def verify_cluster(self, ignite):
        ignite.cu.control_utility('--baseline')
        self.verify_no_assertion_errors(ignite)

        activate_failed = False
        log_print('Check that there is no Error in activate logs', color='yellow')
        if 'Error' in ignite.cu.latest_utility_output:
            activate_failed = True
            log_print('Failed!', color='red')

        if 'Error' in ignite.cu.latest_utility_output:
            log_print('Failed! Second try after sleep 60 seconds', color='red')
            sleep(60)

            ignite.cu.control_utility('--baseline')

            if 'Error' in ignite.cu.latest_utility_output or activate_failed:
                log_print('Cluster looks hang.')

                log_print('Checking assertions.')
                self.verify_no_assertion_errors(ignite)
                # assert False, 'Test failed, check logs'

        log_print('Check that there is no AssertionError in logs', color='yellow')
        self.verify_no_assertion_errors(ignite)

    def verify_no_assertion_errors(self, ignite):
        assertion_errors = ignite.find_exception_in_logs(".*java.lang.AssertionError.*")

        # remove assertions from ignite.nodes to prevent massive output
        for node_id in ignite.nodes.keys():
            if 'exception' in ignite.nodes[node_id] and ignite.nodes[node_id]['exception'] != '':
                log_print("AssertionError found on node %s, text: %s" %
                          (node_id, ignite.nodes[node_id]['exception'][:100]),
                          color='red')
                ignite.nodes[node_id]['exception'] = ''

        if assertion_errors != 0:
            for node_id in ignite.get_alive_default_nodes():
                self.util_get_threads_from_jstack(ignite, node_id)

            assert False, "AssertionErrors found in server logs! Count %d" % assertion_errors

    def util_get_threads_from_jstack(self, ignite, node_id, to_find=''):
        """
        Run jstack and find thread id using thread name.
        """
        # Start grid to get thread names

        commands = []
        host = ignite.nodes[node_id]['host']
        pid = ignite.nodes[node_id]['PID']
        test_dir = self.tiden.config['rt']['remote']['test_dir']
        commands.append('jstack %s > %s/thread_dump_%s_%s.txt; cat %s/thread_dump.txt | grep "%s"' %
                        (pid, test_dir, node_id, "FAILED", test_dir, to_find))

        log_print(commands)
        response = self.tiden.ssh.exec_on_host(host, commands)
        log_print(response)
        out = response[host][0]

        log_print('Node locked id = %s' % out)

        return out

    def nothing(self, ignite):
        pass
        # ignite.cu.set_current_topology_as_baseline()

    def stop_node(self, ignite, node):
        ignite.kill_node(node)

