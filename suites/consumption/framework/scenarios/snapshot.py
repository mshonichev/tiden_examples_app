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

import re

from tiden.apps.ignite import Ignite
from tiden import log_print, log_put, util_sleep_for_a_while
from tiden_gridgain.piclient.helper.cache_utils import IgniteCache
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from suites.consumption.framework.scenarios.abstract import AbstractScenario
from suites.consumption.framework.utils import get_nodes_directory_size

SNAPSHOT_CONFIG_SET = 'snapshot'


class SnapshotScenario(AbstractScenario):
    """
    snapshot-utility.sh snapshot -type=full

    Execution time provided by utility itself

    Scenario description:
        1. Start cluster
        2. Activate
        3. start probes
        4. for try in times_to_run:
                snapshot-utility.sh snapshot -type=full

            collect time above all operations
        5. Deactivate
        6. stop_probes(execution time data)
        7. Cleanup
    """

    def _validate_config(self):
        super()._validate_config()

        assert self.config.get('times_to_run'), 'There is no "times_to_run" variable in config'
        assert self.config.get('data_size'), 'There is no "data_size" variable in config'

    def run_snapshot(self, artifact_name, snapshot_type):
        """
        Run scenario for defined artifact

        :param artifact_name: name from artifact configuration file
        :param snapshot_type: inc/full
        """
        super().run(artifact_name)

        log_print("Running snapshot benchmark with config: %s" % self.config, color='green')

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        incremental_snapshot = True if snapshot_type == 'inc' else False
        try:
            self.test_class.create_app_config_set(
                Ignite, SNAPSHOT_CONFIG_SET,
                caches_list_file='caches_%s.xml' % SNAPSHOT_CONFIG_SET,
                deploy=True,
                snapshots_enabled=True,
                logger=False,
                wal_segment_size=self.test_class.consumption_config.get('wal_segment_size',
                                                                        64 * 1024 * 1024),
                logger_path='%s/ignite-log4j2.xml' %
                            self.test_class.tiden.config['rt']['remote']['test_module_dir'],
                disabled_cache_configs=False,
                zookeeper_enabled=False,
                checkpoint_read_lock_timeout=self.read_lock_property_value(version),
                # caches related variables
                additional_configs=['caches.tmpl.xml', ],
                part_32=self.config.get('part_32',
                                        32),
                part_64=self.config.get('part_64',
                                        64),
                part_128=self.config.get('part_64',
                                         128),
                # artifact config variables
                **self.artifact_config_variables,
            )

            version, ignite = self.test_class.start_ignite_grid(artifact_name,
                                                                activate=True,
                                                                config_set=SNAPSHOT_CONFIG_SET,
                                                                jvm_options=self.artifact_jvm_properties)

            time_results = list()
            directory_size = list()

            self.start_probes(artifact_name)

            client_config = Ignite.config_builder.get_config('client', config_set_name=SNAPSHOT_CONFIG_SET)
            PiClientIgniteUtils.load_data_with_putall(
                ignite,
                client_config,
                end_key=int(self.config.get('data_size'))
            )

            if incremental_snapshot:
                ignite.su.snapshot_utility('snapshot', '-type=full')

            # default times to run
            # plus warmup times
            # plus rerun times
            warmup_runs, prod_runs = self._get_number_of_runs()

            log_print("Running {} iterations".format(warmup_runs + prod_runs))
            for i in range(0, warmup_runs + prod_runs):
                self.write_time_event('iteration_%s start' % i)

                warmup_iteration = False if warmup_runs == 0 else i < warmup_runs

                log_print("Running iteration %s, (%s)" % (i, 'warmup' if warmup_iteration else 'prod'))

                ignite.su.snapshot_utility('snapshot', f'-type={snapshot_type}')

                latest_snapshot_id = ignite.su.snapshots[-1]['id']
                dir_size = get_nodes_directory_size(
                    ignite, self.test_class.ssh,
                    'work/snapshot/%s' %
                    list(SnapshotScenario.util_find_snapshot_folders_on_fs(ignite, latest_snapshot_id).values())[0]
                )

                m = re.search(
                    'Command \[SNAPSHOT\] successfully finished in (\d*) seconds',
                    ignite.su.latest_utility_output
                )

                if incremental_snapshot:
                    # todo user remove operation after dr-master merge
                    with PiClient(ignite, client_config) as piclient:
                        ignite_instance = piclient.get_ignite()
                        for cache_name in ignite_instance.cacheNames().toArray():
                            ignite_instance.cache(cache_name).removeAll()

                    PiClientIgniteUtils.load_data_with_putall(
                        ignite,
                        client_config,
                        end_key=int(self.config.get('data_size'))
                    )

                # skip some operation as warmup
                if not warmup_iteration:
                    assert m, 'Unable to get snapshot time execution'

                    time_results.append(int(m.group(1)))
                    directory_size.append(int(dir_size))

                self.write_time_event('iteration_%s stop' % i)

            ignite.cu.deactivate()

            self.stop_probes(time_results=time_results,
                             avg_snapshot_dir_size=directory_size,
                             seconds=True)

            self.results['evaluated'] = True

            ignite.kill_nodes()
            ignite.delete_lfs()

            log_put("Cleanup Ignite LFS ... ")
            commands = {}
            for node_idx in ignite.nodes.keys():
                host = ignite.nodes[node_idx]['host']
                if commands.get(host) is None:
                    commands[host] = [
                        'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                    ]
                else:
                    commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
            results = self.test_class.tiden.ssh.exec(commands)
            print(results)
            log_put("Ignite LFS deleted.")
            log_print()
        finally:
            # remove config set
            self.test_class.remove_app_config_set(Ignite, SNAPSHOT_CONFIG_SET)

    @staticmethod
    def util_find_snapshot_folders_on_fs(ignite, snapshot_id, remote_dir=None):
        snapshot_dirs = {}
        remote_snapshot_dir = None

        search_in_dir = remote_dir if remote_dir else './work/snapshot/'
        output = SnapshotScenario.run_on_all_nodes(ignite, 'ls -1 %s | grep %s' % (search_in_dir, snapshot_id))

        if len(output) > 0:
            for node_idx in output.keys():
                snapshot_dir = output[node_idx].rstrip()
                if snapshot_dir:
                    # Add only if directory exists
                    snapshot_dirs[node_idx] = snapshot_dir
                    remote_snapshot_dir = snapshot_dirs[node_idx]
                    log_print('Snapshot directory %s for snapshot ID=%s on node %s' %
                              (snapshot_dir if snapshot_dir else 'Not found', snapshot_id, node_idx), color='yellow')

        # if remote dir, it is the same for all servers, so don't need to iterate over all servers
        if remote_dir:
            return '%s/%s' % (remote_dir, remote_snapshot_dir)

        return snapshot_dirs

    @staticmethod
    def run_on_all_nodes(ignite, command):
        output = dict()
        server_nodes = ignite.get_all_default_nodes() + ignite.get_all_additional_nodes()

        for node_id in server_nodes:
            commands = {}
            host = ignite.nodes[node_id]['host']
            ignite_home = ignite.nodes[node_id]['ignite_home']

            commands[host] = ['cd %s;%s' % (ignite_home, command)]
            log_print(commands, color='yellow')
            tmp_output = ignite.ssh.exec(commands)
            log_print(tmp_output, color='yellow')
            output[node_id] = tmp_output[host][0]

        log_print(output)

        return output


class FullSnapshotScenario(SnapshotScenario):
    """
    Full snapshot
    """

    def run(self, artifact_name):
        """
        Run full snapshot restore for defined artifact

        :param artifact_name: name from artifact configuration file
        """
        self.run_snapshot(artifact_name, 'full')


class IncrementalSnapshotScenario(SnapshotScenario):
    """
    Incremental snapshot
    """

    def run(self, artifact_name):
        """
        Run full snapshot restore for defined artifact

        :param artifact_name: name from artifact configuration file
        """
        self.run_snapshot(artifact_name, 'inc')


class RestoreScenario(SnapshotScenario):

    def run_scenario(self, artifact_name, type):
        """
        Run full snapshot restore for defined artifact

        :param artifact_name: name from artifact configuration file
        :param snapshot_type: inc/full
        """
        super().run(artifact_name)

        log_print("Running snapshot benchmark with config: %s" % self.config, color='green')

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        try:
            self.test_class.create_app_config_set(
                Ignite, SNAPSHOT_CONFIG_SET,
                caches_list_file='caches_%s.xml' % SNAPSHOT_CONFIG_SET,
                deploy=True,
                snapshots_enabled=True,
                logger=False,
                wal_segment_size=self.test_class.consumption_config.get('wal_segment_size',
                                                                        64 * 1024 * 1024),
                logger_path='%s/ignite-log4j2.xml' %
                            self.test_class.tiden.config['rt']['remote']['test_module_dir'],
                disabled_cache_configs=False,
                zookeeper_enabled=False,
                pitr_enabled=True,
                checkpoint_read_lock_timeout=self.read_lock_property_value(version),
                # caches related variables
                additional_configs=['caches.tmpl.xml', ],
                part_32=self.config.get('part_32',
                                        32),
                part_64=self.config.get('part_64',
                                        64),
                part_128=self.config.get('part_64',
                                         128),
                # artifact config variables
                **self.artifact_config_variables,
            )

            version, ignite = self.test_class.start_ignite_grid(artifact_name,
                                                                activate=True,
                                                                config_set=SNAPSHOT_CONFIG_SET,
                                                                jvm_options=self.artifact_jvm_properties)

            time_results = list()

            self.start_probes(artifact_name)

            client_config = Ignite.config_builder.get_config('client', config_set_name=SNAPSHOT_CONFIG_SET)

            if type == 'pitr':
                # create almost empty pds to avoid massive snapshot restore time
                with PiClient(ignite, client_config) as piclient:
                    ignite_instance = piclient.get_ignite()
                    for cache_name in ignite_instance.cacheNames().toArray():
                        IgniteCache(cache_name).put(1, 1, key_type='long')
                        break

                ignite.su.snapshot_utility('snapshot', '-type=full')

            PiClientIgniteUtils.load_data_with_putall(
                ignite,
                client_config,
                end_key=int(self.config.get('data_size'))
            )

            ignite.su.snapshot_utility('snapshot', '-type=full')

            restore_id = 'None'
            if type == 'pitr':
                restore_id = self.util_get_restore_point()
            else:
                if len(ignite.su.snapshots) > 0:
                    restore_id = ignite.su.snapshots[-1]['id']

            # default times to run
            # plus warmup times
            # plus rerun times
            warmup_runs, prod_runs = self._get_number_of_runs()

            log_print("Running {} iterations".format(warmup_runs + prod_runs))
            for i in range(0, warmup_runs + prod_runs):
                self.write_time_event('iteration_%s start' % i)

                warmup_iteration = False if warmup_runs == 0 else i < warmup_runs

                log_print("Running iteration %s, (%s)" % (i, 'warmup' if warmup_iteration else 'prod'))

                ignite.su.snapshot_utility('restore', '-%s=%s' % ('to' if type == 'pitr' else 'id', restore_id))

                m = re.search(
                    'Command \[RESTORE\] successfully finished in (\d*) seconds',
                    ignite.su.latest_utility_output
                )

                # skip some operation as warmup
                if not warmup_iteration:
                    assert m, 'Unable to get snapshot time execution'

                    time_results.append(int(m.group(1)))

                self.write_time_event('iteration_%s stop' % i)

            ignite.cu.deactivate()

            self.stop_probes(time_results=time_results,
                             seconds=True)

            self.results['evaluated'] = True

            ignite.kill_nodes()
            ignite.delete_lfs()

            log_put("Cleanup Ignite LFS ... ")
            commands = {}
            for node_idx in ignite.nodes.keys():
                host = ignite.nodes[node_idx]['host']
                if commands.get(host) is None:
                    commands[host] = [
                        'rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home']
                    ]
                else:
                    commands[host].append('rm -rf %s/work/*' % ignite.nodes[node_idx]['ignite_home'])
            results = self.test_class.tiden.ssh.exec(commands)
            print(results)
            log_put("Ignite LFS deleted.")
            log_print()
        finally:
            # remove config set
            self.test_class.remove_app_config_set(Ignite, SNAPSHOT_CONFIG_SET)

    def util_get_restore_point(cls, seconds_ago=1):
        from datetime import datetime, timedelta
        from time import timezone

        util_sleep_for_a_while(2)
        time_format = "%Y-%m-%d-%H:%M:%S.%f"

        restore_point = (datetime.now() - timedelta(seconds=seconds_ago))
        # Hack to handle UTC timezone
        if int(timezone / -(60 * 60)) == 0:
            restore_point = restore_point + timedelta(hours=3)

        return restore_point.strftime(time_format)[:-3]


class RestoreFullSnapshot(RestoreScenario):

    def run(self, artifact_name):
        """
        Run point in time recovery for defined artifact

        :param artifact_name: name from artifact configuration file
        :param snapshot_type: inc/full
        """
        super().run_scenario(artifact_name, 'snapshot')


class PointInTimeRecoveryScenario(RestoreScenario):
    """
    PITR
    """

    def run(self, artifact_name):
        """
        Run point in time recovery for defined artifact

        :param artifact_name: name from artifact configuration file
        :param snapshot_type: inc/full
        """
        super().run_scenario(artifact_name, 'pitr')

