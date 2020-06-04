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

import datetime

# import pause

from tiden.apps.ignite import Ignite
from tiden import log_print, sleep
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from suites.consumption.framework.scenarios.abstract import AbstractScenario

START_TIME_CONFIG_SET = 'start_time'


class StartTimeScenario(AbstractScenario):
    """
    Cluster start time scenario

    Scenario description:
        1. Start cluster
        2. Activate
        3. Load massive amount of data
        4. Kill cluster gracefully
        5. Try to start cluster again and collect: (start coordinator first, then other nodes)
            node start timings
            (auto)activation timings

    Ideally:
    Partitions = 32k, caches - 500, key per partition

    Practically:
    Partitions = 32k, caches - 120, key per partition
    """

    def _validate_config(self):
        super()._validate_config()

        assert self.config.get('times_to_run'), 'There is no "times_to_run" variable in config'

    def run(self, artifact_name):
        """
        Run scenario for defined artifact

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running snapshot benchmark with config: %s" % self.config, color='green')

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        time_to_jfr = int(self.config.get('time_to_jfr', 0))
        now_plus_300 = datetime.datetime.now() + datetime.timedelta(seconds=1)
        try:
            self.test_class.create_app_config_set(
                Ignite, START_TIME_CONFIG_SET,
                caches_list_file='caches_%s.xml' % START_TIME_CONFIG_SET,
                deploy=True,
                snapshots_enabled=True,
                logger=False,
                wal_segment_size=self.test_class.consumption_config.get('wal_segment_size',
                                                                        1024 * 1024 * 1024),
                logger_path='%s/ignite-log4j2.xml' %
                            self.test_class.tiden.config['rt']['remote']['test_module_dir'],
                disabled_cache_configs=False,
                zookeeper_enabled=False,
                checkpoint_read_lock_timeout=self.read_lock_property_value(version),
                # caches related variables
                additional_configs=['caches.tmpl.xml', ],
                part_32=self.config.get('part_32',
                                        10000),
                # 100),
                part_64=self.config.get('part_64',
                                        10000),
                # 100),
                part_128=self.config.get('part_128',
                                         10000),
                # 100),
                # artifact config variables
                **self.artifact_config_variables,
            )
            version, ignite = self.test_class.start_ignite_grid(
                artifact_name,
                activate=True,
                config_set=START_TIME_CONFIG_SET,
                jvm_options=self.artifact_jvm_properties
            )

            ignite.set_snapshot_timeout(1200)

            if time_to_jfr:
                now_plus_300 = datetime.datetime.now() + datetime.timedelta(seconds=350)
                ignite.make_cluster_jfr(300)

            PiClientIgniteUtils.load_data_with_putall(
                ignite,
                Ignite.config_builder.get_config('client', config_set_name=START_TIME_CONFIG_SET),
                # end_key=int(self.config.get('data_size'))
                end_key=10000)

            # kill nodes
            ignite.kill_nodes()

            sleep(120)

            self.start_probes(artifact_name)

            ignite.start_nodes()

            self.stop_probes()

            ignite.kill_nodes()
            ignite.delete_lfs()

            # do some calculations
        finally:
            # if time_to_jfr:
            #     pause.until(now_plus_300)
            # remove config set
            self.test_class.remove_app_config_set(Ignite, START_TIME_CONFIG_SET)

