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
from tiden import log_print, log_put
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_streamer_operation
from tiden_gridgain.piclient.piclient import PiClient
from suites.consumption.framework.scenarios.abstract import AbstractScenario

STREAMER_CONFIG_SET = 'streamer'


class StreamerLoadingScenario(AbstractScenario):
    """
    cache.streamer() scenario

    Execution time provided by piclient.Operation().getStartTime() and getEndTime() methods

    Scenario description:
        1. Start cluster
        2. Activate
        3. start probes
        4. for try in times_to_run:
                for cache in ignite.caches():
                    start_time
                    cache.streamer(keys from prev end_key + data size)
                    end_time

                collect time above all caches
        5. Deactivate
        6. stop_probes(execution time data)
        7. Cleanup

    In case of data_size = 10000, first streamer will insert (0, 10000), next will insert (10001, 20000) etc.
    """

    def _validate_config(self):
        super()._validate_config()

        assert self.config.get('times_to_run'), 'There is no "times_to_run" variable in config'
        assert self.config.get('data_size'), 'There is no "data_size" variable in config'

    def run(self, artifact_name):
        """
        Run scenario for defined artifact

        :param artifact_name: name from artifact configuration file
        """
        super().run(artifact_name)

        log_print("Running streamer() benchmark with config: %s" % self.config, color='green')

        version = self.test_class.tiden.config['artifacts'][artifact_name]['ignite_version']
        try:
            xml_config_set_name = 'caches_%s.xml' % STREAMER_CONFIG_SET
            self.test_class.create_app_config_set(
                Ignite, STREAMER_CONFIG_SET,
                caches_list_file=xml_config_set_name,
                deploy=True,
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
                part_32=self.test_class.consumption_config.get('part_32',
                                                               32),
                part_64=self.test_class.consumption_config.get('part_64',
                                                               64),
                part_128=self.test_class.consumption_config.get('part_64',
                                                                128),
            )

            version, ignite = self.test_class.start_ignite_grid(artifact_name,
                                                                activate=True,
                                                                config_set=STREAMER_CONFIG_SET)
            self.start_probes(artifact_name)

            # default times to run
            # plus warmup times
            # plus rerun times
            warmup_runs, prod_runs = self._get_number_of_runs()

            time_results = list()

            client_config = Ignite.config_builder.get_config('client', config_set_name=STREAMER_CONFIG_SET)
            with PiClient(ignite, client_config) as piclient:
                cache_names = piclient.get_ignite().cacheNames()
                data_size = int(self.config.get('data_size'))

                log_print("Running {} iterations".format(warmup_runs + prod_runs))
                for i in range(0, warmup_runs + prod_runs):
                    self.write_time_event('iteration_%s start' % i)

                    warmup_iteration = False if warmup_runs == 0 else i < warmup_runs

                    log_print("Running iteration %s (%s)" % (i, 'warmup' if warmup_iteration else 'prod'))

                    log_print("Loading %s values per cache into %s caches" % (
                        data_size * (i + 1) - data_size * i, cache_names.size()))

                    async_operations = []
                    self.write_time_event('iteration_%s create streamer' % i)
                    for cache_name in cache_names.toArray():
                        async_operation = create_async_operation(create_streamer_operation, cache_name,
                                                                 data_size * i,
                                                                 data_size * (i + 1),
                                                                 value_type=ModelTypes.VALUE_ACCOUNT.value)
                        async_operations.append(async_operation)
                        async_operation.evaluate()

                    for async_op in async_operations:
                        async_op.getResult()

                        # skip first operations as warmup otherwise write results
                        if not warmup_iteration:
                            time_results.append(
                                async_op.getOperation().getEndTime() - async_op.getOperation().getStartTime())

                    self.write_time_event('iteration_%s streamer done' % i)

                log_print("Loading done")

            ignite.cu.deactivate()

            self.stop_probes(time_results=time_results)

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
            self.test_class.remove_app_config_set(Ignite, STREAMER_CONFIG_SET)

