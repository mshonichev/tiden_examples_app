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
from tiden.util import log_print, log_put, from_camelcase


class AbstractConsumptionTest(AppTestCase):
    """
    Run (newly added config file):

    --tc=config/suite-consumption.yaml
    --tc=config/env_*.yaml
    --tc=config/artifacts-*.yaml
    --ts="consumption.test_*"
    --clean=tests

    """
    client_config = None
    server_config = None

    def __init__(self, *args):
        super().__init__(*args)

        self.on_init_applications(args[0])
        self.consumption_config = None

    def on_init_applications(self, tiden_config):
        for name, artf in tiden_config['artifacts'].items():
            if artf.get('type', '') == 'ignite':
                self.add_app(
                    name,
                    app_class_name='ignite',
                )

    def setup(self):
        self.consumption_config = self.get_consumption_suite_config()
        self.on_setup_application_config_sets()
        super().setup()

    def get_consumption_suite_config_name(self):
        return from_camelcase(self.__class__.__name__)

    def get_consumption_suite_config(self):
        suite_name = self.tiden.config.get('suite_name', 'consumption')
        assert suite_name in self.tiden.config, \
            f"Provide {suite_name} framework config, " \
            "e.g. '--tc=config/suite-{suite_name}.yaml' " \
            "or '--tc=config/suite-{suite_name}_dev.yaml'"

        consumption_suite_config_name = self.get_consumption_suite_config_name()
        assert consumption_suite_config_name in self.tiden.config[suite_name], \
            f"There's no section '{consumption_suite_config_name}' in {suite_name} framework config"

        return self.tiden.config[suite_name][consumption_suite_config_name]

    def get_consumption_test_config(self, name):
        return self.get_consumption_suite_config().get(name, None)

    def on_setup_application_config_sets(self):
        self.create_app_config_set(Ignite, 'base',
                                   snapshots_enabled=True,
                                   caches_list_file='caches.xml',
                                   logger=False,
                                   logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                       'test_module_dir'],
                                   wal_segment_size=self.consumption_config.get('wal_segment_size',
                                                                                64 * 1024 * 1024),
                                   disabled_cache_configs=False,
                                   zookeeper_enabled=False,
                                   node=None)

    def setup_testcase(self):
        pass

    def teardown_testcase(self):
        for ignite in self.get_app_by_type('ignite'):
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
            results = self.tiden.ssh.exec(commands)
            log_print(results)
            log_put("Ignite LFS deleted.")
            log_print()

    def start_ignite_grid(self, name, activate=False, already_nodes=0, config_set='base', jvm_options=None):
        app = self.get_app(name)
        app.set_node_option('*', 'config',
                            Ignite.config_builder.get_config('server', config_set_name=config_set))

        if jvm_options:
            app.set_node_option('*', 'jvm_options', jvm_options)

        artifact_cfg = self.tiden.config['artifacts'][app.name]

        app.reset()
        version = artifact_cfg['ignite_version']
        log_print("Ignite ver. %s, revision %s" % (
            version,
            artifact_cfg['ignite_revision']
        ))

        app.set_snapshot_timeout(360)
        app.set_grid_name(name)
        app.set_additional_grid_name(False)

        # copy piclient to work directories
        self.util_exec_on_all_hosts(app, [
            'cp %s %s/libs/' % (self.tiden.config['artifacts']['piclient']['remote_path'],
                                self.tiden.config['artifacts'][name]['remote_path']),
        ])

        app.start_nodes(already_nodes=already_nodes)

        if activate:
            app.cu.activate(activate_on_particular_node=1)

        return version, app

    @staticmethod
    def assert_result(res, probe_name):
        assert res['probes'][probe_name]['result_passed'], \
            res['probes'][probe_name]['result_message']

