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
from tiden import log_print, TidenException
from tiden.case.apptestcase import AppTestCase
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.piclient import PiClient


class AbstractCompatibilityTest(AppTestCase):
    data_model = ModelTypes.VALUE_ACCOUNT.value

    ignite_old_version = None
    ignite_new_version = None

    current_client_ignite = None

    ignite_app_names = {
        'old': '',
        'new': '',
    }

    client_config = None
    server_config = None

    def __init__(self, *args):
        PiClient.read_timeout = 240

        super().__init__(*args)

        self.artifacts = []
        self.snapshot_storage = None
        self.config_name = 'base'

        for name, artf in args[0]['artifacts'].items():
            if artf.get('type', '') == 'ignite':
                if artf.get('base', False):
                    self.ignite_app_names['new'] = name

                    self.add_app(
                        name,
                        app_class_name='ignite',
                        grid_name='new',
                    )
                else:
                    self.ignite_app_names['old'] = name

                    self.add_app(
                        name,
                        app_class_name='ignite',
                        grid_name='old',
                    )
        if self.tiden.config.get('webconsole_enabled'):
            self.add_app('webconsole')

    def setup(self):
        self.create_app_config_set(Ignite, 'base',
                                   logger_path='%s/ignite-log4j2.xml' % self.tiden.config['rt']['remote'][
                                       'test_module_dir'],
                                   consistent_id=True,
                                   caches='caches.xml',
                                   disabled_cache_configs=True,
                                   zookeeper_enabled=False,
                                   )
        super().setup()

        self.client_config = Ignite.config_builder.get_config('client', config_set_name='base')

    def start_ignite_grid(self, name, activate=False, already_nodes=0, replaced_name=None):
        app = self.get_app(self.ignite_app_names[name])
        app.set_node_option('*', 'config',
                            Ignite.config_builder.get_config('server', config_set_name=self.config_name))
        # app.activate_default_modules()

        artifact_cfg = self.tiden.config['artifacts'][app.name]

        app.reset()
        log_print("Ignite ver. %s, revision %s" % (
            artifact_cfg['ignite_version'],
            artifact_cfg['ignite_revision'],
        ))

        if replaced_name:
            app.grid_name = replaced_name

        cmd = [
            'cp %s %s/libs/' % (app.config['artifacts']['piclient']['remote_path'],
                                app.config['artifacts'][self.ignite_app_names[name]]['remote_path'])
        ]
        self.util_exec_on_all_hosts(app, cmd)

        app.start_nodes(already_nodes=already_nodes, other_nodes=already_nodes)

        if activate:
            app.cu.activate()

        return app

    def setup_mixed_cluster_testcase(self):
        self.ignite_old_version = self.start_ignite_grid(
            'old',
            activate=False
        )
        try:
            self.ignite_new_version = self.start_ignite_grid(
                'new',
                activate=False,
                already_nodes=len(self.ignite_old_version.get_alive_default_nodes())
            )
        except TidenException as e:
            old_version = self.tiden.config['artifacts'][self.ignite_app_names['old']]['ignite_version']
            new_version = self.tiden.config['artifacts'][self.ignite_app_names['new']]['ignite_version']

            raise TidenException("Mixed cluster of versions %s, %s won't start" % (old_version, new_version))

        self.current_client_ignite = self.ignite_old_version

    def setup_old_version_cluster(self):
        self.ignite_new_version = self.start_ignite_grid(
            'new',
            activate=False,
        )
        self.ignite_new_version.kill_nodes()

        self.ignite_old_version = self.start_ignite_grid(
            'old',
            activate=False,
        )
        self.current_client_ignite = self.ignite_old_version

        if self.tiden.config.get('webconsole_enabled'):
            self.setup_webconsole()

    def setup_webconsole(self):
        console = self.get_app_by_type('webconsole')[0]

        console.start(users=True)
        ignite_urls = ['{}:8080'.format(host) for host in console.ssh.hosts]
        console.start_web_agent(ignite_urls=ignite_urls)

        log_print('WebConsole Started', color='debug')

    def empty_setup(self):
        pass

    def setup_new_version_cluster(self):
        self.ignite_old_version = self.start_ignite_grid(
            'old',
            activate=False
        )
        self.ignite_old_version.kill_nodes()

        self.ignite_new_version = self.start_ignite_grid(
            'new',
            activate=False,
        )
        self.current_client_ignite = self.ignite_new_version

    def teardown_mixed_cluster_testcase(self):
        try:
            log_print("Trying to deactivate grid", color='green')
            self.current_client_ignite.cu.deactivate()
        except Exception:
            log_print("Unable to deactivate grid", color='red')

        if self.ignite_old_version:
            if self.snapshot_storage:
                self.ignite_old_version.su.remove_shared_snapshot_storage(self.snapshot_storage)
            self.ignite_old_version.kill_nodes()
            self.ignite_old_version.delete_lfs()
            self.ignite_old_version = None

        if self.ignite_new_version:
            if self.snapshot_storage:
                self.ignite_new_version.su.remove_shared_snapshot_storage(self.snapshot_storage)
            self.ignite_new_version.kill_nodes()
            self.ignite_new_version.delete_lfs()
            self.ignite_new_version = None

