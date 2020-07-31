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

from tiden_gridgain.case.singlegridtestcase import SingleGridTestCase
from tiden.util import with_setup
from tiden.ignite import Ignite
from tiden.tidenexception import TidenException
import os


class TestStatic(SingleGridTestCase):

    def setup_testcase(self):
        self.start_grid()

    def setup_static_ignite_testcase(self):
        self.start_grid()
        self.ignite.dump_nodes_config(nodes_config_path='/tmp')

        self._debug_download_nodes_config()

    def _debug_download_nodes_config(self):
        os.makedirs(self.config['rt']['test_dir'], exist_ok=True)
        self.ssh.download_from_host(
            self.ignite.nodes_config_store_host,
            self.ignite.nodes_config_path + '/' + self.ignite.nodes_config_name,
            os.path.join(self.config['rt']['test_dir'], self.ignite.nodes_config_name),
        )

    def teardown_testcase(self):
        self.stop_grid_hard()
        self.delete_lfs()

    @with_setup(setup_static_ignite_testcase, teardown_testcase)
    def test_static_ignite_init(self):
        try:
            ignite = Ignite(self.config, self.ssh,
                            static_init=True,
                            nodes_config_path='/tmp',
                            nodes_config_store_host=self.config['environment']['server_hosts'][0])
            ignite.setup()
        except TidenException:
            assert False, "Static init failed"

    @with_setup(setup_testcase, teardown_testcase)
    def test_dump_during_run(self):
        try:
            self.ignite.dump_nodes_config(nodes_config_path='/tmp')
            ignite = Ignite(self.config, self.ssh,
                            static_init=True,
                            nodes_config_path='/tmp',
                            nodes_config_store_host=self.config['environment']['server_hosts'][0])
            ignite.setup()
        except TidenException:
            assert False, "Static init failed"

    @with_setup(setup_static_ignite_testcase, teardown_testcase)
    def test_manage_static_ignite(self):
        static_ignite = None
        try:
            static_ignite = Ignite(self.config, self.ssh,
                                   static_init=True,
                                   nodes_config_path='/tmp',
                                   nodes_config_store_host=self.config['environment']['server_hosts'][0])
            static_ignite.setup()
            static_ignite.stop_nodes(force=True)
            static_ignite.start_nodes()
        except TidenException:
            assert False, "Can't manage ignite"
        finally:
            if static_ignite is not None and len(static_ignite.nodes) > 0:
                self.ignite.nodes = static_ignite.nodes

    @with_setup(setup_static_ignite_testcase, teardown_testcase)
    def test_additional_nodes(self):
        static_ignite = None
        try:
            static_ignite = Ignite(self.config, self.ssh,
                                   static_init=True,
                                   nodes_config_path='/tmp',
                                   nodes_config_store_host=self.config['environment']['server_hosts'][0])
            static_ignite.setup()
            range = static_ignite.add_additional_nodes(self.get_server_config(), num_nodes=2)
            static_ignite.start_additional_nodes(range)
        except TidenException:
            assert False, "Can't manage ignite"
        finally:
            if static_ignite is not None and len(static_ignite.nodes) > 0:
                self.ignite.nodes = static_ignite.nodes

