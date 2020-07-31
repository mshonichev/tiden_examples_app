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
from tiden import with_setup, log_print
from tiden_gridgain.piclient.piclient import PiClient
from tiden.sshpool import RemoteOperationTimeout
from time import sleep


class TestClient(SingleGridTestCase):
    def setup(self):
        super().setup()

    def teardown(self):
        super().teardown()

    def setup_testcase(self):
        self.start_grid()

    def setup_testcase_no_activate(self):
        self.ignite.start_nodes()

    def teardown_testcase(self):
        self.stop_grid_hard()
        self.delete_lfs()

    @with_setup(setup_testcase_no_activate, teardown_testcase)
    def test_jmx_utility_activate(self):
        from pt.utilities.jmx_utility import JmxUtility
        jmx = JmxUtility(self.ignite)
        jmx.start_utility()
        jmx.activate(1)
        sleep(1)
        jmx.kill_utility()

    @with_setup(setup_testcase, teardown_testcase)
    def test_client_start_stop(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            log_print(piclient)

    @with_setup(setup_testcase, teardown_testcase)
    def test_client_start_restart_grid_stop(self):
        with PiClient(self.ignite, self.get_client_config()) as piclient:
            log_print(piclient)
            self.stop_grid()
            self.start_grid()
            cache_names = piclient.get_ignite().cacheNames().toArray()
            for cache_name in cache_names:
                log_print(cache_name)

    @with_setup(setup_testcase, teardown_testcase)
    def test_control_utility_deactivate(self):
        try:
            self.cu.deactivate(timeout=30)
        except RemoteOperationTimeout as e:
            assert False, "Deactivate failed"

    @with_setup(setup_testcase, teardown_testcase)
    def test_client_start_load_data_stop(self):
        self.load_data_with_streamer()

