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

from tiden import *
from tiden.logger import get_logger
import random

from tiden.configuration_decorator import test_configuration
from tiden.testconfig import test_config
from suites.snapshots.ult_utils import UltimateUtils
from tiden.priority_decorator import test_priority
from threading import Thread


@test_configuration([
    'zookeeper_enabled',
    'compaction_enabled',
    'pitr_enabled',
], [
    # Public release
    [False, False, False],
    # Sber release
    [False, False, True],
    [False, True, True],
    [True, False, False],
    [True, False, True],
    [True, True, True],
])
class TestSnapshotsStability(UltimateUtils):
    setup_testcase = UltimateUtils.setup_testcase
    teardown_testcase = UltimateUtils.teardown_testcase

    def setup(self):
        super().setup()
        self.logger = get_logger('tiden')
        self.logger.set_suite('[TestSnapshots]')

    @attr('GG-14499')
    @with_setup(setup_testcase, teardown_testcase)
    def test_stability_snapshots_change_non_blt_nodes(self):
        """
        This test is based on https://ggsystems.atlassian.net/browse/GG-14499
        """
        self.load_data_with_streamer(end_key=1000000)

        def change_non_blt_topology(self):
            def start_and_add_new_node():
                additional_node = self.ignite.add_additional_nodes(self.get_server_config())[0]
                self.ignite.start_additional_nodes(additional_node)
                stack_add_nodes.append(additional_node)

            stack_add_nodes = []
            util_sleep_for_a_while(1)
            start_and_add_new_node()

            for j in range(1, 10):
                try:
                    if random.choice([True, False]):
                        util_sleep_for_a_while(1)
                        start_and_add_new_node()
                    else:
                        if stack_add_nodes:
                            util_sleep_for_a_while(1)
                            additional_node = stack_add_nodes.pop()
                            self.ignite.kill_node(additional_node)
                except TidenException:
                    pass

        t = Thread(target=change_non_blt_topology, args=(self,))
        t.start()
        for i in range(1, 6):
            self.su.snapshot_utility('SNAPSHOT', '-type=full')
        t.join()

