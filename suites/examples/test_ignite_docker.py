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

from tiden_gridgain.case.singledockergridtestcase import SingleDockerGridTestCase
from tiden.util import attr, with_setup, log_print, util_sleep_for_a_while


class TestIgniteDocker(SingleDockerGridTestCase):
    folders_to_remove = []

    def setup(self):
        swarm_context = self.create_test_context('swarm')
        # not_default_context.add_config('server.xml', 'server_{}'.format(node_idx))
        default_context_vars = self.contexts['default'].get_context_variables()
        swarm_context.add_context_variables(
            **default_context_vars,
            swarm=True
        )

        super().setup()
        self.ignite.clean_all_hosts()

    def clean(self):
        self.ignite.clean_all_hosts()

    def teardown(self):
        super().teardown()
        self.ignite.clean_all_hosts()

    @attr('cxz')
    @with_setup(clean)
    def test_host_ignite(self):
        self.start_grid('host')
        nodes_num = len(self.ignite.get_alive_default_nodes())
        self.ignite.cu.activate()
        self.ignite.cu.control_utility('--baseline')

        self.load_data_with_streamer(start_key=1, end_key=1000,
                                     value_type='org.apache.ignite.piclient.model.values.AllTypesIndexed', ignite=self.ignite)
        self.ignite.start_node_inside(1)
        self.ignite.wait_for_topology_snapshot(server_num=nodes_num-1)

        util_sleep_for_a_while(10)
        self.ignite.start_node_inside(1)

        self.ignite.wait_for_topology_snapshot(server_num=nodes_num)
        self.load_data_with_streamer(start_key=1, end_key=1000, allow_overwrite=True,
                                     value_type='org.apache.ignite.piclient.model.values.AllTypesIndexed', ignite=self.ignite)
        log_print('Done')

