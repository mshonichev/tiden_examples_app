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

from tiden_gridgain.case.regressapptestcase import RegressAppTestCase
from tiden.util import require_min_client_nodes, with_setup, require, known_issue, test_case_id
from tiden.testconfig import test_config
from tiden.apps.ignite import Ignite


class TestRegress2 (RegressAppTestCase):

    def __init__(self, *args):
        super().__init__(*args)

    def setup(self):
        super().setup()

    setup_shared_folder_test = RegressAppTestCase.setup_shared_folder_test
    teardown_shared_folder_test = RegressAppTestCase.teardown_shared_folder_test

    @with_setup(setup_shared_folder_test, teardown_shared_folder_test)
    @require(test_config.shared_folder_enabled)
    # @known_issue('IGN-10895')
    @test_case_id(176624)
    def test_ign_10895(self):
        """
        from https://ggsystems.atlassian.net/browse/IGN-10895
        1. Start Ignite cluster with Zookeeper discovery, preload data, create snapshot, stop cluster.
        2. restart cluster with TCP discovery, AND without ignite-zookeeper module.
        3. check metadata on snapshot.
        """
        ignite_app = Ignite(self.get_app_by_type('ignite')[0])

        zk_app = self.get_app_by_type('zookeeper')[0]
        ignite_app.set_node_option(
            '*', 'jvm_options', ["-DZK_CONNECTION=%s" % zk_app._get_zkConnectionString()]
        )
        ignite_app.set_node_option(
            '*', 'config', 'server.zk.xml'
        )
        zk_app.start()
        self.run_ignite_cluster()
        ignite_app.su.snapshot_utility(
            'SNAPSHOT',
            '-type=FULL'
        )
        snapshot_id = ignite_app.su.get_created_snapshot_id(1)
        ignite_app.su.snapshot_utility('MOVE', '-id=%s' % snapshot_id, '-dest=%s' % self.get_shared_folder_path())
        self.stop_ignite_cluster()
        zk_app.stop()

        ignite_app.uninstall_module('ignite-zookeeper')

        try:
            ignite_app.su.snapshot_utility(
                'METADATA',
                '-action=print',
                '-src=%s' % self.get_shared_folder_path(),
                '-id=%s' % snapshot_id,
                standalone=True
            )
        except AssertionError as e:
            if 'Failed to find class with given class loader for unmarshalling' in str(e):
                assert False, "IGN-10895 reproduced: %s" % str(e)

        ignite_app.set_node_option('*', 'config', 'server.ignite.xml')
        ignite_app.deactivate_module('ignite-zookeeper')
        self.run_ignite_cluster(preloading=False)
        try:
            ignite_app.su.snapshot_utility(
                'LIST',
                '-src=%s' % self.get_shared_folder_path(),
                standalone=True
            )
        except AssertionError as e:
            if 'Failed to find class with given class loader for unmarshalling' in str(e):
                assert False, "IGN-10895 reproduced: %s" % str(e)
        self.stop_ignite_cluster()

    # @require_min_client_nodes(4)
    # @require(~test_config.shared_folder_enabled)
    # def test_ign_10945(self):
    #     ignite_app = Ignite(self.get_app_by_type('ignite')[0])
    #
    #     zk_app = self.get_app_by_type('zookeeper')[0]
    #     ignite_app.set_node_option(
    #         '*', 'jvm_options', [
    #             "-DZK_CONNECTION=%s" % self.tiden.apps['zookeeper']._get_zkConnectionString(),
    #             "-DEXCHANGE_HISTORY_SIZE=5",
    #             "-ea",
    #         ]
    #     )
    #     ignite_app.set_node_option(
    #         '*', 'config', 'server.zk.xml'
    #     )
    #     zk_app.start()
    #     self.run_ignite_cluster()
    #     for i in range(1, 10):
    #         self.restart_client(1)
    #     self.stop_all_clients()
    #     self.start_all_clients()
    #
    def teardown(self):
        super().teardown()

