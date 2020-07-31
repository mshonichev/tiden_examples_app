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
from tiden.util import attr, require_min_server_nodes, require_min_ignite_version, require
import os
from tiden.testconfig import test_config
from tiden.sshpool import RemoteOperationTimeout


class TestPool(SingleGridTestCase):

    def test_ssh_pool(self):
        # create directory and file
        self.ssh.exec(['mkdir -p /tmp/tiden-selftest'])
        self.ssh.exec(['touch /tmp/tiden-selftest/_1.txt'])
        self.ssh.exec(['echo "12345" > /tmp/tiden-selftest/_1.txt'])
        # download to local machine
        local_path = "%s/dir" % self.config['rt']['test_module_dir']
        os.makedirs(local_path, exist_ok=True)
        self.ssh.download('/tmp/tiden-selftest/_1.txt', local_path)
        # remove remote directory
        self.ssh.exec(['rm -rf /tmp/tiden-selftest'])
        # check that it's removed
        result_ls = self.ssh.exec(['cd /tmp; ls -l'])
        for host in result_ls.keys():
            for output in result_ls[host]:
                assert 'tiden-selftest' not in output

    def test_context(self):
        # change default context
        default_context = self.contexts['default']
        default_context.add_context_variables(
            snapshots_enabled=True,
            # does not define zookeeper_enabled=True
        )

        # create another context
        second_context = self.create_test_context('snapshots_disabled_zookeeper_enabled')
        second_context.add_context_variables(
            snapshots_enabled=False,
            zookeeper_enabled=True
        )

        # create and deploy configs
        self.rebuild_configs()
        self.deploy()

        assert 'default' in self.contexts
        assert 'snapshots_disabled_zookeeper_enabled' in self.contexts

        commands = {}

        # verify default configs
        for host in self.config['environment']['server_hosts']:
            commands[host] = ['cat %s/%s' % (self.config['rt']['remote']['test_module_dir'],
                                             self.get_server_config('default'))]
        results = self.ssh.exec(commands)

        for host in self.config['environment']['server_hosts']:
            assert results[host][0].find('TcpDiscoverySpi') != -1
            assert 'SnapshotConfiguration' in results[host][0]
            assert 'ZookeeperDiscoverySpi' not in results[host][0]

        # verify snapshots_disabled_zookeeper_enabled configs
        for host in self.config['environment']['server_hosts']:
            commands[host] = ['cat %s/%s' % (self.config['rt']['remote']['test_module_dir'],
                                             self.get_server_config('snapshots_disabled_zookeeper_enabled'))]

        results = self.ssh.exec(commands)

        for host in self.config['environment']['server_hosts']:
            assert results[host][0].find('TcpDiscoverySpi') == -1
            assert 'SnapshotConfiguration' not in results[host][0]
            assert 'ZookeeperDiscoverySpi' in results[host][0]

        # set context and check that variables defined
        self.set_current_context('snapshots_disabled_zookeeper_enabled')

        assert self.current_context == 'snapshots_disabled_zookeeper_enabled'
        assert not self.get_context_variable('snapshots_enabled')
        assert self.get_context_variable('zookeeper_enabled')
        assert self.ignite.nodes[1]['config'] == self.get_server_config('snapshots_disabled_zookeeper_enabled')

        # set default context
        self.set_current_context()

        assert self.current_context == 'default'
        assert self.get_context_variable('snapshots_enabled')
        assert not self.get_context_variable('zookeeper_enabled')
        assert self.ignite.nodes[1]['config'] == self.get_server_config('default')

    def test_timeout(self):
        passed = False
        try:
            self.ssh.exec(['sleep 20'], timeout=10)
        except RemoteOperationTimeout as e:
            passed = True
        assert passed, "Timeout exception did not fired"

    # ============================================= test requirements syntax

    @attr('debug')
    @require_min_server_nodes(4)
    def test_skip(self):
        pass

    @attr('debug')
    @require_min_server_nodes(1)
    def test_skip_2(self):
        pass

    @attr('debug')
    @require_min_ignite_version('2.4.2-p8')
    def test_skip_3(self):
        pass

    @attr('debug')
    @require_min_ignite_version('2.4.2-p2')
    def test_skip_4(self):
        pass

    @attr('debug')
    @require_min_server_nodes(1)
    @require_min_ignite_version('2.4.2-p8')
    def test_skip_5(self):
        pass

    @attr('debug')
    @require_min_ignite_version('2.4.2-p8')
    @require_min_server_nodes(1)
    def test_skip_6(self):
        pass

    @attr('debug')
    @require_min_ignite_version('2.4.2-p2')
    @require_min_server_nodes(4)
    def test_skip_7(self):
        pass

    @attr('debug')
    @require_min_server_nodes(4)
    @require_min_ignite_version('2.4.2-p2')
    def test_skip_8(self):
        pass

    # ============================================= alternate, simple syntax

    @attr('debug')
    @require(min_ignite_version='2.4.2-p2', min_server_nodes=4)
    def test_skip_9(self):
        pass

    @attr('debug')
    @require(test_config.ignite.pitr_enabled)
    def test_skip_10(self):
        pass

    @attr('debug_shared')
    @require(test_config.shared_folder_enabled)
    def test_only_with_shared_folder(self):
        pass

    @attr('debug_shared')
    @require(~test_config.shared_folder_enabled)
    def test_only_without_shared_folder(self):
        pass

