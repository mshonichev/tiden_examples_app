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
from tiden.testconfig import test_config
from tiden.util import print_blue, require
from tiden.priority_decorator import test_priority


class TestNasManager(SingleGridTestCase):

    folders_to_remove = []

    def setup(self):
        super().setup()

    def teardown(self):
        super().teardown()

    def get_source_resource_dir(self):
        return "%s/res/client" % self.config['suite_dir']

    # @require(test_config.shared_folder_enabled)
    # def test_shared_non_unique_folder_created(self):
    #     folder = self.ignite.su.create_shared_snapshot_storage()
    #     print_blue(folder)
    #     self.folders_to_remove.append(folder)
    #     assert self.config['environment']['username'] in folder
    #
    #     host1 = self.ssh.get_random_host()
    #     output = self.ssh.exec_on_host(host1, ['touch %s/test_share.tmp && echo "OOKK"' % folder])
    #     print_blue(output)
    #     assert 'OOKK' == output[host1][0].strip()
    #
    #     host2 = self.ssh.get_random_host()
    #     output = self.ssh.exec_on_host(host2, ['ls -la %s/test_share.tmp 2>&1 && echo "OOKK"' % folder])
    #     print_blue(output)
    #     assert 'test_share.tmp' in output[host2][0]
    #     assert 'OOKK' in output[host2][0]

    @require(test_config.shared_folder_enabled)
    def test_shared_unique_folder_created(self):
        folder = self.ignite.su.create_shared_snapshot_storage(unique_folder=True, prefix='wooop')
        print_blue(folder)
        self.folders_to_remove.append(folder)
        assert 'wooop' in folder

        host1 = self.ssh.get_random_host()
        output = self.ssh.exec_on_host(host1, ['touch %s/test_share.tmp && echo "OOKK"' % folder])
        print_blue(output)
        assert 'OOKK' == output[host1][0].strip()

        host2 = self.ssh.get_random_host()
        output = self.ssh.exec_on_host(host2, ['ls -la %s/test_share.tmp 2>&1 && echo "OOKK"' % folder])
        print_blue(output)
        assert 'test_share.tmp' in output[host2][0]
        assert 'OOKK' in output[host2][0]

    @test_priority.LOW
    @require(test_config.shared_folder_enabled)
    def test_shared_folder_removed(self):
        folder = self.ignite.su.create_shared_snapshot_storage(unique_folder=True, prefix='to_remove')
        print_blue(folder)
        self.folders_to_remove.append(folder)
        assert 'to_remove' in folder

        for folder in self.folders_to_remove:
            res = self.ignite.su.remove_shared_snapshot_storage(folder)
            print_blue(res)

            host = self.ssh.get_random_host()
            output = self.ssh.exec_on_host(host, ['ls -la %s/.. 2>/dev/null || echo "OOKK"' % folder])
            print_blue(output)
            assert folder not in output[host][0]
            assert 'OOKK' in output[host][0]

