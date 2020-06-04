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

from tiden_gridgain.case.regresstestcase import RegressTestCase
from tiden.util import is_enabled, with_setup, test_case_id, require, attr
from tiden.configuration_decorator import test_configuration
from tiden.testconfig import test_config


@test_configuration(['zookeeper_enabled'])
class TestSecurityCompatibility(RegressTestCase):

    def setup(self):
        default_context = self.contexts['default']
        self.auth_login = self.config.get('auth_login', 'server_user')
        self.auth_password = self.config.get('auth_password', 'server_password')
        default_context.add_context_variables(
            authentication_enabled=True,
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            auth_login=self.auth_login,
            auth_password=self.auth_password,
        )

        insecure_context = self.create_test_context('insecure')
        insecure_context.add_context_variables(
            authentication_enabled=False,
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
        )

        super().setup()

    def setup_pitr_shared_storage_test(self):
        self.setup_zk()
        super().setup_pitr_shared_storage_test()

    def teardown_pitr_shared_storage_test(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()
        super().teardown_pitr_shared_storage_test()

    def setup_test(self):
        self.setup_zk()
        super().setup_test()

    def teardown_test(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()
        super().teardown_test()

    def setup_zk(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()

            for context_name in ['default', 'insecure']:
                current_context = self.contexts[context_name]
                current_context.add_context_variables(
                    zoo_connection=self.zoo._get_zkConnectionString()
                )
                current_context.build_and_deploy(self.ssh)

    @with_setup(setup_test, teardown_test)
    @test_case_id(81470)
    @attr('common')
    def test_LFS_compatibility_secure_to_insecure(self):
        """
        1. start grid with security enabled
        2. populate caches with data
        3. stop grid
        4. start grid without security on the same LFS
        """
        self.util_enable_security()
        self.start_grid()
        self.load_data_with_streamer()
        dump_before = self.calc_checksums_on_client()
        # print_blue('dump:')
        # print_blue(dump_before)
        self.stop_grid()
        self.util_disable_security()
        self.start_grid()
        result = self.ignite.find_exception_in_logs('AssertionError|Exception')
        # print_blue('res:')
        # print_blue(result)
        assert not result, "Found some exceptions!"
        dump_after = self.calc_checksums_on_client()
        assert dump_after == dump_before, "Checksums failed!"

    @with_setup(setup_test, teardown_test)
    @test_case_id(81469)
    @attr('common')
    def test_LFS_compatibility_insecure_to_secure(self):
        """
        1. start grid without security
        2. populate caches with data
        3. stop grid
        4. start grid with security on the same LFS
        """
        self.util_disable_security()
        self.start_grid()
        self.load_data_with_streamer()
        dump_before = self.calc_checksums_on_client()
        # print_blue('dump:')
        # print_blue(dump_before)
        self.stop_grid()
        self.util_enable_security()
        self.start_grid()
        result = self.ignite.find_exception_in_logs('AssertionError|Exception')
        assert not result, "Found some exceptions!"
        # print_blue('res:')
        # print_blue(result)
        dump_after = self.calc_checksums_on_client()
        assert dump_after == dump_before, "Checksums failed!"

    @with_setup(setup_pitr_shared_storage_test, teardown_pitr_shared_storage_test)
    @test_case_id(81468)
    @require(test_config.shared_folder_enabled)
    @attr('shared')
    def test_snapshots_compatibility_secure_to_insecure(self):
        """
        1. start grid with security and pitr enabled
        2. populate caches with data
        3. create and move snapshot to shared storage
        4. stop grid
        5. start grid with clean LFS without security
        6. restore from shared storage
        :return:
        """
        self.util_enable_security()
        self.start_grid()
        self.load_data_with_streamer()
        dump_before = self.calc_checksums_on_client()
        self.su.snapshot_utility('SNAPSHOT', '-type=FULL')
        s1_snapshot_id = self.get_last_snapshot_id()
        self.su.snapshot_utility('SNAPSHOT', '-type=FULL')
        self.su.snapshot_utility('MOVE', '-id=%s' % s1_snapshot_id, '-dest=%s' % self.get_shared_folder_path())
        self.stop_grid()
        self.delete_lfs(delete_snapshots=False)
        self.util_disable_security()
        self.start_grid()
        self.su.snapshot_utility('RESTORE', '-src=%s' % self.get_shared_folder_path(), '-id=%s' % s1_snapshot_id)
        result = self.ignite.find_exception_in_logs('AssertionError|Exception')
        assert not result, "Found some exceptions!"
        # print_blue('res:')
        # print_blue(result)
        dump_after = self.calc_checksums_on_client()
        assert dump_after == dump_before, "Checksums failed!"

    def util_enable_security(self):
        self.set_current_context('default')
        self.cu.enable_authentication(self.auth_login, self.auth_password)
        self.su.enable_authentication(self.auth_login, self.auth_password)
        self.ignite.enable_authentication(self.auth_login, self.auth_password)

    def util_disable_security(self):
        self.set_current_context('insecure')
        self.cu.disable_authentication()
        self.su.disable_authentication()
        self.ignite.disable_authentication()

