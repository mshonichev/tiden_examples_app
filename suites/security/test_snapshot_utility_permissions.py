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

from tiden_gridgain.regresstestcase import RegressTestCase
from tiden.util import is_enabled, with_setup, print_blue, test_case_id, print_green, print_red, attr

from pprint import PrettyPrinter
from tiden.priority_decorator import test_priority

from tiden.configuration_decorator import test_configuration

@test_configuration(['zookeeper_enabled'])
class TestSnapshotUtilityPermissions(RegressTestCase):

    snapshot_ids = {}

    def setup(self):
        default_context = self.contexts['default']
        default_context.add_context_variables(
            authentication_enabled=True,
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            auth_login='server',
            auth_password='server',
        )

        self.auth_creds = [
            'no_access',
            'task_execute',
            'admin_ops',
            'admin_cache',
        ]

        for auth_cred_name in self.auth_creds:
            context = self.create_test_context(auth_cred_name)
            context.add_context_variables(
                authentication_enabled=True,
                zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
                auth_login=auth_cred_name,
                auth_password=auth_cred_name,
            )

        pp = PrettyPrinter()
        print_blue('Credentials: \n' + pp.pformat(self.auth_creds))

        super().setup()

    def setup_test(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()
            default_context = self.contexts['default']
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString()
            )
            default_context.build_and_deploy(self.ssh)
        super().setup_test()

    def teardown(self):
        self.delete_lfs()

    def teardown_test(self):
        self.stop_grid()
        self.delete_lfs(delete_snapshots=False)
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()

    @test_priority.NORMAL(-2)
    @test_case_id(81478)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_LIST_command(self):
        """
        Run snapshot utility LIST command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility('LIST')
                print_green('Snapshot Utility LIST command could be executed with "%s" credentials ' % auth_cred)
            except Exception as e:
                print_red('Snapshot Utility LIST command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(-1)
    @test_case_id(81479)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_SNAPSHOT_command(self):
        """
        Run snapshot utility SNAPSHOT command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()
        self.load_data_with_streamer()

        for auth_cred in self.auth_creds:
            self.su.snapshot_utility('SNAPSHOT', '-type=FULL')
            self.snapshot_ids[auth_cred] = self.get_last_snapshot_id()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility('SNAPSHOT', '-type=FULL')
                self.snapshot_ids[auth_cred] = self.get_last_snapshot_id()
                print_green('Snapshot Utility SNAPSHOT command could be executed with "%s" credentials ' % auth_cred)

            except Exception as e:
                print_red('Snapshot Utility SNAPSHOT command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(+1)
    @test_case_id(81480)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_INFO_command(self):
        """
        Run snapshot utility INFO command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility('INFO', '-id=%s' % self.snapshot_ids[auth_cred])
                print_green('Snapshot Utility INFO command could be executed with "%s" credentials ' % auth_cred)
            except Exception as e:
                print_red('Snapshot Utility INFO command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(+3)
    @test_case_id(81481)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_RESTORE_command(self):
        """
        Run snapshot utility RESTORE command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility('RESTORE', '-id=%s' % self.snapshot_ids[auth_cred])
                print_green('Snapshot Utility RESTORE command could be executed with "%s" credentials ' % auth_cred)
            except Exception as e:
                print_red('Snapshot Utility RESTORE command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(+4)
    @test_case_id(81482)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_DELETE_command(self):
        """
        Run snapshot utility DELETE command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility('DELETE', '-id=%s' % self.snapshot_ids[auth_cred])
                print_green('Snapshot Utility DELETE command could be executed with "%s" credentials ' % auth_cred)
            except Exception as e:
                print_red('Snapshot Utility DELETE command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(+5)
    @test_case_id(81483)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_SCHEDULE_CREATE(self):
        """
        Run snapshot utility SCHEDULE -CREATE command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility(
                    'SCHEDULE',
                    '-command=create',
                    '-name=sched_%s' % auth_cred,
                    '-full_frequency=daily'
                )
                print_green('Snapshot Utility SCHEDULE -create command could be executed with "%s" credentials ' % auth_cred)
                self.su.snapshot_utility(
                    'SCHEDULE',
                    '-delete',
                    '-name=sched_%s' % auth_cred,
                )
            except Exception as e:
                print_red('Snapshot Utility SCHEDULE -create command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(+6)
    @test_case_id(81484)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_SCHEDULE_LIST_command(self):
        """
        Run snapshot utility SCHEDULE -LIST command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility(
                    'SCHEDULE',
                    '-list',
                )
                print_green('Snapshot Utility SCHEDULE -list command could be executed with "%s" credentials ' % auth_cred)
            except Exception as e:
                print_red('Snapshot Utility SCHEDULE -list command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    @test_priority.NORMAL(+7)
    @test_case_id(81485)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_check_permissions_to_run_SCHEDULE_DELETE_command(self):
        """
        Run snapshot utility SCHEDULE -DELETE command against all permissions configuration (see self.auth_creds).
        :return:
        """
        self.util_enable_server_security()
        self.start_grid()

        for auth_cred in self.auth_creds:
            self.su.snapshot_utility(
                'SCHEDULE',
                '-command=create',
                '-name=sys_sched_%s' % auth_cred,
                '-full_frequency=daily'
            )

        for auth_cred in self.auth_creds:
            self.util_enable_client_security(auth_cred)
            try:
                self.su.snapshot_utility(
                    'SCHEDULE',
                    '-delete',
                    '-name=sys_sched_%s' % auth_cred,
                )
                print_green('Snapshot Utility SCHEDULE -delete command could be executed with "%s" credentials ' % auth_cred)
            except Exception as e:
                print_red('Snapshot Utility S0CHEDULE -create command could NOT be executed with "%s" credentials ' % auth_cred)
                pass

    def util_enable_client_security(self, auth_cred_name):
        login = auth_cred_name
        password = auth_cred_name
        context_name = auth_cred_name
        self.set_current_context(context_name)
        self.cu.enable_authentication(login, password)
        self.su.enable_authentication(login, password)
        self.ignite.enable_authentication(login, password)

    def util_enable_server_security(self):
        login = 'server'
        password = 'server'
        self.set_current_context('default')
        self.cu.enable_authentication(login, password)
        self.su.enable_authentication(login, password)
        self.ignite.enable_authentication(login, password)

