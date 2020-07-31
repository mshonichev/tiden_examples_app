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

import os.path

from tiden_gridgain.case.regresstestcase import RegressTestCase
from tiden.util import is_enabled, with_setup, print_blue, test_case_id, attr, known_issue, log_print
from pprint import PrettyPrinter
from tiden_gridgain.piclient.piclient import PiClient
from tiden.configuration_decorator import test_configuration
from tiden.assertions import tiden_assert, tiden_assert_equal, tiden_assert_not_equal

@test_configuration(['zookeeper_enabled'])
class TestSecurityAccess(RegressTestCase):

    def setup(self):
        self.auth_creds = {
            '': {
                'name': 'server',
                'context': 'default',
            },
            'read_only': {
                'name': 'read_only',
                'context': 'read_only',
            },
            'no_access': {
                'name': 'no_access',
                'context': 'no_access',
            },
        }
        pp = PrettyPrinter()

        for auth_cred_name in self.auth_creds:
            # aka 'server_login' / 'server_password', etc.
            default_login = self.auth_creds[auth_cred_name]['name'] + '_user'
            default_password = self.auth_creds[auth_cred_name]['name'] + '_password'
            self.auth_creds[auth_cred_name].update({
                'login': self.config.get('auth_' + auth_cred_name + 'login', default_login),
                'password': self.config.get('auth_' + auth_cred_name + 'password', default_password),
            })
            context_name = self.auth_creds[auth_cred_name]['context']
            if context_name != 'default':
                context = self.create_test_context(context_name)
            else:
                context = self.contexts['default']

            context.add_context_variables(
                authentication_enabled=True,
                zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
                auth_login=self.auth_creds[auth_cred_name]['login'],
                auth_password=self.auth_creds[auth_cred_name]['password'],
            )

        print_blue('Credentials: \n' + pp.pformat(self.auth_creds))
        super().setup()

    def setup_test(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()
            for context_name in self.contexts.keys():
                default_context = self.contexts[context_name]
                default_context.add_context_variables(
                    zoo_connection=self.zoo._get_zkConnectionString()
                )
                default_context.build_and_deploy(self.ssh)
        super().setup_test()

    def teardown_test(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()
        super().teardown_test()

    @known_issue('GG-14090')
    @with_setup(setup_test, teardown_test)
    @test_case_id(81471)
    @attr('common')
    def test_basic_cache_access(self):
        """
        1. read only user is enabled access to only `cache_ro_*`
        2. start grid
        3. populate caches with data and calc hashsums

        4. calc hashsums from client with read-only access to some caches and no access to other caches
        5. cache sums dump for read only client must not contain information about caches client has no access to
        """
        self.util_enable_client_security('')  # load data with 'server' access
        self.start_grid(activate_on_particular_node=1)
        self.load_data_with_streamer()

        dump_before = self.calc_checksums_distributed()
        # self.util_save_dump('dump_all.log', dump_before)
        checksums_before = self.convert_to_dict(dump_before)
        self.util_enable_client_security('read_only')
        checksums_after = {}
        try:
            dump_after = self.calc_checksums_distributed(config_file=self.get_client_config('read_only'))
            checksums_after = self.convert_to_dict(dump_after)
        except Exception as e:
            log_print('Exception in calc_checksums_distributed method:\n{}'.format(e))

        # self.util_save_dump('dump_read_only.log', dump_before)

        for cache_name, checksum in checksums_before.items():
            if 'cache_no' in cache_name:
                tiden_assert_not_equal(checksum, checksums_after.get(
                    cache_name), f"Checksums must differ due to no access to several cache {cache_name}")
            else:
                tiden_assert_equal(checksum, checksums_after.get(cache_name),
                                   f"Checksums must be equal for cache {cache_name}")

    @staticmethod
    def convert_to_dict(checksums):
        from re import match
        checksums_dict = {}
        for checksum in checksums.split('\n'):
            m = match('Cache \'(.*)\' .*hash sum: (.*)', checksum)
            if m:
                checksums_dict[m.group(1)] = m.group(2)
        return checksums_dict

    @with_setup(setup_test, teardown_test)
    @test_case_id(81472)
    @attr('common')
    def test_cache_groups_access(self):
        """
        1. two caches in the same cache group, read only user has access to only part of caches in the group
        2. start grid
        3. populate caches with data and calc hashsums
        4. start client with read-only access to some caches and no access to other caches

        5. try to read from read only cache - must succeed
        6. try to read from inaccessible cache - must fail
        7. try to write to read only cache - must fail
        8. try to write to read-write cache - must succeed
        :return:
        """
        self.util_enable_client_security('')  # load data with 'server' access
        self.start_grid()
        self.load_data_with_streamer()
        with PiClient(self.ignite, self.get_client_config('read_only'), nodes_num=1) as piclient:
            ignite = piclient.get_ignite()
            cache_names = ignite.cacheNames()
            cache_number_utils = piclient.get_gateway().entry_point.getUtilsService().getNumberUtils()

            for cache_name in ['cache_ro_1', 'cache_rw_1', 'cache_no_1', 'cache_ro_2', 'cache_rw_2', 'cache_no_2']:
                assert cache_names.contains(cache_name), "Expected cache not found!"
                expect_read_ok = False
                expect_write_ok = False
                if '_rw_' in cache_name:
                    expect_read_ok = True
                    expect_write_ok = True
                elif '_ro_' in cache_name:
                    expect_read_ok = True
                    expect_write_ok = False

                cache = ignite.cache(cache_name)
                key = 0
                val = None
                if expect_read_ok:
                    val = cache_number_utils.getTypedKey(cache, key, 'long')
                    print_blue("Read '%s' from '%s' - OK" % (val, cache_name))
                    assert val
                else:
                    try:
                        val = cache_number_utils.getTypedKey(cache, key, 'long')
                        assert val is None
                    except Exception as e:
                        print_blue("NOT Read from '%s' : %s - OK" % (cache_name, str(e)))
                    else:
                        assert False, "Expected no READ access to '%s'!" % cache_name

                new_val = 12345
                if expect_write_ok:
                    cache_number_utils.putTypedKey(cache, key, new_val, 'long')
                    print_blue("Write '%s' to '%s' - OK" % (new_val, cache_name))
                else:
                    try:
                        cache_number_utils.putTypedKey(cache, key, new_val, 'long')
                    except Exception as e:
                        print_blue("NOT Write '%s' to '%s' : %s - OK" % (str(new_val), cache_name, str(e)))
                        if expect_read_ok:
                            old_val = cache_number_utils.getTypedKey(cache, key, 'long')
                            print_blue("Read '%s' from '%s' - OK" % (old_val, cache_name))
                            assert old_val.toString() == val.toString(), "Seems that value was overwritten anyway!"
                    else:
                        assert False, "Expected no WRITE access to '%s'!" % cache_name

    def util_enable_client_security(self, auth_cred_name):
        login = self.auth_creds[auth_cred_name]['login']
        password = self.auth_creds[auth_cred_name]['password']
        context_name = self.auth_creds[auth_cred_name]['context']
        self.set_current_context(context_name)
        self.cu.enable_authentication(login, password)
        self.su.enable_authentication(login, password)
        self.ignite.enable_authentication(login, password)

    def util_save_dump(self, name, data):
        with open(os.path.join(self.config['suite_var_dir'], name), 'w') as f:
            f.write(data)
            f.close()

