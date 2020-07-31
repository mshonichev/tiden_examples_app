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

from subprocess import Popen

from tiden import tiden_assert
from tiden.dockermanager import DockerManager
from tiden_gridgain.case.regresstestcase import RegressTestCase
from tiden.util import is_enabled, with_setup, print_blue, log_print, test_case_id, attr, util_sleep_for_a_while
import os.path
from pprint import PrettyPrinter
from tiden_gridgain.piclient.piclient import PiClient


class TestJaasSecurity(RegressTestCase):

    def start_and_config_jass_docker(self):
        jaas_remote_config_path = self.config['rt']['remote']['test_module_dir']
        jaas_host = self.config['environment']['server_hosts'][0]
        # replace host in jaas configs
        self.ssh.exec([f'sed -i \'s/_HOST_/{jaas_host}/g\' {jaas_remote_config_path}/jaas.config'])
        self.ssh.exec([f'sed -i \'s/_HOST_/{jaas_host}/g\' {jaas_remote_config_path}/jaas_ssl.config'])
        # change permission of docker start script and run it
        self.ssh.exec_on_host(jaas_host,
                              ['docker run --name ldap -d -p 389:10389 -p 636:10636 openmicroscopy/apacheds'])
        util_sleep_for_a_while(5, 'wait ldap docker start')
        # setuo jaas users
        for auth_cred_name in self.auth_creds.values():
            user = auth_cred_name['user']
            password = auth_cred_name['pwd']
            description = auth_cred_name['description']
            log_print(f'Try to add user {user}:{password} with description {description}')
            for file in ['new_user', 'add_description', 'set_password']:
                self.ssh.exec([
                    f'sed \'s/_USER_/{user}/g\' {jaas_remote_config_path}/{file}.ldif | '
                    f'sed \'s/_DESCRIPTION_/{description}/g\' | '
                    f'sed \'s/_PASSWORD_/{password}/g\' > {jaas_remote_config_path}/{file}_{user}.ldif'])
                ldap_command = f"ldapmodify -a -D uid=admin,ou=system -w secret -h {jaas_host}:389 " \
                               f"-f {jaas_remote_config_path}/{file}_{user}.ldif"
                log_print(f'execute command - "{ldap_command}"')
                result = self.ssh.exec_on_host(jaas_host, [ldap_command])
                log_print(result)
            log_print(f'Successfully add user {user} to ldap', color='green')

    def setup(self):
        self.auth_creds = {
            '': {
                'user': 'gg_admin',
                'pwd': 'qwe123',
                'context': 'default',
                'description': '{ "defaultAllow": "true" }'
            },
            'read_only': {
                'user': 'read_only',
                'pwd': 'qwe123',
                'context': 'read_only',
                'description': '{'
                               '{ "cache": "cache_ro*", "permissions":["CACHE_READ"] }, '
                               '{ "cache": "cache_rw*", "permissions":["CACHE_READ", "CACHE_PUT", "CACHE_REMOVE"] }, '
                               '{ "task": "*", "permissions":["TASK_EXECUTE"] }, '
                               '{ "system":["ADMIN_CACHE", "CACHE_CREATE"] }, '
                               '"defaultAllow":"false"'
                               '}'
            },
            'utility': {
                'user': 'utility',
                'pwd': 'qwe123',
                'context': 'utility',
                'description': '{ "defaultAllow":"true" }'
            },
        }
        pp = PrettyPrinter()

        for auth_cred_name in self.auth_creds:
            context_name = self.auth_creds[auth_cred_name]['context']
            if context_name != 'default':
                context = self.create_test_context(context_name)
            else:
                context = self.contexts['default']

            context.add_context_variables(
                ssl_enabled=is_enabled(self.config.get('ssl_enabled')),
                authentication_enabled=True,
                client_key_store_file_path='%s/client.jks' %
                                           self.config['rt']['remote']['test_module_dir'],
                server_key_store_file_path='%s/server.jks' %
                                           self.config['rt']['remote']['test_module_dir'],
                trust_store_file_path='%s/trust.jks' %
                                      self.config['rt']['remote']['test_module_dir'],
                zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
                pitr_enabled=is_enabled(self.config.get('pitr_enabled')),
                rolling_updates_enabled=is_enabled(self.config.get('rolling_updates_enabled')),
                auth_login=self.auth_creds[auth_cred_name]['user'],
                auth_password=self.auth_creds[auth_cred_name]['pwd'],
            )

        self.cu.enable_authentication('utility', 'qwe123')
        self.su.enable_authentication('utility', 'qwe123')

        print_blue('Credentials: \n' + pp.pformat(self.auth_creds))
        super().setup()

    def clear_host_from_docker(self):
        self.docker_manager = DockerManager(self.config, self.ssh)
        containers_count = self.docker_manager.print_and_terminate_containers(True)
        if containers_count == 0:
            tiden_assert('Tests requare no dockers on host')
        self.docker_manager.remove_containers(image_name='openmicroscopy/apacheds')
        self.docker_manager.remove_images(name='openmicroscopy/apacheds:latest')

    def setup_test(self):
        self.clear_host_from_docker()
        self.start_and_config_jass_docker()
        if self.get_context_variable('zookeeper_enabled'):
            self.start_zookeeper()
            default_context = self.contexts['default']
            default_context.add_context_variables(
                zoo_connection=self.zoo._get_zkConnectionString()
            )
            default_context.build_and_deploy(self.ssh)
        super().setup_test()

    def teardown_test(self):
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()
        super().teardown_test()
        self.clear_host_from_docker()

    @with_setup(setup_test, teardown_test)
    @test_case_id(81471)
    @attr('jaas')
    def test_jaas_basic_cache_access(self):
        """
        1. read only user is enabled access to only `cache_ro_*`
        2. start grid
        3. populate caches with data and calc hashsums

        4. calc hashsums from client with read-only access to some caches and no access to other caches
        5. cache sums dump for read only client must not contain information about caches client has no access to
        """
        self.util_enable_client_security('')  # load data with 'server' access
        self.set_server_jvm_security_options()

        self.start_grid(activate_on_particular_node=1)
        self.load_data_with_streamer()

        self.util_enable_client_security('')
        dump_before = self.calc_checksums_on_client(jvm_options=self.get_client_jvm_security_options())
        self.util_save_dump('dump_all.log', dump_before)
        self.util_enable_client_security('read_only')
        dump_after = self.calc_checksums_on_client(config_file=self.get_client_config('read_only'),
                                                   jvm_options=self.get_client_jvm_security_options(),
                                                   allow_exception=False)
        self.util_save_dump('dump_read_only.log', dump_after)
        assert not dump_before == dump_after, "Checksums must differ due to no access to several caches!"

    @with_setup(setup_test, teardown_test)
    @test_case_id(81472)
    @attr('jaas')
    def test_jaas_cache_groups_access(self):
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
        self.set_server_jvm_security_options()

        self.start_grid()
        self.load_data_with_streamer()
        self.util_enable_client_security('read_only')
        with PiClient(self.ignite, self.get_client_config('read_only'),
                      nodes_num=1,
                      jvm_options=self.get_client_jvm_security_options()) as piclient:
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

    def get_client_jvm_security_options(self):
        if self.get_context_variable('ssl_enabled'):
            return ['-Djavax.net.ssl.keyStore=%s/client.jks' %
                    self.config['rt']['remote']['test_module_dir'],
                    '-Djavax.net.ssl.keyStorePassword=123456',
                    '-Djavax.net.ssl.trustStore=%s/trust.jks' %
                    self.config['rt']['remote']['test_module_dir'],
                    '-Djavax.net.ssl.trustStorePassword=123456']
        else:
            return []

    def set_server_jvm_security_options(self):
        jaas_filename = 'jaas.config' if not self.get_context_variable('ssl_enabled') else 'jaas_ssl.config'

        if self.get_context_variable('ssl_enabled'):
            # https://ggsystems.atlassian.net/browse/GG-14146
            self.ignite.set_node_option('*', 'jvm_options',
                                        ['-Djava.security.auth.login.config=%s/%s' %
                                         (self.config['rt']['remote']['test_module_dir'], jaas_filename),
                                         '-Djavax.net.ssl.keyStore=%s/server.jks' %
                                         self.config['rt']['remote']['test_module_dir'],
                                         '-Djavax.net.ssl.keyStorePassword=123456',
                                         '-Djavax.net.ssl.trustStore=%s/trust.jks' %
                                         self.config['rt']['remote']['test_module_dir'],
                                         '-Djavax.net.ssl.trustStorePassword=123456'
                                         ])
        else:
            self.ignite.set_node_option('*', 'jvm_options',
                                        ['-Djava.security.auth.login.config=%s/jaas.config' %
                                         self.config['rt']['remote']['test_module_dir'],
                                         '-Djava.security.debug=all'
                                         ])

    def util_enable_client_security(self, auth_cred_name):
        login = self.auth_creds[auth_cred_name]['user']
        password = self.auth_creds[auth_cred_name]['pwd']
        context_name = self.auth_creds[auth_cred_name]['context']
        self.set_current_context(context_name)
        self.ignite.enable_authentication(login, password)

    def util_save_dump(self, name, data):
        with open(os.path.join(self.config['suite_var_dir'], name), 'w') as f:
            f.write(data)
            f.close()

