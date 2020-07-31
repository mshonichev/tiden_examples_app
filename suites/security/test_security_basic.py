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

from collections import namedtuple

from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.case.regresstestcase import RegressTestCase
from tiden.util import is_enabled, with_setup, test_case_id, attr, log_print, known_issue
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from tiden.utilities import Sqlline
from tiden.assertions import tiden_assert
from tiden.configuration_decorator import test_configuration


@test_configuration(['zookeeper_enabled'])
class TestSecurityBasic(RegressTestCase):
    snapshot_ids = {}

    def setup(self):
        default_context = self.contexts['default']
        default_context.add_context_variables(
            authentication_enabled=False,
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
        )

        default_context = self.create_test_context('auth_enabled')
        default_context.add_context_variables(
            authentication_enabled=True,
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            auth_login='server',
            auth_password='server',
            client_auth_login='client',
            client_auth_password='client',
        )

        context_auth_server = self.create_test_context('auth_enabled_server_creds')
        context_auth_server.add_context_variables(
            authentication_enabled=True,
            zookeeper_enabled=is_enabled(self.config.get('zookeeper_enabled')),
            auth_login='server',
            auth_password='server',
            client_auth_login='server',
            client_auth_password='server',
        )

        super().setup()

    def setup_test_with_ssl(self):
        self.ssl_conn_tuple = self.util_enable_ssl()
        self.setup_test()

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

    def teardown(self):
        self.delete_lfs()

    def teardown_test(self):
        self.stop_grid()
        self.delete_lfs(delete_snapshots=False)
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.stop_zookeeper()

    @test_case_id(1111)
    @with_setup(setup_test_with_ssl, teardown_test)
    @attr('common')
    def test_run_cluster_with_ssl(self):
        """
        Run cluster with SSL. Test client node with SSL can connect. Test Sqlline with SSL can connect to cluster.
        :return:
        """
        self.base_test(ssl_connection=self.ssl_conn_tuple)

    @test_case_id(1111)
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_run_cluster_with_ssl_and_auth(self):
        """
        Run cluster with SSL and auth. Test client node with SSL and auth can connect.
        Test Sqlline with SSL and user/password can connect to cluster.
        :return:
        """
        auth_info = self.util_enable_server_security()
        ssl_conn_tuple = self.util_enable_ssl(context_template='auth_enabled_server_creds')

        self.base_test(ssl_connection=ssl_conn_tuple, auth=auth_info)

    @test_case_id(1111)
    # @known_issue('GG-21795')
    @with_setup(setup_test, teardown_test)
    @attr('common')
    def test_run_cluster_with_ssl_and_auth_with_all_users(self):
        """
        Run cluster with SSL and auth. Test client node with SSL and auth can connect.
        Test Sqlline with SSL and user/password can connect to cluster.
        :return:
        """
        self.util_enable_server_security()
        ssl_conn_tuple = self.util_enable_ssl(context_template='auth_enabled_server_creds')
        self.base_test_with_all_users(ssl_conn_tuple)

    def base_test_with_all_users(self, ssl_connection):
        cache_to_test = 'cache_group_1_001'
        check_commands_read = ['!tables',
                               '!index',
                               '\'select count(*) from \"%s\".ALLTYPESINDEXED;\'' % cache_to_test]

        check_commands_update = ['!tables',
                                 '!index',
                                 '\'update \"%s\".ALLTYPESINDEXED set LONGCOL=1;\'' % cache_to_test]

        expected_read = ['COUNT\(\*\)',
                         '1000']
        expected_update = ['1,000 rows affected']
        expected_for_no_access_user = ['Authorization failed']

        self.set_current_context('ssl_enabled')

        self.start_grid(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config('ssl_enabled'),
                                                    value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                                    end_key=1000,
                                                    allow_overwrite=True)

        users = [
            {'login': 'server', 'password': 'server',
             'read_check': {'run': check_commands_read,
                            'expected': expected_read
                            },
             'update_check': {'run': check_commands_update,
                              'expected': expected_update}},
            {'login': 'admin_user', 'password': 'admin_password',
             'read_check': {'run': check_commands_read,
                            'expected': expected_read
                            },
             'update_check': {'run': check_commands_update,
                              'expected': expected_update}},
            {'login': 'read_only_user', 'password': 'read_only_password',
             'read_check': {'run': check_commands_read,
                            'expected': expected_read
                            },
             'update_check': {'run': check_commands_update,
                              'expected': expected_for_no_access_user}
             },
            {'login': 'no_access_user', 'password': 'no_access_password',
             'read_check': {'run': check_commands_read,
                            'expected': expected_for_no_access_user
                            },
             'update_check': {'run': check_commands_update,
                              'expected': expected_for_no_access_user}
             },
        ]

        def check_output(user_info):
            auth_info = namedtuple('auth_info', 'user password')
            auth = auth_info(user=user['login'], password=user['password'])
            sql_tool = Sqlline(self.ignite, auth=auth, ssl_connection=ssl_connection)

            for operation in ['read_check', 'update_check']:
                output = sql_tool.run_sqlline(user_info[operation].get('run'))
                self.su.check_content_all_required(output, user_info[operation].get('expected'))

        for user in reversed(users):
            check_output(user)

        for user in users:
            check_output(user)

    def base_test(self, **kwargs):
        cache_to_test = 'cache_group_1_001'
        check_commands = ['!tables',
                          '!index',
                          '\'select count(*) from \"%s\".ALLTYPESINDEXED;\'' % cache_to_test]

        expected = ['COUNT\(\*\)',
                    '1000']

        if 'ssl_connection' in kwargs:
            self.set_current_context('ssl_enabled')

        self.start_grid(activate_on_particular_node=1)

        PiClientIgniteUtils.load_data_with_streamer(self.ignite,
                                                    self.get_client_config('ssl_enabled'),
                                                    value_type=ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
                                                    end_key=1000,
                                                    allow_overwrite=True)

        sql_tool = Sqlline(self.ignite, **kwargs)
        output = sql_tool.run_sqlline(check_commands)
        self.su.check_content_all_required(output, expected)

        # base on GG-17465 (validate index with secure cluster)
        self.cu.control_utility('--cache', 'validate_indexes')

    @test_case_id(1111)
    @with_setup(setup_test, teardown_test)
    def test_permissions_over_caches(self):
        """
        Based on GG-14323 (Backport GG-20998).

        This issue checked:
        Apparently, the problem is deeper than I thought in the first place.
        1) SQL permissions have never worked correctly. If query is executed through cache API, then we only check
        permissions against this cache. It means, that if one has read permission to one cache,
        it could be used as a "window" for all other caches.

        Issue with client getting security context should be fixed in 8.5.10. You can uncomment authenticator section in
        client.xml to test this fix without getting the security context.
        :return:
        """
        from pt.piclient.piclient import PiClient
        from pt.piclient.helper.cache_utils import IgniteCacheConfig, IgniteCache
        from pt.piclient.helper.class_utils import create_all_types
        from pt import TidenException

        self.util_enable_server_security()
        self.util_enable_ssl(context_template='auth_enabled')
        self.set_current_context('ssl_enabled')

        self.start_grid(activate_on_particular_node=1)
        try:
            with PiClient(self.ignite, self.get_client_config(), nodes_num=1) as piclient:
                gateway = piclient.get_gateway()
                ignite = piclient.get_ignite()

                # Configure cache1
                cache_config = IgniteCacheConfig()
                cache_config.set_name('cache1')
                cache_config.set_cache_mode('replicated')
                cache_config.set_atomicity_mode('transactional')
                cache_config.set_write_synchronization_mode('full_sync')
                cache_config.set_affinity(False, 32)

                # set query entities
                query_indices_names = gateway.jvm.java.util.ArrayList()
                query_indices_names.add("strCol")
                query_indices = gateway.jvm.java.util.ArrayList()
                query_indices.add(
                    gateway.jvm.org.apache.ignite.cache.QueryIndex().setFieldNames(query_indices_names, True))

                query_entities = gateway.jvm.java.util.ArrayList()
                query_entities.add(
                    gateway.jvm.org.apache.ignite.cache.QueryEntity("java.lang.Integer",
                                                                    ModelTypes.VALUE_ALL_TYPES.value)
                        .addQueryField("strCol", "java.lang.String", None)
                        .addQueryField("longCol", "java.lang.Long", None)
                        .setIndexes(query_indices)
                )

                cache_config.get_config_object().setQueryEntities(query_entities)
                cache_config.get_config_object().setStatisticsEnabled(False)

                ignite.getOrCreateCache(cache_config.get_config_object())

                # Configure cache2
                cache_config2 = IgniteCacheConfig()
                cache_config2.set_name('cache2')
                cache_config2.set_cache_mode('partitioned')
                cache_config2.set_backups(3)
                cache_config2.set_atomicity_mode('transactional')
                cache_config2.set_write_synchronization_mode('full_sync')
                cache_config2.set_affinity(False, 32)

                # set query entities
                query_indices_names2 = gateway.jvm.java.util.ArrayList()
                query_indices_names2.add("strCol")
                query_indices2 = gateway.jvm.java.util.ArrayList()
                query_indices2.add(
                    gateway.jvm.org.apache.ignite.cache.QueryIndex().setFieldNames(query_indices_names2, True))
                query_entities2 = gateway.jvm.java.util.ArrayList()
                query_entities2.add(
                    gateway.jvm.org.apache.ignite.cache.QueryEntity("java.lang.Integer",
                                                                    ModelTypes.VALUE_ALL_TYPES.value)
                        .addQueryField("strCol", "java.lang.String", None)
                        .addQueryField("longCol", "java.lang.Long", None)
                        .setIndexes(query_indices)
                )

                cache_config2.get_config_object().setQueryEntities(query_entities2)
                cache_config2.get_config_object().setStatisticsEnabled(False)

                ignite.getOrCreateCache(cache_config2.get_config_object())

            self.wait_for_running_clients_num(0, 90)
            # Restart client
            with PiClient(self.ignite, self.get_client_config()) as piclient:
                cache1 = IgniteCache('cache1')
                cache2 = IgniteCache('cache2')

                run_num = 3
                for i in range(0, run_num):
                    ('Run %s from %s' % (str(i + 1), run_num))
                    for j in range(i * 100, i * 100 + 101):
                        cache1.put(j, create_all_types(j))
                        cache2.put(j, create_all_types(j))

                    log_print('Create sqlFieldsQueries')
                    sqlFieldsQuery1 = piclient.get_gateway().jvm.org.apache.ignite.cache.query \
                        .SqlFieldsQuery('select * from "cache1".AllTypes')
                    sqlFieldsQuery2 = piclient.get_gateway().jvm.org.apache.ignite.cache.query \
                        .SqlFieldsQuery('select * from "cache2".AllTypes')

                    log_print('Assert sqlFieldsQuery is not empty')
                    tiden_assert(not cache1.cache.query(sqlFieldsQuery1).getAll().isEmpty(),
                                 "Value %s could be selected from cache1" % str(i + 1))

                    try:
                        cache1.cache.query(sqlFieldsQuery2).getAll().isEmpty(), "%s" % str(i + 1)
                    except Exception as e:
                        tiden_assert('Authorization failed' in str(e),
                                     'Expecting "Authorization failed" error when querying from cache2 '
                                     'using cache1 as a proxy')
                        log_print(str(e), color='debug')
                    else:
                        raise TidenException('Expected "Authorization failed" error but did not get one')

        except TidenException as e:
            assert "Some new problem arises during reproducing GG-14323: %s" % e

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
        auth_info = namedtuple('auth_info', 'user password')
        auth_conn_tuple = auth_info(user=login, password=password)
        self.set_current_context('auth_enabled')
        self.cu.enable_authentication(login, password)
        self.su.enable_authentication(login, password)
        self.ignite.enable_authentication(login, password)

        return auth_conn_tuple

    def util_enable_ssl(self, context_template='default'):
        ssl_context = self.create_test_context('ssl_enabled')
        keystore_pass = truststore_pass = '123456'
        ssl_config_path = self.config['rt']['remote']['test_module_dir']
        ssl_params = namedtuple('ssl_conn', 'keystore_path keystore_pass truststore_path truststore_pass')

        self.ssl_conn_tuple = ssl_params(keystore_path='{}/{}'.format(ssl_config_path, 'server.jks'),
                                         keystore_pass=keystore_pass,
                                         truststore_path='{}/{}'.format(ssl_config_path, 'trust.jks'),
                                         truststore_pass=truststore_pass)

        self.cu.enable_ssl_connection(self.ssl_conn_tuple)
        self.su.enable_ssl_connection(self.ssl_conn_tuple)
        default_context_vars = self.contexts[context_template].get_context_variables()

        ssl_context.add_context_variables(
            **default_context_vars,
            ssl_enabled=True,
            ssl_config_path=ssl_config_path,
        )
        ssl_context.set_server_result_config('server_with_ssl.xml')
        ssl_context.set_client_result_config('client_with_ssl.xml')
        self.contexts['ssl_enabled'].build_and_deploy(self.ssh)
        return self.ssl_conn_tuple

