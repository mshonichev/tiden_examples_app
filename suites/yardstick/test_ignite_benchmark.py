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

from tiden.apps.ignite.yardstick import Yardstick
from tiden.apps.profiler import Profiler
from tiden.case.apptestcase import AppTestCase
from tiden.util import *
import numpy as np

# No pain No GridGain


class TestIgniteBenchmark(AppTestCase):

    def __init__(self, *args):
        super().__init__(*args)
        for name in self.tiden.config['artifacts'].keys():
            if args[0]['artifacts'][name].get('type', '') == 'ignite':
                self.add_app(
                    name,
                    app_class_name='ignite',
                )

        self.profiler_app = None
        if self.tiden.config['environment'].get('yardstick', {}).get('profiler') in Profiler.available_profilers:
            self.add_app('profiler')

        self.started = time()
        self.ignite = {}
        self.attempts = self.tiden.config.get('attempts', 3)
        self.ranges = [-3, 10]
        self.backups = self.tiden.config.get('backups', 1)
        self.driver_options = {
            '-w': self.tiden.config.get('warmup', 60),
            '-d': self.tiden.config.get('duration', 60),
            '-b': self.backups,
            '-t': self.tiden.config.get('threads', 64)
        }
        self.server_jvm_options = self.tiden.config['environment'].get('server_jvm_options', [])
        self.client_jvm_options = self.tiden.config['environment'].get('client_jvm_options', [])
        self.base_artifact = None
        self.throughput_latency_probe_csv_name = 'ThroughputLatencyProbe.csv'
        self.data = {}

        self.persistence_enabled = self.tiden.config.get('persistence_enabled', False)
        self.wal_mode = self.tiden.config.get('wal_mode', '')
        self.sync_mode = self.tiden.config.get('sync_mode', 'PRIMARY_SYNC')
        self.cache_mode = self.tiden.config.get('cache_mode', 'PARTITIONED')
        self.ssl_enabled = self.tiden.config.get('ssl_enabled', False)
        self.cache_stats_enabled = self.tiden.config.get('cache_stats_enabled', False)
        self.data_region_max_size_mb = self.tiden.config.get('data_region_max_size_mb', 500)

        # GG EE/UE only
        self.use_auth = self.tiden.config.get('use_auth', False)

        self.ignite_configs = {
            'templates': {
                'server': 'server*.tmpl.xml',
                'driver': 'driver*.tmpl.xml',
                'caches': 'caches*.tmpl.xml'
            }
        }

    def setup(self):
        self.generate_configs()

        super().setup()

        for ignite_app in self.get_app_by_type('ignite'):
            artf_name = ignite_app.name
            self.ignite[artf_name] = ignite_app
            self.ignite[artf_name].yardstick = Yardstick(ignite_app)
            if self.tiden.config['artifacts'][artf_name].get('base') is True:
                self.base_artifact = artf_name
            if self.use_auth:
                self.ignite[artf_name].cu.enable_authentication('client_login', 'client_password')
        if self.tiden.config['environment'].get('yardstick', {}).get('profiler') in Profiler.available_profilers:
            self.profiler_app = self.get_app('profiler')

        self.servers_count = len(
            self.tiden.config['environment']['server_hosts'] * self.tiden.config['environment'].get('servers_per_host',
                                                                                                    1)
        )
        self.drivers_count = len(
            self.tiden.config['environment']['client_hosts'] * self.tiden.config['environment'].get('clients_per_host',
                                                                                                    1)
        )

        log_print("Benchmark environment configuration: %s server(s) X %s driver(s)" % (
            self.servers_count,
            self.drivers_count)
                  )
        self.data['java_versions'] = self.tiden.ssh.exec(['$JAVA_HOME/bin/java -version'])
        self.data['desc'] = "Run %s" ','.join(self.tiden.config['attrib'])
        self.data['ignite'] = {}
        for ignite_name in self.ignite.keys():
            self.data['ignite'][ignite_name] = {
                'ignite_version': self.tiden.config['artifacts'][ignite_name]['ignite_version'],
                'ignite_revision': self.tiden.config['artifacts'][ignite_name]['ignite_revision']
            }
        self.data['benchmarks'] = {}

    def teardown(self):
        # Store data
        write_yaml_file(
            "%s/%s.yaml" % (self.config['rt']['test_module_dir'], 'ignite_benchmark'),
            self.data['benchmarks']
        )
        with open("%s/%s.txt" % (self.config['rt']['test_module_dir'], 'ignite_benchmark'), 'w') as w:
            header = ""
            for benchmark in self.data['benchmarks'].keys():
                artifacts = sorted(self.data['benchmarks'][benchmark]['artifacts'].keys())
                if header == "":
                    header = 'ignite_version:'
                    for artifact in artifacts:
                        header += " %s," % self.data['benchmarks'][benchmark]['artifacts'][artifact]['ignite_version']
                    header = header[:-1] + "\nignite_revision:"
                    for artifact in artifacts:
                        header += " %s," % self.data['benchmarks'][benchmark]['artifacts'][artifact]['ignite_revision']
                    header = "%s\n" % header[:-1]
                    header += '\n ----------------------\n'
                    w.write(header)

                for col in ['avg_client_throughput', 'percents']:
                    data_str = "%s, %s: " % (benchmark, col)
                    for artifact in artifacts:
                        if self.data['benchmarks'][benchmark]['aggr_results'][artifact].get(col):
                            val = self.data['benchmarks'][benchmark]['aggr_results'][artifact][col]
                            if isinstance(val, float):
                                val = "%.2f" % val
                            data_str += " %s," % val
                        else:
                            data_str += " None,"
                    data_str = "%s\n" % data_str[:-1]
                    w.write(data_str)
                lat_details = ''
                # Last attempt contains throughput, latency statistic of all attempts
                last_attempt = self.data['benchmarks'][benchmark]['attempts'][-1]
                for art in last_attempt['ignite']:
                    lat_details += "ver: %s\n" % \
                                    self.data['benchmarks'][benchmark]['artifacts'][art]['ignite_version']
                    lat_details += "rev: %s\n" % \
                                    self.data['benchmarks'][benchmark]['artifacts'][art]['ignite_revision']
                    for h in last_attempt['raw_results'][art]['latency'].keys():
                        lat_details += '>> %s\n' % h
                        for lat in last_attempt['raw_results'][art]['latency'][h].keys():
                            lat_details += '>>> %s: %d\n' % (lat, int(last_attempt['raw_results'][art]['latency'][h][lat]))
                w.write(lat_details)
                w.write('-----------------------------------------\n\n')

    def generate_configs(self):
        for artifact_name in self.tiden.config['artifacts'].keys():
            if self.tiden.config['artifacts'][artifact_name]['type'] == 'ignite':
                ignite_version = str(self.tiden.config['artifacts'][artifact_name]['ignite_version'])
                cur_options = {
                    'backups': self.backups,
                    'persistence_enabled': self.persistence_enabled,
                    'wal_mode': self.wal_mode,
                    'sync_mode': self.sync_mode,
                    'cache_mode': self.cache_mode,
                    'use_memory_configuration': False,
                    'ssl_enabled': self.ssl_enabled,
                    'use_auth': self.use_auth,
                    'cache_stats_enabled': self.cache_stats_enabled,
                    'data_region_max_size_mb': self.data_region_max_size_mb,
                    'enable_event_page_replacement': False
                }
                # Override common config
                if self.tiden.config['artifacts'][artifact_name].get('config'):
                    cur_options.update(self.tiden.config['artifacts'][artifact_name]['config'])

                # Fix naming for old Ignite versions
                if search('^2\.[1-3]', ignite_version) and cur_options['wal_mode'] == 'FSYNC':
                    # Use DEFAULT WAL mode instead of FSYNC
                    cur_options['wal_mode'] = 'DEFAULT'
                if version_num(ignite_version) < version_num('2.3'):
                    # Use MemoryConfiguration for older releases
                    cur_options['use_memory_configuration'] = True
                # https://ggsystems.atlassian.net/browse/GG-21383
                if version_num(ignite_version) >= version_num('8.7.7'):
                    cur_options['enable_event_page_replacement'] = True

                cur_options.update({'addresses': self.tiden.ssh.hosts})
                for cfg_type in self.ignite_configs['templates'].keys():
                    tmpl_file = self.ignite_configs['templates'][cfg_type]
                    cur_options.update(render_template(
                        "%s/%s" % (self.tiden.config['rt']['test_resource_dir'], tmpl_file),
                        artifact_name,
                        cur_options
                    ))
                self.ignite_configs.update(
                    {
                        artifact_name: cur_options.copy()
                    }
                )

    @test_case_id('74995')
    @attr('release_smoke', 'release_full', 'all_cache', 'all_atomic')
    def test_ignite_atomic_put(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutBenchmark',
            'atomic-put',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_atomic_put_random_value(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutRandomValueSizeBenchmark',
            'atomic-put-random-value',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('79595')
    @attr('release_full', 'all_cache', 'all_atomic')
    def test_ignite_atomic_get(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteGetBenchmark',
            'atomic-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('74997')
    @attr('release_smoke', 'release_full', 'all_cache', 'all_atomic')
    def test_ignite_atomic_put_all(self):
        additional_driver_args = {'-bs': 10}
        self.run_benchmark(
            'IgnitePutAllBenchmark',
            'atomic-put-all-bs-10',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_atomic_put_all_bs_100(self):
        additional_driver_args = {'-bs': 100}
        self.run_benchmark(
            'IgnitePutAllBenchmark',
            'atomic-put-all-bs-100',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('162556')
    @attr('all_cache', 'all_atomic')
    def test_ignite_atomic_get_all(self):
        additional_driver_args = {'-bs': 100}
        self.run_benchmark(
            'IgniteGetAllBenchmark',
            'atomic-get-all-bs-100',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('74999')
    @attr('all_cache', 'all_atomic')
    def test_ignite_atomic_put_get(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutGetBenchmark',
            'atomic-put-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75001')
    @attr('all_cache', 'all_tx')
    def test_ignite_tx_invoke(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteInvokeTxBenchmark',
            'tx-invoke',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75003')
    @attr('all_cache', 'all_tx')
    def test_ignite_tx_put(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutTxBenchmark',
            'tx-put',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75005')
    @attr('release_full', 'all_cache', 'all_tx')
    def test_ignite_tx_opt_put(self):
        additional_driver_args = {'-txc': 'OPTIMISTIC'}
        self.run_benchmark(
            'IgnitePutTxImplicitBenchmark',
            'tx-opt-put',
            additional_driver_args=additional_driver_args
        )

    @attr('all_cache', 'all_tx')
    def test_ignite_tx_opt_serial_put(self):
        additional_driver_args = {'-txc': 'OPTIMISTIC', '-txi': 'SERIALIZABLE'}
        self.run_benchmark(
            'IgnitePutTxImplicitBenchmark',
            'tx-opt-serial-put',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_tx_implicit_put(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutTxImplicitBenchmark',
            'tx-implicit-put',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75007')
    @attr('release_smoke', 'release_full', 'all_cache', 'all_tx')
    def test_ignite_tx_put_all(self):
        additional_driver_args = {'-bs': 10}
        self.run_benchmark(
            'IgnitePutAllTxBenchmark',
            'tx-put-all-bs-10',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_tx_put_all_bs_100(self):
        additional_driver_args = {'-bs': 100}
        self.run_benchmark(
            'IgnitePutAllTxBenchmark',
            'tx-put-all-bs-100',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75009')
    @attr('all_cache', 'all_tx')
    def test_ignite_tx_put_get(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutGetTxBenchmark',
            'tx-put-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('162557')
    @attr('all_cache', 'all_tx')
    def test_ignite_tx_get(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteGetTxBenchmark',
            'tx-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('162558')
    @attr('all_cache', 'all_tx')
    def test_ignite_tx_get_all(self):
        additional_driver_args = {'-bs': 100}
        self.run_benchmark(
            'IgniteGetAllTxBenchmark',
            'tx-get-all-bs-100',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75011')
    @attr('release_smoke', 'release_full', 'all_cache', 'all_tx')
    def test_ignite_tx_opt_rep_read_put_get(self):
        additional_driver_args = {'-txc': 'OPTIMISTIC'}
        self.run_benchmark(
            'IgnitePutGetTxBenchmark',
            'tx-opt-repread-put-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75013')
    @attr('release_smoke', 'release_full', 'all_cache', 'all_tx')
    def test_ignite_tx_pess_rep_read_put_get(self):
        additional_driver_args = {'-txc': 'PESSIMISTIC'}
        self.run_benchmark(
            'IgnitePutGetTxBenchmark',
            'tx-pess-repread-put-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75015')
    @attr('release_full', 'all_cache', 'all_tx')
    def test_ignite_tx_opt_serial_put_get(self):
        additional_driver_args = {'-txc': 'OPTIMISTIC', '-txi': 'SERIALIZABLE'}
        self.run_benchmark(
            'IgnitePutGetTxBenchmark',
            'tx-opt-serial-put-get',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75017')
    @attr('release_smoke', 'release_full', 'all_sql_query')
    def test_ignite_sql_query(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlQueryBenchmark',
            'sql-query',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75019')
    @attr('all_sql_query')
    def test_ignite_sql_query_join(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlQueryJoinBenchmark',
            'sql-query-join',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75021')
    @attr('release_full', 'all_sql_query')
    def test_ignite_sql_query_put(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlQueryPutBenchmark',
            'sql-query-put',
            additional_driver_args=additional_driver_args
        )

# >>>>>>>>>>> Byte array as key <<<<<<<<<<<<<<<<<

    def test_ignite_tx_get_all_put_all_bytes_key(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteGetAllPutAllTxBytesKeyBenchmark',
            'tx_get_all_put_all_bytes_key',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_atomic_get_and_put_bytes_key(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteGetAndPutBytesKeyBenchmark',
            'atomic_get_and_put_bytes_key',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_tx_get_and_put_bytes_key(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteGetAndPutTxBytesKeyBenchmark',
            'tx_get_and_put_bytes_key',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_atomic_put_bytes_key(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutBytesKeyBenchmark',
            'atomic_put_bytes_key',
            additional_driver_args=additional_driver_args
        )

# >>>>>>>>>>> Byte array as key <<<<<<<<<<<<<<<<<

    def test_ignite_atomic_page_replacement(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgnitePutGetWithPageReplacements',
            'atomic_cache_page_replacement',
            additional_driver_args=additional_driver_args
        )

# >>>>>>>>>>> HASH JOIN BENCHMARKS <<<<<<<<<<<<<<<<<

    def test_ignite_sql_query_hash_join_hj(self):
        additional_driver_args = {
            '-Dinit': 'true',
            '-DcreateIndex': 'true',
            '-DqryName': 'SQL_HJ_2_TBLS_BY_LONG_FILTERED_BY_IDX',
            '-DfirstParam': 'true',
            '-DsecondParam': 'true',
            '-DfirstParamRange': '900000',
            '-Drange': '100000'
        }
        self.run_benchmark(
            'IgniteSqlHashJoinBenchmark',
            'SQL_HJ_2_TBLS_BY_LONG_FILTERED_BY_IDX',
            additional_driver_args=additional_driver_args,
        )

    def test_ignite_sql_query_hash_join_NL_idx_fx_d(self):
        additional_driver_args = {
            '-Dinit': 'true',
            '-DcreateIndex': 'true',
            '-DqryName': 'SQL_NL_2_TBLS_BY_LONG_JO0_FILTERED_BY_IDX',
            '-DfirstParam': 'true',
            '-DsecondParam': 'true',
            '-DfirstParamRange': '900000',
            '-Drange': '100000'
        }
        self.run_benchmark(
            'IgniteSqlHashJoinBenchmark',
            'SQL_NL_2_TBLS_BY_LONG_JO0_FILTERED_BY_IDX',
            additional_driver_args=additional_driver_args,
        )

    def test_ignite_sql_query_hash_join_nl_idx_d_fx(self):
        additional_driver_args = {
            '-Dinit': 'true',
            '-DcreateIndex': 'true',
            '-DqryName': 'SQL_NL_2_TBLS_BY_LONG_JO1_FILTERED_BY_IDX ',
            '-DfirstParam': 'true',
            '-DsecondParam': 'true',
            '-DfirstParamRange': '900000',
            '-Drange': '100000'
        }
        self.run_benchmark(
            'IgniteSqlHashJoinBenchmark',
            'SQL_NL_2_TBLS_BY_LONG_JO1_FILTERED_BY_IDX',
            additional_driver_args=additional_driver_args,
        )

# >>>>>>>>>>> HASH JOIN BENCHMARKS <<<<<<<<<<<<<<<<<

    # Compute
    @test_case_id('75023')
    @attr('all_compute')
    def test_affcall_compute(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteAffinityCallBenchmark',
            'affcall-compute',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75024')
    @attr('all_compute')
    def test_apply_compute(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteApplyBenchmark',
            'apply-compute',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75025')
    @attr('all_compute')
    def test_broad_compute(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteBroadcastBenchmark',
            'broad-compute',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75026')
    @attr('all_compute')
    def test_exec_compute(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteExecuteBenchmark',
            'exec-compute',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('75027')
    @attr('all_compute')
    def test_run_compute(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteRunBenchmark',
            'run-compute',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('81490')
    @attr('all_sql_query')
    def test_ignite_sql_merge(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlMergeBenchmark',
            'sql-merge',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('81496')
    @attr('all_sql_query')
    def test_ignite_sql_merge_query(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlMergeQueryBenchmark',
            'sql-merge-query',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('81504')
    @attr('all_sql_query')
    def test_ignite_sql_merge_indexed1(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlMergeIndexedValue1Benchmark',
            'sql-merge-indexed1',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('81512')
    @attr('all_sql_query')
    def test_ignite_sql_merge_indexed2(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlMergeIndexedValue2Benchmark',
            'sql-merge-indexed2',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('81520')
    @attr('all_sql_query')
    def test_ignite_sql_merge_indexed8(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlMergeIndexedValue8Benchmark',
            'sql-merge-indexed8',
            additional_driver_args=additional_driver_args
        )

    # Cannot run with more than 1 driver. https://ggsystems.atlassian.net/browse/IGN-11454
    # Removed in 'master' and '8.8-master'. See https://ggsystems.atlassian.net/browse/IGN-11693
    # Replaced with NativeSqlInsertDeleteBenchmark
    @test_case_id('81522')
    def test_ignite_sql_insert_indexed1(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlInsertIndexedValue1Benchmark',
            'sql-insert-indexed1',
            additional_driver_args=additional_driver_args
        )

    # Cannot run with more than 1 driver. https://ggsystems.atlassian.net/browse/IGN-11454
    # Removed in 'master' and '8.8-master'. See https://ggsystems.atlassian.net/browse/IGN-11693
    # Replaced with NativeSqlInsertDeleteBenchmark
    @test_case_id('81524')
    def test_ignite_sql_insert_indexed2(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlInsertIndexedValue2Benchmark',
            'sql-insert-indexed2',
            additional_driver_args=additional_driver_args
        )

    # Cannot run with more than 1 driver. https://ggsystems.atlassian.net/browse/IGN-11454
    # Removed in 'master' and '8.8-master'. See https://ggsystems.atlassian.net/browse/IGN-11693
    # Replaced with NativeSqlInsertDeleteBenchmark
    @test_case_id('81526')
    def test_ignite_sql_insert_indexed8(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlInsertIndexedValue8Benchmark',
            'sql-insert-indexed8',
            additional_driver_args=additional_driver_args
        )

    # Removed in 'master' and '8.8-master'. See https://ggsystems.atlassian.net/browse/IGN-11693
    # Replaced with NativeSqlInsertDeleteBenchmark
    # IgniteSqlDeleteBenchmark does not work:
    #  - https://issues.apache.org/jira/browse/IGNITE-4512
    #  - https://issues.apache.org/jira/browse/IGNITE-9397
    # @test_case_id('81528')
    # @attr('all_sql_query')
    # def test_ignite_sql_delete(self):
    #     additional_driver_args = {'-r': 300000}
    #     self.run_benchmark(
    #         'IgniteSqlDeleteBenchmark',
    #         'sql-delete',
    #         additional_driver_args=additional_driver_args
    #     )

    @test_case_id('81530')
    @attr('all_sql_query')
    def test_ignite_sql_delete_filtered(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlDeleteFilteredBenchmark',
            'sql-delete-filtered',
            additional_driver_args=additional_driver_args
        )

    @test_case_id('81532')
    @attr('all_sql_query')
    def test_ignite_sql_update(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlUpdateBenchmark',
            'sql-update',
            additional_driver_args=additional_driver_args
        )

    # Removed in 'master' and '8.8-master'. See https://ggsystems.atlassian.net/browse/IGN-11693
    @test_case_id('81534')
    def test_ignite_sql_update_filtered(self):
        additional_driver_args = {}
        self.run_benchmark(
            'IgniteSqlUpdateFilteredBenchmark',
            'sql-update-filtered',
            additional_driver_args=additional_driver_args
        )

    @attr('release_full', 'all_sql_query')
    def test_ignite_sql_mixed_date_inline_r1(self):
        additional_driver_args = {
            '--sqlRange': '1',
        }
        self.run_benchmark(
            'NativeSqlMixedDateInlineBenchmark',
            'sql-mixed-date-inline-r1',
            additional_driver_args=additional_driver_args
        )

    @attr('all_sql_query')
    def test_ignite_sql_insert_delete_native_r1(self):
        additional_driver_args = {
            '--sqlRange': 1,
            '--range': 100000  # Default range of 1_000_000 can lead to very long preloading of data
        }
        self.run_benchmark(
            'NativeSqlInsertDeleteBenchmark',
            'sql-insert-delete-native-r1',
            additional_driver_args=additional_driver_args
        )

    @attr('all_sql_jdbc')
    def test_ignite_sql_insert_delete_jdbc_thin_r1(self):
        additional_driver_args = {
            '--sqlRange': 1,
            '-jdbc': 'jdbc:ignite:thin://auto.find/',
            '--range': 100000
        }
        self.run_benchmark(
            'JdbcSqlInsertDeleteBenchmark',
            'sql-insert-delete-jdbc-thin-r1',
            additional_driver_args=additional_driver_args
        )

    @attr('all_sql_query')
    def test_ignite_sql_update_native_r1(self):
        additional_driver_args = {
            '--sqlRange': 1,
            '--range': 100000
        }
        self.run_benchmark(
            'NativeSqlUpdateRangeBenchmark',
            'sql-update-native-r1',
            additional_driver_args=additional_driver_args
        )

    @attr('all_sql_jdbc')
    def test_ignite_sql_update_jdbc_thin_r1(self):
        additional_driver_args = {
            '--sqlRange': 1,
            '-jdbc': 'jdbc:ignite:thin://auto.find/',
            '--range': 100000
        }
        self.run_benchmark(
            'JdbcSqlUpdateBenchmark',
            'sql-update-jdbc-thin-r1',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_sql_update_native_r1000(self):
        additional_driver_args = {
            '-sqlRange': 1000,
            '--range': 100000
        }
        self.run_benchmark(
            'NativeSqlUpdateRangeBenchmark',
            'sql-update-native-r1000',
            additional_driver_args=additional_driver_args
        )

    def test_ignite_sql_update_jdbc_thin_r1000(self):
        additional_driver_args = {
            '--sqlRange': 1000,
            '-jdbc': 'jdbc:ignite:thin://auto.find/',
            '--range': 100000
        }
        self.run_benchmark(
            'JdbcSqlUpdateBenchmark',
            'sql-update-jdbc-thin-r1000',
            additional_driver_args=additional_driver_args
        )

    @attr('GG-18627')
    def test_ignite_sql_limit(self):
        additional_driver_args = {
            '-r': self.tiden.config.get('DATA_RANGE', 10000),
            '-t': 8,
            '-DqryName': 'SQL_LIMIT',
            '-Dinit': 'true',
            '-DqryQuota': self.tiden.config.get('qryQuota', -1)
        }
        add_jvm_opts = (
            '-Xmx16g',
            '-Xms16g',
            "-DIGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE=%s" %
                self.tiden.config.get('IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE', -1),
            "-DIGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE=%s" %
                self.tiden.config.get('IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE', 134217728)
        )
        self.run_benchmark(
            'IgniteSqlMemTrackerBenchmark',
            'SQL_LIMIT',
            additional_driver_args=additional_driver_args,
            add_server_jvm_opts=add_jvm_opts,
            add_driver_jvm_opts=add_jvm_opts
        )

    @attr('GG-18627')
    def test_ignite_sql_sort_idx(self):
        additional_driver_args = {
            '-r': self.tiden.config.get('DATA_RANGE', 10000),
            '-t': 8,
            '-DqryName': 'SQL_SORT_IDX',
            '-Dinit': 'true',
            '-DqryQuota': self.tiden.config.get('qryQuota', -1)
        }
        add_jvm_opts = (
            '-Xmx16g',
            '-Xms16g',
            "-DIGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE=%s" %
                self.tiden.config.get('IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE', -1),
            "-DIGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE=%s" %
                self.tiden.config.get('IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE', 134217728)
        )
        self.run_benchmark(
            'IgniteSqlMemTrackerBenchmark',
            'SQL_SORT_IDX',
            additional_driver_args=additional_driver_args,
            add_server_jvm_opts=add_jvm_opts,
            add_driver_jvm_opts=add_jvm_opts
        )

    @attr('GG-18627')
    def test_ignite_sql_distinct(self):
        additional_driver_args = {
            '-r': self.tiden.config.get('DATA_RANGE', 10000),
            '-t': 8,
            '-DqryName': 'SQL_DISTINCT',
            '-Dinit': 'true',
            '-DqryQuota': self.tiden.config.get('qryQuota', -1)
        }
        add_jvm_opts = (
            '-Xmx16g',
            '-Xms16g',
            "-DIGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE=%s" %
                self.tiden.config.get('IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE', -1),
            "-DIGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE=%s" %
                self.tiden.config.get('IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE', 134217728)
        )
        self.run_benchmark(
            'IgniteSqlMemTrackerBenchmark',
            'SQL_DISTINCT',
            additional_driver_args=additional_driver_args,
            add_server_jvm_opts=add_jvm_opts,
            add_driver_jvm_opts=add_jvm_opts
        )

    @attr('GG-18627')
    def test_ignite_sql_group_idx(self):
        additional_driver_args = {
            '-r': self.tiden.config.get('DATA_RANGE', 10000),
            '-t': 8,
            '-DqryName': 'SQL_GROUP_IDX',
            '-Dinit': 'true',
            '-DqryQuota': self.tiden.config.get('qryQuota', -1)
        }
        add_jvm_opts = (
            '-Xmx16g',
            '-Xms16g',
            "-DIGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE=%s" %
                self.tiden.config.get('IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE', -1),
            "-DIGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE=%s" %
                self.tiden.config.get('IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE', 134217728)
        )
        self.run_benchmark(
            'IgniteSqlMemTrackerBenchmark',
            'SQL_GROUP_IDX',
            additional_driver_args=additional_driver_args,
            add_server_jvm_opts=add_jvm_opts,
            add_driver_jvm_opts=add_jvm_opts
        )

    @attr('GG-18627')
    def test_ignite_sql_group_distinct(self):
        additional_driver_args = {
            '-r': self.tiden.config.get('DATA_RANGE', 10000),
            '-t': 8,
            '-DqryName': 'SQL_GROUP_DISTINCT',
            '-Dinit': 'true',
            '-DqryQuota': self.tiden.config.get('qryQuota', -1)
        }
        add_jvm_opts = (
            '-Xmx16g',
            '-Xms16g',
            "-DIGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE=%s" %
                self.tiden.config.get('IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE', -1),
            "-DIGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE=%s" %
                self.tiden.config.get('IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE', 134217728)
        )
        self.run_benchmark(
            'IgniteSqlMemTrackerBenchmark',
            'SQL_GROUP_DISTINCT',
            additional_driver_args=additional_driver_args,
            add_server_jvm_opts=add_jvm_opts,
            add_driver_jvm_opts=add_jvm_opts
        )

    @attr('GG-18627')
    def test_ignite_sql_group_non_idx(self):
        additional_driver_args = {
            '-r': self.tiden.config.get('DATA_RANGE', 10000),
            '-t': 8,
            '-DqryName': 'SQL_GROUP_NON_IDX',
            '-Dinit': 'true',
            '-DqryQuota': self.tiden.config.get('qryQuota', -1)
        }
        add_jvm_opts = (
            '-Xmx16g',
            '-Xms16g',
            "-DIGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE=%s" %
                self.tiden.config.get('IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE', -1),
            "-DIGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE=%s" %
                self.tiden.config.get('IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE', 134217728)
        )
        self.run_benchmark(
            'IgniteSqlMemTrackerBenchmark',
            'SQL_GROUP_NON_IDX',
            additional_driver_args=additional_driver_args,
            add_server_jvm_opts=add_jvm_opts,
            add_driver_jvm_opts=add_jvm_opts
        )

    def run_benchmark(
            self,
            benchmark_class,
            benchmark_name,
            **kwargs
    ):
        benchmark_started = time()
        self.data['benchmarks'][benchmark_name] = {
            'attempts': [],
            'aggr_results': {},
            'artifacts': self.data['ignite'],
            'class': benchmark_class,
        }
        for ignite_name in self.ignite.keys():
            self.data['benchmarks'][benchmark_name]['aggr_results'][ignite_name] = {}

        acceptable_result = True
        unacceptable_strings = []

        # Attempts loop
        for attempt in range(1, self.attempts + 1):
            self.data['benchmarks'][benchmark_name]['attempts'].append({
                'attempt': attempt,
                'ignite': {}
            })
            iteration_start_time = time()
            driver_options = self.driver_options.copy()
            self.data['benchmarks'][benchmark_name]['attempts'][attempt - 1]['driver_options'] = driver_options

            if kwargs.get('additional_driver_args'):
                for k, v in kwargs.get('additional_driver_args').items():
                    # Dynamic driver options are passed as "-Da=b"
                    if isinstance(k, str) and k.startswith('-D'):
                        driver_options[f"{k}={v}"] = ''
                    else:
                        driver_options[k] = v

            driver_options['-d'] = int(attempt * driver_options['-d'])
            driver_options['-dn'] = benchmark_class
            driver_options['-ds'] = benchmark_name
            driver_options['-sm'] = self.ignite_configs[ignite_name]['sync_mode']
            driver_options['-nn'] = self.servers_count + self.drivers_count
            # Clean up
            self.tiden.ssh.killall('java')

            # Ignite clusters loop
            log_print(' === Iteration %s started ===' % attempt)

            for ignite_name in self.ignite.keys():
                self.data['benchmarks'][benchmark_name]['attempts'][attempt - 1]['ignite'][ignite_name] = {}
                # Get ignite version/revision
                ignite_version = str(self.tiden.config['artifacts'][ignite_name]['ignite_version'])
                ignite_revision = str(self.tiden.config['artifacts'][ignite_name]['ignite_revision'])
                log_print("Ignite %s: version %s, revision %s" % (
                    ignite_name, ignite_version, ignite_revision)
                          )
                # Configuration files for driver/server nodes and cache configuration file
                (server_config, driver_config, cache_config) = self.get_configs(ignite_name)
                self.print_configs(['server', 'driver', 'cache'], (server_config, driver_config, cache_config))

                server_jvm_options = self.server_jvm_options.copy()
                server_jvm_options.extend(
                    [
                        '-DCACHE_CONFIG=%s' % cache_config,
                        "-Xloggc:./gc-%s.log" % time()
                    ]
                )

                if self.tiden.config['artifacts'][ignite_name].get('server_jvm_options'):
                    server_jvm_options.extend(self.tiden.config['artifacts'][ignite_name].get('server_jvm_options'))

                if kwargs.get('add_server_jvm_opts'):
                    server_jvm_options.extend(
                        kwargs.get('add_server_jvm_opts')
                    )

                driver_options['-cfg'] = \
                    "%s/%s" % (self.tiden.config['rt']['remote']['test_module_dir'], driver_config)
                driver_jvm_options = self.client_jvm_options.copy()
                driver_jvm_options.extend(
                    [
                        '-DCACHE_CONFIG=%s' % cache_config,
                        "-Xloggc:./gc-%s.log" % time()
                    ]
                )

                if self.tiden.config['artifacts'][ignite_name].get('client_jvm_options'):
                    driver_jvm_options.extend(self.tiden.config['artifacts'][ignite_name].get('client_jvm_options'))

                if kwargs.get('add_driver_jvm_opts'):
                    driver_jvm_options.extend(
                        kwargs.get('add_driver_jvm_opts')
                    )

                self.ignite[ignite_name].set_grid_name("%s-%s" % (ignite_name, attempt))

                # Profilers JVM options
                if self.profiler_app:
                    self.profiler_app.update_options(
                        **{
                            'warmup': driver_options['-w'],
                            'duration': int(driver_options['-d'] / 2),
                            'bench_name': '%s_%s' % (ignite_name, benchmark_name)
                        }
                    )
                    server_jvm_options.extend(self.profiler_app.get_jvm_options())
                    driver_jvm_options.extend(self.profiler_app.get_jvm_options())

                self.data['benchmarks'][benchmark_name]['attempts'][attempt - 1]['ignite'][ignite_name][
                    'server_jvm_options'] \
                    = server_jvm_options
                self.data['benchmarks'][benchmark_name]['attempts'][attempt - 1]['ignite'][ignite_name][
                    'driver_jvm_options'] \
                    = driver_jvm_options

                try:
                    # Start statistics process
                    # self.tiden.apps['hoststat'].start('dstat')

                    # Start Ignite cluster
                    self.start_ignite_cluster(ignite_name, server_config, server_jvm_options)

                    # Start profiler if needed
                    if self.profiler_app:
                        self.profiler_app.update_options(**{'nodes': self.ignite[ignite_name].nodes})
                        self.profiler_app.start()

                    # Start drivers
                    self.run_drivers(
                        ignite_name,
                        driver_options,
                        driver_jvm_options,
                        attempt
                    )
                finally:
                    if self.profiler_app:
                        self.profiler_app.stop()

                    # Stop Ignite cluster
                    self.stop_ignite_cluster(ignite_name)

                    # Stop statistics process
                    # self.tiden.apps['hoststat'].stop()

                    self.data['benchmarks'][benchmark_name]['started'] = benchmark_started
                    self.data['benchmarks'][benchmark_name]['finished'] = time()
                    self.data['benchmarks'][benchmark_name]['exec_time'] = time() - benchmark_started
                    self.data['benchmarks'][benchmark_name]['drivers'] = self.drivers_count
                    self.data['benchmarks'][benchmark_name]['servers'] = self.servers_count
                    self.data['benchmarks'][benchmark_name]['desc'] = self.data['desc']
                    self.data['benchmarks'][benchmark_name]['java_versions'] = self.data['java_versions']
                    self.data['benchmarks'][benchmark_name]['config'] = self.ignite_configs[ignite_name]
                    self.data['benchmarks'][benchmark_name]['test_method'] = self.tiden.config['rt']['test_method']

            collected = self.collect()
            self.data['benchmarks'][benchmark_name]['attempts'][attempt - 1]['raw_results'] = collected.copy()

            for ignite_name in self.ignite.keys():
                assert collected[ignite_name], "%s: no results found" % ignite_name
                self.ignite[ignite_name].yardstick.draw_charts()

            acceptable_result = True
            aggr_results = self.acceptable_results(collected, self.ranges)

            for artf_name in aggr_results.keys():
                if self.base_artifact == artf_name:
                    continue
                if not aggr_results[artf_name]['acceptable']:
                    acceptable_result = False

                    unacceptable_str = 'avg_client_throughput: %s %s, base %s, diff %s%%' % (
                        artf_name,
                        aggr_results[artf_name]['avg_client_throughput'],
                        aggr_results[self.base_artifact]['avg_client_throughput'],
                        aggr_results[artf_name]['percents'],
                    )

                    log_print(unacceptable_str)
                    unacceptable_strings.append('Attempt: %s. %s' % (attempt, unacceptable_str))

            self.data['benchmarks'][benchmark_name]['aggr_results'] = aggr_results
            log_print(' === Iteration %s completed in %s sec '
                      '===' % (attempt, int(time() - iteration_start_time)))
            if acceptable_result:
                # No further attempts are needed
                break

        assert acceptable_result, "Result is beyond the acceptable range %s\n" % self.ranges + \
                                  "\n".join(unacceptable_strings)

    def run_drivers(self, ignite_name, driver_options, jvm_options, attempt):
        self.ignite[ignite_name].yardstick.configure(
            driver_options,
            jvm_options,
            node_index=50000 + 1000 * attempt
        )
        self.ignite[ignite_name].yardstick.run()

    def start_ignite_cluster(self, name, server_config, jvm_options):
        # Print configuration
        self.ignite[name].set_node_option('*', 'config', server_config)
        self.ignite[name].set_node_option('*', 'jvm_options', jvm_options)
        self.ignite[name].start_nodes()
        self.ignite[name].cu.activate()

    def stop_ignite_cluster(self, name):
        self.ignite[name].cu.deactivate()
        self.ignite[name].stop_nodes()
        self.ignite[name].delete_lfs()

    def print_configs(self, names, ignite_configs):
        for name_idx in range(0, len(names)):
            log_print("Ignite %s configuration: %s" % (names[name_idx], list(ignite_configs)[name_idx]))

    def get_configs(self, ignite_artifact):
        return (
            self.ignite_configs[ignite_artifact]['server'],
            self.ignite_configs[ignite_artifact]['driver'],
            self.ignite_configs[ignite_artifact]['caches']
        )

    def collect(self):
        results = {}
        for ignite_name in self.ignite.keys():
            results[ignite_name] = {}
            result_root_dir = "%s/%s" % (self.tiden.config['rt']['remote']['test_dir'], ignite_name)

            # Cat csv files on driver hosts
            cmd_on_hosts = {}
            for h in self.tiden.config['environment'].get('client_hosts'):
                cmd_on_hosts[h] = ['cat %s.*/*/%s' % (result_root_dir, self.throughput_latency_probe_csv_name)]
            log_print(cmd_on_hosts)
            data = self.tiden.ssh.exec(cmd_on_hosts)

            cur = {}
            raw_csv = {}
            for host in data.keys():
                raw = data[host]
                if not cur.get(host):
                    cur[host] = {
                        'throughput_sum': 0,
                        'throughput_avg': 0,
                        'latency': {
                            'latency_sum': 0,
                            'latency_avg': 0,
                            'latency_min': 0,
                            'latency_max': 0,
                            'latency_P95': 0,
                        },
                        'count': 0,
                    }
                    raw_csv[host] = []
                cur_latency_list = []
                # Parse the results and summarize them
                for line in raw[0].split("\n"):

                    raw_csv[host].append(line)
                    m = search('^(\d+),([0-9\.]+),([0-9\.]+)$', line)
                    if m:
                        cur[host]['throughput_sum'] += float(m.group(2))
                        cur[host]['latency']['latency_sum'] += float(m.group(3))
                        cur[host]['count'] += 1
                        cur_latency_list.append(float(m.group(3)))
                if len(cur_latency_list):
                    arr = np.array(cur_latency_list)
                    p95 = int(float(np.percentile(arr, 95)))
                    cur[host]['latency']['latency_min'] = int(min(cur_latency_list))
                    cur[host]['latency']['latency_max'] = int(max(cur_latency_list))
                    cur[host]['latency']['latency_P95'] = p95
            if len(cur) > 0:
                total_throughput = 0
                total_latency = 0
                hosts_with_data = 0
                lat_stat = {}
                for host in cur.keys():
                    if cur[host]['count'] > 0:
                        cur[host]['throughput_avg'] = int(cur[host]['throughput_sum'] / cur[host]['count'])
                        cur[host]['latency']['latency_avg'] = int(cur[host]['latency']['latency_sum']/cur[host]['count'])
                        total_throughput += cur[host]['throughput_avg']
                        total_latency += cur[host]['latency']['latency_avg']
                        hosts_with_data += 1
                        lat_stat[host] = cur[host]['latency']

                if hosts_with_data > 0:
                    results[ignite_name] = {
                        'total_throughput': total_throughput,
                        'avg_client_throughput': total_throughput / hosts_with_data,
                        'latency': lat_stat,
                        self.throughput_latency_probe_csv_name.replace('.csv', ''): raw_csv
                    }
                    res_str = ''
                    for res_name in results[ignite_name].keys():
                        if isinstance(results[ignite_name][res_name], dict) and res_name != 'latency':
                            continue
                        elif isinstance(results[ignite_name][res_name], dict) and res_name == 'latency':
                            for h in results[ignite_name][res_name].keys():
                                res_str += "\nip_addr: %s latency stat: %s\n" % (h, str(results[ignite_name][res_name][h]))
                        elif isinstance(results[ignite_name][res_name], list):
                            res_str += "%s: %s, " % (res_name, str(results[ignite_name][res_name]))
                            continue
                        else:
                            res_str += "%s: %.2f, " % (res_name, results[ignite_name][res_name])
                    res_str = res_str[:-2]
                    log_print("%s results: %s" % (ignite_name, res_str))
        return results

    def acceptable_results(self, result, ranges):
        base_client_throughput = result[self.base_artifact]['avg_client_throughput']
        aggr_results = {}
        for ignite_name in self.ignite.keys():
            aggr_results[ignite_name] = {}
            cur_client_throughput = result[ignite_name]['avg_client_throughput']
            assert cur_client_throughput != 0, \
                f"Zero average throughput for {ignite_name} artifact. Check logs"
            aggr_results[ignite_name]['avg_client_throughput'] = cur_client_throughput
            if ignite_name == self.base_artifact:
                aggr_results[ignite_name]['acceptable'] = True
                aggr_results[ignite_name]['percents'] = None
                continue
            client_diff = 100 * (cur_client_throughput - base_client_throughput) / base_client_throughput
            log_print("'%s' client throughput against '%s' client throughput: %.2f" %
                      (ignite_name, self.base_artifact, client_diff))

            diff_text = "%.2f" % client_diff
            aggr_results[ignite_name]['percents'] = diff_text
            aggr_results[ignite_name]['ranges'] = ranges
            if ranges[0] < client_diff < ranges[1]:
                aggr_results[ignite_name]['acceptable'] = True
            else:
                aggr_results[ignite_name]['acceptable'] = False
        return aggr_results

