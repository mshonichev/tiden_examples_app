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


import argparse
import sys
import datetime
from pathlib import Path, PurePath
import yaml


# Command line parser
def create_parser():
    p = argparse.ArgumentParser()
    p.add_argument('-p', required=True, help="Yardstick properties file")
    return p


# Dump cases to yaml file
def dump_data_to_yaml(some_dict, output_file):
    if Path(output_file).exists():
        timestamp = datetime.datetime.now().strftime("%y%m%d-%H%m%S")
        # Create file with {timestamp} before the name in case if file already exists

        output_file = Path(PurePath(output_file).parent).joinpath(
            '{timestamp}-{output_file}'.format(timestamp=timestamp, output_file=PurePath(output_file).name))
    print('Dump data to {output_file}'.format(output_file=output_file))
    with open(output_file, 'w') as outfile:
        yaml.dump(some_dict, outfile, default_flow_style=False)


def write_methods_code(out_file, data):
    if Path(out_file).exists():
        timestamp = datetime.datetime.now().strftime("%y%m%d-%H%m%S")
        # Create file with {timestamp} before the name in case if file already exists
        output_file = Path(PurePath(out_file).parent).joinpath(
            '{timestamp}-{out_file}'.format(out_file=PurePath(out_file).name, timestamp=timestamp))
        print('Dump data to {output_file}'.format(output_file=output_file))
    else:
        output_file = out_file
    with open(output_file, 'a') as outfile:
        for i in data:
            outfile.write(i)


def generate_tiden_methods(dct, methods_file, cases_file):
    tr_cases = {'tests': []}
    methods_list = []
    method_code = "@test_case_id('MUST_BE_REPLACED_WITH_TR_CASE_ID')\n" \
                  "{attrs}\n" \
                  "def test_{method_name}(self):\n" \
                  "    cache_mode = '{cache_mode}'\n" \
                  "    ignite_memory_mode = '{ignite_memory_mode}'\n" \
                  "    additional_driver_args = {add_opts}\n" \
                  "    self.run_benchmark(\n" \
                  "        '{class_name}',\n" \
                  "        '{bench_name_sync_mode_wal_mode}',\n " \
                  "        ignite_memory_mode,\n" \
                  "        cache_mode,\n" \
                  "        additional_driver_args=additional_driver_args)\n" \
                  "\n"

    mem_modes = ['in-memory', 'pds']
    wal_modes = ['fsync', 'log_only', 'background']
    cache_sync_modes = ['primary_sync', 'full_sync']
    for b_nm in dct.keys():
        print('Current benchmark name {b_nm}'.format(b_nm=b_nm))

        for mmode in mem_modes:
            if mmode == 'in-memory':
                for sync_mode in cache_sync_modes:
                    method_name = '{b_nm}_{sync_mode}'.format(b_nm=b_nm, sync_mode=sync_mode)
                    attr = ""
                    if b_nm.startswith('sql'):
                        attr = '@attr(\'{sync_mode}\', \'in-memory\', \'sql_query\')'.format(sync_mode=sync_mode)
                    elif b_nm.startswith('tx'):
                        attr = '@attr(\'{sync_mode}\', \'in-memory\', \'cache\', \'tx\')'.format(sync_mode=sync_mode)
                    elif b_nm.startswith('atomic'):
                        attr = '@attr(\'{sync_mode}\', \'in-memory\', \'cache\', \'atomic\')'.format(
                            sync_mode=sync_mode)
                    elif b_nm.startswith('compute'):
                        attr = '@attr(\'{mmode}\', \'compute\')'.format(mmode=mmode)
                        method_name = b_nm
                    else:
                        print(' ------> Undefined benchmark {b_nm}'.format(b_nm=b_nm))

                    tmp_code = method_code
                    m_code = tmp_code.format(attrs=attr,
                                             method_name=method_name,
                                             cache_mode=sync_mode,
                                             ignite_memory_mode=mmode,
                                             add_opts=str(dct[b_nm]['add_options']),
                                             class_name=dct[b_nm]['class_name'],
                                             bench_name_sync_mode_wal_mode='{b_nm}_{sync_mode}'.format(
                                                 b_nm=b_nm,
                                                 sync_mode=sync_mode))

                    tr_cases['tests'].append({'name': method_name})
                    methods_list.append(m_code)

            elif mmode == 'pds':
                for wmode in wal_modes:
                    for sync_mode in cache_sync_modes:
                        method_name = '{b_nm}_{sync_mode}_{wmode}'.format(b_nm=b_nm, sync_mode=sync_mode, wmode=wmode)
                        attr = ""
                        if b_nm.startswith('sql'):
                            attr = '@attr(\'{sync_mode}\', \'pds\', \'{wmode}_wal\', \'sql_query\')'.format(
                                sync_mode=sync_mode, wmode=wmode)
                        elif b_nm.startswith('tx'):
                            attr = '@attr(\'{sync_mode}\', \'pds\', \'{wmode}_wal\', \'cache\', \'tx\')'.format(
                                sync_mode=sync_mode, wmode=wmode)
                        elif b_nm.startswith('atomic'):
                            attr = '@attr(\'{sync_mode}\', \'pds\', \'{wmode}_wal\', \'cache\', \'atomic\')'.format(
                                sync_mode=sync_mode, wmode=wmode)
                        elif b_nm.startswith('compute'):
                            attr = '@attr({mmode}, compute)'.format(mmode=mmode)
                            method_name = '{b_nm}_{wmode}'.format(b_nm=b_nm, wmode=wmode)
                        else:
                            print(' ------> Undefined benchmark {b_nm}'.format(b_nm=b_nm))

                        tmp_code = method_code
                        m_code = tmp_code.format(attrs=attr,
                                                 method_name=method_name,
                                                 cache_mode=sync_mode,
                                                 ignite_memory_mode='pds_{wmode}'.format(wmode=wmode),
                                                 add_opts=str(dct[b_nm]['add_options']),
                                                 class_name=dct[b_nm]['class_name'],
                                                 bench_name_sync_mode_wal_mode='{b_nm}_{sync_mode}_{wmode}'.format(
                                                     b_nm=b_nm, sync_mode=sync_mode, wmode=wmode))

                        tr_cases['tests'].append({'name': method_name})
                        methods_list.append(m_code)
    print('Will be added %s methods' % len(methods_list))
    print('Will be added %s cases' % len(tr_cases['tests']))
    write_methods_code(methods_file, methods_list)
    if len(tr_cases['tests']) > 0:
        dump_data_to_yaml(some_dict=tr_cases, output_file=cases_file)
    return tr_cases


def prepare_name(bench_name):
    """
    :param bench_name: "tx-pessim-repRead-put-getEntry"
    :return: "tx_pessim_rep_read_put_get_entry"
    """
    bench_name = bench_name.replace('-', '_')
    new_bn = []
    for char in bench_name:
        if char.isupper():
            char = '_' + char.lower()
        new_bn.append(char)
    b_name = ''.join(new_bn)
    return b_name


def remove_spec_characters(line, signs=[]):
    for s in signs:
        line = line.replace(s, '')
    return line


if __name__ == '__main__':
    # Create parser
    parser = create_parser()
    # Yardstick properties file
    namespace = parser.parse_args(sys.argv[1:])
    properties_file = namespace.p
    if not Path(properties_file).exists():
        print('No such file {properties_file}'.format(properties_file=properties_file))
        sys.exit(1)
    bench_dict = {}

    general_options = ['-cfg', '-nn', '-b', '-w', '-d', '-t', '-sm', '-dn', '-sn', '-ds']
    with open(properties_file) as pf:
        p_lines = pf.readlines()

    for l in p_lines:
        if l.startswith('-cfg'):
            curr_options = []
            add_opts = {}
            n_line = remove_spec_characters(l, ['\n', '${ver}', '\\', ',', '-${b}-backup'])
            n_list = n_line.split(' ')
            # Where is benchmark name
            name_index = n_list.index('-ds') + 1
            # Convert names like
            # tx-pessim-repRead-put-getEntry
            # to
            # tx_pessim_rep_read_put_get_entry
            name = prepare_name(n_list[name_index])
            # Where is benchmark class name
            class_index = n_list.index('-dn') + 1
            # Collect all parameter names line -dn, -cfg and so on
            for item in n_list:
                if item.startswith('-'):
                    curr_options.append(item)
            # Are there additional options
            diff_list = list(set(curr_options) - set(general_options))
            if len(diff_list):
                for ops in diff_list:
                    add_opts[ops] = n_list[n_list.index(ops)+1]
            # This dict contains entries like:
            #  tx-optim-rep-read-put-get-entry':
            #       {'class_name': 'IgnitePutGetEntryTxBenchmark', 'add_options': {'-txc': 'OPTIMISTIC'}})
            bench_dict[name] = {'class_name': n_list[class_index], 'add_options': add_opts}

            generate_tiden_methods(bench_dict, 'methods.py', 'cases.yaml')
