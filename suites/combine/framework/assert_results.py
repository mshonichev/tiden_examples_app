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

from datetime import datetime
from itertools import chain
from os.path import join
from re import search
from uuid import uuid4

from requests import post

from tiden import util_sleep_for_a_while, log_print
from tiden.ignite import Ignite
from tiden.tidenconfig import TidenConfig
from suites.combine.framework.common import step


class AssertResultsMixin:

    def __init__(self):
        self.context: dict = None
        self.tiden: TidenConfig = None
        self.cluster: Ignite = None

        # limit amount of fails found in cluster
        self.fails_processed_limit = 600

        self.exception_patterns = {'\\] Fail': 25,
                                   '\\] Critical': 25,
                                   'Error: Failed': 25,
                                   '\\(err\\) Failed': 25,
                                   ': Failed': 25}

    @step(lambda args, kwargs: 'Assert results')
    def assert_results(self, allowed_asserts: list, cycle_start: datetime, start_time: datetime, base_name: str):
        """
        Get all exceptions from all files on remote host

        :param allowed_asserts:     filters list [(method, args), (method, args), ... ]
        :param cycle_start:         latest full cluster clean
        :param start_time:          case start time
        :param base_name:           local directory name
        """

        log_print('assert')
        util_sleep_for_a_while(10)
        to_write_all = []

        # find all log files on remote host
        logs_to_check = []
        for host, results in self.cluster.ssh.exec([f"ls {self.tiden.config['rt']['remote']['test_dir']}"]).items():
            for file_name in results[0].split('\n'):
                if file_name.endswith('.log'):
                    logs_to_check.append({
                        'log_path': f"{self.tiden.config['rt']['remote']['test_dir']}/{file_name}",
                        'name': file_name,
                        'host': host
                    })

        # go through all patterns with errors
        found_fails = self.cluster.find_fails(files_to_check=logs_to_check,
                                              store_files=self.tiden.config['rt']['test_module_dir'],
                                              ignore_node_ids=True)
        for file_name, fail_info in found_fails.items():
            exceptions_on_this_cycle = False
            for exceptions_info in fail_info['exceptions']:
                found_time: datetime = exceptions_info.get('time')
                if found_time and found_time > start_time:
                    if not exceptions_on_this_cycle:
                        exceptions_on_this_cycle = True
                    valid_exception = False
                    for assert_item in allowed_asserts:
                        if assert_item['method'](*assert_item['args'], exceptions_info['exception']):
                            valid_exception = True
                            break
                    if not valid_exception:
                        to_write_all.append({
                            'time': found_time,
                            'file': file_name,
                            'text': '\n'.join(exceptions_info['exception'])
                        })
                elif exceptions_on_this_cycle:
                    to_write_all.append({
                        'file': file_name,
                        'text': '\n'.join(exceptions_info['exception'])
                    })

        if to_write_all:
            # aggregate fails in one file
            self.context['step_failed'] = 'Found exceptions in logs!'
            self.context['report']['status'] = 'failed'
            log_print("Found exceptions!", color='red')

            fails_text = ''
            with_time = [i for i in to_write_all if i.get('time')]
            with_time.sort(key=lambda i: i['time'])
            for exception_with_time in with_time:
                fails_text += f'\n\n{exception_with_time["file"]} {exception_with_time["time"].isoformat()}\n' \
                              f'{exception_with_time["text"]}\n'
            without_time = [i for i in to_write_all if not i.get('time')]
            without_time.sort(key=lambda i: i['file'])
            for exception_without_time in without_time:
                fails_text += f'\n\n{exception_without_time["file"]}\n' \
                              f'{exception_without_time["text"]}\n'

            if self.tiden.config['combinations'].get('save_attachments'):
                run_dir = self.ensure_dir(join(self.tiden.config['rt']['test_module_dir'], cycle_start.isoformat()))
                combine_run_dir = self.ensure_dir(join(run_dir, base_name))
                with open(join(combine_run_dir, 'fails.log'), 'w') as f:
                    f.write(fails_text)

            files_unique_name = f'{uuid4()}_fails.log'
            files_receiver_url = self.tiden.config['environment'].get('report_files_url')
            if files_receiver_url:
                post(f'{files_receiver_url}/files/add',
                     headers={'filename': files_unique_name},
                     files={'file': fails_text.encode('utf-8')})
            self.context['attachments'].append({
                'name': 'fails.log',
                'source': files_unique_name,
                'type': 'file'
            })
            self.download_run_logs()
        log_print('end up with assert')

    def assert_pattern(self, pattern, exception_lines: list = None):
        """
        Default filter to skip all expected exceptions
        :param pattern:     expected exception filter
        :param results:     exceptions found for each node
        :return:            filtered exceptions
        """
        fail_str = '\n'.join(exception_lines)
        return search(pattern, fail_str)

    def get_allowed_asserts(self, run):
        """
        Get patterns depend on current case
        Examples: if action is nodes_join
                  then take out from self.context['options']['allowed_exceptions'] by key nodes_join
                  and for each form new assert object: {'method': method, 'args': args}
        :return:  all found asserts [{'method': method, 'args': args}, {'method': method2, 'args': args2}]
        """
        asserts = []
        get_data = lambda param: [param] if isinstance(param, str) else param
        active_parts = ['setup_cluster', 'operations', 'action']
        active_parts = list(chain(*[get_data(run[part]) for part in active_parts]))
        active_parts += ['common']

        for name, asserts_list in self.context['options']['allowed_exceptions'].items():

            if '+' in name:
                # if 2 or more parts in target name then use this assert
                name_parts = name.split('+')
                if len([name_part for name_part in name_parts if name_part in active_parts]) < 2:
                    continue
            elif name not in active_parts:
                continue

            if not isinstance(asserts_list, list):
                asserts_list = [asserts_list]
            for assert_method in asserts_list:
                if isinstance(assert_method, str):
                    asserts.append({
                        'method': self.assert_pattern,
                        'args': [assert_method]
                    })
                else:
                    asserts.append({
                        'method': assert_method[0],
                        'args': assert_method[1:],
                    })
        return asserts

