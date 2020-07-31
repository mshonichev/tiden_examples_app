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

from builtins import range
from datetime import datetime
from random import Random
from time import time, sleep
from os import environ

from requests import post
from requests.auth import HTTPBasicAuth

from suites.combine.framework.combiner import CombineTestCase
from suites.combine.framework.common import RunType


class TestCombine(CombineTestCase):

    def test_combiner(self):
        # run verify every 7 runs
        verify_frequency = 7

        if self.tiden.config.get('multiple_hosts'):
            self.run_threads()
            return

        if self.tiden.config['combinations'].get('parallel_type', RunType.BASIC_WORKER.value) == RunType.BASIC_WORKER.value:
            self._run_default()
        else:
            self._run_server_based_worker()

    def _run_server_based_worker(self):
        counter = 0
        self.context['suite_name'] = self.current_time_pretty
        while True:
            run = self.get_new_test()
            if run is None:
                return
            cycle_start = datetime.now()
            self.runner(run, cycle_start, counter, RunType.SERVER_BASE_WORKER)
            counter += 1

    def _run_default(self):
        while True:
            cycle_start = datetime.now()
            self.context['suite_name'] = self.current_time_pretty
            for idx, run in enumerate(self.runs):
                self.runner(run, cycle_start, idx, RunType.BASIC_WORKER)

            # combinations.duration
            if self.end_time is not None and self.end_time > time():
                continue
            break

    def run_threads(self):
        if self.tiden.config['combinations']['parallel_type'] == RunType.BASIC_WORKER.value:
            self._run_basic_workers()
        elif self.tiden.config['combinations']['parallel_type'] == RunType.SERVER_BASE_WORKER.value:
            self._run_server_based_workers()

    def _run_server_based_workers(self):
        seeds_server = self.tiden.config['combinations']['seeds_server']
        seed = self.tiden.config.get('seed', Random().randint(1000, 1000000))
        post(f'{seeds_server}/seed', verify=False, json={
            'tests_count': len(self.runs),
            'seed': seed,
            'run_name': f'{self.current_time_pretty} seed: {seed}'.replace(' ', '_')
        })

        custom_suite_name = self.tiden.config.get('custom_suite_name', '')
        if custom_suite_name:
            custom_suite_name = f"combinations.suite_name=.{custom_suite_name}."

        for idx, host in enumerate(str(self.tiden.config['multiple_hosts']).split('+')):
            if idx > 0:
                sleep(30)
            post(self.tiden.config['combinations']['jenkins_run_url'],
                 verify=False,
                 params={
                     "IGNITE_VERSION": self.tiden.config['artifacts']['ignite']['ignite_version'],
                     "GRIDGAIN_VERSION": self.tiden.config['artifacts']['ignite']['gridgain_version'],
                     'OPTION': f'seed={seed} '
                               f'{custom_suite_name}',
                     'HOST_NUM': f'{host}',
                     'BRANCH': environ.get('BRANCH'),
                 },
                 auth=HTTPBasicAuth(self.tiden.config['environment']['jenkins']['user'],
                                    self.tiden.config['environment']['jenkins']['token']))

    def _run_basic_workers(self):
        """
        Separate all runs on equal parts by amount of runs
        Generate random seed for all parts
        Run jenkins tasks with different range and same seed
        """
        hosts = str(self.tiden.config['multiple_hosts']).split('+')
        runs_count = len(self.runs)
        threads = len(hosts)
        one_thread_count = int(runs_count / threads)
        ranges = []
        true_ranges = []
        added = 0
        for i in range(threads):
            # define average amount of runs in one thread
            if i + 1 != threads:
                fresh = added + one_thread_count
                ranges.append(fresh)
                added = fresh
        for idx, item_range in enumerate(ranges):
            # define ranges for each thread
            if idx + 1 == len(ranges):
                # last
                val = 0 if idx == 0 else ranges[idx - 1]
                true_ranges.append([val, item_range])
                true_ranges.append([item_range, len(self.runs) - 1])
            elif idx == 0:
                # first
                true_ranges.append([0, item_range])
            else:
                # middle
                true_ranges.append([ranges[idx - 1], item_range])

        run_name = f'{self.current_time_pretty} hosts: {len(hosts)}'
        run_name = run_name.replace(' ', '_')
        seed = Random().randint(1000, 1000000)
        for idx, range_item in enumerate(true_ranges):

            custom_suite_name = self.tiden.config.get('custom_suite_name')
            if custom_suite_name:
                custom_suite_name = f"combinations.suite_name=.{custom_suite_name}."

            start, end = range_item

            if idx > 0:
                sleep(50)
            post(self.tiden.config['combinations']['jenkins_run_url'],
                 verify=False,
                 params={
                     "IGNITE_VERSION": self.tiden.config['artifacts']['ignite']['ignite_version'],
                     "GRIDGAIN_VERSION": self.tiden.config['artifacts']['ignite']['gridgain_version'],
                     'OPTION': f'ranges={start}-{end} '
                               f'run_name={run_name} '
                               f'seed={seed} '
                               f'{custom_suite_name}',
                     'HOST_NUM': f'{hosts[idx]}',
                     'BRANCH': environ.get('BRANCH'),
                 },
                 auth=HTTPBasicAuth(self.tiden.config['environment']['jenkins']['user'],
                                    self.tiden.config['environment']['jenkins']['token']))

