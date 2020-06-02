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
from glob import glob
from json import dumps, loads
from os import environ, getcwd, path, walk, chdir
from re import sub, search
from platform import python_version
from requests import post
from sys import argv, platform
from time import time
from yaml import load, dump, FullLoader
from ftplib import FTP
import zipfile

import sys
sys.path.append(
    path.abspath(path.join(path.dirname(__file__), path.pardir)))


class EcBenchReport:

    def __init__(self):
        self.started = time()

        self.testrail_report_file = None
        self.eco_system_comment = None

        for arg in argv:
            if arg.startswith('--config='):
                with open(arg[9:]) as r:
                    self.conf = load(r, Loader=FullLoader)
            elif arg.startswith('--report='):
                self.report_file = arg[9:]
                with open(self.report_file) as r:
                    self.report = load(r, Loader=FullLoader)
            elif arg.startswith('--testrail_report'):
                self.testrail_report_file = arg[18:]
                with open(self.testrail_report_file) as r:
                    self.testrail_report = load(r, Loader=FullLoader)
            elif arg.startswith('--eco_system_comment'):
                self.eco_system_comment = arg[21:]

        key = next(iter(self.report.values()))
        self.persistence_enabled = key['config']['persistence_enabled']
        self.sync_mode = key['config']['sync_mode']
        self.wal_mode = key['config'].get('wal_mode', '')
        self.cache_mode = key['config'].get('cache_mode', 'PARTITIONED')
        self.ssl_enabled = key['config'].get('ssl_enabled', False)
        if self.persistence_enabled:
            mem_mode_str = 'pds-' + self.wal_mode
        else:
            mem_mode_str = 'in-mem'
        self.artifacts = key['artifacts']
        self.versions = []
        for artifact in self.artifacts:
            if self.artifacts[artifact].get('ignite_version'):
                self.versions.append(self.artifacts[artifact]['ignite_version'])

        self.servers = key['servers']
        self.clients = key['drivers']
        self.warmup = key['config'].get('warmup', 60)
        self.durations = set()
        self.backups = key['config'].get('backups', 1)
        self.jobs = key['config'].get('jobs', 10)

        self.eco_root = '/'.join('https://eco-system.gridgain.com/api/%s'.split('/')[:-2])
        self.test_plan_url = ''
        self.benchmark_methods = []

        self.ftp_url = '172.25.2.50'
        self.ftp_remote_path = 'benchmarks/fosters'
        # self.ftp_remote_path = 'benchmarks'
        self.ftp_user = 'gg'
        self.ftp_passwd = 'gg'

        self.artifact_filename = '-'.join((
            datetime.fromtimestamp(self.started).strftime('%Y%m%d_%H%M%S'),
            '-'.join(self.versions),
            'c-' + str(self.clients),
            's-' + str(self.servers),
            'sm-' + self.sync_mode,
            mem_mode_str,
        ))
        if self.cache_mode == 'REPLICATED':
            self.artifact_filename += '-repl'
        if self.ssl_enabled:
            self.artifact_filename += '-ssl'
        self.artifact_filename += '.zip'

        for bench_res in self.report.values():
            for attempt in bench_res.get('attempts', []):
                duration = attempt.get('driver_options', {}).get('-d')
                if duration:
                    self.durations.add(duration)

        # Store ZIP in a directory one level upper than the directory of report file
        self.artifact_path = path.join(
            path.abspath(
                path.dirname(path.dirname(self.report_file))
            ),
            self.artifact_filename
        )

    def zip(self):
        # Zip all files in a directory where report.yaml is located
        dir = path.dirname(self.report_file)
        exclude_dirs = ['./artifacts']

        with zipfile.ZipFile(self.artifact_path, 'w') as fzip:
            chdir(dir)
            for dirpath, dirnames, files in walk('.'):
                if dirpath in exclude_dirs:
                    continue
                for file in files:
                    fzip.write(path.join(dirpath, file))

    def upload_to_ftp(self):
        with FTP(self.ftp_url) as ftp:
            ftp.login(self.ftp_user, self.ftp_passwd)
            ftp.cwd(self.ftp_remote_path)
            with open(self.artifact_path, 'rb') as f:
                ftp.storbinary('STOR ' + self.artifact_filename, f)

    def send(self):
        # Calculate start time
        for benchmark in self.report.keys():
            self.benchmark_methods.append(self.report[benchmark]['test_method'])
            if self.started > self.report[benchmark]['started']:
                self.started = self.report[benchmark]['started']

        description = f"N: {len(self.report.keys())}."
        if self.persistence_enabled:
            description += f" {self.wal_mode}."
        if self.eco_system_comment:
            description += f" {self.eco_system_comment}"
        add_options = []
        if self.cache_mode == 'REPLICATED':
            add_options.append('REPL')
        if self.ssl_enabled:
            add_options.append('SSL')
        if add_options:
            description += ' (' + ', '.join(add_options) + ')'
        duration = '0'
        if self.durations:
            duration = f'{min(self.durations)}-{max(self.durations)}' if len(self.durations) > 1 \
                else f'{self.durations.copy().pop()}'

        headers = {'content-type': 'application/json'}
        # Create page stub on eco-system
        tr_data = {
            "type_id": 0,
            "starttime": datetime.fromtimestamp(self.started).strftime('%Y-%m-%d %H:%M:%S'),
            "artifact_filename": self.artifact_filename,
            "description": description,
            "properties":
                {
                    "type": "properties",
                    "value": [
                        {"cls": "TestsRunProperty", "name": "location", "value": "SPb_Test_Lab"},
                        {"cls": "TestsRunProperty", "name": "servers", "value": str(self.servers)},
                        {"cls": "TestsRunProperty", "name": "clients", "value": str(self.clients)},
                        {"cls": "TestsRunProperty", "name": "duration", "value": duration},
                        {"cls": "TestsRunProperty", "name": "warmup", "value": str(self.warmup)},
                        {"cls": "TestsRunProperty", "name": "backups", "value": str(self.backups)},
                        {"cls": "TestsRunProperty", "name": "syncmode", "value": self.sync_mode},
                        {"cls": "TestsRunProperty", "name": "persistence_enabled", "value": str(self.persistence_enabled)},
                        {"cls": "TestsRunProperty", "name": "jobs", "value": str(self.jobs)}
                    ]
                }
        }

        response = post(
            self.conf['eco-system']['api_url'] % 'da/tests_runs',
            data=dumps(tr_data),
            headers=headers,
            auth=(
                self.conf['eco-system']['api_key'],
                self.conf['eco-system']['api_secret_key'],
            )
        )
        # print(response.text)
        test_run = loads(response.text)
        build_idx = 1
        builds = {}
        for artifact in self.artifacts.keys():
            idx = build_idx
            if 'base' in artifact:
                idx = 0
            build_data = {
                "test_run_id": test_run.get('id'),
                "starttime": datetime.fromtimestamp(self.started).strftime('%Y-%m-%d %H:%M:%S'),
                "version": self.artifacts[artifact]['ignite_version'],
                "revision": self.artifacts[artifact]['ignite_revision'][:8],
                "properties":
                    {
                        "type": "properties",
                        "value":
                            [
                                {"cls": "BuildResultProperty", "name": "priority", "value": str(idx)},
                                {"cls": "BuildResultProperty", "name": "jvmopts", "value": "java options"}
                            ]
                    }
            }
            response = post(
                self.conf['eco-system']['api_url'] % 'da/build_results',
                data=dumps(build_data),
                headers=headers,
                auth=(
                    self.conf['eco-system']['api_key'],
                    self.conf['eco-system']['api_secret_key'],
                )
            )
            builds[artifact] = loads(response.text).get('id')
            build_idx += 1

        for benchmark in self.report.keys():
            for artifact in self.artifacts.keys():
                throughput = self.report[benchmark].get('aggr_results', {})\
                    .get(artifact, {})\
                    .get('avg_client_throughput', 0)
                benchmark_data = {
                    "build_result_id": int(builds[artifact]),
                    "value": throughput
                }
                response = post(
                    self.conf['eco-system']['api_url'] % ('da/benchmarks_value/' + benchmark),
                    data=dumps(benchmark_data),
                    headers=headers,
                    auth=(
                        self.conf['eco-system']['api_key'],
                        self.conf['eco-system']['api_secret_key'],
                    )
                )

        self.test_plan_url = self.eco_root + '/da/test_runs/benchmarks/' + str(test_run.get('id'))
        print("Report was uploaded to " + self.test_plan_url)

        # Attach test plan URL to TestTail report
        for test_case in self.testrail_report.keys():
            if self.testrail_report[test_case]['function'] in self.benchmark_methods:
                self.testrail_report[test_case]['test_res_url'] = self.test_plan_url
        with open(self.testrail_report_file, 'w') as fw:
            dump(self.testrail_report, fw)

# Main
if __name__ == '__main__':
    print('Eco-System Benchmark report ver 0.0.2')
    python_path = str("%s|%s/suites" % (getcwd(), getcwd())).replace('\\', '/')
    python_path = python_path.replace('|', ';') if platform == 'win32' else python_path.replace('|', ';')
    environ['PYTHONPATH'] = python_path
    print("Python version %s" % python_version())
    ecb_report = EcBenchReport()
    if len(ecb_report.report) == 0:
        print('Report file(s) not found')
        exit(1)
    ecb_report.send()
    print("Compressing artifacts to %s" % ecb_report.artifact_filename)
    ecb_report.zip()
    print("Uploading %s to QA FTP" % ecb_report.artifact_filename)
    ecb_report.upload_to_ftp()
