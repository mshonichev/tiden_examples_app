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

from copy import deepcopy
from enum import Enum
from traceback import format_exc


class RunType(Enum):
    BASIC_WORKER = 'workers'
    SERVER_BASE_WORKER = 'server+workers'


def step(get_name):
    def upper(func):
        def inner(*args, **kwargs):
            self = args[0]
            step = {'name': get_name(args, kwargs),
                    'time': {'start': self.current_time,
                             'start_pretty': self.current_time_pretty}}

            self.context['report']['children'] = []
            self.context['attachments'] = []
            self.context['step_status'] = ''
            self.context['step_failed'] = ''
            try:
                func(*args, **kwargs)
                step['status'] = 'passed'
                step['stacktrace'] = ''
            except:
                step['status'] = 'failed'
                step['stacktrace'] = format_exc()
                raise
            finally:
                step['time']['end'] = self.current_time
                step['time']['end_pretty'] = self.current_time_pretty
                diff = step['time']['end'] - step['time']['start']
                diff_sec = round(diff // 1000)
                if diff_sec > 60:
                    step['time']['diff'] = f'{diff_sec // 60}m {diff_sec % 60}s'
                else:
                    step['time']['diff'] = f'{diff_sec}s'
                if self.context['report'].get('children'):
                    step['children'] = self.context['report']['children']
                if self.context['report'].get('attachments'):
                    step['attachments'] = self.context['report']['attachments']
                if self.context['attachments']:
                    step['attachments'] = self.context['attachments']
                if self.context['step_failed']:
                    step['status'] = 'failed'
                    step['stacktrace'] = self.context['step_failed']
                self.context['report']['steps'].append(step)

        return inner

    return upper


def dict_merge(src: dict, dist: dict, copy_instances=False):
    if copy_instances:
        _src = deepcopy(src)
        _dist = deepcopy(dist)
    else:
        _src = src
        _dist = dist

    for d_k, d_v in _dist.items():
        if _src.get(d_k):
            if isinstance(_src[d_k], dict) and isinstance(d_v, dict):
                _src[d_k] = dict_merge(_src[d_k], d_v)
                continue
        _src[d_k] = d_v
    return _src

