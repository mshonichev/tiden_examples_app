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

import base64
from copy import deepcopy
from json import loads
from random import shuffle, Random
from re import search
from time import time

from tiden.ignite import Ignite
from tiden.tidenconfig import TidenConfig


class SetupMixin:

    def __init__(self):
        self.context: dict = None
        self.tiden: TidenConfig = None
        self.cluster: Ignite = None

    def code(self, obj: list) -> str:
        # return base64.b85encode(bytes(dumps(obj), "utf-8")).decode("utf-8")
        all_code = []
        for item in obj:
            result = []
            for k, v in item.items():
                part = ''
                part += f'{k}+'
                if isinstance(v, list):
                    part += '-'.join(v)
                else:
                    part += v
                result.append(part)
            all_code.append('*'.join(result))
        return '^'.join(all_code)

    def decode(self, obj: str) -> list:
        try:
            return loads(base64.b85decode(obj).decode("utf-8", "ignore"))
        except:
            history = []
            for item in obj.split('^'):
                result = {}
                for part in item.split('*'):
                    key, combine_value = part.split('+')
                    value = combine_value.split('-')
                    result[key] = value
                history.append(result)
            return history

    def _setup_runs(self):
        """
        Generate runs
        """
        duration = self.tiden.config['combinations'].get('duration')
        if duration:
            _, hours, _, minutes = search('((\d+)h|)((\d+)m|)', duration).groups()
            hours = 0 if hours is None else int(hours)
            minutes = 0 if minutes is None else int(minutes)
            self.end_time = round(time()) + hours * 60 * 60 + minutes * 60

        # create runs from code (can be list of different codes)
        if self.tiden.config['combinations'].get('code'):
            codes = self.tiden.config['combinations']['code']
            if not isinstance(codes, list):
                codes = [codes]
            self.runs = []
            for code in codes:
                self.runs.extend(self.decode(code))
            return

        operations = self.tiden.config['combinations']['operations'].split('+')
        states = self.tiden.config['combinations']['action'].split('+')
        cluster_state = self.tiden.config['combinations']['setup_cluster'].split('+')

        """
        Input data example
        
            combinations:
              operations: 'jdbc+*'
              action: 'stable'
              setup_cluster: 'empty'
        
        Iterate through each option and multiple runs with each new option
        """
        for run_prop_name, items_to_iterate, available_options in [
            ['operations', operations, self.operations_methods],
            ['action', states, self.action_methods],
            ['setup_cluster', cluster_state, self.cluster_setup_methods],
        ]:
            """
            going through all items (operations for example)
            if * then take all type of operations from self.operations_methods
            if jdbc (for example) then take only jdbc
            """
            target_items_to_add = []
            for prop in items_to_iterate:
                if prop == '*':
                    if target_items_to_add:
                        old_len = len(target_items_to_add)
                        for i in range(old_len):
                            for available_option in available_options.keys():
                                if available_option == 'none':
                                    continue
                                target_items_to_add.append(target_items_to_add[i] + [available_option])
                        target_items_to_add = target_items_to_add[old_len:]
                    else:
                        target_items_to_add = [[item] for item in available_options.keys() if item != 'none']
                else:
                    if target_items_to_add:
                        for idx, target_item_to_add in enumerate(target_items_to_add):
                            target_items_to_add[idx] = target_item_to_add + [prop]
                    else:
                        target_items_to_add = [[prop]]

            """
            Take all runs
            Multiple all runs on all found operations
            """
            old_len = len(self.runs)
            for idx in range(old_len):
                for target_item_to_add in target_items_to_add:
                    run = deepcopy(self.runs[idx])
                    if run_prop_name != 'operations':
                        run[run_prop_name] = target_item_to_add
                    else:
                        run[run_prop_name] = sorted(target_item_to_add)
                    self.runs.append(run)
            self.runs = self.runs[old_len:]

        # ignore duplications
        idx_to_delete = []
        for idx, run in enumerate(self.runs):
            if idx not in idx_to_delete:
                # ignore same operations (jdbc + jdbc for example)
                for operation in run['operations']:
                    found_ops = [_operation for _operation in run['operations'] if _operation == operation]
                    if len(found_ops) > 1:
                        idx_to_delete.append(idx)
            if idx not in idx_to_delete:
                # ignore all same cases
                for other_run in self.runs[idx + 1:]:
                    if all([
                        run['action'] == other_run['action'],
                        run['setup_cluster'] == other_run['setup_cluster'],
                        run['operations'] == other_run['operations']
                    ]):
                        idx_to_delete.append(self.runs.index(other_run))
        self.runs = [run for idx, run in enumerate(self.runs) if idx not in idx_to_delete]

        """
        ignore combinations
        Examples: Delete runs where operations have activation and deactivation 
        """
        idx_to_delete = []
        items = ['operations', 'action', 'setup_cluster']
        for name, ignore_list in self.context['options'].get('ignore_combinations', {}).items():
            for ignore_item in ignore_list:
                for idx, run in enumerate(self.runs):
                    all_items = []
                    for item in items:
                        values_to_search = run[item]
                        if isinstance(values_to_search, list):
                            all_items.extend(values_to_search)
                        else:
                            all_items.append(values_to_search)
                    if ignore_item in all_items and name in all_items:
                        idx_to_delete.append(idx)

        self.runs = [run for idx, run in enumerate(self.runs) if idx not in idx_to_delete]

        # shuffle
        if self.tiden.config.get('ranges'):
            # shuffle with custom seed and get only defined cases range
            Random(self.tiden.config.get('seed', Random().randint(0, 10000))).shuffle(self.runs)
            start, end = self.tiden.config['ranges'].split('-')
            self.runs = self.runs[int(start):int(end)]
        else:
            shuffle(self.runs)

