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

from tiden.apps.ignite import Ignite
from tiden import with_setup, sleep, SshPool
from tiden_gridgain.piclient.helper.class_utils import ModelTypes
from tiden_gridgain.piclient.piclient import PiClient
from tiden_gridgain.piclient.utils import PiClientIgniteUtils
from suites.compatibility.abstract import AbstractCompatibilityTest


class TestRollingUpgrade(AbstractCompatibilityTest):

    def empty_setup(self):
        super().empty_setup()

    def teardown_mixed_cluster_testcase(self):
        super().teardown_mixed_cluster_testcase()

    @with_setup(empty_setup, teardown_mixed_cluster_testcase)
    def test_start_on_lfs(self):
        PiClient.read_timeout = 3600
        SshPool.default_timeout = 1200

        self.ignite_old_version = self.start_ignite_grid(
            'old',
            activate=True
        )

        key_map = {
            'cache_group_1_001': ModelTypes.KEY_ALL_TYPES_MAPPED.value,
            'cache_group_1_002': 'java.lang.Long',
            'cache_group_1_003': ModelTypes.KEY_DEFAULT_TABLE.value,
            'cache_group_1_004': ModelTypes.KEY_ACCOUNT.value,
        }

        batch_size = 10000
        PiClientIgniteUtils.load_data_with_putall(self.ignite_old_version,
                                                  Ignite.config_builder.get_config('client',
                                                                                   config_set_name=self.config_name),
                                                  start_key=0,
                                                  end_key=250000,
                                                  batch_size=batch_size,
                                                  key_map=key_map,
                                                  value_type=ModelTypes.VALUE_ALL_TYPES_4_INDEX.value
                                                  )

        sleep(120)

        self.ignite_old_version.kill_nodes()

        cmd = dict()
        for node_id, dscr in self.ignite_old_version.nodes.items():
            ln_to_work = 'ln -s %s/%s %s/%s' % (
                dscr['ignite_home'], 'work', self.get_app(self.ignite_app_names['new']).nodes[node_id]['ignite_home'],
                'work',
            )
            if dscr['host'] in cmd:
                cmd[dscr['host']].append(ln_to_work)
            else:
                cmd[dscr['host']] = [ln_to_work, ]

        self.tiden.ssh.exec(cmd)

        sleep(10)

        self.ignite_old_version = self.start_ignite_grid(
            'new',
            activate=True,
            replaced_name='old'
        )

        sleep(120)

        self.ignite_old_version.cu.control_utility('--cache validate_indexes',
                                                   all_required=['no issues found'])

        PiClientIgniteUtils.load_data_with_putall(self.ignite_old_version,
                                                  Ignite.config_builder.get_config('client',
                                                                                   config_set_name=self.config_name),
                                                  start_key=250001,
                                                  end_key=500001,
                                                  batch_size=batch_size,
                                                  key_map=key_map,
                                                  value_type=ModelTypes.VALUE_ALL_TYPES_4_INDEX.value
                                                  )

        sleep(120)

        self.ignite_old_version.cu.control_utility('--cache', 'idle_verify')

