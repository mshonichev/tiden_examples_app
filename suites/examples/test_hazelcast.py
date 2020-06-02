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

from tiden_gridgain.apps import Hazelcast
from tiden.case.apptestcase import AppTestCase
from tiden.util import render_template, attr

from shutil import move
from time import sleep


class TestHazelcast (AppTestCase):

    def __init__(self, *args):
        super().__init__(*args)
        self.add_app('hazelcast')

    def setup(self):
        conf = self.tiden.config['environment']['hazelcast']
        res_dir = self.tiden.config['rt']['test_resource_dir']
        # Collect HZ addresses:port pairs
        addresses = []
        cur_port = 57500
        for host_type in ['server', 'client']:
            if conf.get("%s_hosts" % host_type):
                instances_per_host = conf.get("%ss_per_host" % host_type, 1)
                for addr in conf["%s_hosts" % host_type]:
                    for isinstance_num in range(0, instances_per_host):
                        addresses.append("%s:%s" % (addr, cur_port))
                        cur_port += 1
        # Render config file
        render_template(
            "%s/hazelcast.tmpl.xml" % res_dir,
            'default',
            {
                'addresses': addresses
            }
        )
        # Rename rendered config to default HZ config name
        move(
            "%s/hazelcast.default.xml" % res_dir,
            "%s/hazelcast.xml" % res_dir
        )
        super().setup()

    @attr('hazelcast')
    def test_start_hz_cluster(self):
        """
        Start and stop Hazelcast cluster
        """
        hz_app = self.get_app_by_type('hazelcast')[0]
        hz_app.start_nodes()
        sleep(10)
        hz_app.stop_nodes()

    def teardown(self):
        super().teardown()

