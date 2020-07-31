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

from tiden_gridgain.case.singlegridtestcase import SingleGridTestCase
from tiden.tidenexception import TidenException
from tiden_gridgain.apps.webconsole import Webconsole
from tiden.util import *


class TestWebConsole(SingleGridTestCase):

    @skip("Can't run ignite")
    def test_web_console_start(self):
        console = None
        try:
            console = Webconsole('WebConsole', self.config, self.ssh)
            console.start(users=True)
            ignite_urls = ['{}:8080'.format(host) for host in self.ssh.hosts]
            console.start_web_agent(ignite_urls=ignite_urls)
        except TidenException:
            assert False, "Console start failed"
        finally:
            if console is not None:
                console.stop()

