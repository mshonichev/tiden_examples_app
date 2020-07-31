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

from tiden import log_print
from suites.consumption.framework.probes.abstract import AbstractProbe


class TestProbe(AbstractProbe):
    """
    """

    def validate_config(self):
        assert 'test_probe_param' in self.probe_config, 'There is no "test_probe_param" value in config'

    def start(self):
        """
        Store start time work/db directory size
        """
        super().start()

    def stop(self, **kwargs):
        """
        Store stop time work/db directory size
        """
        super().stop()

        test_results = kwargs.get('test_results', True)

        self.results[self.version] = test_results

    def print_results(self):
        log_print('Test probe results: {}'.format(self.results))

    def is_passed(self, **kwargs):
        """
        Formula:
        base_version - observable_version / base_version

        I.e. difference between  base and observable version in percentage

        :return is verification passed
        """
        if self.check_single_artifact():
            return True

        self.result_message = "Probe %s" % self.results.get(self.base_version)
        self.result_passed = self.results.get(self.base_version)

        return self.result_passed

