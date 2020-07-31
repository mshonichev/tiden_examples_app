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

import abc
from abc import ABC

from pt import log_print
from suites.consumption.framework.utils import get_current_time


class AbstractProbe(ABC):
    """
    Abstract class for all probes
    """

    def __init__(self, probe_config):
        # artifact with base: True
        self.base_version = None

        self.probe_config = probe_config
        self.ignite = None
        self.test_class = None
        self.version = 'Unknown'

        # results (main and additional time points - events)
        self.results = {}
        self.events = {}
        # computed results after analyze() run
        self.result_passed = False
        self.result_message = ''

        self.start_time = 0
        self.end_time = 0

        self.timeout = 10

    def initialize(self, test_class, artifact_name, app_name=None):
        """
        Initialize probe:

        If current artifact is base - save this information (we do know that there is at least one base artifact,
        here we need to save which one - to run analyze() method)

        Save all variables we need

        Call abstract validate_config (Each probe may need their own parameters to work)

        :param test_class:
        :param artifact_name:
        :param app_name:
        :return:
        """
        artifact = test_class.tiden.config['artifacts'][artifact_name]
        if artifact.get('base', False):
            self.base_version = "%s_%s" % (artifact_name, artifact['ignite_version'])
        self.version = "%s_%s" % (artifact_name, artifact['ignite_version'])

        if app_name is None:
            app_name = artifact_name
        self.ignite = test_class.get_app(app_name)
        self.test_class = test_class

        self.validate_config()

    def start(self):
        """
        start probe (store start time)
        """
        self.start_time = get_current_time()

    def stop(self, **kwargs):
        """
        stop probe (store stop time)
        """
        self.end_time = get_current_time()

    def write_event(self, name):
        self.events[get_current_time()] = name

    def check_single_artifact(self):
        if len(self.results.keys()) < 2:
            log_print('There is 0 or 1 result for time probe')
            self.result_passed = True
            return True
        else:
            return False

    def get_probe_name(self):
        return self.probe_config.get('name', self.__class__.__name__)

    name = property(get_probe_name, None)

    @staticmethod
    def get_out_color(is_passed, **kwargs):
        out_color = 'green' if not kwargs.get('colorless', False) else 'blue'
        if not is_passed:
            out_color = 'red'

        return out_color

    @abc.abstractmethod
    def validate_config(self):
        """
        Assert that probe config is correct: it's contains all necessary arguments
        E.g. time difference is defined
        """
        pass

    @abc.abstractmethod
    def is_passed(self, **kwargs):
        """
        Validate scenario results

        NB! Do not forget to use following code to avoid failed single artifact runs

        if self.check_single_artifact():
            return True
        """
        pass

