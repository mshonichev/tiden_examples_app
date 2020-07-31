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

import re

from pt import log_print
from suites.consumption.framework.probes.abstract import AbstractProbe


class StartTimeProbe(AbstractProbe):
    """
    if Ignite node stopped in the middle of checkpoint. Will restore memory state and finish checkpoint on node start:
        Binary recovery performed in 293686 ms.

    Finished restoring partition state for local groups [groupsProcessed67partitionsProcessed=507434, time=37366ms]
    Finished applying WAL changes [updatesApplied=279350, time=42356 ms]
    Finished applying memory changes [changesApplied=7911983, time=31364 ms]
    """

    def validate_config(self):
        """
        diff is mandatory for this probe
        """
        assert 'diff' in self.probe_config, 'There is no "diff" value in config'

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

        self.results[self.version] = {}

        self.collect_new_start_time()
        self.print_pre_259_start_times()

    def collect_new_start_time(self):
        """
            [16:34:42,487][INFO][main][G] Node started :
            [stage="Configure system pool" (220 ms),
            stage="Start managers" (700 ms),
            stage="Configure binary metadata" (180 ms),
            stage="Start processors" (2744 ms),
            stage="Start 'GridGain' plugin" (11 ms),
            stage="Init and start regions" (174 ms),
            stage="Restore binary memory" (916 ms),
            stage="Restore logical state" (5773 ms),
            stage="Finish recovery" (176 ms),
            stage="Join topology" (435 ms),
            stage="Await transition" (27 ms),
            stage="Await exchange" (1295 ms),
            stage="Total time" (12651 ms)]
        :return:
        """

        collected_start_time = self.collect_specific_text_from_logs('.*Node started.*:.*[stage=.')

        if collected_start_time:
            configure_sys_pool = self.collect_events('.*stage="Configure system pool" (\d+) ms', collected_start_time)
            start_managers = self.collect_events('.*stage="Start managers" (\d+) ms', collected_start_time)
            configure_binary_meta = self.collect_events('.*stage="Configure binary metadata" (\d+) ms',
                                                        collected_start_time)
            start_procs = self.collect_events('.*stage="Start processors" (\d+) ms', collected_start_time)
            gg_plugin = self.collect_events('.*stage="Start \'GridGain\' plugin" (\d+) ms', collected_start_time)
            init_regions = self.collect_events('.*stage="Init and start regions" (\d+) ms', collected_start_time)
            restore_binary = self.collect_events('.*stage="Restore binary memory" (\d+) ms', collected_start_time)
            restore_logical = self.collect_events('.*stage="Restore logical state" (\d+) ms', collected_start_time)
            finish_recovery = self.collect_events('.*stage="Finish recovery" (\d+) ms', collected_start_time)
            join_topology = self.collect_events('.*stage="Join topology" (\d+) ms', collected_start_time)
            transition = self.collect_events('.*stage="Await transition" (\d+) ms', collected_start_time)
            exchange = self.collect_events('.*stage="Await exchange" (\d+) ms', collected_start_time)
            total = self.collect_events('.*stage="Total time" (\d+) ms', collected_start_time)

            self.results[self.version] = {
                'system pool': configure_sys_pool,
                'start managers': start_managers,
                'configure binary meta': configure_binary_meta,
                'start processors': start_procs,
                'gg_plugin': gg_plugin,
                'init and start regions': init_regions,
                'restore binary memory': restore_binary,
                'restore logical state': restore_logical,
                'finish recovery': finish_recovery,
                'join topology': join_topology,
                'await transition': transition,
                'await exchange': exchange,
                'total': total,
            }

    def print_pre_259_start_times(self):
        stopped_in_cp = {}

        for node, logs_content in self.collect_specific_text_from_logs(
                '.*Ignite node stopped in the middle of checkpoint.').items():
            stopped_in_cp[node] = 'Ignite node stopped in the middle of checkpoint.' in logs_content
        binary_recovery_time = self.collect_events('.*performed in (\d+) ms',
                                                   self.collect_specific_text_from_logs('Binary recovery performed in'))
        part_states = self.collect_events('.*partitionsProcessed=(\d+), time=(\d+)ms]',
                                          self.collect_specific_text_from_logs(
                                              'Finished restoring partition state for local groups'))
        wal_changes = self.collect_events('.*updatesApplied=(\d+), time=(\d+) ms',
                                          self.collect_specific_text_from_logs('Finished applying WAL changes'))
        memory_changes = self.collect_events('.*changesApplied=(\d+), time=(\d+) ms',
                                             self.collect_specific_text_from_logs('Finished applying memory changes'))
        log_print(stopped_in_cp, color='yellow')
        log_print(binary_recovery_time, color='yellow')
        log_print(part_states, color='yellow')
        log_print(wal_changes, color='yellow')
        log_print(memory_changes, color='yellow')

        for node in stopped_in_cp.keys():
            log_print("Report for node %s %s" % (node, '(coordinator)' if node == 1 else ''), color='green')

            if stopped_in_cp[node]:
                log_print("Stopped in the middle of checkpoint", color='green')
            else:
                log_print("Stopped gracefully", color='green')

            log_print("Binary was restored in %s ms" % binary_recovery_time[node][0], color='green')

            part_stated_time = part_states[node][1]
            part_stated_speed = (part_states[node][1] / part_states[node][0]) if part_states[node][0] != 0 else 0
            log_print("Partitions recovery time = %s ms, (time / partitions) = %.2f" %
                      (part_stated_time, part_stated_speed), color='green')

            wal_changes_time = wal_changes[node][1]
            wal_changes_speed = wal_changes[node][0]
            log_print("Applying WAL changes time = %s ms, updates = %s" %
                      (wal_changes_time, wal_changes_speed), color='green')

            if node in memory_changes:
                memory_changes_time = memory_changes[node][1]
                memory_changes_speed = (memory_changes[node][1] / memory_changes[node][0]) \
                    if memory_changes[node][0] != 0 else 0
                log_print("Applying memory changes time = %s ms, (time / changes) = %.2f" %
                          (memory_changes_time, memory_changes_speed), color='green')
            else:
                log_print("There is no memory changes log message found", color='green')

    def collect_events(self, msg, vals):
        collected_cps = {}

        for node, part_state in vals.items():
            m = re.search(msg, part_state)

            if m:
                for group in m.groups():
                    if node in collected_cps:
                        collected_cps[node].append(int(group))
                    else:
                        collected_cps[node] = [int(group), ]

        return collected_cps

    def print_results(self):
        log_print('Results for checkpointLockHoldTime probe: {}'.format(self.results))

    def is_passed(self, **kwargs):
        """
        Formula:
        base_version - observable_version / base_version

        I.e. difference between  base and observable version in percentage

        :return is verification passed
        """
        if self.check_single_artifact():
            return True

        is_passed = True

        # todo add each time check
        # for version in self.results.keys():
        #     if version == self.base_version:
        #         continue
        #
        #     cp_speed = percentage_diff(self.results[self.base_version], self.results[version])
        #
        #     min_value = float(self.probe_config.get('diff')[0])
        #     max_value = float(self.probe_config.get('diff')[1])
        #
        #     is_passed = min_value < cp_speed < max_value
        #
        #     self.result_message = \
        #         'checkpointLockHoldTime 95 percentile difference: {:.2f}. {} (base) - {:.2f} ms, {} - {:.2f} ms' \
        #             .format(cp_speed,
        #                     self.base_version,
        #                     self.results[self.base_version],
        #                     version,
        #                     self.results[version])
        #
        #     log_print(self.result_message,
        #               color=self.get_out_color(is_passed, **kwargs))
        #
        # self.result_passed = is_passed

        return is_passed

    def collect_specific_text_from_logs(self, text_to_collect):
        collected_events = {}

        prop_name = 'part_state'
        self.ignite.get_data_from_log(
            'server',
            text_to_collect,
            '((.|\n)*)',
            prop_name,
            default_value=''
        )

        for node_id, content in self.ignite.nodes.items():
            if prop_name in content:
                collected_events[node_id] = content[prop_name]

        return collected_events

