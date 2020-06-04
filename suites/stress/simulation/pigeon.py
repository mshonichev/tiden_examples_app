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

import random
from enum import Enum


class SimulationEvent(Enum):
    pass


class ClusterEvents(Enum):
    ROUND_RESTART = 0
    PARALLEL_RESTART = 1
    SET_CURRENT_BASELINE = 2
    TAKE_SNAPSHOT = 2


class NodeEvents(Enum):
    FAIL_NODE = 0
    START_NODE = 1
    RESTART_NODE = 2


class FailNodeEvent(Enum):
    KILL_NODE = 0
    DROP_NETWORK = 1
    DROP_PDS = 2
    REMOVE_FROM_BASELINE = 3


class PigeonSimulation:
    """
    Cluster life simulation algorithm

    Stupid as pigeon
    """

    STARTED = 1
    KILLED = 0

    def __init__(self, num):
        self.interrupted = False

        self.nodes_num = num

        self.array = [0]
        for index in range(1, num + 1):
            self.array.append(self.STARTED)

    def next_event(self):
        alive_nodes_percentage = self.get_alive_nodes_num()

        print("Current cluster")
        print("Alive nodes: %s" % alive_nodes_percentage)
        print("Nodes: %s" % self.array[1:])

        random_node = random.randint(1, self.nodes_num)

        print("============New iteration=================")

        print("Random node: %s" % random_node)

        if self.array[random_node] == self.KILLED:
            self.array[random_node] = self.STARTED

            return NodeEvents.START_NODE, random_node

        if alive_nodes_percentage < 0.6:
            random_restart = random.randint(0, 100)

            if random_restart > 70:
                cluster_kill_type = "parallel"
                if random_restart > 85:
                    cluster_kill_type = "round"

                print("Restart %s all cluster" % cluster_kill_type)

                # do magic
                for index in range(1, self.nodes_num + 1):
                    self.array[index] = self.STARTED

                if cluster_kill_type == "round":
                    return ClusterEvents.ROUND_RESTART, random_node
                else:
                    return ClusterEvents.PARALLEL_RESTART, random_node

            if random_restart > 60:
                return ClusterEvents.TAKE_SNAPSHOT, random_node

            return ClusterEvents.SET_CURRENT_BASELINE, random_node

        random_event = random.randint(0, 100)

        # generate something bad
        self.array[random_node] = self.KILLED
        if random_event < 75:
            return FailNodeEvent.KILL_NODE, random_node
        elif random_event < 80:
            return FailNodeEvent.DROP_NETWORK, random_node
        elif random_event < 85:
            return FailNodeEvent.DROP_PDS, random_node
        elif random_event < 95:
            self.array[random_node] = self.STARTED
            return NodeEvents.RESTART_NODE, random_node
        elif random_event < 99:
            return FailNodeEvent.REMOVE_FROM_BASELINE, random_node
        else:
            return None, None

    def get_alive_nodes_num(self):
        alive_nodes = 0
        for index in range(1, self.nodes_num + 1):
            if self.array[index] == self.STARTED:
                alive_nodes += 1

        return float(alive_nodes) / self.nodes_num

if __name__ == "__main__":
    sim = PigeonSimulation(16)

    for i in range(0, 100):
        print(sim.next_event())

