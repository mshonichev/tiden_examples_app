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

from time import sleep
from typing import List

from tiden import TidenException, log_print
from tiden_gridgain.piclient.helper.cache_utils import IgniteCacheConfig
from tiden_gridgain.piclient.helper.operation_utils import create_async_operation, create_checksum_operation
from tiden_gridgain.piclient.piclient import PiClient
from tiden.report.steps import step
from suites.dr.dr_cluster import Node, Cluster

@step()
def wait_for_replication_is_up_for_clusters(clusters: List[Cluster]) -> None:
    log_print('Waiting for replication goes ONLINE...', color='debug')
    for cluster in clusters:
        for node in cluster.nodes:
            wait_for_replication_is_up_for_node(node)

@step(attach_parameters=True)
def wait_for_replication_is_up_for_node(node: Node) -> None:
    for receiver_node in node.receiver_nodes:
        replication_is_up_count = 0
        for attempt in range(1, 60):
            replication_is_up_pattern = {
                'replication is up':
                    {'regex': '(Remote data center switched to online mode: .+dataCenterId={})'.format(
                        receiver_node.cluster.id),
                        'remote_grep_options': '-E'
                    }
            }
            result_from_log = node.cluster.grid.grep_log(node.id, **replication_is_up_pattern)
            for node_id in result_from_log:
                for pattern_name, matched in result_from_log[node_id].items():
                    if matched:
                        replication_is_up_count += 1
                        log_print(f'Replication is up between cluster {node.cluster.id} node {node.id} ' +
                                  f'and cluster {receiver_node.cluster.id} node {receiver_node.id}')
            if replication_is_up_count != len(node.receiver_nodes):
                sleep(2)
            else:
                break
        if replication_is_up_count != len(node.receiver_nodes):
            log_print(f'{replication_is_up_count} != {len(node.receiver_nodes)}', color='blue')
            raise TidenException(f'Replication is not up between cluster {node.cluster.id} node {node.id} ' +
                                 f'and cluster {receiver_node.cluster.id} node {receiver_node.id}')

@step('Run DR state transfer')
def dr_state_transfer(piclient, cache_name, cluster):
    piclient.get_ignite().cache(cache_name)
    piclient.get_ignite().plugin(
        piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().stateTransfer(
        cache_name, bytes([cluster]))

@step('Stop replication')
def dr_stop_replication(piclient, cache_name):
    piclient.get_ignite().plugin(
        piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().stopReplication(cache_name)


def dr_is_stopped(piclient, cache_name):
    try:
        piclient.get_ignite().cache(cache_name)
        stopped = piclient.get_ignite().plugin(
            piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().senderCacheStatus(
            cache_name).stopped()
    except Exception as e:
        log_print(e)
        stopped = False

    finally:
        return stopped

@step('Start replication')
def dr_start_replication(piclient, cache_name):
    piclient.get_ignite().cache(cache_name)
    piclient.get_ignite().plugin(
        piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().startReplication(cache_name)

@step('Pause replication')
def dr_pause(piclient, cache_name):
    piclient.get_ignite().plugin(piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().pause(
        cache_name)


def dr_is_on_pause(piclient, cache_name):
    return piclient.get_ignite().plugin(
        piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().senderCacheStatus(cache_name).paused()

@step('Resume replication')
def dr_resume(piclient, cache_name):
    piclient.get_ignite().plugin(piclient.get_gateway().jvm.org.gridgain.grid.GridGain.PLUGIN_NAME).dr().resume(
        cache_name)


def create_piclient(cluster):
    return PiClient(cluster.grid, cluster.piclient_config, new_instance=True, nodes_num=1)


def create_cache_config(piclient, cache_name, sender_group):
    gateway = piclient.get_gateway()
    cache_config = IgniteCacheConfig(gateway)
    cache_config.set_name(cache_name)
    cache_config.set_cache_mode('replicated')
    cache_config.set_atomicity_mode('ATOMIC')
    cache_config.set_write_synchronization_mode('full_sync')
    if sender_group:
        cache_config.set_plugin_configurations(sender_group)
    return cache_config


def calculate_and_compare_checksums(piclient1, piclient2, start_key=None, end_key=None):
    checksum1 = calc_checksums_on_client(piclient1, start_key=start_key, end_key=end_key, dict_mode=True)
    checksum2 = calc_checksums_on_client(piclient2, start_key=start_key, end_key=end_key, dict_mode=True)
    return checksum1, checksum2

@step()
def calc_checksums_on_client(
        piclient,
        start_key=0,
        end_key=1000,
        dict_mode=False
):
    """
    Calculate checksum based on piclient
    :param start_key: start key
    :param end_key: end key
    :param dict_mode:
    :return:
        """
    log_print("Calculating checksums using cache.get() from client")
    cache_operation = {}
    cache_checksum = {}

    sorted_cache_names = []
    for cache_name in piclient.get_ignite().cacheNames().toArray():
        sorted_cache_names.append(cache_name)

    sorted_cache_names.sort()

    async_operations = []
    for cache_name in sorted_cache_names:
        async_operation = create_async_operation(create_checksum_operation,
                                                 cache_name,
                                                 start_key,
                                                 end_key,
                                                 gateway=piclient.get_gateway())
        async_operations.append(async_operation)
        cache_operation[async_operation] = cache_name
        async_operation.evaluate()

    checksums = ''

    for async_operation in async_operations:
        result = str(async_operation.getResult())
        cache_checksum[cache_operation.get(async_operation)] = result
        checksums += result

    log_print('Calculating checksums done')

    if dict_mode:
        return cache_checksum
    else:
        return checksums

