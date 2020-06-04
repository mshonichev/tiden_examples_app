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

import asyncio
from enum import Enum
from json import load, dump
from os import environ
from os.path import exists
from random import Random
from time import time

from aiohttp import web
from aiohttp.web_request import Request

files_path = environ.get('FILESPATH', '/data/storage.json')
attempts = 6000

seeds_storage = {}
seeds_saver_running = False

seeds_demons = []


class TestStatus(Enum):
    NOT_STARTED = 'not started'
    CLAIMED = 'claimed'
    PASSED = 'passed'

async def set_seed(request: Request):
    """
    Set seed in seeds storage with run name and tests count

    :param request:          params to set seed for workers
                seed:           seed to generate tests
                tests_count     common amount of tests count to run
                run_name        pretty run name
    :return:                 status
    """
    await check_seeds_saver_running()
    request_params = await request.json()
    seed = str(request_params['seed'])
    seeds_storage[seed] = {
        'tests': dict([(str(test), {'status': TestStatus.NOT_STARTED.value}) for test in range(request_params['tests_count'])]),
        'run_name': request_params['run_name']
    }
    loop = asyncio.get_event_loop()
    loop.create_task(ensure_tests_not_claimed_too_long(seed))
    return web.Response(text='added')

async def get_test(request: Request):
    """
    Get random test number and suite name for combinator
    This test will be CLAIMED in seeds storage for 30 min
        and returned in NOT_STARTED if it's not being passed on this moment

    :param request:     request with seed number in json body
    :return:            test number and suite name
    """
    await check_seeds_saver_running()
    seed = str(request.rel_url.query['seed'])
    loop = asyncio.get_event_loop()
    loop.create_task(ensure_tests_not_claimed_too_long(seed))
    available_numbers = {}
    run_name = seeds_storage[seed]['run_name']

    if len(seeds_storage[seed]['tests']) == 0:
        return web.json_response({
            'test_number': None
        })

    # find all available tests numbers
    for test_number, data in seeds_storage[seed]['tests'].items():
        if data['status'] == TestStatus.NOT_STARTED.value:
            available_numbers[test_number] = 1
    rand = Random()

    selected_test = None
    for i in range(attempts):
        if len(available_numbers) == 0:
            break

        sorted_available_numbers = sorted(list(available_numbers.keys()))
        rand_int = rand.randint(0, len(sorted_available_numbers) - 1)

        # claim test
        rand_test_number = str(sorted_available_numbers[rand_int])
        if seeds_storage[seed]['tests'][rand_test_number]['status'] == TestStatus.NOT_STARTED.value:
            seeds_storage[seed]['tests'][rand_test_number]['status'] = TestStatus.CLAIMED.value
            seeds_storage[seed]['tests'][rand_test_number]['time_claimed'] = time()
            selected_test = int(rand_test_number)
            break
        else:
            # delete from available if already in use
            del available_numbers[sorted_available_numbers[rand_int]]

    return web.json_response({
        'test_number': selected_test,
        'run_name': run_name
    })

async def test_passed(request: Request):
    """
    Submit test as passed which is changed it status in seeds storage on PASSED

    :param request:     request with seed and test_number
    :return:            result that all good or that test already passed in past
    """
    await check_seeds_saver_running()
    request_params = await request.json()
    seed = str(request_params.get('seed'))
    loop = asyncio.get_event_loop()
    loop.create_task(ensure_tests_not_claimed_too_long(seed))
    test_number = str(request_params.get('test_number'))
    if seeds_storage[seed]['tests'][test_number]['status'] == TestStatus.CLAIMED.value:
        seeds_storage[seed]['tests'][test_number]['status'] = TestStatus.PASSED.value
        return web.Response(text='Good')
    else:
        return web.Response(
            reason=f'Seed {seed} test {test_number} failed to pass result '
                   f'because current status is {seeds_storage[seed]["tests"][test_number]}',
            status=500)

async def delete_seed(request: Request):
    """
    Delete old seed from seeds storage

    :param request:
    :return:
    """
    await check_seeds_saver_running()
    request_params = await request.json()
    del seeds_storage[str(request_params['seed'])]
    return web.Response(text='deleted')

async def stat(request: Request):
    """
    Get statistics for all seeds and tests statuses in it

    :param request:     useless request
    :return:            stat
    """
    seeds_stats = {}
    for seed, data in seeds_storage.items():
        seed_stat = {}
        tests = data.get('tests', [])
        for test_data in tests.values():
            seed_stat[test_data['status']] = seed_stat.get(test_data['status'], 0) + 1
        seeds_stats[seed] = dict([(k, v) for k, v in seed_stat.items()])

    return web.json_response({
        'seeds': list(seeds_storage.keys()),
        'seeds_stats': seeds_stats
    })

async def ensure_tests_not_claimed_too_long(seed):
    """
    Every minute going through all seeds data and watching on tests status
    Test status should not be in CLAIMED more then 30 minutes
    In a different way test rollback status on NOT_STARTED
    If all tests in PASSED status then shutdown watchdog

    :param seed:    seed to watch
    """
    global seeds_demons
    if seed not in seeds_demons:
        seeds_demons.append(seed)
    else:
        return
    cycle_check = 60
    time_restore = 60 * 30
    while True:
        await asyncio.sleep(cycle_check)
        if not seeds_storage.get(seed):
            return
        not_passed = True
        for test_number, test_data in seeds_storage[seed]['tests'].items():
            if not_passed and test_data['status'] in [TestStatus.CLAIMED.value, TestStatus.NOT_STARTED.value]:
                not_passed = False
            if test_data['status'] == TestStatus.CLAIMED.value:
                if test_data['time_claimed'] + time_restore < time():
                    if seeds_storage[seed]['tests'][test_number]['status'] == TestStatus.CLAIMED.value:
                        print(f'{seed} {test_number} claimed back')
                        seeds_storage[seed]['tests'][test_number]['status'] = TestStatus.NOT_STARTED.value
                        del seeds_storage[seed]['tests'][test_number]['time_claimed']
        if not_passed:
            return

async def check_seeds_saver_running():
    """
    Check that seeds saver is running and run if it's not
    """
    global seeds_saver_running
    if seeds_saver_running:
        return

    loop = asyncio.get_event_loop()
    loop.create_task(ensure_seeds_save())

async def ensure_seeds_save():
    """
    Periodically save all seeds in file
    """
    global seeds_saver_running
    seeds_saver_running = True
    cycle_period = 60 * 5
    try:
        while True:
            await asyncio.sleep(cycle_period)
            dump(seeds_storage, open(files_path, 'w'))
    except:
        seeds_saver_running = False
        raise

if exists(files_path):
    # load from file if already exist
    seeds_storage = load(open(files_path, 'r'))

app = web.Application()
app.add_routes([
    web.post('/seed', set_seed),
    web.get('/get_test', get_test),
    web.post('/test_passed', test_passed),
    web.delete('/seed', delete_seed),
    web.get('/stat', stat)
]),
web.run_app(app, port=int(environ['PORT']))

