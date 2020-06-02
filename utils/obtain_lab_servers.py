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

import sys
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import time
import argparse

url_base = 'https://eco-system.gridgain.com'
headers = {'content-type': 'application/json'}
api_key = ''
api_secret_key = ''

user_ids = {
    'isuntsov@gridgain.com': 6,
    'iartukhov@gridgain.com': 83
}


def print_host_info(servers):
    for srv in servers:
        user_name = "None"
        if srv['locked_user']:
            user_name = srv['locked_user'].get('name')
        print(" * Group: %s; User: %s; IP address: %s; Idle since: %s" % (
            srv['resource_group']['name'],
            user_name,
            srv['public_ip'],
            srv['status'].get('start_idle', "N/A")
        ))
    return


def lab_lock(servers, user_id):
    print("Locking servers...")

    r = requests.get(url_base + '/api/tx/start', auth=HTTPBasicAuth(api_key, api_secret_key))
    tx_id = json.loads(r.text).get('tx_id')

    d = {
        'locked': True,
        'locked_user_id': user_id,
    }
    data = {'actions': [{'cls': 'Server', 'method': 'bulk_update', 'ids': [s.get('id') for s in servers], 'data': d}]}
    r = requests.post(url_base + '/api/tx/' + tx_id + '/add', data=json.dumps(data), headers=headers, auth=HTTPBasicAuth(api_key, api_secret_key))

    if r.status_code == 200:
        requests.get(url_base + '/api/tx/' + tx_id + '/commit', auth=HTTPBasicAuth(api_key, api_secret_key))
    else:
        print("Failed to lock servers! Status code: %s\n%s" % (r.status_code, r.text))
        sys.exit(1)
    return

# TODO: get user id via eco-system's API

def get_eco_user_id(email):
    # 3 -- integration user ID
    return user_ids.get(email, 3)


def write_hosts_file(servers, clients):
    f_name = "test_hosts.properties"
    srv_ips = []
    drv_ips = []

    for srv in servers:
        srv_ips.append(srv['public_ip'])
    for drv in clients:
        drv_ips.append(drv['public_ip'])

    with open(f_name, 'w') as f:
        f.write('SERVER_HOSTS=' + ','.join(srv_ips) + '\n')
        f.write('CLIENT_HOSTS=' + ','.join(drv_ips) + '\n')

    print('Hosts were written to %s' % f_name)
    return


def write_notification_text(servers, preferred_users=[]):
    if not servers:
        print("No need to notify")
        return

    f_name_text = "notification_text.txt"
    f_name_recipients = "notification_recipients.txt"
    recipients = {}

    for server in servers:
        # Skip unlocked servers
        if not server['locked_user']:
            continue
        else:
            email = server['locked_user']['email']
            if email in preferred_users:
                continue
            ip = server['public_ip']
            idle_since = server['status'].get('start_idle')
            if not email in recipients.keys():
                recipients[email] = []
            recipients[email].append((ip, idle_since))

    if recipients:
        with open(f_name_recipients, 'w') as f:
            email_to = ['eng-qa@gridgain.com']
            for e in recipients.keys():
                if e == 'integration@gridgain.com':
                    continue
                email_to.append(e)
            f.write(', '.join(email_to))
        print("%s written" % f_name_recipients)

        with open(f_name_text, 'w') as f:
            f.write("The following lab servers are found to be idle and will be relocked for automated tests:\n\n")
            for email in recipients.keys():
                for info in recipients[email]:
                    f.write("* User: %s, IP address: %s, idle since: %s\n" % (
                        email,
                        info[0],
                        info[1]
                    ))
        print("%s written" % f_name_text)
    return


def lab_get_idle(preferred_users=[], relock_other_users=False):
    # Servers locked by users from preferred users list
    s_prio = []
    # Unlocked servers
    s_unlocked = []
    # Sorted servers of preferred users
    s_prio_sorted = []
    # Servers locked by other users
    s_other = []

    # Use only enabled servers
    url_query = '?filter=enable=true'
    r = requests.get(url_base + '/api/resources/servers/search' + url_query,
                     auth=HTTPBasicAuth(api_key, api_secret_key))
    if r.status_code != 200:
        print("Status code is %s" % r.status_code)
        sys.exit(1)
    # All available servers in the lab
    srv_all = json.loads(r.text)

    time_now = datetime.now()
    for server in srv_all:
        # Skip servers from lower lab
        if server['resource_group_id'] != 2:
            continue
        if server['locked']:
            if server.get('status'):
                if server['status'].get('java') == 0:
                    if server['locked_user'].get('email') in preferred_users:
                        s_prio.append(server)
                    elif relock_other_users:
                        # Skip servers locked till some moment
                        if server['locked_till']:
                            continue
                        idle_since = server['status'].get('start_idle')
                        if idle_since:
                            time_idle_since = datetime.strptime(idle_since, "%Y-%m-%d %H:%M")
                            time_delta = time_now - time_idle_since
                            if time_delta.seconds > idle_hours_min * 60 * 60:
                                s_other.append(server)
        else:
            s_unlocked.append(server)

        # Sort priority servers by preferred user
        s_prio_sorted = sorted(s_prio, key=lambda s: preferred_users.index(s['locked_user']['email']))
        # Always use unlocked servers
        s_prio_sorted += s_unlocked

    return s_prio_sorted, s_other

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-srv_host_num', type=int, default=4, required=False,
                        help='Number of server hosts to lock')
    parser.add_argument('-drv_host_num', type=int, default=8, required=False,
                        help='Number of driver hosts to lock')
    parser.add_argument('-idle_hours_min', type=int, default=12, required=False,
                        help='Minimum amount of time a host runs without Java processes (hours)')
    parser.add_argument('-retries', type=int, default=10, required=False,
                        help='Number of attempts to lock servers')
    parser.add_argument('-retry_interval_s', type=int, default=900, required=False,
                        help='Pause between attempts (seconds)')
    parser.add_argument('-preferred_users', default="", required=False,
                        help='Comma-separated list of user emails preferred for locking. Hosts will be re-locked under first user.')
    parser.add_argument('-relock_other_users', required=False, default=False, action='store_true',
                        help='Search for hosts locked by other users. Unlocked hosts will always be used.')
    parser.add_argument('-dry_run', required=False, default=False, action='store_true',
                        help='Get hosts but do not lock anything')
    parser.add_argument('-k', required=True,
                        help='Eco-system API key')
    parser.add_argument('-sk', required=True,
                        help='Eco-system secret API key')
    args = parser.parse_args()

    api_key = args.k
    api_secret_key = args.sk

    # Lock servers which is idle for more than this threshold
    idle_hours_min = args.idle_hours_min
    preferred_users = []
    if args.preferred_users:
        for user in args.preferred_users.split(','):
            preferred_users.append(user)
    relock_other_users = args.relock_other_users

    retries = args.retries
    retry_interval_s = args.retry_interval_s

    srv_host_num = args.srv_host_num
    drv_host_num = args.drv_host_num

    dry_run = args.dry_run

    print("Have to lock %s hosts. Will search between:" % (srv_host_num + drv_host_num))
    print(" * All unlocked")
    if preferred_users:
        print(" * Hosts locked by", preferred_users)
    if relock_other_users:
        print(" * Hosts locked by other users")

    for retry in range(1, retries+1):
        print('-'*10)
        print('Try %s out of %s' % (retry, retries))
        (srv_free_prio, srv_free_other) = lab_get_idle(preferred_users, relock_other_users)
        print("Hosts with no Java processes last %s hour(s) (total %s):" % (
            idle_hours_min,
            len(srv_free_prio + srv_free_other)))
        print_host_info((srv_free_prio + srv_free_other))

        if len(srv_free_prio + srv_free_other) >= (srv_host_num + drv_host_num):
            print("Found needed amount of hosts!")
            srv_to_lock = (srv_free_prio + srv_free_other)[:(srv_host_num + drv_host_num)]
            print_host_info(srv_to_lock)
            write_notification_text(srv_to_lock, preferred_users)
            write_hosts_file(srv_to_lock[:srv_host_num], srv_to_lock[srv_host_num:])
            if not dry_run:
                # print("Will lock found servers after 1 minute. You may abort the job")
                # time.sleep(1*60)
                lock_user_id = 3  # Integration user
                if preferred_users:
                    lock_user_id = get_eco_user_id(preferred_users[0])
                lab_lock(srv_to_lock, lock_user_id)
            else:
                print("Dry run. Will not lock anything")
            break

        print("Cannot find needed amount of hosts. Will retry after %s seconds (%s minutes)" % (
            retry_interval_s,
            int(retry_interval_s/60)
        ))
        time.sleep(retry_interval_s)
    else:
        print("Failed obtain %s idle servers from lab. Exiting" % (srv_host_num + drv_host_num))
        sys.exit(2)
