#!/usr/bin/env python3

from tiden.apps.app import App
from tiden.apps.appexception import AppException
from tiden.apps.nodestatus import NodeStatus

from tiden.util import log_print, log_put
from tiden.logger import get_logger

from sys import stdout
from time import sleep, time


class HzException(AppException):
    pass

class Hazelcast(App):
    start_stop_timeout = 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_logger('tiden')

    def check_requirements(self):
        self.require_artifact('hazelcast')
        self.require_environment('hazelcast')

    def setup(self):
        # Find directory list for symlinks for remote hosts
        self._mark_scripts_executable('hazelcast')
        cmds = {}
        node_idx = 1
        # All servers use own directory
        for server_host in self.config['environment']['hazelcast']['server_hosts']:
            cmds[server_host] = []
            server_per_host = 1
            if self.config['environment']['hazelcast'].get('servers_per_host'):
                server_per_host = self.config['environment']['hazelcast']['servers_per_host']
            for idx in range(0, server_per_host):
                home = '%s/%s.server.%s' % (self.config['rt']['remote']['test_module_dir'], self.name, node_idx)
                self.nodes[node_idx] = {
                    'host': server_host,
                    'home': home,
                    'run_counter': 0,
                    'status': NodeStatus.NEW,
                    'log': None
                }
                self.nodes[node_idx]['jvm_options'] = []
                if self.config['environment'].get('server_jvm_options'):
                    self.nodes[node_idx]['jvm_options'].extend(
                        self.config['environment']['server_jvm_options']
                    )
                node_idx += 1
                cmds[server_host].extend(
                    [
                        'mkdir -p %s' % home,
                        'cp -rf %s/bin /%s' % (self.config['artifacts'][self.name]['remote_path'], home),
                        'ln -s %s/lib %s/lib' % (self.config['artifacts'][self.name]['remote_path'], home),
                        'cp -f %s/hazelcast.xml %s/bin/' % (self.config['rt']['remote']['test_module_dir'], home)
                    ]
                )
        res = self.ssh.exec(cmds)

    def __make_node_log(self, node_idx):
        self.nodes[node_idx]['log'] = '%s/hz-grid.%s.node.%s.%s.log' % (
            self.config['rt']['remote']['test_dir'],
            self.name,
            node_idx,
            self.nodes[node_idx]['run_counter']
        )

    def start_nodes(self, **kwargs):
        """
        Start Ignite server nodes
        :return: none
        """
        started_ids = [1, ]

        # Start coordinator
        self.__make_node_log(1)
        node_1 = self.nodes[1]
        node_start_line = "cd %s; nohup bin/start.sh > %s 2>&1 &"
        commands_node_1 = {
            node_1['host']: [
                node_start_line %
                (
                    node_1['home'],
                    node_1['log']
                )
            ]
        }
        self.nodes[1]['run_counter'] += 1
        log_print('Hazelcast: start node 1 on host %s' % node_1['host'])
        self.nodes[1]['status'] = NodeStatus.STARTING
        self.ssh.exec(commands_node_1)
        self.wait_for_started_nodes(
            1,
            None,
            ", host %s" % node_1['host']
        )
        commands = {}
        server_num = 1
        for node_idx in self.nodes.keys():
            server_num += 1
            if node_idx == 1:
                continue
            self.__make_node_log(node_idx)
            host = self.nodes[node_idx]['host']
            if not commands.get(host):
                commands[host] = []
            commands[host].append(
                node_start_line % (
                    self.nodes[node_idx]['home'],
                    self.nodes[node_idx]['log']
                )
            )
            self.nodes[node_idx]['status'] = NodeStatus.STARTING
            self.nodes[node_idx]['run_counter'] += 1
            started_ids.append(node_idx)
        if server_num > 1:
            log_print('Hazelcast: start rest %s node(s)' % (server_num - 1))
            self.ssh.exec(commands)
            self.wait_for_started_nodes(
                server_num - 1,
                None,
                ''
            )

    def update_nodes_status(self, started_ids):
        for node_idx in started_ids:
            if 'PID' in self.nodes[node_idx]:
                self.nodes[node_idx]['status'] = NodeStatus.STARTED

    def stop_nodes(self):
        """
        Stop all server nodes
        :return:
        """
        log_print('Hazelcast: stop nodes')
        self.ssh.exec(['killall -9 java; sleep 1;'])

    def wait_for_started_nodes(self, server_num=None, client_num=None, comment=''):
        """
        Wait for topology snapshot
        :param server_num:  the servers number that expected in topology snapshot
        :param client_num:  the servers number that expected in topology snapshot
        :param comment:     the additional text printed out during waiting process
        :return: none
        """

        wait_for = {
            'servers': server_num,
            'clients': client_num
        }

        started = int(time())
        timeout_counter = 0
        commands = {}
        for node_idx in self.nodes.keys():
            host = self.nodes[node_idx]['host']
            if self.nodes[node_idx]['status'] in [NodeStatus.NEW, NodeStatus.KILLED]:
                continue
            if not commands.get(host):
                commands[host] = []
            if self.nodes[node_idx].get('log') is not None:
                commands[host].append(
                    'echo %s; cat %s | grep -c -E "^INFO: .+ is STARTED$" | tail -1' % (
                        node_idx,
                        self.nodes[node_idx]['log'],
                    )
                )
        succes = False
        while timeout_counter < self.start_stop_timeout:
            res = self.ssh.exec(commands)
            started_servers_num = 0
            for host in res.keys():
                for lines in res[host]:
                    # first line is node id
                    node_idx = int(lines.split('\n')[0].rstrip())
                    # second line is count of found lines by grep
                    node_status = lines.split('\n')[1].rstrip()
                    if node_status == '1':
                        started_servers_num += 1
                        self.nodes[node_idx]['status'] = NodeStatus.STARTED
            if started_servers_num == server_num:
                log_put("Hazelcast: %s server(s) and %s client(s) started in %s/%s" %
                        (
                            started_servers_num,
                            0,
                            timeout_counter,
                            self.start_stop_timeout
                        ))
                succes = True
                break
            log_put(
                "Hazelcast: waiting for started nodes: server(s) %s/%s, client(s) %s/%s, timeout %s/%s sec %s " %
                (
                    started_servers_num,
                    '*' if server_num is None else server_num,
                    0,
                    '*' if client_num is None else client_num,
                    timeout_counter,
                    self.start_stop_timeout,
                    comment
                )
            )
            stdout.flush()
            sleep(2)
            timeout_counter = int(time()) - started
        log_print()

    def get_pids(self):
        read_pids_cmds = {}
        for node_idx in self.nodes.keys():
            host = self.nodes[node_idx]['host']
            if not read_pids_cmds.get('host'):
                read_pids_cmds[host] = []
            read_pids_cmds[host].append(
                ['echo %s;cat %s/bin/hazelcast_instance.pid' % (node_idx, self.nodes[node_idx]['home'])]
            )
        res = self.ssh.exec(read_pids_cmds)
        from pprint import PrettyPrinter
        pp = PrettyPrinter()
        log_print('pids: %s' + pp.pformat(res))
        for host in res.keys():
            for lines in res[host]:
                # first line is node id
                node_idx = int(lines.split('\n')[0].rstrip())
                # second line is count of found lines by grep
                pid = int(lines.split('\n')[1].rstrip())
                self.nodes[node_idx]['pid'] = pid
