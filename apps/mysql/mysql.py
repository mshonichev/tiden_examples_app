#!/usr/bin/env python3

from tiden.apps.app import App
from tiden.apps.nodestatus import NodeStatus

from tiden.util import *


class Mysql(App):
    tmp_pwd_log_tag = "A temporary password is generated for root@localhost:"

    account_tmpl = [
        "CREATE USER '__USER__'@'__HOST__' IDENTIFIED BY '__PWD__';",
        "GRANT ALL PRIVILEGES ON *.* TO '__USER__'@'__HOST__' WITH GRANT OPTION;"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_type = 'mysql'
        self.init_file = None
        self.username = None
        self.password = None
        self.mysql_home = None
        if self.config.get('rt'):
            self.mysql_home = "%s/%s" % (self.config['rt']['remote']['test_module_dir'], self.name)
            mod_dir = self.config['rt']['remote']['test_module_dir']
            self.killall_mysqld_mark = self.config['dir_prefix']
            server_id = 1
            port_start = 30000
            servers_per_host = 1
            if self.config['environment']['mysql'].get('servers_per_host') is not None:
                servers_per_host = int(self.config['environment']['mysql']['servers_per_host'])
            for host in self.config['environment']['mysql']['server_hosts']:
                for x in range(0, servers_per_host):
                    self.nodes[server_id] = {
                        'host': host,
                        'server_id': server_id,
                        'data_dir': "%s/data.%s" % (self.mysql_home, server_id),
                        'log_dir': "%s/mysql.server.%s.logs" % (mod_dir, server_id)
                    }
                    server_id += 1

    def setup(self):
        prepare_dirs = {}
        self.mysql_home = "%s/%s" % (self.config['rt']['remote']['test_module_dir'], self.name)
        for server_id in self.nodes.keys():
            host = self.nodes[server_id]['host']
            if not prepare_dirs.get(host):
                prepare_dirs[host] = []
            self.nodes[server_id]['status'] = NodeStatus.NEW
            prepare_dirs[host].extend(
                [
                    "ln -s %s %s" % (
                        self.config['artifacts'][self.name]['remote_path'],
                        self.mysql_home
                    ),
                    'mkdir %s' % self.nodes[server_id]['data_dir'],
                    'mkdir %s' % self.nodes[server_id]['log_dir']
                ]
            )
            server_id += 1
        self.ssh.exec(prepare_dirs)

    def init_db(self):
        init = {}
        for server_id in self.nodes.keys():
            host = self.nodes[server_id]['host']
            if not init.get(host):
                init[host] = []
            init[host].extend(
                [
                    'cd %s; bin/mysqld --defaults-file=%s --initialize-insecure' % (
                        self.mysql_home,
                        self.nodes[server_id]['config']
                    )
                ]
            )
            server_id += 1
        self.ssh.exec(init)

    def check_requirements(self):
        self.require_artifact('mysql')
        self.require_environment('mysql')

    def teardown(self):
        self.killall()

    def start(self):
        start_cmd = {}
        pid_cmd = {}
        log_print('Start MySQL server(s)')
        server_options = ''
        if self.init_file:
            server_options += " --init-file=%s" % self.init_file
        for server_id in self.nodes.keys():
            host = self.nodes[server_id]['host']
            if not start_cmd.get(host):
                start_cmd[host] = []
                pid_cmd[host] = []
            self.nodes[server_id]['status'] = NodeStatus.STARTING
            start_cmd[host].extend(
                [
                    "cd %s; nohup bin/mysqld --defaults-file=%s -D%s;" % (
                        self.mysql_home,
                        self.nodes[server_id]['config'],
                        server_options
                    ),
                ]
            )
            pid_cmd[host].extend(
                [
                    "cat %s/mysqld.pid | awk '{print %s, $1}'" % (
                        self.nodes[server_id]['data_dir'],
                        server_id
                    )
                ]
            )
        self.ssh.exec(start_cmd)
        res = self.ssh.exec(pid_cmd)
        for host in res.keys():
            lines = res[host]
            for line in lines:
                for server_id in self.nodes.keys():
                    m = search('^' + str(server_id) + ' ([0-9]+)$', line.rstrip())
                    if m:
                        self.nodes[server_id]['pid'] = m.group(1)
                        self.nodes[server_id]['status'] = NodeStatus.STARTED
                        log_print('MySQL server %s started on %s, pid %s' % (server_id, host, m.group(1)))

    def stop(self):
        log_print('Stop MySQL server(s)')
        cmd = {}
        wait_cmd = {}
        killing_server_num = 0
        for server_id in self.nodes.keys():
            if self.nodes[server_id]['status'] == NodeStatus.STARTED:
                host = self.nodes[server_id]['host']
                if not cmd.get(host):
                    cmd[host] = []
                    wait_cmd[host] = []
                cmd[host].extend(
                    [
                        "nohup kill %s & 2>&1 >> %s/mysql.kill.log" % (
                            self.nodes[server_id]['pid'], self.config['rt']['remote']['test_dir']
                        )
                    ]
                )
                wait_cmd[host].extend(
                    [
                        "ps -p %s | grep -c mysql" % (
                            self.nodes[server_id]['pid']
                        )
                    ]
                )
                self.nodes[server_id]['status'] = NodeStatus.KILLING
                killing_server_num += 1
        self.ssh.exec(cmd)
        started = int(time())
        timeout_counter = 0
        while timeout_counter < 60:
            res = self.ssh.exec(wait_cmd)
            cur_killed_server_num = 0
            for host in res.keys():
                for line in res[host]:
                    if line.rstrip() == '0':
                        cur_killed_server_num += 1
                log_put("Wait for stopped MySQL servers %s/%s in %s/%s sec:" %
                        (
                            cur_killed_server_num,
                            killing_server_num,
                            timeout_counter,
                            60
                        )
                        )
            if cur_killed_server_num == killing_server_num:
                log_put("MySQL servers stopped in %s sec:" % timeout_counter)
                break
            sleep(2)
            timeout_counter = int(time() - started)
        log_print()

    def set_account(self, username, pwd):
        accounts_stmts = []
        for host in (self.config['environment']['mysql']['server_hosts'] + ['127.0.0.1', 'localhost']):
            for line in self.account_tmpl:
                accounts_stmts.append(
                    "%s\n" % line.replace('__HOST__', host).replace('__USER__', username).replace('__PWD__', pwd)
                )
        accounts_file = "%s/set_accounts.sql" % self.config['rt']['test_resource_dir']
        with open(accounts_file, 'w') as w:
            w.writelines(accounts_stmts)
        self.ssh.upload_for_hosts(
            self.config['environment']['mysql']['server_hosts'],
            [accounts_file],
            self.config['rt']['remote']['test_dir']
        )
        self.init_file = "%s/set_accounts.sql" % self.config['rt']['remote']['test_dir']
        self.username = username
        self.password = pwd

    def change_master(self, master_id, slave_id):
        cmds = {
            self.nodes[slave_id]['host']: [
                "echo \"CHANGE MASTER TO "
                "MASTER_HOST='%s', "
                "MASTER_USER='%s', "
                "MASTER_PASSWORD='%s', "
                "MASTER_PORT=%s;\" > "
                "%s/change_master.sql" % (
                    self.nodes[master_id]['host'],
                    self.username,
                    self.password,
                    self.nodes[master_id]['port'],
                    self.config['rt']['remote']['test_dir'],
                ),
                "cd %s;bin/mysql -u%s -p%s -h127.0.0.1 -P%s "
                " < %s/change_master.sql" % (
                    self.mysql_home,
                    self.username,
                    self.password,
                    self.nodes[slave_id]['port'],
                    self.config['rt']['remote']['test_dir']
                )
            ]
        }
        res = self.ssh.exec(cmds)

    def reset_master(self, master_id):
        cmd = {
            self.nodes[master_id]['host']: [
                "cd %s;bin/mysql -u%s -p%s -h127.0.0.1 -P%s "
                "--execute='%s'" % (
                    self.mysql_home,
                    self.username,
                    self.password,
                    self.nodes[master_id]['port'],
                    'RESET MASTER;'
                )
            ]
        }
        res = self.ssh.exec(cmd)

    def start_slave(self, slave_id):
        cmds = {
            self.nodes[slave_id]['host']: [
                "cd %s;bin/mysql -u%s -p%s -h127.0.0.1 -P%s "
                "--execute='START SLAVE;'" % (
                    self.mysql_home,
                    self.username,
                    self.password,
                    self.nodes[slave_id]['port'],
                )
            ]
        }
        res = self.ssh.exec(cmds)

    def get_node(self, id):
        return self.nodes[id]

    def get_non_collocated_nodes(self):
        nodes = {}
        for server_id in self.nodes.keys():
            host = self.nodes[server_id]['host']
            if not nodes.get(host):
                nodes[host] = server_id
        return list(nodes.values())

    def exec_statement(self, node_id, stmt):
        rs = []
        host = self.nodes[node_id]['host']
        cmd = {
            host: [
                "cd %s;bin/mysql -u%s -p%s -h127.0.0.1 -P%s -E "
                "--execute='%s;'" % (
                    self.mysql_home,
                    self.username,
                    self.password,
                    self.nodes[node_id]['port'],
                    stmt
                )
            ]
        }
        res = self.ssh.exec(cmd)
        row_idx = 1
        cur = None
        for line in res[host][0].split('\n'):
            if line.startswith('*') and str("* %s. row *" % row_idx) in line:
                if cur is not None:
                    rs.append(cur)
                row_idx += 1
                cur = {}
            elif ':' in line and row_idx > 1:
                sep_idx = line.find(':')
                if sep_idx > 0:
                    cur[line[0:sep_idx].strip()] = line[sep_idx + 1:].strip()
        if cur is not None:
            rs.append(cur)
        return rs

    def killall(self):
        get_pid_cmd = ["ps ax | grep '%s'" % self.killall_mysqld_mark]
        raw_pid_data = self.ssh.exec(get_pid_cmd)
        kill_cmd = {}
        for host in raw_pid_data.keys():
            for line in raw_pid_data[host][0].split('\n'):
                m = search('^([0-9]+)\s+', line.strip())
                if m and 'mysqld' in line:
                    pid = m.group(0)
                    if kill_cmd.get(host) is None:
                        kill_cmd[host] = []
                    kill_cmd[host].append('kill -9 %s; sleep 1;' % pid)
        if len(kill_cmd) > 0:
            self.ssh.exec(kill_cmd)
            started = int(time())
            timeout_counter = 0
            while timeout_counter < 60:
                raw_pid_data = self.ssh.exec(get_pid_cmd)
                cur_running_server_num = 0
                for host in raw_pid_data.keys():
                    for line in raw_pid_data[host][0].split('\n'):
                        if 'mysqld' in line and self.killall_mysqld_mark in line:
                            cur_running_server_num += 1
                log_put(
                    "Wait for killed MySQL servers, still running %s, %s/%s sec:" % (
                        cur_running_server_num,
                        timeout_counter,
                        60
                    ))
                if cur_running_server_num == 0:
                    log_put("MySQL servers killed in %s sec:" % timeout_counter)
                    break
                sleep(2)
                timeout_counter = int(time() - started)
            log_print()
