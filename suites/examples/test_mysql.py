#!/usr/bin/env python3

from tiden.case.apptestcase import AppTestCase
from tiden.util import log_print, render_template, attr
from tiden_gridgain.apps import Mysql

from time import sleep

class TestMysql (AppTestCase):

    def __init__(self, *args):
        super().__init__(*args)
        self.add_app('mysql')

    def setup(self):
        super().setup()
        self.tiden.ssh.killall('mysqld')
        # Generate config file per server
        res_dir = self.tiden.config['rt']['test_resource_dir']
        mod_dir = self.tiden.config['rt']['remote']['test_module_dir']
        app: Mysql = self.get_app_by_type('mysql')[0]
        port_start = 30000
        for server_id in app.nodes.keys():
            # Path to remote config file
            remote_server_config = "%s/my.%s.cnf" % (mod_dir, server_id)
            # Render config file by template
            render_template(
                "%s/my.tmpl.cnf" % res_dir,
                server_id,
                {
                    'id': server_id,
                    'port': str(port_start+server_id),
                    'config': remote_server_config,
                    'data_dir': app.nodes[server_id]['data_dir'],
                    'log_dir': app.nodes[server_id]['log_dir'],
                    'unique_id': "%s_%s" % (self.tiden.config['dir_prefix'], server_id),
                    'tmp_dir': self.tiden.config['environment'].get('tmp_dir', '/tmp'),
                }
            )
            # Update node attributes
            app.nodes[server_id].update({
                'port': str(port_start + server_id),
                'config': remote_server_config
            })
            # Upload config files
            self.tiden.ssh.upload_for_hosts(
                [
                    app.nodes[server_id]['host']
                ],
                [
                    "%s/my.%s.cnf" % (res_dir, server_id)
                ],
                mod_dir
            )
        # Initialize clean database
        app.init_db()

    @attr('mysql')
    def test_mysql_replication(self):
        """ Start MySQL replication 1 master and N slaves.
        Load data in master database and verify that the data replicated on the slaves
        """
        ssh = self.tiden.ssh
        app = self.get_app_by_type('mysql')[0]
        select = "SELECT * FROM test_app.t1"
        # Set account
        app.set_account('replication_user', 'replication_password')
        # Start servers
        app.start()
        # Set server 1 as master for rest of servers
        master_id = 1
        slaves = []
        for node_id in app.nodes.keys():
            if node_id != 1:
                slaves.append(node_id)
        log_print("Set up replication %s -> %s" % (master_id, str(slaves)))
        app.reset_master(master_id)
        # Get binary log file and position on master
        master_before = app.exec_statement(master_id, "SHOW MASTER STATUS")
        log_print("Master %s reached %s:%s" % (
            master_id,
            master_before[0]['File'],
            master_before[0]['Position'])
        )
        for slave_id in slaves:
            app.change_master(master_id, slave_id)
        # Start slaves
        log_print("Start slaves")
        for slave_id in slaves:
            app.start_slave(slave_id)
        # Show master binary log file and position reached on slave
        self.slave_reached_file_pos(app, slaves)
        # Load data on master
        log_print("Run SQL statements in master")
        load_cmd = {
            app.get_node(master_id)['host']: [
                "cd %s;bin/mysql -u%s -p%s -h127.0.0.1 -P%s < %s/load.sql" % (
                    app.mysql_home,
                    app.username,
                    app.password,
                    app.nodes[1]['port'],
                    self.tiden.config['rt']['remote']['test_module_dir']
                ),
            ],

        }
        ssh.exec(load_cmd)
        # Get binary log file and position on master
        master_after = app.exec_statement(master_id, "SHOW MASTER STATUS")
        log_print("Master %s reached %s:%s" % (
            master_id,
            master_after[0]['File'],
            master_after[0]['Position'])
        )
        # Select loaded data on master
        log_print("Select on master: %s" % select)
        master_select = app.exec_statement(master_id, select)
        # Verify loaded data
        self.print_rs(master_select)
        assert len(master_select) == 2, "Two records selected"
        # Give the time for replication sync
        sleep(2)
        # Show binary log file and position reached on slave
        self.slave_reached_file_pos(app, slaves)
        # Show Select on slaves
        for slave_id in slaves:
            log_print("Select on slave %s:" % slave_id)
            slave_select = app.exec_statement(slave_id, select)
            assert master_select == slave_select, "Comparison master and slave %s selects" % slave_id
            self.print_rs(slave_select)
        app.stop()

    @staticmethod
    def slave_reached_file_pos(app, slaves):
        """
        Helper method.
        Print reached file and position on teh slave
        :param app:     application instance
        :param slaves:  the list of slave ids
        :return:        None
        """
        slaves_status = {}
        for slave_id in slaves:
            slave_status = app.exec_statement(slave_id, "SHOW SLAVE STATUS")
            file_pos = "%s:%s" % (slave_status[0]['Master_Log_File'], slave_status[0]['Read_Master_Log_Pos'])
            if slaves_status.get(file_pos) is None:
                slaves_status[file_pos] = []
            slaves_status[file_pos].append(slave_id)
        for file_pos in slaves_status.keys():
            log_print("Slaves %s reached %s" % (str(slaves_status[file_pos]), file_pos))

    @staticmethod
    def print_rs(rs):
        """
        Helper method.
        Print rows.
        :param rs:  the list of dictionary formatted as follow:
                    [{'field1': value1, 'field2': value2}, {'field1': value1, 'field2': value2}]
        :return:    None
        """
        if not rs:
            log_print('Result Set empty', color='red')
        else:
            for row in rs:
                line = ""
                for col in sorted(row.keys()):
                    line += "%s: %s, " % (col, row[col])
                log_print(line[:-2])

    def teardown(self):
        super().teardown()
