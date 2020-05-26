#!/usr/bin/env python3
from datetime import datetime
from os import environ
from time import time
from uuid import uuid4
from re import sub, search
from traceback import format_exc

from requests import post

from tiden.tidenplugin import TidenPlugin
from tiden.util import log_print
TIDEN_PLUGIN_VERSION = '1.0.0'


class ExamplePlugin(TidenPlugin):

    def __init__(self, *args, **kwargs):
        TidenPlugin.__init__(self, *args, **kwargs)
        log_print('ExamplePlugin: __init__(' + repr(args) + ", " + repr(kwargs) + ")", color='green')

    def before_test_method(self, *args, **kwargs):
        """
        Start report
        :param kwargs:
                test_module         'snapshots.test_snapshots'
                artifacts  :dict    All found artifacts
                test_name           'test_snapshot(wal_compaction=False,...)'
        """
        log_print("ExamplePlugin: before_test_method("+repr(args)+","+repr(kwargs)+")", color='green')

    def after_test_method(self, *args, **kwargs):
        """
        Send test reports
        :param kwargs:
                test_status     'passe'/'fail'
                exception       Exception name
                stacktrace      Exception stacktrace
                known_issue     Ticket ID
                description     Test method doc
        """
        log_print("ExamplePlugin: before_test_method("+repr(args)+","+repr(kwargs)+")", color='green')
