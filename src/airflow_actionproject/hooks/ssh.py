# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------
# Copyright (c) 2021
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import os
import json
import time
import shlex
import datetime
import subprocess

# ---------------
# Airflow imports
# ---------------

from airflow.hooks.base import BaseHook

#--------------
# local imports
# -------------

from airflow_actionproject import __version__

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class MissingKeyfileError(ValueError):
    '''Missing 'key_file' path in password'''
    def __str__(self):
        s = self.__doc__
        if self.args:
            s = ' {0}: {1}'.format(s, str(self.args[0]))
        s = '{0}.'.format(s)
        return s


class SCPHook(BaseHook):

    DEFAULT_CONN_TYPE = "scp"
    DEFAULT_PORT      = 22
    DEFAULT_TIMEOUT   = None
    DEFAULT_DOC_ROOT  = "/"

    def __init__(self, ssh_conn_id):
        super().__init__()
        self._conn_id = ssh_conn_id
        self._key_file = None
        self._jump_hosts = list()
        self._timeout    = self.DEFAULT_TIMEOUT  

    def get_conn(self):
        if not self._key_file:
            self.log.info(f"{self.__class__.__name__} version {__version__}")
            self.log.debug(f"getting connection information from {self._conn_id}")
            config = self.get_connection(self._conn_id)
            self._host     = config.host
            self._login    = config.login
            self._key_file = config.password
            self._port     = config.port or self.DEFAULT_PORT
            self._doc_root = config.schema or self.DEFAULT_DOC_ROOT

            self.log.info(f"KEY FILE =  {config.password}")
            if not os.path.isfile(config.password):
                raise MissingKeyfileError()
            if config.extra:
                try:
                    extra  = json.loads(config.extra)
                except json.decoder.JSONDecodeError:
                    pass
                else:
                    self._jump_hosts = extra.get("jump_hosts", [])   
                    self._timeout    = extra.get("timeout", self.DEFAULT_TIMEOUT)                  
        return self._host, self._port, self._login, self._key_file, self._jump_hosts


    # ----------
    # Public API
    # ----------

    def __enter__(self):
        '''Support for hook context manager'''
        self.get_conn()
        return self

    def __exit__(self, type, value, traceback):
        '''Support for hook context manager'''
        self.close()

    def doc_root(self):
        return self._doc_root

    def scp_to_remote(self, from_path, to_path):
        '''
        Copy file from local to remote site.

        Parameters
        —————
        from_path : str
        Local file path
        to_path : str
        Remote file path.
        '''
        host, port, user, key_file, jump_hosts = self.get_conn()
        to_path = os.path.join(self._doc_root, to_path)
        if jump_hosts:
            jump_hosts = ','.join(jump_hosts)
            command = f"scp -i {key_file} -P {port} -J {jump_hosts} {from_path} {user}@{host}:{to_path}"
        else:
            command = f"scp -i {key_file} -P {port}  {from_path} {user}@{host}:{to_path}"
        self.log.info(f"Executing {command}")
        command = shlex.split(command)
        result = subprocess.run(command, capture_output=True, timeout=self._timeout)
        return result.returncode
       
        
    def close(self):
        self.log.info(f"Closing SSH hook")
        self._key_file = None