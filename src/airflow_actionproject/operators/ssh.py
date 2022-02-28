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

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#--------------
# local imports
# -------------

from airflow_actionproject import __version__
from airflow_actionproject.hooks.ssh import SCPHook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class SCPCopyError(Exception):
    '''SCP Failed failed to copy'''
    def __str__(self):
        s = self.__doc__
        if self.args:
            s = ' {0}: {1}'.format(s, str(self.args[0]))
        s = '{0}.'.format(s)
        return s


class SCPOperator(BaseOperator):
    '''
    Operator that copies a local file to a remote location.

    Parameters
    —————
    ssh_conn_id : str
    Aiflow connection id to connect by SCP. 
    local_path : str
    (Templated) Local file path.
    remote_path : str
    (Templated) remote file path (relative to a document root set by the hook).
    '''
    
    template_fields = ("_local_path", "_remote_path")

    @apply_defaults
    def __init__(self, ssh_conn_id, local_path, remote_path, **kwargs):
        super().__init__(**kwargs)
        self._ssh_conn_id = ssh_conn_id
        self._local_path = local_path
        self._remote_path = remote_path


    def execute(self, context):
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        scp_hook = SCPHook(ssh_conn_id = self._ssh_conn_id)
        status_code = scp_hook.scp_to_remote(self._local_path, self._remote_path)
        if status_code != 0:
            raise SCPCopyError("local {self._local_path} to remote {self._remote_path}")