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


# Access SQL scripts withing the package
from pkg_resources import resource_filename

#--------------
# local imports
# -------------

from airflow_actionproject._version import get_versions


# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

RESOURCES_DAGS_DIR  = resource_filename(__name__, os.path.join('data','dags'))
RESOURCES_DBASE_DIR = resource_filename(__name__, os.path.join('data','sql'))


__version__ = get_versions()['version']

del get_versions
