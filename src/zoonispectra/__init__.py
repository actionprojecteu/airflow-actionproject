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

# ---------------
# Airflow imports
# ---------------

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

# All this must be stripped from the final script after we polish it

DEFAULT_TIMEOUT = 20*60 # Twenty minutes
DEFAULT_LANGUAGE = 'en'

__version__ = get_versions()['version']

del get_versions
