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

import datetime

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

DEFAULT_TPS     = 1
DEFAULT_LIMIT   = 20
DEFAULT_PGSZ    = 5

# Default dates whend adjusting in a rwnge of dates
DEFAULT_START_DATE = datetime.datetime(year=2019,month=1,day=1)
DEFAULT_END_DATE   = datetime.datetime(year=2999,month=12,day=31)



__version__ = get_versions()['version']

del get_versions
