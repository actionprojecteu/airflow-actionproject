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


# ---------------
# Airflow imports
# ---------------

#--------------
# local imports
# -------------

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

with open(resource_filename(__name__, os.path.join('data', 'zooniverse.sql'))) as fd:
	SQL_ZOONIVERSE_SCHEMA = ''.join(fd.readlines())
