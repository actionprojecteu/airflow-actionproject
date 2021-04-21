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
import logging
import shutil
import datetime
import glob


from . import RESOURCES_DAGS_DIR

# ----------------
# Module constants
# ----------------


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("actiontool")

# ----------------------
# Command implementation
# ----------------------


# ----------------------
# COMMAND IMPLEMENTATION
# ----------------------

def install(options):
	log.info(f"Installing dag '{options.name}' into '{options.directory}'")
	os.makedirs(options.directory, exist_ok=True)
	filename = options.name + '.py'
	src_filename = os.path.join(RESOURCES_DAGS_DIR, filename)
	dest_filename = os.path.join(options.directory, filename)
	shutil.copy2(src_filename, dest_filename)
	log.info(f"Copied dag file'{src_filename}' into '{dest_filename}'")