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



# ----------------
# Module constants
# ----------------


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("dbase")

# ----------------------
# Command implementation
# ----------------------



# ----------------------
# COMMAND IMPLEMENTATION
# ----------------------

def install(options):
	log.info(f"Installing database {options.name} into {options.directory}")