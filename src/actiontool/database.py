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
import glob
import logging
import sqlite3

# -------------
# Local imports
# -------------

from . import RESOURCES_DBASE_DIR

# ----------------
# Module constants
# ----------------

SQL_TEST_STRING = "SELECT COUNT(*) FROM zooniverse_export_t"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("actiontool")

# ----------------------
# Command implementation
# ----------------------




def open_database(dbase_path):
	os.makedirs(os.path.dirname(dbase_path), exist_ok=True)
	if not os.path.exists(dbase_path):
		with open(dbase_path, 'w') as f:
			pass
		log.info(f"Created database file {dbase_path}")
	return sqlite3.connect(dbase_path)


def create_schema(connection, schema_path, test_query, data_dir_path=None):
	created = True
	cursor = connection.cursor()
	try:
		cursor.execute(test_query)
	except Exception:
		created = False
	if not created:
		with open(schema_path) as f: 
			lines = f.readlines() 
		script = ''.join(lines)
		connection.executescript(script)
		connection.commit()
		log.info(f"Created data model from {os.path.basename(schema_path)}")
		if data_dir_path is not None:
			file_list = glob.glob(os.path.join(data_dir_path, '*.sql'))
			for sql_file in file_list:
				log.info(f"Populating data model from {os.path.basename(sql_file)}")
				with open(sql_file) as f: 
					lines = f.readlines() 
				script = ''.join(lines)
				connection.executescript(script)
		connection.commit()

# ----------------------
# COMMAND IMPLEMENTATION
# ----------------------

def install(options):
	log.info(f"Installing '{options.name}'' database into {options.directory}")
	connection = open_database(os.path.join(options.directory, options.name + '.db'))
	schema_path = os.path.join(RESOURCES_DBASE_DIR, options.name + '.sql')
	create_schema(connection, schema_path, SQL_TEST_STRING)
	log.info(f"Installed '{options.name}'' database into {options.directory}")
