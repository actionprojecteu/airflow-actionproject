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

import hashlib
import logging

# ---------------
# Airflow imports
# ---------------

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

#--------------
# local imports
# -------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# ----------------
# Module constants
# ----------------


def check_new_subjects(conn_id, true_task_id, false_task_id):
	'''Callable to use with BranchOperator'''
	hook = SqliteHook(sqlite_conn_id=conn_id)
	new_subjects = hook.get_first('''
			SELECT COUNT (DISTINCT subject_id)
			FROM spectra_classification_t 
			WHERE source_id IS NULL
		''')
	if new_subjects[0] != 0:
		next_task = true_task_id
	else:
		next_task = false_task_id
	return next_task


def check_new_csv_version(conn_id, input_path, input_type, true_task_id, false_task_id, execution_date, **context):
	'''Callable to use with BranchOperator'''

	# Compute impu file hash
	BLOCK_SIZE = 1048576//2
	file_hash = hashlib.md5()
	with open(input_path, 'rb') as f:
		block = f.read(BLOCK_SIZE) 
		while len(block) > 0:
			file_hash.update(block)
			block = f.read(BLOCK_SIZE)
	new_hash = file_hash.digest()
	version = execution_date.strftime("%y.%m")
	log.info(f"Checking CSV file {input_path}, version {version}, hash {new_hash}")
	hook = SqliteHook(sqlite_conn_id=conn_id)
	(n,) = hook.get_first('''SELECT COUNT (*) FROM zenodo_csv_t WHERE hash = :hash''', 
		parameters={'hash': new_hash})
	if n:
		log.info(f"Duplicate version of {input_path} has been found, so we skip uploading it")
		return false_task_id

	(n,) = hook.get_first('''SELECT COUNT (*) FROM zenodo_csv_t WHERE type = :type AND version == :version''', 
		parameters={'version': version, 'type': input_type})
	if n:
		log.info(f"Another version {version} for {input_type} CSV has already been uploaded for this month, so we skip uploading it.")
		return false_task_id

	log.info(f"A new CSV file {input_path}, version {version}, hash {new_hash} is been inserted into the database")
	hook.insert_many(
            table        = 'zenodo_csv_t',
            rows         = {'hash': new_hash, 'version': version, 'type': input_type},
            commit_every = 500,
            replace      = False,
        )       
	return true_task_id
	