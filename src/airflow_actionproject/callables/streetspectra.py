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

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

#--------------
# local imports
# -------------

# -----------------------
# Module global variables
# -----------------------

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
