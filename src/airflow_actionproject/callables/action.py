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

from airflow_actionproject.hooks.action import ActionDatabaseHook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------


def check_number_of_entries(conn_id, start_date, n_entries, project, true_task_id, false_task_id, obs_type='observation'):
	'''To use with BranchPythonOperator'''
	next_task = false_task_id
	with ActionDatabaseHook(conn_id) as hook:
		observations = list(
			hook.download( 
				start_date = start_date,
				end_date   = '2999-12-31T23:59:59.99999Z',	# far away,
				n_entries  = n_entries+1,
				project    = project,
				obs_type   = obs_type,
			)
		)
	if len(observations) >= (n_entries+1):
		next_task = true_task_id
	return next_task
