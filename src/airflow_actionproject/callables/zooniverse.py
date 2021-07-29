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
import json

# ---------------
# Airflow imports
# ---------------

from panoptes_client import Panoptes, Project, Workflow, SubjectSet, Subject, SubjectWorkflowStatus

#--------------
# local imports
# -------------

from airflow_actionproject.hooks.zooniverse import ZooniverseHook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------


def zooniverse_manage_subject_sets(conn_id, threshold):
	'''Callable to use with ShortCircuitOperator'''
	complete = False
	with ZooniverseHook(conn_id) as hook:
		workflows_summary = hook.get_workflows_summary()
		for workflow in workflows_summary:
			numerator   = workflow['retired_count']
			denominator = workflow['subjects_count']
			if denominator == 0:
				complete = True
				break
			elif int(100*numerator/denominator) >= threshold:
				complete = True
				break
		if complete:
			hook.remove_subject_sets(workflows_summary)
	return complete
