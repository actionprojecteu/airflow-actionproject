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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#--------------
# local imports
# -------------

from airflow_actionproject.hooks.epicollect5 import EpiCollect5Hook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class EC5ExportEntriesOperator(BaseOperator):
	"""
	Operator that fetches entries from Epicollect V API.
	Parameters
	—————
	conn_id : str
	ID of the connection to use to connect to the Epicollect V API. 
	output_path : str
	Path to write the fetched entries to.
	start_date : str
	(Templated) start date to start fetching entries from (inclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	end_date : str
	(Templated) end date to fetching entries up to (exclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	"""
	
	template_fields = ("_start_date", "_end_date", "_output_path")

	@apply_defaults
	def __init__(self, conn_id, output_path, start_date, end_date, **kwargs):
		super(EC5ExportEntriesOperator, self).__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._start_date = start_date
		self._end_date = end_date


	def execute(self, context):
		with EpiCollect5Hook(self._conn_id) as hook:
			self.log.info(f"Fetching entries for {self._start_date} to {self._end_date}")
			entries = list(
				hook.get_entries( 
					start_date = self._start_date,
					end_date = self._end_date
				)
			)
			self.log.info(f"Fetched {len(entries)} entries")
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(entries, indent="",fp=fd)
			self.log.info(f"Written entries to {self._output_path}")


