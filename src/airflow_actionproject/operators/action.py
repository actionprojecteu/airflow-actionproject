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

from airflow_actionproject.hooks.action import ActionDatabaseHook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class ActionUploadOperator(BaseOperator):
	"""
	Operator that uploads observations into the ACTION database.
	Parameters
	—————
	conn_id : str
	ID of the connection to use to connect to the ACTION database API. 
	(Templated) input_path : str
	Path to the JSON file with observations.
	"""
	
	template_fields = ("_input_path",)

	@apply_defaults
	def __init__(self, conn_id, input_path, **kwargs):
		super(ActionUploadOperator, self).__init__(**kwargs)
		self._conn_id = conn_id
		self._input_path = input_path


	def execute(self, context):
		with open(self._input_path) as fd:
			observations = json.load(fd)
			self.log.info(f"Parsed observations from {self._input_path}")
		with ActionDatabaseHook(self._conn_id) as hook:
			hook.upload(observations)


class ActionDownloadOperator(BaseOperator):
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
	
	template_fields = ("_start_datetime", "_end_datetime", "_output_path")

	@apply_defaults
	def __init__(self, conn_id, output_path, start_datetime, end_datetime, project, obs_type='observation', **kwargs):
		super(ActionDownloadOperator, self).__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._start_datetime = start_datetime
		self._end_datetime = end_datetime
		self._obs_type = obs_type
		self._project = project


	def execute(self, context):
		with ActionDatabaseHook(self._conn_id) as hook:
			self.log.info(f"Fetching entries for {self._start_date} to {self._end_date}")
			observations = list(
				hook.download( 
					start_datetime = self._start_datetime,
					end_datetime = self._end_datetime,
					project = self._project,
					obs_type = self._obs_type,
				)
			)
			self.log.info(f"Fetched {len(entries)} entries")
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(observations, indent="",fp=fd)
			self.log.info(f"Written entries to {self._output_path}")


