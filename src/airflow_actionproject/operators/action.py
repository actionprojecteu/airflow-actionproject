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

from airflow.models import BaseOperator, Variable
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
		super().__init__(**kwargs)
		self._conn_id = conn_id
		self._input_path = input_path


	def execute(self, context):
		with open(self._input_path) as fd:
			observations = json.load(fd)
			self.log.info(f"Parsed observations from {self._input_path}")
		with ActionDatabaseHook(self._conn_id) as hook:
			hook.upload(observations)


class ActionRangedDownloadOperator(BaseOperator):
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
	def __init__(self, conn_id, output_path, start_date, end_date, project, obs_type='observation', **kwargs):
		super().__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._start_date = start_date
		self._end_date = end_date
		self._obs_type = obs_type
		self._project = project


	def execute(self, context):
		with ActionDatabaseHook(self._conn_id) as hook:
			self.log.info(f"Fetching entries from date {self._start_date} to date {self._end_date}")
			observations = list(
				hook.download( 
					start_date = self._start_date,
					end_date   = self._end_date,
					n_entries  = 16000000000,	# high enough
					project    = self._project,
					obs_type   = self._obs_type,	
				)
			)
			self.log.info(f"Fetched {len(observations)} entries")
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(observations, indent="",fp=fd)
			self.log.info(f"Written entries to {self._output_path}")


class ActionDownloadFromStartDateOperator(BaseOperator):
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
	
	template_fields = ("_start_date", "_output_path")

	@apply_defaults
	def __init__(self, conn_id, output_path, start_date,  project, n_entries, obs_type='observation', **kwargs):
		super().__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._start_date = start_date
		self._end_date = '2999-12-31'	# far away
		self._n_entries = n_entries
		self._obs_type = obs_type
		self._project = project


	def execute(self, context):
		with ActionDatabaseHook(self._conn_id) as hook:
			self.log.info(f"Fetching {self._n_entries} entries from date {self._start_date}")
			observations = list(
				hook.download( 
					start_date = self._start_date,
					end_date   = self._end_date,
					n_entries  = self._n_entries,
					project    = self._project,
					obs_type   = self._obs_type,
				)
			)
			N = len(observations)
			excess = N - self._n_entries
			if excess > 0:
				self.log.info(f"Got {N} entries, excess of {excess} entries found")
				observations = observations[:-excess]
			self.log.info(f"Fetched {len(observations)} entries")
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(observations, indent="",fp=fd)
			self.log.info(f"Written entries to {self._output_path}")


class ActionDownloadFromVariableDateOperator(BaseOperator):
	"""
	Operator that fetches N observations from the ACTION database
	starting from a date specified by an Airflow Variable name.
	After fetching that many observations, it updates the variable
	with the timestamp of the next observation pending to read.
	Parameters
	—————
	conn_id : str
	ID of the connection to use to connect to the Epicollect V API. 
	output_path : str
	Path to write the fetched entries to.
	variable_name : str
	Name of the Airflow Variable that contaoins the start timestamp.
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	end_date : str
	(Templated) end date to fetching entries up to (exclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	"""
	
	
	template_fields = ("_output_path",)

	@apply_defaults
	def __init__(self, conn_id, output_path, variable_name,  project, n_entries, obs_type='observation', **kwargs):
		super().__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._key = variable_name
		self._end_date = '2999-12-31'	# far away
		self._n_entries = n_entries + 1
		self._obs_type = obs_type
		self._project = project


	def execute(self, context):
		start_date = Variable.get(self._key)
		with ActionDatabaseHook(self._conn_id) as hook:
			self.log.info(f"Fetching {self._n_entries} entries from date {self._start_date}")
			observations = list(
				hook.download( 
					start_date = start_date,
					end_date   = self._end_date,
					n_entries  = self._n_entries,
					project    = self._project,
					obs_type   = self._obs_type,
				)
			)
			N = len(observations)
			excess = N - self._n_entries
			if excess > 0:
				self.log.info(f"Got {N} entries, excess of {excess} entries found")
				observations = observations[:-excess]
			self.log.info(f"Fetched {len(observations)} entries")
		# Removes the last item and updates the timestamp marker
		last_item = observations.pop()
		Variable.set(self._key, last_item['created_at'])
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(observations, indent="",fp=fd)
			self.log.info(f"Written entries to {self._output_path}")

