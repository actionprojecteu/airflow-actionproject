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
import datetime

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
	'''
	Operator that uploads observations into the ACTION database.

	Parameters
	—————
	conn_id : str
	Aiflow connection id to connect to the ACTION database. 
	(Templated) input_path : str
	Path to the JSON file with observations.
	'''
	
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
	'''
	Operator that fetches entries from Epicollect V API.

	Parameters
	—————
	conn_id : str
	Aiflow connection id to connect to Epicollect V. 
	output_path : str
	Path to write the fetched entries to.
	start_date : str
	(Templated) start date to start fetching entries from (inclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	end_date : str
	(Templated) end date to fetching entries up to (exclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	obs_type : str
	Observation type, either "observation" or "classification". Defaults to "observation"
	'''
	
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
	'''
	Operator that fetches project entries from the ACTION database starting 
	from a given start date. Entries may be either observations or classifications.

	Parameters
	—————
	conn_id : str
	Aiflow connection id to connect to the ACTION database.
	output_path : str
	Path to write the fetched entries to.
	start_date : str
	(Templated) start date to start fetching entries from (inclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	end_date : str
	(Templated) end date to fetching entries up to (exclusive).
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	project : str
	Project name
	n_entries : str
	Number of entries to download
	obs_type : str
	Observation type, either "observation" or "classification". Defaults to "observation"
	'''
	
	template_fields = ("_start_date", "_output_path")

	@apply_defaults
	def __init__(self, conn_id, output_path, start_date,  project, n_entries, obs_type='observation', **kwargs):
		super().__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._start_date = start_date
		self._end_date = '2999-12-31T23:59:59.99999Z'	# far away
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
				self.log.info(f"Got {N} entries, discarding last {excess} entries")
				observations = observations[:-excess]
			self.log.info(f"Fetched {len(observations)} entries")
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(observations, indent="",fp=fd)
			self.log.info(f"Written entries to {self._output_path}")


class ActionDownloadFromVariableDateOperator(BaseOperator):
	'''
	Operator that fetches project entries from the ACTION database starting 
	from a given start date. Entries may be either observations or classifications.
	The start date is managed as an Airflow Variable.
	The variable is updated with the next available start date for the project.
	
	Parameters
	—————
	conn_id : str
	Aiflow connection id to connect to the ACTION database. 
	output_path : str
	(Templated) Path to write the fetched entries to.
	variable_name : str
	Airflow Variable name that controlling the start date as a UTC timestamp 'YYYY-MM-DDTHH:MM:SS.SSSSSZ'.
	Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
	project : str
	Project name
	n_entries : str
	Number of entries to download
	obs_type : str
	Observation type, either "observation" or "classification". Defaults to "observation"
	'''
	
	
	template_fields = ("_output_path",)

	@apply_defaults
	def __init__(self, conn_id, output_path, variable_name,  project, n_entries, obs_type='observation', **kwargs):
		super().__init__(**kwargs)
		self._conn_id = conn_id
		self._output_path = output_path
		self._key = variable_name
		self._end_date = '2999-12-31T23:59:59.99999Z'	# far away
		self._n_entries = n_entries + 1 # taking into account that we will discard the tast one
		self._obs_type = obs_type
		self._project = project


	def execute(self, context):
		def remap_date(item):
			# Patches MongoDB dates to appear like in ACTION format with milliseconds and Z ending
			tstamp = datetime.datetime.strptime(item["created_at"], "%Y-%m-%d %H:%M:%S")
			item["created_at"] = tstamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
			tstamp = datetime.datetime.strptime(item["uploaded_at"], "%Y-%m-%d %H:%M:%S")
			item["uploaded_at"] = tstamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
			return item
		start_date = Variable.get(self._key)
		with ActionDatabaseHook(self._conn_id) as hook:
			self.log.info(f"Fetching {self._n_entries} entries from date {start_date}")
			observations = list(
				hook.download( 
					start_date = start_date,
					end_date   = self._end_date,
					n_entries  = self._n_entries,
					project    = self._project,
					obs_type   = self._obs_type,
				)
			)
		observations = list(map(remap_date, observations))	# The map call is to patch dates
		N = len(observations)
		excess = self._n_entries - N 
		if excess > 0:
			self.log.info(f"Got only {N} entries, discarding last {excess} entries")
			observations = observations[:-excess]
		self.log.info(f"Fetched {len(observations)} entries")
		# Removes the last item and updates the timestamp marker
		last_item = observations.pop()
		Variable.set(self._key, last_item['created_at'])
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(observations, indent=2,fp=fd)
			self.log.info(f"Written entries to {self._output_path}")

