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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

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



class ZooniverseExportOperator(BaseOperator):

	template_fields = ("_output_path",)

	@apply_defaults
	def __init__(self, output_path, conn_id, generate=False, wait=True, timeout=120, **kwargs):
		super().__init__(**kwargs)
		self._output_path = output_path
		self._conn_id     = conn_id
		self._generate    = generate
		self._wait        = wait
		self._timeout     = timeout


	def execute(self, context):
		with ZooniverseHook(self._conn_id) as hook:
			exported = list(
				hook.export_classifications(
					self._generate,
					self._wait,
					self._timeout
				)
			)
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, 'w') as fd:
			json.dump(exported, fp=fd, indent=2)
			self.log.info(f"Written Project Classification export to {self._output_path}")



class ZooniverseDeltaOperator(BaseOperator):
	"""
	Operator that extract ONLY new classifications from a
	whole Zooniverse export file since the last run. 
	This necessary to load into the ACTION database only new classifications.

	Parameters
	—————
	input_path : str
	(Templated) Path to input Zooniverse export JSON file.
	output_path : str
	(Templated) Path to ouput Zooniverse subset export JSON file.
	conn_id : str
	SQLite connection id for the internal Consolidation Database.
	"""

	template_fields = ("_input_path", "_output_path")


	@apply_defaults
	def __init__(self, input_path, output_path, conn_id, **kwargs):
		super().__init__(**kwargs)
		self._output_path = output_path
		self._input_path  = input_path
		self._conn_id     = conn_id


	def _reencode(self, classification):
		classification_id, user_name, user_id, user_ip, workflow_id, workflow_name, workflow_version,  \
		created_at, gold_standard, expert, metadata, annotations, subject_data, subject_ids = classification
		return {
			'classification_id': classification_id,
			'user_name'        : user_name,
			'user_id'          : user_id,
			'user_ip'          : user_ip,
			'workflow_id'      : workflow_id,
			'workflow_name'    : workflow_name,
			'workflow_version' : workflow_version,
			'created_at'       : created_at,
			'gold_standard'    : json.loads(gold_standard),
			'expert'           : json.loads(expert),
			'metadata'         : json.loads(metadata),
			'annotations'      : json.loads(annotations),
			'subject_data'     : json.loads(subject_data),
			'subject_ids'      : json.loads(subject_ids)
		}


	def _extract(self, hook, context):
		self.log.info(f"Consolidating data from {self._input_path}")
		with open(self._input_path) as fd:
			raw_exported = json.load(fd)
		(before,) = hook.get_first('''SELECT MAX(created_at) FROM zooniverse_export_t''')
		for record in raw_exported:
			record['gold_standard'] = json.dumps(record['gold_standard'])
			record['expert']        = json.dumps(record['expert'])
			record['annotations']   = json.dumps(record['annotations'])
			record['metadata']      = json.dumps(record['metadata'])
			record['subject_data']  = json.dumps(record['subject_data'])
			record['subject_ids']   = json.dumps(record['subject_ids'])
			hook.run(
				'''
				INSERT OR REPLACE INTO zooniverse_export_t (
					classification_id,
					user_name,
					user_id,
					user_ip,
					workflow_id,
					workflow_name,  
					workflow_version,
					created_at, 
					gold_standard,  
					expert, 
					metadata,   
					annotations,    
					subject_data,
					subject_ids
				) VALUES (
					:classification_id,
					:user_name,
					:user_id,
					:user_ip,
					:workflow_id,
					:workflow_name, 
					:workflow_version,
					:created_at,    
					:gold_standard, 
					:expert,    
					:metadata,  
					:annotations,   
					:subject_data,
					:subject_ids
				)
				''', parameters=record)
		(after,) = hook.get_first('''SELECT MAX(created_at) FROM zooniverse_export_t''')
		timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		differences = {'executed_at': timestamp, 'before': before, 'after': after}
		self.log.info(f"Logging classifications differences {differences}")
		hook.run(
			'''
			INSERT INTO zooniverse_export_window_t (executed_at, before, after) VALUES (:executed_at, :before, :after)
			''', parameters=differences)


	def _generate(self, hook, context):
		self.log.info(f"Exporting new classifications to {self._output_path}")
		before, after = hook.get_first(
			'''
			SELECT before, after 
			FROM zooniverse_export_window_t
			ORDER BY executed_at DESC
			LIMIT 1
			'''
		)
		if before == after:
			self.log.info("No new classifications to generate")
			new_classifications = list()
		else:

			threshold = '2000-02-01T:00:00:00.00000Z' if before is None else before
			new_classifications = hook.get_records(
				'''
				SELECT * 
				FROM zooniverse_export_t
				WHERE created_at > :threshold
				ORDER BY created_at ASC
				''',
				parameters={'threshold': threshold}
			)
			new_classifications = list(map(self._reencode, new_classifications))
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._output_path, "w") as fd:
			json.dump(new_classifications, indent=2,fp=fd)
		self.log.info(f"Written {len(new_classifications)} entries to {self._output_path}")


	def execute(self, context):
		hook = SqliteHook(sqlite_conn_id=self._conn_id)
		self._extract(hook, context)
		self._generate(hook, context)

	