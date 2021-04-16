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

# This hook lnows how to insert StreetSpectra metadata into subjects
from airflow_actionproject.hooks.streetspectra import ZooniverseHook


# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------



class EC5TransformOperator(BaseOperator):
	"""
	Operator that transforms entries exported from 
	Epicollect V API to ACTION  StreetSpectra JSON.
	
	Parameters
	—————
	input_path : str
	(Templated) Path to read the input JSON to transform to.
	output_path : str
	(Templated) Path to write the output transformed JSON.
	"""

	# This mapping is unique to StreetSpectra
	NAME_MAP = {
		'ec5_uuid'            : 'id',
		'created_at'          : 'created_at',
		'uploaded_at'         : 'uploaded_at',
		'title'               : 'title',
		# New form names
		'1_Share_your_nick_wi': 'observer', 
		'2_Location'          : 'location',
		'3_Take_an_image_of_a': 'url',
		'4_Observations'      : 'comment',
		# Old form names
		'1_Date'              : 'date',
		'2_Time'              : 'time',
		'3_Location'          : 'location',
		'4_Take_an_image_of_a': 'url',
		'5_Observations'      : 'comment',
	}
	
	template_fields = ("_input_path", "_output_path")

	@apply_defaults
	def __init__(self, input_path, output_path, **kwargs):
		super().__init__(**kwargs)
		self._input_path  = input_path
		self._output_path = output_path


	def execute(self, context):
		self.log.info(f"Transforming EC5 observations from JSON file {self._input_path}")
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
		with open(self._input_path) as fd:
			entries = json.load(fd)
		result = list(self._ec5_remapper(entries))
		with open(self._output_path,'w') as fd:
			json.dump(result, fp=fd, indent=2)
		self.log.info(f"Transformed EC5 observations to output JSON file {self._output_path}")

	# --------------
	# Helper methods
	# --------------
	def _remap(self, item):
		# Fixes timestamps format
		dt = datetime.datetime.strptime(item['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0)
		item['created_at'] = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
		dt = datetime.datetime.strptime(item['uploaded_at'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0)
		item['uploaded_at'] = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
		
		# Handle old Epicollect form entries not needed
		if "time" in item:
			del item["time"]
		if "date" in item:
			del item["date"]
		# Cleans up location info
		location = item['location']
		item['location'] = {
			'latitude' : location.get('latitude', None), 
			'longitude': location.get('longitude', None),
			'accuracy':  location.get('accuracy', None)
		}
		# Add extra items
		item["project"] = "street-spectra"
		item["source"] = "Epicollect5"
		item["type"] = "observation"
		return item


	def _ec5_remapper(self, entries):
		'''Map Epicollect V metadata to an internal, more convenient representation'''
		# Use generators instead of lists
		g1 = ({self.NAME_MAP[name]: val for name, val in entry.items()} for entry in entries)
		g2 =  map(self._remap, g1)
		return g2


class ZooniverseImportOperator(BaseOperator):

	template_fields = ("_input_path, _display_name")


	@apply_defaults
	def __init__(self, input_path, display_name, **kwargs):
		super().__init__(**kwargs)
		self._input_path   = input_path
		self._display_name = display_name


	def execute(self, context):
		self.log.info(f"Uploading observations to Zooniverse from {self._input_path}")
		with open(self._input_path) as fd:
			subjects_metadata = json.load(fd)
		with ZooniverseHook(self._conn_id) as hook:
			hook.add_subject_set(self._display_name, subjects_metadata)
		

class ZooniverseAccumulateOperator(BaseOperator):
	"""
	Operator that accumulate Zooniverse exports for a givem project. 
	This necessary to load into the ACTIOndatabase only new classifications.

	Parameters
	—————
	input_path : str
	(Templated) Path to input Zooniverse export JSON file.
	sqlite_conn_id : str
	SQLite connection id for the Consolidation Database.
	"""

	template_fields = ("_input_path, _display_name")


	@apply_defaults
	def __init__(self, input_path, sqlite_conn_id, **kwargs):
		super().__init__(**kwargs)
		self._input_path = input_path
		self._conn_id    = sqlite_conn_id

	def execute(self, context):
		self.log.info(f"Consolidating data from {self._input_path}")
		with open(self._input_file) as fd:
			raw_exported = json.load(fd)
		hook = SqliteHook(sqlite_conn_id=self._conn_id)
		for record in raw_exported:
			record['gold_standard'] = json.dumps(record['gold_standard'])
			record['expert']       = json.dumps(record['expert'])
			record['annotations']  = json.dumps(record['annotations'])
			record['subject_data'] = json.dumps(record['subject_data'])
			record['subject_ids']  = json.dumps(record['subject_ids'])
	     #    hook.run(
	     #    	'''
	     #    		INSERT OR REPLACE INTO zooniverse_export_t (
	     #    			classification_id,
						# user_name,
						# user_id,
						# workflow_id,
						# workflow_name,	
						# workflow_version,
						# created_at,	
						# gold_standard,	
						# expert,	
						# metadata,	
						# annotations,	
						# subject_data,
						# subject_ids
	     #    		) VALUES (
	     #    			:classification_id,
						# :user_name,
						# :user_id,
						# :workflow_id,
						# :workflow_name,	
						# :workflow_version,
						# :created_at,	
						# :gold_standard,	
						# :expert,	
						# :metadata,	
						# :annotations,	
						# :subject_data,
						# :subject_ids
	     #    		)
	     #    	''', parameters=record)
	      
