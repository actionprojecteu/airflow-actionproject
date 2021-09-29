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
	'''
	Operator that requests and downloads a Zooniverse project export file.
	It may take a while to execute due to the Zooniverse export process.

	Parameters
	—————
	conn_id : str
	Aiflow connection id to connect to Zooniverse. 
	output_path : str
	(Templated) Path to the complete Zooniverse export JSON file.
	generate : bool
	Flag to generate a new export. Defaults to 'True'. 'False' can be used for testing purposes.
	In this case, the previous export is download
	wait : bool
	Flag to wait for the new export file. Defaults to 'True'. 'False' can be used for testing purposes.
	timeout : int
	Wait timeout in seconds. Defaults to 120 seconds. 
	'''

	template_fields = ("_output_path",)

	@apply_defaults
	def __init__(self, conn_id, output_path, generate=False, wait=True, timeout=120, **kwargs):
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
	'''
	Operator that extract ONLY new classifications since the last run from a
	already downloaded Zooniverse export file. 
	This necessary to avoid inserting duplicated classifications to the ACTION Database
	or to simply not run all the classficiation process for the whole export.

	Parameters
	—————
	conn_id : str
	Aiflow connection id to an internal consolidation SQLite database.
	input_path : str
	(Templated) Path to input Zooniverse export JSON file.
	output_path : str
	(Templated) Path to ouput Zooniverse subset export JSON file.
	start_date_threshold : str
	Start Y-M-d date when classifications are taken as valid. None if all classifications are valid.
	'''

	template_fields = ("_input_path", "_output_path")


	@apply_defaults
	def __init__(self, conn_id, input_path, output_path, start_date_threshold=None, **kwargs):
		super().__init__(**kwargs)
		self._output_path = output_path
		self._input_path  = input_path
		self._conn_id     = conn_id
		if start_date_threshold:
			self._start_date_threshold = datetime.datetime.strptime(start_date_threshold, "%Y-%m-%d")
		else:
			self._start_date_threshold = None


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
		(before,) = hook.get_first('''SELECT MAX(created_at) FROM zoo_export_t''')
		for record in raw_exported:
			created_at = datetime.datetime.strptime(record['created_at'],"%Y-%m-%d %H:%M:%S UTC")
			if (self._start_date_threshold and created_at < self._start_date_threshold):
				continue
			record['gold_standard'] = json.dumps(record['gold_standard'])
			record['expert']        = json.dumps(record['expert'])
			record['annotations']   = json.dumps(record['annotations'])
			record['metadata']      = json.dumps(record['metadata'])
			record['subject_data']  = json.dumps(record['subject_data'])
			record['subject_ids']   = json.dumps(record['subject_ids'])
			hook.run(
				'''
				INSERT OR REPLACE INTO zoo_export_t (
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
		(after,) = hook.get_first('''SELECT MAX(created_at) FROM zoo_export_t''')
		timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		differences = {'executed_at': timestamp, 'before': before, 'after': after}
		self.log.info(f"Logging classifications differences {differences}")
		hook.run(
			'''
			INSERT INTO zoo_export_window_t (executed_at, before, after) VALUES (:executed_at, :before, :after)
			''', parameters=differences)
		(N,) = hook.get_first('''SELECT count(*) FROM zoo_export_t''')
		self.log.info(f"Zooniverse export size contains {N} records")


	def _generate(self, hook, context):
		self.log.info(f"Exporting new classifications to {self._output_path}")
		before, after = hook.get_first(
			'''
			SELECT before, after 
			FROM zoo_export_window_t
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
				FROM zoo_export_t
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



class ZooniverseTransformOperator(BaseOperator):
    '''
    Operator that transforms classifications exported from 
    the Zooniverse exported JSON file (or subset file). 
    It adds three extra metadata: 
		"project": "<your project>"
		"source": "Zooniverse"
		"obs_type": "classification"
    
    
    Parameters
    —————
    input_path : str
    (Templated) Path to read the input JSON to transform to.
    output_path : str
    (Templated) Path to write the output transformed JSON.
    project : str
    Project name whose classifications are being transformed
    '''
    
    template_fields = ("_input_path", "_output_path")

    @apply_defaults
    def __init__(self, input_path, output_path, project, **kwargs):
        super().__init__(**kwargs)
        self._input_path  = input_path
        self._output_path = output_path
        self._project     = project


    def execute(self, context):
        self.log.info(f"Transforming Zooniverse classifications from JSON file {self._input_path}")
        with open(self._input_path) as fd:
            entries = json.load(fd)
        result = list(self._zoo_remapper(entries))
        # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path,'w') as fd:
            json.dump(result, fp=fd, indent=2)
        self.log.info(f"Transformed Zooniverse classifications to output JSON file {self._output_path}")

    # --------------
    # Helper methods
    # --------------
    def _remap(self, item):
        # Fixes timestamps format
        dt = datetime.datetime.strptime(item['created_at'],'%Y-%m-%d %H:%M:%S UTC').replace(microsecond=0)
        item['created_at'] = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # Delete items not needed
        # Add extra items
        item["project"] = self._project
        item["source"] = "Zooniverse"
        item["obs_type"] = "classification"
        return item


    def _zoo_remapper(self, entries):
        '''Map Zooniverse to an ernal, more convenient representation'''
        # Use generators instead of lists
        return map(self._remap, entries)
