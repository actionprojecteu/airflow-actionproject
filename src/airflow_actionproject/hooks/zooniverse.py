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

import json
import os

# ---------------
# Airflow imports
# ---------------

from airflow.hooks.base import BaseHook

from panoptes_client import Panoptes, Project, Workflow, SubjectSet, Subject, SubjectWorkflowStatus

#--------------
# local imports
# -------------

from airflow_actionproject import __version__

# -----------------------
# Module global variables
# -----------------------


class MissingLoginError(ValueError):
    '''Missing Zooniverse login in Airflow connection'''
    def __str__(self):
        s = self.__doc__
        if self.args:
        	s = f"{s}: {self.args[0]}"
        return s

class MissingPasswordError(ValueError):
    '''Missing Zooniverse password in Airflow connection'''
    def __str__(self):
        s = self.__doc__
        if self.args:
        	s = f"{s}: {self.args[0]}"
        return s

class MissingSchemaError(ValueError):
    '''Missing Zooniverse schema (project slug) in Airflow connection'''
    def __str__(self):
        s = self.__doc__
        if self.args:
        	s = f"{s}: {self.args[0]}"
        return s

# ----------------
# Module constants
# ----------------

class ZooniverseHook(BaseHook):

	DEFAULT_HOST      = "www.zooniverse.org" 
	DEFAULT_CONN_TYPE = "https"
	DEFAULT_PORT      = 443

	def __init__(self, conn_id):
		super().__init__()
		self._conn_id = conn_id
		self._panoptes_client = None
		self._project = None


	def get_conn(self):
		if self._panoptes_client is None:
			self.log.info(f"{self.__class__.__name__} version {__version__}")
			self.log.debug(f"getting connection information from {self._conn_id}")
			config = self.get_connection(self._conn_id)
			ctyp     = config.conn_type or self.DEFAULT_CONN_TYPE
			host     = config.host      or self.DEFAULT_HOST
			port     = config.port      or self.DEFAULT_PORT
			slug     = config.schema
			login    = config.login
			password = config.password
			if config.extra:
				try:
					extra  = json.loads(config.extra)
				except json.decoder.JSONDecodeError:
					self._auto_disable_subject_sets = False
				else:
					self._auto_disable_subject_sets = extra.get("auto_disable_ssets", False)
			if not login:
				raise MissingLoginError(self._conn_id)
			if not password:
				raise MissingPasswordError(self._conn_id)
			if not slug:
				raise MissingSchemaError(self._conn_id)
			project_slug = f"{login}/{slug}" 
			endpoint = f"{ctyp}://{host}:{port}"
			self._panoptes_client = Panoptes.connect(username=login, password=password, endpoint=endpoint)
			self._project = Project.find(slug=project_slug)
			self.log.info(f"Searching project by slug {project_slug} found: {self._project}")
		return self._panoptes_client, self._project


	# ----------
	# Public API
	# ----------

	def __enter__(self):
		'''Support for hook context manager'''
		self.get_conn()
		return self

	def __exit__(self, type, value, traceback):
		'''Support for hook context manager'''
		self.close()

	def get_workflows_summary(self):
		'''
		Get workflows summary completion status and per active subject set completion status.
		Parameters
		—————
		None

		Return
		—————
		A list of workflows and nested subject sets.
		Example
		[{
			'id': '17959', 
			'display_name': 'Light Source Classification', 
			'subjects_count': 5, 
			'retired_count': 3, 
			'percentage': 60, 
			'subject_sets': [
				{'id': '93295', 'display_name': 'Street Spectra Test Subject Set 2', 'subjects_count': 5, 'retired_count': 3,},
			]
		}]
		'''
		panoptes_client, project   = self.get_conn()
		workflows = list()
		for workflow in project.links.workflows:
			self.log.info("Workflow id: {0}, display_name: {1}".format(workflow.id, workflow.display_name))
			numerator   = workflow.retired_set_member_subjects_count
			denominator = workflow.subjects_count
			subject_sets = list()
			for ss in workflow.links.subject_sets:
				retired_count  = 0
				subjects_count = 0
				for subject in ss.subjects:
					status = next(SubjectWorkflowStatus.where(subject_id=subject.id))
					subjects_count += 1
					if status.retirement_reason is not None:
						retired_count += 1
				subject_sets.append({
					'id'             : ss.id,
					'display_name'   : ss.display_name,
					'subjects_count' : subjects_count,
					'retired_count'  : retired_count,
				})
			workflows.append({
				'id'             : workflow.id,
				'display_name'   : workflow.display_name,
				'subjects_count' : denominator,
				'retired_count'  : numerator,
				'subject_sets'   : subject_sets
			})
			ratio = int(100*numerator/denominator) if denominator != 0 else 0
			self.log.info(f"Global completion state for workflow '{workflow.display_name}' is {numerator}/{denominator} ({ratio}%)")
		return workflows


	def remove_subject_sets(self, workflows_summary):
		'''
		Removes completed subject sets from workflow by analyzing the workflows summary
		returned by get_workflows_summary()
		'''
		self.log.info(f"Trying to disable completed Subject Sets from workflows")
		if not self._auto_disable_subject_sets:
			self.log.info(f"Configuration prevents from disabling Subject Sets automatically")
			return
		panoptes_client, project   = self.get_conn()
		for wsum in workflows_summary:
			workflow = Workflow.find(wsum['id'])
			subject_sets = list()
			for ss in wsum['subject_sets']:
				if (ss['subjects_count'] != 0) and (ss['retired_count'] == ss['subjects_count']):
					subject_set = SubjectSet.find(ss['id'])
					subject_sets.append(subject_set)
			workflow.remove_subject_sets(subject_sets)
			self.log.info(f"Disabled: {len(subject_sets)} Subject Sets from '{workflow.display_name}'")


	def add_subject_set(self, **kwargs):
		'''
		Create and Add a new subject set to a workflow
		returned by get_workflows_summary()
		This is specific to each project, as the subject metadata is different
		'''
		raise NotImplementedError("This operation must be subclassed")


	def export_classifications(self, generate, wait, timeout):
		'''Export Zooniverse classification data'''
		panoptes_client, project   = self.get_conn()
		self.log.info(f"Requesting export for '{project.display_name}'. This may take a while")
		http_response = project.get_export(
			'classifications',
			generate = generate, 
			wait = wait, 
			wait_timeout = timeout
		)
		http_response.raise_for_status()
		for row in http_response.csv_dictreader(): 
			# These three fields are already encoded to JSON, so we must
			# load them as Python dicts before we marcsall all the row to JSON again
			row['metadata']     = json.loads(row['metadata'])
			row['annotations']  = json.loads(row['annotations'])
			row['subject_data'] = json.loads(row['subject_data'])
			yield row
		


	def close(self):
		self.log.info(f"Closing Zooniverse hook")
		self._project = None
		self._panoptes_client = None
