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

# ---------------
# Airflow imports
# ---------------

from panoptes_client import Panoptes, Project, Workflow, SubjectSet, Subject, SubjectWorkflowStatus

#--------------
# local imports
# -------------

from airflow_actionproject.hooks.zooniverse import ZooniverseHook as ZooniverseBaseHook

# -----------------------
# Module global variables
# -----------------------


# ----------------
# Module constants
# ----------------

class ZooniverseHook(ZooniverseBaseHook):

	EPICOLLECT5_SOURCE = "Epicollect5"

	def __init__(self, conn_id):
		super().__init__(conn_id)


	def _create_subjects_from_epicollect5(self, project, subjects_metadata):
		subjects = list()
		for metadata in subjects_metadata:
			subject = Subject()
			subject.metadata['id']         = metadata['id']
			subject.metadata['id_type']    = metadata['id_type']
			subject.metadata['url']        = metadata['url']
			subject.metadata['created_at'] = metadata['created_at']
			subject.metadata['observer']   = metadata['observer']
			subject.metadata['longitude']  = metadata['location']['longitude']
			subject.metadata['latitude']   = metadata['location']['latitude']
			subject.metadata['comment']    = metadata['comment']
			subject.add_location({'image/jpg': metadata['url']})
			subject.links.project = project
			subject.save()
			subjects.append(subject)
		return subjects


	# ----------
	# Public API
	# ----------


	def add_subject_set(self, display_name, subjects_metadata):
		'''
		Create and Add a new subject set to a workflow
		returned by get_workflows_summary()
		'''
		project    = self._project
		subject_set = SubjectSet()
		subject_set.display_name = display_name
		subject_set.links.project = project
		subject_set.save()
		source = subjects_metadata[0]['source']
		if source == self.EPICOLLECT5_SOURCE:
			self.log.info(f"Creating {len(subjects_metadata)} subjects to Subject Set {display_name}")
			subjects = self._create_subjects_from_epicollect5(project, subjects_metadata)
		else:
			raise NotImplementedError()
		subject_set.add(subjects)
		for workflow in project.links.workflows:
			workflow.add_subject_sets(subject_set)
			self.log.info(f"Added new Subject Set '{display_name}' to workflow '{workflow.display_name}'")

