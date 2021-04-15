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
import logging

#----------------------
# Panoptes Client stuff
# ---------------------

from panoptes_client import Panoptes, Project, SubjectSet, Subject, Workflow, Classification, SubjectWorkflowStatus
from panoptes_client.panoptes import PanoptesAPIException


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("zoonis")

# ----------------------
# Command implementation
# ----------------------

def find(options):
	'''Only for debugging purposes'''
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		log.info("project id: {0}, display_name: {1}".format(project.id, project.display_name))
		log.info("project private: {0}, primary_language: {1}".format(project.private, project.primary_language))
		for workflow in project.links.workflows:
			log.info("Workflow id: {0}, display_name: {1}".format(workflow.id, workflow.display_name))
			numerator   = workflow.retired_set_member_subjects_count
			denominator = workflow.subjects_count
			percent     = int(100*numerator/denominator) if denominator != 0 else 0
			log.info("Workflow subject stats = {0} / {1} ({2}%)".format(numerator,denominator,percent))
			for ss in workflow.links.subject_sets:
				log.info("Workflow Subject Set Id: {0}, Name: {1}".format(ss.id, ss.display_name))
				

def create(options):
	'''Only for debugging purposes'''
	with Panoptes(username=options.username, password=options.password):
		project = Project()
		project.display_name     = " ".join(options.name)
		project.description      = " ".join(options.description)
		project.primary_language = options.language
		project.private          = options.private
		project.save()
		log.info("{0}".format(project))


def _export(options):
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		log.info("Exporting project {0} classifications. This may take a while".format(slug))
		export_response = project.get_export(
			'classifications',
			generate=options.generate, 
			wait=options.wait, 
			wait_timeout=options.timeout
		)
		log.debug("Response is {0}".format(export_response))
		log.debug("Text is {0}".format(export_response.text))
		export_response.raise_for_status()
		for row in export_response.csv_dictreader():
			row['metadata']     = json.loads(row['metadata'])
			row['annotations']  = json.loads(row['annotations'])
			row['subject_data'] = json.loads(row['subject_data'])
			yield row

def export(options):
	log.info("Getting Project Classification export")
	exported = _export(options)
	with open(options.file, 'w') as fd:
		json.dump(list(exported), fp=fd, indent=2)
		log.info("Written Project Classification export to {0}".format(options.file))


def classifications(options):
	log.info("Getting Project Classification export")
	with Panoptes(username=options.username, password=options.password):
		log.info("Finding project by slug: {0}".format(options.project))
		project   = Project.find(slug=options.project)
		classifications = list()
		for classification in Classification.where(project_id=project.id):
			row = {}
			row['id'] = classification.id
			row['created_at'] = classification.created_at
			row['updated_at'] = classification.updated_at
			row['completed'] = classification.completed
			row['metadata'] = classification.metadata
			#row['gold_standard'] = classification.gold_standard
			row['annotations'] = classification.annotations
			log.debug("Classification: {0}".format(row))
			classifications.append(row)
		with open(options.file, 'w') as fd:
			json.dump(classifications, fp=fd, indent=2)

