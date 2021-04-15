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

def create(options):
	'''Only for debugging purposes'''
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		workflow = Workflow()
		workflow.links.project    = project
		workflow.display_name     = " ".join(options.name)
		workflow.primary_language = options.language
		workflow.active           = True
		workflow.kk = "foo"
		
		workflow.save()
		log.info("{0}".format(workflow))
		project.reload()
		log.info("{0}".format(project.links.workflows))

		
def subjectsets(options):
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		with open(options.file, 'w') as fd:
			workflows = list()
			for workflow in project.links.workflows:
				subject_sets = list()
				for ss in workflow.links.subject_sets:
					subject_sets.append({
						'subjectset_id' : ss.id,
						'display_name'  : ss.display_name,
						})
				workflows.append({
					'workflow_id': workflow.id, 'subject_sets': subject_sets
				})
			json.dump(workflows, fp=fd, indent=2)



def completion(options):
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		workflows = list()
		for workflow in project.links.workflows:
			log.info("Workflow id: {0}, display_name: {1}".format(workflow.id, workflow.display_name))
			numerator   = workflow.retired_set_member_subjects_count
			denominator = workflow.subjects_count
			percentage = int(100*numerator/denominator) if denominator != 0 else 0
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
					'percentage'     : int(100*retired_count/subjects_count) if subjects_count != 0 else 0,
				})
			workflows.append({
				'id'             : workflow.id,
				'display_name'   : workflow.display_name,
				'subjects_count' : denominator,
				'retired_count'  : numerator,
				'percentage'     : percentage,
				'subject_sets'   : subject_sets
			})
			log.info(f"Global completion percentage for workflow '{workflow.display_name}' is {percentage}%")
	with open(options.file, 'w') as fd:
		json.dump(workflows, fp=fd, indent=2)
	print(workflows)
