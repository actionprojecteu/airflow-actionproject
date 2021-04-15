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

# ------------------
# Auxiliar functions
# ------------------

def epi_v_remapper(collection):
	'''Map Epicollect V metadata to an internal, more convenient representation'''
	def remap_location(item):
		item['id_type'] = 'ec5_uuid'
		location = item['location']
		item['location'] = {
			'latitude' : location.get('latitude', None), 
			'longitude': location.get('longitude', None),
			 'accuracy': location.get('accuracy', None)
			}
		return item

	name_map = {
		'ec5_uuid'            : 'id',
		'created_at'          : 'created_at',
		'uploaded_at'         : 'uploaded_at',
		'title'               : 'title',
		'1_Share_your_nick_wi': 'nickname', 
		# This is for the old mobile APP form
		'2_Date'              : 'local_date', 
		'3_Time'              : 'local_time',
		'4_Location'          : 'location',
		'5_Take_an_image_of_a': 'url',
		'6_Observations'      : 'comment',
	}
	# remaps each dictionary entry in collection with new names
	result =  [{name_map[name]: val for name, val in row.items()} for row in collection]
	return list(map(remap_location,result))


def remap_to_subject(item):
	'''Converts an epicollect V item to a Zooniverse Subject'''
	subject = Subject()
	subject.metadata['id']         = item['id']
	subject.metadata['id_type']    = item['id_type']
	subject.metadata['url']        = item['url']
	subject.metadata['created_at'] = item['created_at']
	# ---------------------------------------------------
	#      Use 'created_at' instead
	# subject.metadata['local_date'] = item['local_date']
	# subject.metadata['local_time'] = item['local_time']
	# ---------------------------------------------------
	subject.metadata['longitude']  = item['location']['longitude']
	subject.metadata['latitude']   = item['location']['latitude']
	subject.metadata['comment']    = item['comment']
	subject.add_location({'image/jpg': item['url']})
	return subject


def create_subjects(project, metadata_file):
	with open(metadata_file,'r') as fd:
		log.info("Reading metadata file {0}".format(metadata_file))
		result = fd.readlines()
		result = " ".join(result)
		result = json.loads(result)
		collection = result["data"]
		collection = epi_v_remapper(collection)
		subjects   = list(map(remap_to_subject, collection))
		log.info("Saving {0} subjects".format(len(subjects)))
		for subject in subjects:
			subject.links.project = project.id
			subject.save()
		return subjects
		
# ----------------------
# Coomand implementation
# ----------------------

def create(options):
	log.info("Creating new Subject Set for project {0}".format(options.project))
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		workflows = project.links.workflows
		subject_set = SubjectSet()
		subject_set.links.project = project
		subject_set.display_name = " ".join(options.name)
		subject_set.save()
		subjects = create_subjects(project, options.file)
		subject_set.add(subjects)
		for workflow in workflows:
			workflow.add_subject_sets(subject_set)
			log.info("AddingSubject Set {0} to Workflow {1}".format(subject_set.id,workflow.id))
		log.info("Created new Subject Set #{0}".format(subject_set.id))



def show(options):
	'''Only for debugging purposes'''
	log.info("Getting subject sets for this project")
	with Panoptes(username=options.username, password=options.password):
		slug = f"{options.username}/{options.project}"
		log.info("Finding project by slug: {0}".format(slug))
		project   = Project.find(slug=slug)
		for ss in SubjectSet.where(project_id=project.id):
			log.info("SubjectSet id #{0}, name {1}, metadata {2}".format(ss.id, ss.display_name, ss.metadata))
			for subject in ss.subjects:
				status = next(SubjectWorkflowStatus.where(subject_id=subject.id))
				log.info("  Subject id #{0}, Status id #{1}".format(subject.id, status.id))
				log.info("  Subject Retirement Reason {0}".format(status.retirement_reason))
				log.info("  Subject Classification Count {0}".format(status.classifications_count))

