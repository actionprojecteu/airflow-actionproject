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

# This hook knows how to insert StreetSpectra metadata o subjects
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


	def execute(self, con):
		self.log.info(f"Transforming EC5 observations from JSON file {self._input_path}")
		with open(self._input_path) as fd:
			entries = json.load(fd)
		result = list(self._ec5_remapper(entries))
		# Make sure the output directory exists.
		output_dir = os.path.dirname(self._output_path)
		os.makedirs(output_dir, exist_ok=True)
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
		item["obs_type"] = "observation"
		return item


	def _ec5_remapper(self, entries):
		'''Map Epicollect V metadata to an ernal, more convenient representation'''
		# Use generators instead of lists
		g1 = ({self.NAME_MAP[name]: val for name, val in entry.items()} for entry in entries)
		g2 =  map(self._remap, g1)
		return g2


# --------------------------------------------------------------
# This class is specific to StreetSpectra because 
# this particular ZooniverseHook is specific to StreetSpectra
# --------------------------------------------------------------

class ZooniverseImportOperator(BaseOperator):

	template_fields = ("_input_path", "_display_name")

	@apply_defaults
	def __init__(self, conn_id, input_path, display_name, **kwargs):
		super().__init__(**kwargs)
		self._conn_id      = conn_id
		self._input_path   = input_path
		self._display_name = display_name


	def execute(self, con):
		self.log.info(f"Uploading observations to Zooniverse from {self._input_path}")
		with open(self._input_path) as fd:
			subjects_metadata = json.load(fd)
		with ZooniverseHook(self._conn_id) as hook:
			hook.add_subject_set(self._display_name, subjects_metadata)


class ZooniverseTransformOperator(BaseOperator):
	"""
	Operator that transforms classifications exported from 
	Zooniverse API to an ACTION  StreetSpectra JSON file.
	
	Parameters
	—————
	input_path : str
	(Templated) Path to read the input JSON to transform to.
	output_path : str
	(Templated) Path to write the output transformed JSON.
	"""
	
	template_fields = ("_input_path", "_output_path")

	@apply_defaults
	def __init__(self, input_path, output_path, **kwargs):
		super().__init__(**kwargs)
		self._input_path  = input_path
		self._output_path = output_path


	def execute(self, con):
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
		item["project"] = "street-spectra"
		item["source"] = "Zooniverse"
		item["obs_type"] = "classification"
		return item


	def _zoo_remapper(self, entries):
		'''Map Zooniverse to an ernal, more convenient representation'''
		# Use generators instead of lists
		return map(self._remap, entries)



class StreetSpectraLoadInternalDBOperator(BaseOperator):
	"""
	Operator that transforms classifications exported from 
	Zooniverse API to an ACTION  StreetSpectra JSON file.
	
	Parameters
	—————
	input_path : str
	(Templated) Path to read the input JSON to transform to.
	output_path : str
	(Templated) Path to write the output transformed JSON.
	"""
	
	template_fields = ("_input_path",)

	SPECTRUM_TYPE = {
		0: 'LED',	# Light Emitting Diode
		1: 'HPS',	# High Pressure Sodium
		2: 'LPS',	# Low Pressure Sodium
		3: 'MH',	# Metal Halide
		4: 'MV',	# Mercury Vapor
	}

	@apply_defaults
	def __init__(self, input_path, conn_id, **kwargs):
		super().__init__(**kwargs)
		self._input_path  = input_path
		self._conn_id     = conn_id


	def _extract(self, classification):
		new = dict()
		# General info
		new["id"]         = classification["classification_id"]
		new["subject_id"] = classification["subject_ids"]
		new["user_id"]    = classification["user_id"]
		sd = classification["metadata"]["subject_dimensions"][0]
		if sd:
			new["width"]      = sd["naturalWidth"]
			new["height"]     = sd["naturalHeight"]
		else:
			new["width"]      = None
			new["height"]     = None
		value =  classification["annotations"][0]["value"]
		if value:
			# Light source info
			new["source_x"]   = value[0]["x"]
			new["source_y"]   = value[0]["y"]
			# Spectrum tool info
			if len(value) > 1:	# The user really used this tool
				new["spectrum_x"] = value[1]["x"]
				new["spectrum_y"] = value[1]["y"]
				new["spectrum_width"]  = value[1]["width"]
				new["spectrum_height"] = value[1]["height"]
				new["spectrum_angle"]  = value[1]["angle"]
				new["spectrum_type"]   = value[1]["details"][0]["value"]
				new["spectrum_type"] = self.SPECTRUM_TYPE[new["spectrum_type"]] # remap spectrum type codes to strings
			else:
				new["spectrum_x"] = None
				new["spectrum_y"] = None
				new["spectrum_width"]  = None
				new["spectrum_height"] = None
				new["spectrum_angle"]  = None
				new["spectrum_type"]   = None
		else:	# The user skipped this observation
			# Light source info
			new["source_x"]   = None
			new["source_y"]   = None
			# Spectrum tool info
			new["spectrum_x"] = None
			new["spectrum_y"] = None
			new["spectrum_width"]  = None
			new["spectrum_height"] = None
			new["spectrum_angle"]  = None
			new["spectrum_type"]   = None

		# Metadata coming from the Observing Platform
		key = list(classification["subject_data"].keys())[0]
		value = classification["subject_data"][key]
		new["image_id"]         = value["id"]
		new["image_url"]        = value["url"]
		new["image_long"]       = value["longitude"]
		new["image_lat"]        = value["latitude"]
		new["image_observer"]   = value["observer"]
		new["image_comment"]    = value["comment"]
		new["image_source"]     = value["source"]
		new["image_created_at"] = value["created_at"]
		return new

	def _insert(self, classifications):
		hook = SqliteHook(sqlite_conn_id=self._conn_id)
		for classification in classifications:
			hook.run(
					'''
					INSERT OR IGNORE INTO zooniverse_classification_t (
						id                  ,
					    subject_id          ,
					    user_id             ,
					    width               ,
					    height              ,
					    source_x            ,
					    source_y            ,
					    spectrum_x          ,
					    spectrum_y          ,
					    spectrum_width      ,
					    spectrum_height     ,
					    spectrum_angle      ,
					    spectrum_type       ,
					    image_id            ,
					    image_url           ,
					    image_long          ,
					    image_lat           ,
					    image_observer      ,
					    image_comment       ,
					    image_source        ,
					    image_created_at
					) VALUES (
						:id                  ,
					    :subject_id          ,
					    :user_id             ,
					    :width               ,
					    :height              ,
					    :source_x            ,
					    :source_y            ,
					    :spectrum_x          ,
					    :spectrum_y          ,
					    :spectrum_width      ,
					    :spectrum_height     ,
					    :spectrum_angle      ,
					    :spectrum_type       ,
					    :image_id            ,
					    :image_url           ,
					    :image_long          ,
					    :image_lat           ,
					    :image_observer      ,
					    :image_comment       ,
					    :image_source        ,
					    :image_created_at
					)
					''', parameters=classification)


	def execute(self, context):
		with open(self._input_path) as fd:
			classifications = json.load(fd)
		classifications = list(map(self._extract, classifications))
		self._insert(classifications)
		

		

		