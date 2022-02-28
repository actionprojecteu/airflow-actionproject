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

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#--------------
# local imports
# -------------

from airflow_actionproject import __version__
from airflow_actionproject.hooks.zenodo import ZenodoHook

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

# ----------
# Exceptions
# ----------

class MissingCommunityError(ValueError):
	'''Missing Zenodo community identifier in Airflow connection'''
	def _str__(self):
		s = self.__doc__
		if self.args:
			s = f"{s}: {self.args[0]}"
		return s


class MissingMetadata(ValueError):
	'''Missing Zenodo metadata'''
	def _str__(self):
		s = self.__doc__
		if self.args:
			s = f"{s}: {self.args[0]}"
		return s

class MissingName(ValueError):
	'''Missing 'name' key in creator/contributor'''
	def _str__(self):
		s = self.__doc__
		if self.args:
			s = f"{s} {self.args[0]}: {self.args[1]}"
		return s

class NewVersionOfDraftError(ValueError):
	'''New version of a draft attempted'''
	def _str__(self):
		s = self.__doc__
		if self.args:
			s = f"{s}: {self.args[0]}"
		return s

# -------
# Classes
# -------

class ZenodoPublishDatasetOperator(BaseOperator):
	'''
	Operator that Publishes a dataset to Zenodo.

	Parameters
	—————
	conn_id : str
	Aiflow connection id to connect to Zenodo.
	file_path : str
	(Templated) Dataset file path.
	title : str
	Dataset title.
	description : str
	Dataset brief description.
	version : str
	(Version) reseach object version. May be templated (i.e YY.MM).
	creators : sequence
	Sequence of dictionaries cointaining each "<surname, name>" dataset maintainers under the "name" key.
	[{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}]
	communities : sequence
	Sequence of dictionaries cointaining each "<title>" under the "title" key and optionally "<ident>" under "id" key.
	[{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}]
	contributors : sequence
	Sequence of additional contributors as data collectors.  Same format as the creators parameter.
	status : str
	Publication status. Either 'draft' or 'published'. Defaults to 'published'
	access_rights : str
	Dataset access rights. Defaults to 'open'
 
	'''
	
	template_fields = ("_version", "_file_path")

	@apply_defaults
	def __init__(self, conn_id, file_path, title, description, version, creators, communities=None, contributors=None, status='published', access_right='open', **kwargs):
		super().__init__(**kwargs)
		self._conn_id      = conn_id
		self._file_path    = file_path
		self._status       = status
		self._title        = title
		self._version      = version
		self._description  = description
		self._communities  = communities or []
		self._creators     = creators
		self._contributors = contributors or []
		self._access_right = access_right


	def _build_metadata(self, hook):
		'''
		Each entry of creators/contrubutors is a dictionary with mandatory 'name' key and optionals 'affiliation' & 'orcid' keys
		'''
		for creator in self._creators:
			if not 'name' in creator:
				raise MissingName("creator", contributor)
		for contributor in self._contributors:
			if not 'name' in self._contributor:
				raise MissingName("contributor", contributor)
			contributor['type'] = 'DataCollector'

		if self._communities:
			# Searching communities is an expensive operation which should only be invoked if there is an entry
			# with no 'id' key known.
			initial   = list(filter(lambda entry: 'id' in entry, self._communities))
			to_search = list(filter(lambda entry: 'title' in entry and not 'id' in entry, self._communities))
			found     = hook.communities_search(to_search, page_size=1000)
			initial.extend(found)
			self._communities = [{'identifier': community['id']} for community in initial]

		return {
			'title'       : self._title,
			'version'     : self._version,
			'upload_type' : 'dataset',
			'communities' : self._communities,
			'creators'    : self._creators,
			'contributors': self._contributors,
			'description' : self._description,
			'access_right': self._access_right,
		}


	def execute(self, context):
		self.log.info(f"{self.__class__.__name__} version {__version__}")
		with ZenodoHook(self._conn_id) as hook:
			metadata  = self._build_metadata(hook)
			title     = metadata['title']
			status    = self._status
			file_path = self._file_path
			latest_id = hook.deposit_search(title, status)
			if latest_id is None:
				new_id = hook.deposit_new_entry(title)
			elif status == 'draft':
				raise NewVersionOfDraftError(title)
			else:
				new_id = hook.deposit_new_version(latest_id)
			bucket_url = hook.deposit_metadata(new_id, metadata)
			hook.deposit_contents(file_path, bucket_url)
			if status == 'published':
				doi = hook.deposit_publish(new_id)
			else:
				doi = None
			return doi


# =================
# TESTING OPERATORS
# =================

# class ZenodoTestDepositSearchOperator(BaseOperator):

# 	@apply_defaults
# 	def __init__(self, conn_id, title, status='published', **kwargs):
# 		super().__init__(**kwargs)
# 		self._conn_id      = conn_id
# 		self._status       = status
# 		self._title        = title

# 	def execute(self, context):
# 		with ZenodoHook(self._conn_id) as hook:
# 			ident = hook._deposit_search(
# 				title        = self._title,
# 				status       = self._status
# 			)


# class ZenodoTestDepositNewEntryOperator(BaseOperator):


# 	@apply_defaults
# 	def __init__(self, conn_id, title, **kwargs):
# 		super().__init__(**kwargs)
# 		self._conn_id      = conn_id
# 		self._title        = title

# 	def execute(self, context):
# 		with ZenodoHook(self._conn_id) as hook:
# 			new_id = hook._deposit_new_entry(
# 				title        = self._title,
# 			)


# class ZenodoTestDepositNewVersionOperator(BaseOperator):

# 	@apply_defaults
# 	def __init__(self, conn_id, latest_id, **kwargs):
# 		super().__init__(**kwargs)
# 		self._conn_id      = conn_id
# 		self._ident        = latest_id

# 	def execute(self, context):
# 		with ZenodoHook(self._conn_id) as hook:
# 			new_id = hook._deposit_new_version(
# 				latest_id        = self._ident,
# 			)


# class ZenodoTestDepositMetadataOperator(BaseOperator):
	
# 	template_fields = ("_version",)

# 	@apply_defaults
# 	def __init__(self, conn_id, ident, title, description, version, creators, contributors=None, upload_type='dataset', access_right='open', **kwargs):
# 		super().__init__(**kwargs)
# 		self._conn_id      = conn_id
# 		self._identifier   = ident
# 		self._title        = title
# 		self._version      = version
# 		self._description  = description
# 		self._creators     = creators
# 		self._contributors = contributors or []
# 		self._upload_type  = upload_type
# 		self._access_right = access_right


# 	def execute(self, context):
# 		with ZenodoHook(self._conn_id) as hook:
# 			hook.set_metadata(
# 				title        = self._title,
# 				description  = self._description,
# 				version      = self._version,
# 				creators     = self._creators,
# 				contributors = self._contributors,
# 				upload_type  = self._upload_type,
# 				access_right = self._access_right,
# 			)
# 			bucket_url = hook._deposit_metadata(self._identifier)


# class ZenodoTestDepositContentsOperator(BaseOperator):

# 	template_fields = ("_file_path",)

# 	@apply_defaults
# 	def __init__(self, conn_id, file_path, bucket_url, **kwargs):
# 		super().__init__(**kwargs)
# 		self._conn_id      = conn_id
# 		self._file_path    = file_path
# 		self._bucket_url   = bucket_url

# 	def execute(self, context):
# 		with ZenodoHook(self._conn_id) as hook:
# 			new_id = hook._deposit_contents(
# 				file_path        = self._file_path,
# 				bucket_url       = self._bucket_url,
# 			)


# class ZenodoTestDepositPublishOperator(BaseOperator):

# 	@apply_defaults
# 	def __init__(self, conn_id, ident, **kwargs):
# 		super().__init__(**kwargs)
# 		self._conn_id      = conn_id
# 		self._ident        = ident
	
# 	def execute(self, context):
# 		with ZenodoHook(self._conn_id) as hook:
# 			doi = hook._deposit_publish(
# 				identifier        = self._ident,
# 			)
