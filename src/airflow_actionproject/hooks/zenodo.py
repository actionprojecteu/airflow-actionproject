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
import mimetypes
import time

import requests

# ---------------
# Airflow imports
# ---------------

from airflow.hooks.base import BaseHook

#--------------
# local imports
# -------------

from airflow_actionproject import __version__

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

# ----------
# Exceptions
# ----------

class MissingAPIKeyError(ValueError):
	'''Missing Zenodo API Key in Airflow connection'''
	def _str__(self):
		s = self.__doc__
		if self.args:
			s = f"{s}: {self.args[0]}"
		return s


class UnknownMimeTypeError(ValueError):
	'''Unknown MIME type for file extension'''
	def _str__(self):
		s = self.__doc__
		if self.args:
			s = f"{s} {self.args[0]}: {self.args[1]}"
		return s


# -------
# Classes
# -------

class ZenodoHook(BaseHook):

	DEFAULT_HOST      = "zenodo.org" 
	DEFAULT_CONN_TYPE = "https"
	DEFAULT_PORT      = 443
	DEFAULT_PAGE_SIZE = 100
	DEFAULT_TPS       = 1


	def __init__(self, conn_id):
		super().__init__()
		self._conn_id = conn_id
		self._session = None
		self._base_url = None

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


	def get_conn(self):
		'''
		Get connection parameters
		'''
		if self._session is None:
			self.log.info(f"getting connection information from '{self._conn_id}'")
			config = self.get_connection(self._conn_id)
			# Define API base url.
			ctyp   = config.conn_type or self.DEFAULT_CONN_TYPE
			host   = config.host      or self.DEFAULT_HOST
			port   = config.port      or self.DEFAULT_PORT
			if not config.password:
				raise MissingAPIKeyError(self._conn_id)
			self._page_size = self.DEFAULT_PAGE_SIZE
			self._delay     = 1.0/self.DEFAULT_TPS
			if config.extra:
				try:
					extra  = json.loads(config.extra)
				except json.decoder.JSONDecodeError:
					pass
				else:
					self._page_size = extra.get("page_size", self.DEFAULT_PAGE_SIZE)
					self._delay = 1.0/extra.get("tps", self.DEFAULT_TPS)
			self._base_url = f"{ctyp}://{host}:{port}"
			self._session = requests.Session()
			auth = f"Bearer {config.password}"
			self._session.headers.update({"Content-Type": "application/json", "Authorization": auth})
		return self._session, self._base_url



	def deposit_search(self, title, status):
		'''
		Search Zenodo for a given entry exactly matching title
		status may be 'published or 'draft'
		'''
		session, base_url = self.get_conn()
		self.log.info(f"Searching Zenodo digital object with title '{title}' and status '{status}'")
		query   = f"title:{title}"
		params  = {
			'status'      : status, 
			'sort'        : 'bestmatch', 
			'q'           : query,
			'all_versions': "true",
			'size'        : self._page_size,
		}
		url     = f"{base_url}/api/deposit/depositions"
		responses = list(self._paginated_entry_search(session, url, params))
		ident = None
		if responses:
			n = len(responses)
			responses.sort(key =lambda item: item['created'], reverse=True)
			responses = list(filter(lambda item: item['title'] == title, responses))
			if responses:
				ident = responses[0]['id']
				self.log.info(f"Found {len(responses)} entries, picking one with id '{ident}'")
			else:
				self.log.info(f"No exact match found, but at least {n} partial matches found")

		else:
			self.log.info(f"No digital object found")
			ident = None
		return ident


	def deposit_new_entry(self, title):
		'''
		Deposit a brand new Zenodo entry given by a title
		'''
		session, base_url = self.get_conn()
		params  = {}
		self.log.info(f"Creating new Deposition for object title '{title}'")
		url = f"{base_url}/api/deposit/depositions"
		response = session.post(url, params=params, json={})
		if not response.ok:
			self.log.error(f"{response.text}")	# get the JSON string error code
			response.raise_for_status()
		response_body = response.json()
		self.log.info(f"Deposition created with id {response_body['id']}")
		return response_body['id']


	def deposit_new_version(self, latest_id):
		'''
		Deposit a new version of Zenodo entry given by latest_id
		'''
		session, base_url = self.get_conn()
		params  = {}
		url = f"{base_url}/api/deposit/depositions/{latest_id}/actions/newversion"
		self.log.info(f"Creating New Version deposition for digital object from {latest_id}")
		response = session.post(url, params=params, json={})
		if not response.ok:
			self.log.error(f"{response.text}")	# get the JSON string error code
			response.raise_for_status()
		response_body = response.json()
		new_id = os.path.basename(response_body['links']['latest_draft'])
		self.log.info(f"Deposition New Version succesful, new id = {new_id}")
		return new_id


	def deposit_metadata(self, identifier, metadata):
		'''
		Each entry of creators/contributors is a dictionary 
		with mandatory 'name' key and optionals 'affiliation' & 'orcid' keys
		'''
		session, base_url = self.get_conn()
		self.log.info(f"Deposit Metadata for object identifier {identifier}")
		params  = {}
		url = f"{base_url}/api/deposit/depositions/{identifier}"
		response = session.put(url, params=params, json={'metadata': metadata})
		if not response.ok:
			self.log.error(f"{response.text}")	# get the JSON string error code
			response.raise_for_status()
		response_body = response.json()
		self.log.info(f"Metadata updated for object identifier {identifier}")
		bucket_url = response_body["links"]["bucket"]
		return bucket_url


	def deposit_contents(self, file_path, bucket_url):
		'''
		Upload file given by file path into a given bucket_url
		'''
		session, base_url = self.get_conn()
		extension = os.path.splitext(file_path)[-1]
		try:
			mime      = mimetypes.types_map[extension]
		except KeyError:
			raise UnknownMimeTypeError(extension)
		self.log.info(f"Deposit Contents from file '{file_path}', MIME type '{mime}'")
		headers   = {'Content-Type': mime}  # This has to be reviewed,
		params    = {}
		filename  = os.path.basename(file_path)
		url = f"{bucket_url}/{filename}"
		with open(file_path, "rb") as fp:
			response = session.put(url, data=fp, headers=headers, params=params)
		if not response.ok:
			self.log.error(f"{response.text}")	# get the JSON string error code
			response.raise_for_status()
		self.log.info(f"Deposit Contents successful")
		response_body = response.json()


	def deposit_publish(self, identifier):
		'''
		Mark the draft depòsition as published
		'''
		session, base_url = self.get_conn()
		params  = {}
		url = f"{base_url}/api/deposit/depositions/{identifier}/actions/publish"
		self.log.info(f"Marking state as 'published' for object identifier {identifier}")
		response = session.post(url, params=params, json={})
		if not response.ok:
			self.log.error(f"{response.text}")	# get the JSON string error code
			response.raise_for_status()
		response_body = response.json()
		doi = response_body['doi']
		self.log.info(f"Object state is succesfully 'published'. DOI '{doi}'")
		return doi


	def communities_search(self, titles, page_size=None):
		'''Search for a list communities given by a titles sequence'''
		# We provide a customize page size as parameter, 
		# because there is a lot of entries in communities
		session, base_url = self.get_conn()
		if page_size is None:
			page_size = self._page_size
		url = f"{base_url}/api/communities/"
		self.log.info(f"Searching for community identifiers from titles {titles}")
		self._commu_titles  = [entry['title'] for entry in titles]
		self._commu_matched = 0
		result = filter(self._filter_community, self._paginated_community_list(session, url, page_size))
		result = list(map(self._map_community, result))
		self.log.info(f"Returned communities from search is '{result}'")
		return result


	def close(self):
		'''Close Zenodo hook'''
		self.log.info(f"Closing Zenodo hook")
		self._session   = None
		self._base_url  = None

	# ----------------
	# Helper functions
	# ----------------

	def _map_community(self, item):
		'''
		Maps from JSON output dictionary returned by the 
		HTTP communities GET request
		into a simpler map
		'''
		return {'id': item['id'], 'title': item['title']}


	def _filter_community(self, item):
		'''
		Matches an entry from the returned communities list  
		against the desired search titles
		'''
		found = False
		if item['title'] in self._commu_titles:
			self.log.debug(f"Found community identifier '{item['id']}'' for title '{item['title']}'")
			self._commu_matched += 1
			found = True
		return found


	def _paginated_community_list(self, session, url, page_size):
		premature_exit = False
		while not premature_exit and url is not None:
			self.log.debug(f"Requesting page {url}")
			response = session.get(url, params={'size' : page_size})
			if not response.ok:
				self.log.error(f"{response.text}")	# get the JSON string error code
				response.raise_for_status()
			response_json = response.json()
			url  = response_json["links"].get("next", None)
			yield from response_json["hits"]["hits"]
			premature_exit = self._commu_matched == len(self._commu_titles)
			time.sleep(self._delay)


	def _paginated_entry_search(self, session, url, params):
		'''
		Do the HTTP search query taking care of pagination
		'''
		exit_flag = False
		page      = 1
		while not exit_flag:
			self.log.debug(f"Doing a request from page {page}")
			response = session.get(url, params={**params, **{"page": page,}})
			if not response.ok:
				self.log.error(f"{response.text}")
				response.raise_for_status()
			response_body = response.json()
			exit_flag = len(response_body) == 0
			# Because response_body is a sequence, we iterate individually
			# with yield from
			# If we used plain 'yield' we would gather a sequence of sequences
			# in _deposit_search()
			yield from response_body	
			page += 1
