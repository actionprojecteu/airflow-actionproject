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
import requests
import time
import datetime

# ---------------
# Airflow imports
# ---------------

from airflow.hooks.base import BaseHook

#--------------
# local imports
# -------------

from airflow_actionproject import __version__

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class EpiCollect5Hook(BaseHook):

	DEFAULT_HOST      = "five.epicollect.net" 
	API_SLUG          = "/api/export/entries"
	DEFAULT_CONN_TYPE = "https"
	DEFAULT_PORT      = 443
	DEFAULT_PAGE_SIZE = 50


	def __init__(self, conn_id):
		super().__init__()
		self._conn_id = conn_id
		self._project_slug = None
		self._session = None
		self._url = None
		self._page_size = None


	def get_conn(self):
		if self._session is None:
			self.log.info(f"{self.__class__.__name__} version {__version__}")
			self.log.debug(f"getting connection information from {self._conn_id}")
			config = self.get_connection(self._conn_id)
			# Define API base url.
			ctyp   = config.conn_type or self.DEFAULT_CONN_TYPE
			host   = config.host      or self.DEFAULT_HOST
			port   = config.port      or self.DEFAULT_PORT
			self._project_slug = config.schema
			self._page_size =  self.DEFAULT_PAGE_SIZE
			if config.extra:
				try:
					extra  = json.loads(config.extra)
				except json.decoder.JSONDecodeError:
					self._page_size = self.DEFAULT_PAGE_SIZE
				else:
					self._page_size = extra.get("page_size", self.DEFAULT_PAGE_SIZE)
			self._url = f"{ctyp}://{host}:{port}{self.API_SLUG}/{self._project_slug}"
			self._session = requests.Session()
		return self._session, self._url, self._page_size


	def _do_get_entries(self, session, url, params):
		while url is not None:
			self.log.debug(f"Requesting page {url}")
			response = session.get(url, params=params)
			response.raise_for_status()
			response_json = response.json()
			url  = response_json["links"]["next"]
			page = response_json["meta"]["current_page"]
			self.log.debug(f"Page {page} received")
			# To comply with EpiCollect V 1 transaction/second rate limiting.
			time.sleep(1)	
			yield from response_json["data"]["entries"]

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


	def get_entries(self, start_date, end_date, page_size=50):
		'''
		Fetches entries from Epicollect V between given start/end date.
		Parameters
		—————
		project_slug : str
		The slugified project name
		start_date : str
		Start date to start fetching ratings from (inclusive). Expected
		format is YYYY-MM-DD (equal to Airflow"s ds formats).
		end_date : str
		End date to fetching ratings up to (exclusive). Expected
		format is YYYY-MM-DD (equal to Airflow"s ds formats).
		batch_size : int
		Page size to fetch from the API. Larger values
		mean less requests, but more data transferred per request.
		'''
		# Corrects end time by removing one day (fixing loop boundary problem)
		end_date = (datetime.datetime.strptime(end_date, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
		session, url, page_size = self.get_conn()
		self.log.info(f"Getting Epicollect V Entries for project {self._project_slug}")
		params = {
			"per_page"   : page_size,
			"filter_by"  : "created_at",
			"filter_from": start_date,
			"filter_to"  : end_date,
			"sort_by"    : "created_at",
			"sort_order" : "ASC"
		}
		yield from self._do_get_entries(session, url, params)

	def close(self):
		self.log.info(f"Closing Epicollect5 hook")
		self._session = None
