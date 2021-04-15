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
import time
import logging
import datetime

import requests

# ----------------
# Module constants
# ----------------

ENDPOINT = "https://five.epicollect.net/api/export/entries"
# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("epi5")

# ----------------------
# Command implementation
# ----------------------


def get_entries_session(slug,  start_date, end_date, page_size):
	url = f"{ENDPOINT}/{slug}"
	session = requests.Session()
	params = {
			"per_page"   : page_size,
			"filter_by"  : "created_at",
			"filter_from": start_date,
			"filter_to"  : end_date,
			"sort_by"    : "created_at",
			"sort_order" : "ASC"
		}
	return session, url, params


def do_get_entries(session, url, params):
	while url is not None:
		log.debug("Requesting page")
		response = session.get(url, params=params)
		response.raise_for_status()
		response_json = response.json()
		time.sleep(1)	# To comply with Epic Collect V 1 TPS rate limiting.
		#print(json.dumps(response_json, indent=4, sort_keys=False))
		url  = response_json["links"]["next"]
		page = response_json["meta"]["current_page"]
		log.debug("Page {0} received".format(page))
		yield from response_json["data"]["entries"]



NAME_MAP = {
		'ec5_uuid'            : 'id',
		'created_at'          : 'created_at',
		'uploaded_at'         : 'uploaded_at',
		'title'               : 'title',
		# New Epicollect V form
		'1_Share_your_nick_wi': 'nickname', 
		'2_Location'          : 'location',
		'3_Take_an_image_of_a': 'url',
		'4_Observations'      : 'comment',
		# Old Epicollect V form
		'1_Date'              : 'date',
		'2_Time'              : 'time',
		'3_Location'          : 'location',
		'4_Take_an_image_of_a': 'url',
		'5_Observations'      : 'comment',
	}

def remap(item):

	# Fixes timestamps format
	dt = datetime.datetime.strptime(item['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0)
	item['created_at'] = dt.strftime('%Y-%m-%dT%H:%M:%S UTC')
	dt = datetime.datetime.strptime(item['uploaded_at'],'%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0)
	item['uploaded_at'] = dt.strftime('%Y-%m-%dT%H:%M:%S UTC')
	
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
	item["type"] = "observation"
	return item



def ec5_remapper(entries):
	'''Map Epicollect V metadata to an internal, more convenient representation'''
	
	# remaps each dictionary entry in collection with new names
	g1 = ({NAME_MAP[name]: val for name, val in entry.items()} for entry in entries)
	g2 =  map(remap, g1)
	return g2

# ----------------------
# COMMAND IMPLEMENTATION
# ----------------------

def export(options):
	log.info("Getting Epicollect V Entries for slug {0}".format(options.slug))
	with open(options.file,'w') as fd:
		session, url, params = get_entries_session(options.slug,options.start_date, options.end_date, options.page_size)
		entries = list(do_get_entries(session, url, params))
		json.dump(entries, fd, indent="")
	log.info("Epicollect V export ended ({1} entries) for slug {0}".format(options.slug, len(entries)))


def transform(options):
	log.info("Transforming Epicollect V Entries for input file {0}".format(options.input_file))
	with open(options.input_file) as fd:
		entries = json.load(fd)
	result = list(ec5_remapper(entries))
	with open(options.output_file,'w') as fd:
		json.dump(result, fp=fd, indent=2)
	log.info("Epicollect V transform ended")
