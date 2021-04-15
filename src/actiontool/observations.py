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
import time
import logging
import datetime

import requests

# ----------------
# Module constants
# ----------------

DEFAULT_URL = "https://api.actionproject.eu/observations"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("action")

# ------------------
# Auxiliar functions
# ------------------

def _get_conn(options):
    delay    = 1.0/options.tps
    page_size = options.page_size
    base_url = DEFAULT_URL
    session  = requests.Session()
    session.headers.update({'Authorization': f"Bearer {options.token}"})
    return session, base_url, page_size, delay


# For download

def _do_get_entries(session, url, params, limit, delay):
    '''Get connection details valid for both upload and download'''
    offset = 0
    while offset < limit:
        log.info(f"Requesting page {url}")
        response = session.get(
            url, params={**params, **{"page": offset}}
        )
        response.raise_for_status()
        response_json = response.json()
        time.sleep(delay) 
        log.debug(f"Page {page} received")
        yield from response_json["result"]
        offset += params["limit"]


def _download(start_datetime, end_datetime, project, limit, obs_type, page_size, delay):
    log.info(f"Getting Observations from ACTION Database for {project}")
    params = {
        "begin_date" : start_datetime,
        "finish_date": end_datetime,
        "limit"      : page_size,
        "project"    : project,
        "obs_type"   : obs_type,
    }
    yield from _do_get_entries(session, url, params, limit, page_size, delay)


def _dbg_request(response):
    request = response.request
    log.info(f"request url: {request.url}")
    log.info(f"request method: {request.method}")
    log.info(f"request headers: {request.headers}")
    log.info(f"request body: {request.body}")
    
   

def _upload(observations, session, url, page_size, delay):
    
    log.info(f"Uploading {len(observations)} observations to ACTION Database")
    for observation in observations:
        observation["written_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S UTC")
        response = session.post(url, json=observation)
        _dbg_request(response)
        response.raise_for_status()
        time.sleep(delay)

# ----------------------
# Command implementation
# ----------------------

def upload(options):
    log.info(f"Uploading observations from {options.file}")
    with open(options.file) as fd:
        observations = json.load(fd)
        log.info(f"Parsed {len(observations)} observations from {options.file}")
    session, url, page_size, delay = _get_conn(options)
    _upload(observations, session, url, page_size, delay)



def download(options):
    log.info(f"Downloading observations to {options.file}")
    session, base_url, page_size, delay = _get_conn(options)
    observations = list(
        _download(
            start_datetime = options.start_date.strfmt("&Y-%m-%dT%H:%M:%SZ"),
            end_datetime   = options.end_date.strfmt("&Y-%m-%dT%H:%M:%SZ"),
            project        = options.project,
            limit          = options.limit,
            obs_type       = 'observations',
            page_size      = options.page_size,
            delay          = delay,
        )
    )
    log.info(f"Fetched {len(observations)} entries")
    # Make sure the output directory exists.
    output_dir = os.path.dirname(options.file)
    os.makedirs(output_dir, exist_ok=True)
    with open(options.file, "w") as fd:
        json.dump(observations, indent="",fp=fd)
        self.log.info(f"Written entries to {options.file}")
  