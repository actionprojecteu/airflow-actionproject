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
import re
import csv
import json
import math
import time
import datetime
import collections
import itertools

# Access Jinja2 templates withing the package
from pkg_resources import resource_filename

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults


# -------------------
# Third party imports
# -------------------

#--------------
# local imports
# -------------

from airflow_actionproject import __version__
from airflow_actionproject.hooks.sqlite import SqliteHook

# -----------------------
# Module global variables
# -----------------------

# -------------------
# Auxiliar functions
# -------------------


# ----------------
# Module constants
# ----------------


class InsertObservationsOperator(BaseOperator):
    '''
    Operator that uploads Epicollect5 observations into an internal SQLite database.

    Parameters
    —————
    conn_id : str
    Aiflow connection id to SQLite internal database. 
    (Templated) input_path : str
    Path to the JSON file with observations.
    
    '''
    
    template_fields = ("_input_path",)

  
    @apply_defaults
    def __init__(self, conn_id, input_path, **kwargs):
        super().__init__(**kwargs)
        self._input_path  = input_path
        self._conn_id     = conn_id


    def _insert(self, observations):

        def remap_items(item):
            item['longitude'] = item['location']['longitude']
            item['latitude']  = item['location']['latitude']
            item['accuracy']  = item['location']['accuracy']
            item["written_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            item['spectrum_type'] = item.get('spectrum_type')
            del item['location']
            return item

        observations = tuple(map(remap_items, observations))
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        hook.run_many('''
            INSERT OR IGNORE INTO epicollect5_t (
                image_id   ,    -- Image GUID
                created_at ,    -- Original entry creation timestamp
                uploaded_at,    -- Image upload into database timestamp
                written_at ,    -- Database insertion timestamp
                title      ,    -- Image title, usually the GUID
                observer   ,    -- observer's nickname
                latitude   ,    -- image latitude in degrees
                longitude  ,    -- image longitude in degrees
                accuracy   ,    -- coordinates accuracy
                url        ,    -- image URL
                spectrum_type,  -- optional spectrum type set by observer
                comment    ,    -- optional observer comment
                project    ,    -- source project identifier ('street-spectra')
                source     ,    -- Observing platform ('Epicollect5')
                obs_type        -- Entry type ('observation')
            ) VALUES (
                :id         ,   
                :created_at ,    
                :uploaded_at, 
                :written_at ,      
                :title      ,    
                :observer   ,    
                :latitude   ,    
                :longitude  ,    
                :accuracy   ,    
                :url        ,   
                :spectrum_type,  
                :comment    ,   
                :project    ,   
                :source     ,    
                :obs_type
            )
            ''',
            parameters   = observations,
            commit_every = 500,
        )


    def execute(self, context):
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        self.log.info("Inserting transformed Epicollect5 observations into SQLite database")
        with open(self._input_path) as fd:
            observations = json.load(fd)
            self.log.info(f"Parsed {len(observations)} observations from {self._input_path}")
        self._insert(observations)
        self.log.info("Inserted {len(observations)} StreetSpectra Epicollect observations")



class ActionDownloadFromVariableDateOperator(BaseOperator):
    '''
    Operator that fetches project entries from the SQLite ACTION database starting 
    from a given start date. Entries may be either observations or classifications.
    The start date is managed as an Airflow Variable.
    The variable is updated with the next available start date for the project.
    
    Parameters
    —————
    conn_id : str
    Aiflow connection id to connect to the ACTION database. 
    output_path : str
    (Templated) Path to write the fetched entries to.
    variable_name : str
    Airflow Variable name that controlling the start date as a UTC timestamp 'YYYY-MM-DDTHH:MM:SS.SSSSSZ'.
    project : str
    Project name
    n_entries : str
    Number of entries to download
    obs_type : str
    Observation type, either "observation" or "classification". Defaults to "observation"
    '''
    
    
    template_fields = ("_output_path",)

    @apply_defaults
    def __init__(self, conn_id, output_path, variable_name,  project, n_entries, obs_type='observation', **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._output_path = output_path
        self._key = variable_name
      
        self._n_entries = n_entries + 1 # taking into account that we will discard the tast one
        self._obs_type = obs_type
        self._project = project

    def _to_dict(self, item):
        keys = ('id', 'created_at', 'uploaded_at', 'written_at', 'title', 'observer', 'latitude', 'longitude', 'accuracy', 
                'url', 'spectrum_type', 'comment', 'project', 'source', 'obs_type')
        output = dict(zip(keys, item))
        output['location'] = {'latitude': output['latitude'], 'longitude': output['longitude'], 'accuracy': output['accuracy']}
        del output['latitude']; del output['longitude']; del output['accuracy']; 
        return output


    def execute(self, context):
        start_date = Variable.get(self._key)
        filter_dict = {'start_date': start_date, 'project': self._project, 'obs_type': self._obs_type, 'n_entries': self._n_entries}
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        observations = hook.get_records('''
            SELECT
                image_id,   
                created_at,    
                uploaded_at,    
                written_at,    
                title,   
                observer,    
                latitude,    
                longitude,    
                accuracy, 
                url,    
                spectrum_type,    
                comment,    
                project,   
                source,    
                obs_type   
            FROM epicollect5_t
            WHERE project = :project
            AND   obs_type = :obs_type
            AND   created_at >= :start_date
            ORDER BY created_at ASC
            LIMIT :n_entries
        ''',
            filter_dict
        )
        observations = list(map(self._to_dict, observations))
        self.log.info(f"Fetched {len(observations)} entries from SQLite database")       
        # Removes the last item and updates the timestamp marker
        last_item = observations.pop()
        Variable.set(self._key, last_item['created_at'])
        self.log.info(f"Dropped last observation at {last_item['created_at']}")
        # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path, "w") as fd:
            json.dump(observations, indent=2,fp=fd)
            self.log.info(f"Written {len(observations)} entries to {self._output_path}")


class ActionRangedDownloadOperator(BaseOperator):
    '''
    Operator that fetches project entries from the ACTION SQLite database between 
    a given start and end dates. 

    Parameters
    —————
    conn_id : str
    Aiflow connection id to connect to Epicollect V. 
    output_path : str
    Path to write the fetched entries to.
    start_date : str
    (Templated) start date to start fetching entries from (inclusive).
    Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
    end_date : str
    (Templated) end date to fetching entries up to (exclusive).
    Expected format is YYYY-MM-DD (equal to Airflow"s ds formats).
    obs_type : str
    Observation type, Defaults to "observation" for the time being
    '''

    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(self, conn_id, output_path, start_date, end_date, project, obs_type='observation', **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id
        self._output_path = output_path
        self._start_date  = start_date
        self._end_date    = end_date
        self._project     = project
        self._obs_type    = obs_type
        

    def _to_dict(self, item):
        keys = ('id', 'created_at', 'uploaded_at', 'written_at', 'title', 'observer', 'latitude', 'longitude', 'accuracy', 
                'url', 'spectrum_type', 'comment', 'project', 'source', 'obs_type')
        output = dict(zip(keys, item))
        output['location'] = {'latitude': output['latitude'], 'longitude': output['longitude'], 'accuracy': output['accuracy']}
        del output['latitude']; del output['longitude']; del output['accuracy']; 
        return output

    def execute(self, context):
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        filter_dict = {
            'start_date': datetime.datetime.strptime(self._start_date,'%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'end_date'  : datetime.datetime.strptime(self._end_date,  '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'project'   : self._project,
            'obs_type'  : self._obs_type
        }
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        observations = hook.get_records('''
            SELECT
                image_id,   
                created_at,    
                uploaded_at,    
                written_at,    
                title,   
                observer,    
                latitude,    
                longitude,    
                accuracy, 
                url,    
                spectrum_type,    
                comment,    
                project,   
                source,    
                obs_type   
            FROM epicollect5_t
            WHERE project = :project
            AND   obs_type = :obs_type
            AND   created_at BETWEEN :start_date AND :end_date
            ORDER BY created_at ASC
        ''',
            filter_dict
        )
        observations = list(map(self._to_dict, observations))
        self.log.info(f"Fetched {len(observations)} entries from SQLite database")       
        # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path, "w") as fd:
            json.dump(observations, indent=2,fp=fd)
            self.log.info(f"Written {len(observations)} entries to {self._output_path}")
