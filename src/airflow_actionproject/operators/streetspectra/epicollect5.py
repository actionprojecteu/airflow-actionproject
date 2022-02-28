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
import json
import datetime

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

# -----------------------
# Module global variables
# -----------------------

# -------------------
# Auxiliar functions
# -------------------

def strip_email(nickname):
    regex = r'(\b[A-Za-z0-9._%+-]+)@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    reobj = re.compile(regex)
    result = nickname
    matchobj = reobj.search(nickname)
    if matchobj:
        result = matchobj.groups(1)[0]
    return result

# ----------------
# Module constants
# ----------------


class EC5TransformOperator(BaseOperator):
    '''
    Operator that transforms entries exported from Epicollect V 
    to an specific ACTION StreetSpectra JSON file.
    
    Parameters
    —————
    input_path : str
    (Templated) Path to read the input JSON to transform to.
    output_path : str
    (Templated) Path to write the output transformed JSON.
    '''

    # This mapping is unique to EC5 StreetSpectra project.
    NAME_MAP = {
        'ec5_uuid'            : 'id',
        'created_at'          : 'created_at',
        'uploaded_at'         : 'uploaded_at',
        'title'               : 'title',
        # Newest form names
        '1_Share_your_nick_wi': 'observer', 
        '2_Location'          : 'location',
        '3_Take_an_image_of_a': 'url',
        '4_Illumination_sourc': 'spectrum_type',
        '5_Comments'          : 'comment',
        # Old form names from old street-spectra project
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


    def execute(self, context):
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        self.log.info(f"Transforming EC5 observations from JSON file {self._input_path}")
        with open(self._input_path) as fd:
            entries = json.load(fd)
        result = list(self._ec5_remapper(entries))
        # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path,'w') as fd:
            json.dump(result, fp=fd, indent=2)
        self.log.info(f"Transformed {len(result)} EC5 observations to output JSON file {self._output_path}")

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
            'latitude' : location.get('latitude'), 
            'longitude': location.get('longitude'),
            'accuracy':  location.get('accuracy')
        }
        # Get rid of possible email addresses for privacy issues by stripping out the domain
        item["observer"] = strip_email(item.get("observer",""))
        # Add extra items
        item["project"] = "street-spectra"
        item["source"] = "Epicollect5"
        item["obs_type"] = "observation"
        return item

    def _non_empty_image(self, item):
        return True if item['url'] else False

    def _with_coordinates(self, item):
        location = item['location']
        return location['longitude'] != '' and location['latitude'] != ''


    def _ec5_remapper(self, entries):
        '''Map Epicollect V metadata to an ernal, more convenient representation'''
        # Use generators instead of lists
        g = ({self.NAME_MAP[name]: val for name, val in entry.items()} for entry in entries)
        g = map(self._remap, g)
        g = filter(self._non_empty_image, g)
        g = filter(self._with_coordinates, g)
        return g


