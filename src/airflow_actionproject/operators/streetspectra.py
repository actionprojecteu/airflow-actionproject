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

# Access Jinja2 templates withing the package
from pkg_resources import resource_filename

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
#from airflow.providers.sqlite.hooks.sqlite import SqliteHook


# -------------------
# Third party imports
# -------------------

import requests
import folium
import numpy as np
import sklearn.cluster as cluster
import jinja2


#--------------
# local imports
# -------------

# This hook knows how to insert StreetSpectra metadata on subjects
from airflow_actionproject.hooks.streetspectra import ZooSpectraHook
from airflow_actionproject.hooks.sqlite import SqliteHook
from airflow_actionproject.hooks.ssh import SCPHook



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


    def _ec5_remapper(self, entries):
        '''Map Epicollect V metadata to an ernal, more convenient representation'''
        # Use generators instead of lists
        g1 = ({self.NAME_MAP[name]: val for name, val in entry.items()} for entry in entries)
        g2 = map(self._remap, g1)
        g3 = filter(self._non_empty_image, g2)
        return g3


# --------------------------------------------------------------
# This class is specific to StreetSpectra because 
# this particular ZooSpectraHook is specific to StreetSpectra
# --------------------------------------------------------------

class ZooImportOperator(BaseOperator):
    '''
    Operator that uploads StreetSpectra observations to Zooniverse
    for citizen scientist to classify.
    Observations are contained in an input JOSN file previously 
    downloaded from the ACTION database. 
    
    Parameters
    —————
    conn_id : str
    Aiflow connection id to connect to Zooniverse.
    input_path : str
    (Templated) Path to read the input JSON to transform to.
    output_path : str
    (Templated) Path to write the output transformed JSON.
    '''

    template_fields = ("_input_path", "_display_name")

    @apply_defaults
    def __init__(self, conn_id, input_path, display_name, **kwargs):
        super().__init__(**kwargs)
        self._conn_id      = conn_id
        self._input_path   = input_path
        self._display_name = display_name


    def execute(self, context):
        self.log.info(f"Uploading observations to Zooniverse from {self._input_path}")
        with open(self._input_path) as fd:
            subjects_metadata = json.load(fd)
        with ZooSpectraHook(self._conn_id) as hook:
            hook.add_subject_set(self._display_name, subjects_metadata)




class PreprocessClassifOperator(BaseOperator):
    '''
    Operator that extracts a subset of StreetSpectra relevant information 
    from a Zooniverse export file and writes into a handy, simplified 
    classification SQLite table for further processing.
    
    Parameters
    —————
    conn_id : str
    Airflow connection id to an internal SQLite database where the individual 
    classification analysis takes place.
    input_path : str
    (Templated) Path to read the Zooniverse transformed input JSON file.
    
    '''
    
    template_fields = ("_input_path",)

    SPECTRUM_TYPE = {
        0: 'HPS',   # High Pressure Sodium
        1: 'MV',    # Mercury Vapor
        2: 'LED',   # Light Emitting Diode
        3: 'MH',    # Metal Halide 
    }

    @apply_defaults
    def __init__(self, conn_id, input_path, **kwargs):
        super().__init__(**kwargs)
        self._input_path  = input_path
        self._conn_id     = conn_id


    def _extract(self, classification):
        # The previos street spectra workflow
        # The first workflow id is the production value, the second one is a test environment
        if int(classification["workflow_id"]) in (13033,17959):
            return self._extractOld(classification)
        else:
            return self._extractNew(classification)

    def _extractOld(self, classification):
        new = dict()
        # General info
        new["classification_id"] = classification["classification_id"]
        new["subject_id"]        = classification["subject_ids"]
        new["workflow_id"]       = classification["workflow_id"]
        new["user_id"]           = classification["user_id"]
        new["user_ip"]           = classification["user_ip"]
        new["started_at"]        = classification["metadata"]["started_at"]
        new["finished_at"]       = classification["metadata"]["finished_at"]
        sd = classification["metadata"]["subject_dimensions"][0]
        new["sources"] = list()
        if sd:
            new["width"]      = sd["naturalWidth"]
            new["height"]     = sd["naturalHeight"]
        else:
            new["width"]      = None
            new["height"]     = None
        value =  classification["annotations"][0]["value"]
        if value:
            # Light source info
            new["sources"].append({
                'classification_id': new["classification_id"],
                'source_x'      : value[0]["x"],
                'source_y'      : value[0]["y"],
                'spectrum_type' : None,
                'aggregated'    : None,
                'cluster_id'     : None,
            })
            # Spectrum tool info
            if len(value) > 1:  # The user really used this tool
                # new["spectrum_x"] = value[1].get("x")
                # new["spectrum_y"] = value[1].get("y")
                # new["spectrum_width"]  = value[1].get("width")
                # new["spectrum_height"] = value[1].get("height")
                # new["spectrum_angle"]  = value[1].get("angle")
                details   = value[1].get("details")
                if details:
                    spectrum_type = details[0]["value"]
                    # remap spectrum type codes to strings
                    spectrum_type = self.SPECTRUM_TYPE.get(spectrum_type, spectrum_type) #
                    new["sources"][0]["spectrum_type"] = spectrum_type
                      
        # Metadata coming from the Observing Platform
        key = list(classification["subject_data"].keys())[0]
        value = classification["subject_data"][key]
        new["image_id"]         = value.get("id")
        new["image_url"]        = value.get("url")
        new["image_long"]       = value.get("longitude")
        new["image_lat"]        = value.get("latitude")
        new["image_observer"]   = value.get("observer")
        new["image_comment"]    = value.get("comment")
        new["image_source"]     = value.get("source")
        new["image_created_at"] = value.get("created_at")
        new["image_spectrum"]   = value.get("spectrum_type")  # original classification made by observer
        new["image_spectrum"]   = None if new["image_spectrum"] == "" else new["image_spectrum"]
        return new



    def _extractNew(self, classification):
        new = dict()
        # General info
        new["classification_id"] = classification["classification_id"]
        new["subject_id"]        = classification["subject_ids"]
        new["workflow_id"]       = classification["workflow_id"]
        new["user_id"]           = classification["user_id"]
        new["user_ip"]           = classification["user_ip"]
        new["started_at"]        = classification["metadata"]["started_at"]
        new["finished_at"]       = classification["metadata"]["finished_at"]
        new["sources"] = list()
        sd = classification["metadata"]["subject_dimensions"][0]
        if sd:
            new["width"]      = sd["naturalWidth"]
            new["height"]     = sd["naturalHeight"]
        else:
            new["width"]      = None
            new["height"]     = None
        values =  classification["annotations"][0]["value"]
        if values:
            for value in values:
                spectrum = value["details"][0]["value"]
                spectrum = self.SPECTRUM_TYPE.get(spectrum, spectrum) 
                new["sources"].append({
                    'classification_id' : new["classification_id"],
                    'source_x': value["x"],
                    'source_y': value["y"],
                    'spectrum_type' : spectrum,
                    'aggregated'    : None,
                    'cluster_id'    : None,
                    })
        # Metadata coming from the Observing Platform
        key = list(classification["subject_data"].keys())[0]
        value = classification["subject_data"][key]
        new["image_id"]         = value.get("id")
        new["image_url"]        = value.get("url")
        new["image_long"]       = value.get("longitude")
        new["image_lat"]        = value.get("latitude")
        new["image_observer"]   = value.get("observer")
        new["image_comment"]    = value.get("comment")
        new["image_source"]     = value.get("source")
        new["image_created_at"] = value.get("created_at")
        new["image_spectrum"]   = value.get("spectrum_type")  # original classification made by observer
        new["image_spectrum"]   = None if new["image_spectrum"] == "" else new["image_spectrum"]
        return new


    def _is_useful(self, classification):
        '''False for classifications with either:
         - missing image_url metadata
         - No GPS pòsition
         - no individual light source data (spectrum_type)
        
         '''
        return  classification.get("image_url")  and \
                classification.get("image_long") and \
                classification.get("image_lat")  and \
                len(classification.get("sources")) > 0 and \
                all(map(lambda s: s['spectrum_type'] is not None, classification.get("sources")))


    def _insert(self, classifications):
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        self.log.info(f"Logging classifications differences")
        hook.run_many(
            '''
            INSERT INTO spectra_classification_t (
                classification_id,
                subject_id, 
                workflow_id, 
                user_id,  
                user_ip, 
                started_at, 
                finished_at, 
                width,  
                height, 
                image_id, 
                image_url, 
                image_long, 
                image_lat, 
                image_observer, 
                image_comment, 
                image_source,  
                image_created_at, 
                image_spectrum
            ) VALUES (
                :classification_id,
                :subject_id, 
                :workflow_id, 
                :user_id,  
                :user_ip, 
                :started_at, 
                :finished_at, 
                :width,  
                :height, 
                :image_id, 
                :image_url, 
                :image_long, 
                :image_lat, 
                :image_observer, 
                :image_comment, 
                :image_source,  
                :image_created_at, 
                :image_spectrum
            )''',
            parameters   = classifications,
            commit_every = 500,
        )
        sources = list()
        for classification in classifications:
            sources.extend(classification['sources'])
        hook.run_many(
            '''
            INSERT INTO light_sources_t (
                classification_id,
                cluster_id, 
                source_x, 
                source_y,  
                spectrum_type, 
                aggregated
            ) VALUES (
                :classification_id,
                :cluster_id, 
                :source_x, 
                :source_y,  
                :spectrum_type, 
                :aggregated
            )''',
            parameters   = sources,
            commit_every = 500,
        )   
           
        


    def execute(self, context):
        with open(self._input_path) as fd:
            classifications = json.load(fd)
        self.log.info(f"Classification initial list size is {len(classifications)}")
        classifications = list(filter(self._is_useful, map(self._extract, classifications)))
        self.log.info(f"Classification final list size is {len(classifications)}")
        self._insert(classifications)
        


class AggregateOperator(BaseOperator):
    '''
    Operator that aggregates individual classifications from users
    into a combined classification, for each source identified
    in a given Zooniverse subject. All this is handled in an internal
    SQLite database
    
    Parameters
    —————
    conn_id : str
    Airflow connection id to an internal SQLite database where the individual 
    classification analysis takes place.
    '''

    CATEGORIES = ('HPS','MV','LED','MH', None)

    @apply_defaults
    def __init__(self, conn_id, distance = 30, **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id
        self._distance    = distance # max distance to belong to a cluster


    def _cluster(self, hook):
        '''Perform clustering analysis over source light selection'''
        self.log.info(f"Cluster analysis by DBSCAN with epsilon distance {self._distance}")
        user_selections = hook.get_records('''
            SELECT subject_id, classification_id, source_x, source_y
            FROM spectra_classification_v
            WHERE cluster_id IS NULL
        ''')
        classifications_per_subject = dict()
        for subject_id, classification_id, source_x, source_y in user_selections:
            coordinates = classifications_per_subject.get(subject_id, [])
            coordinates.append({'classification_id': classification_id, 'coords': (source_x, source_y)})
            classifications_per_subject[subject_id] = coordinates
        
        clustered_classifications = list()
        for subject_id, values in classifications_per_subject.items():
            coordinates = tuple(value['coords'] for value in values)
            ids = tuple(value['classification_id'] for value in values)
            N_Classifications = len(coordinates)
            # Define the model
            coordinates = np.array(coordinates)
            ids         = np.array(ids)
            model = cluster.DBSCAN(eps=self._distance, min_samples=2)
            # Fit the model and predict clusters
            yhat = model.fit_predict(coordinates)
            # retrieve unique clusters
            clusters = np.unique(yhat)
            self.log.info(f"Subject {subject_id}: {len(clusters)} clusters from {N_Classifications} classifications, initial cluster ids: {clusters}")
            for cl in clusters:
                # get row indexes for samples with this cluster
                row_ix = np.where(yhat == cl)
                X = coordinates[row_ix, 0][0]; Y = coordinates[row_ix, 1][0]
                ID = ids[row_ix]
                if cl != -1:
                    alist = np.column_stack((X,Y,ID))
                    alist = list( map(lambda t: 
                            {'cluster_id': cl+1 if cl>-1 else cl, 'source_x':t[0], 'source_y': t[1], 'classification_id': t[2], 'epsilon': self._distance}, 
                            alist))
                    clustered_classifications.extend(alist)
                else:
                    start = max(clusters)+2 # we will shift also the normal ones ...
                    for i in range(len(X)) :
                        cluster_id = start + i
                        row = {'cluster_id': cluster_id, 'source_x': X[i], 'source_y': Y[i], 'classification_id': ID[i], 'epsilon': self._distance}
                        clustered_classifications.append(row)
                        
        hook.run_many(
            '''
            UPDATE light_sources_t
            SET 
                cluster_id    = :cluster_id,
                epsilon       = :epsilon
            WHERE classification_id = :classification_id
            AND  source_x = :source_x
            AND  source_y = :source_y
            ''',
            parameters = clustered_classifications,
            commit_every = 500,
        )



    def _classify(self, hook):
        ratings = list()
        info1 = hook.get_records('''
            SELECT subject_id, cluster_id, spectrum_type
            FROM spectra_classification_v 
            WHERE aggregated IS NULL AND cluster_id IS NOT NULL
            ''',
        )
        counters = dict()
        for subject_id, cluster_id, spectrum_type in info1:
            key = tuple([subject_id, cluster_id])
            spectra_type = counters.get(key,[])
            spectra_type.append(spectrum_type)
            counters[key] = spectra_type
        # Compute aggregated ratings per cluster_id per subject_id
        ratings = dict()
        for key, spectra_type in counters.items():
            subject_id = key[0]
            cluster_id  = key[1]
            counter = collections.Counter(spectra_type)
            votes = counter.most_common()
            self.log.info(f"Subject Id: {subject_id}, Source Id: {cluster_id}, VOTES: {votes}")
            if len(votes) > 1 and votes[0][1] == votes[1][1]:
                spectrum_type    = None
                spectrum_absfreq = votes[0][1]
                rejection_tag    = 'Ambiguous'
            elif votes[0][0] is None:
                spectrum_type    = None
                spectrum_absfreq = None
                rejection_tag    = 'Never classified'
            else:
                spectrum_type    = votes[0][0]
                spectrum_absfreq = votes[0][1]
                rejection_tag    = None
            rating = {
                'subject_id'       : subject_id, 
                'cluster_id'       : cluster_id, 
                'spectrum_type'    : spectrum_type,
                'spectrum_absfreq' : spectrum_absfreq,
                'cluster_size'     : len(spectra_type),
                'spectrum_distr'   : str(votes), 
                'rejection_tag'    : rejection_tag,
                'counter'          : counter,
                'epsilon'          : self._distance,
            }
            rateds = ratings.get(key,[])
            rateds.append(rating)
            ratings[key] = rateds
        
        final_classifications = list()
        for key, rateds in ratings.items():
            final_classifications.extend(rateds)
        hook.run('''
            UPDATE light_sources_t
            SET aggregated = 1
            WHERE aggregated IS NULL AND cluster_id IS NOT NULL
            ''',
        )
        self._update(final_classifications, hook)
    


    def _update(self, final_classifications, hook):
        # Update everything not depending on the aggregate classifications first
        hook.run('''
                INSERT OR IGNORE INTO spectra_aggregate_t (
                subject_id,
                cluster_id,
                width,
                height,
                source_x,
                source_y,
                image_id,
                image_url,
                image_long,
                image_lat,
                image_observer,
                image_comment,
                image_source,
                image_created_at,
                image_spectrum
            )
            SELECT 
                subject_id, 
                cluster_id, 
                width,
                height,
                ROUND(AVG(source_x),2), -- Cluster X centroid
                ROUND(AVG(source_y),2), -- Cluster Y centroid
                image_id, 
                image_url, 
                image_long, 
                image_lat, 
                image_observer, 
                image_comment, 
                image_source, 
                image_created_at, 
                image_spectrum
            FROM spectra_classification_v 
            GROUP BY subject_id, cluster_id
            '''
        )
        hook.run_many(
            '''
            UPDATE spectra_aggregate_t
            SET 
                spectrum_type    = :spectrum_type,
                spectrum_distr   = :spectrum_distr,
                cluster_size     = :cluster_size,
                spectrum_absfreq = :spectrum_absfreq,
                rejection_tag    = :rejection_tag,
                epsilon          = :epsilon 
            WHERE subject_id  = :subject_id
            AND cluster_id    = :cluster_id
            ''',
            parameters = final_classifications,
            commit_every = 500,
        )
       

    def execute(self, context):
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        self._cluster(hook)
        self._classify(hook)


class IndividualCSVExportOperator(BaseOperator):
    '''
    Operator that export raw individual StreetSpectra classifications 
    into a CSV file ready to be uploaded to Zenodo. There is a document describing
    this file format.
    
    Parameters
    —————
    conn_id : str
    Airflow connection id to an internal SQLite database where the individual 
    classification analysis takes place.
    output_path : str
    (Templated) Path to write the individual classifications in CSV format.
    '''
    
    HEADER = (
            'csv_version', 
            'subject_id',
            'cluster_id',
            'user_id',
            'width',
            'height',
            'epsilon',
            'light_source_x',
            'light_source_y',
            'spectrum_type',
            'image_url',
            'image_long',
            'image_lat',
            'image_comment',
            'image_source',
            'image_created_at',
            'image_spectrum',
    )
    
    template_fields = ("_output_path",)


    @apply_defaults
    def __init__(self, conn_id, output_path, **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id
        self._output_path = output_path


    def execute(self, context):
        self.log.info(f"Exporting StreetSpectra individual classifications to CSV file {self._output_path}")
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        individual_classifications = hook.get_records('''
            SELECT
                '1',  -- CSV file format export version
                subject_id,
                cluster_id,
                iif(user_id,user_id,user_ip),
                width,
                height,
                epsilon,
                ROUND(source_x,3),
                ROUND(source_y,3),
                spectrum_type,
                -- Metadata from Epicollect 5
                image_url,
                image_long,
                image_lat,
                image_comment,
                image_source,
                image_created_at,
                image_spectrum

            FROM spectra_classification_v
            ORDER BY subject_id DESC
        ''');
        # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path,'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(self.HEADER)
            for classification in individual_classifications:
                writer.writerow(classification)
        self.log.info(f"Exported individual StreetSpectra classifications to CSV file {self._output_path}")
   


class AggregateCSVExportOperator(BaseOperator):
    '''
    Operator that export aggregated StreetSpectra classifications 
    into a CSV file ready to be uploaded to Zenodo. There is a document describing
    this file format.

    
    Parameters
    —————
    conn_id : str
    Airflow connection id to an internal SQLite database where the individual 
    classification analysis takes place.
    output_path : str
    (Templated) Path to write the aggregated classifications in CSV format.
    '''
    
    HEADER = (
            'csv_version',  
            'light_source_id',
            'light_source_x',
            'light_source_y',
            'spectrum_type',
            'spectrum_distr',
            'spectrum_absfreq',
            'cluster_size',
            'epsilon',
            'rejection_tag',
            'image_url',
            'image_long',
            'image_lat',
            'image_comment',
            'image_source',
            'image_created_at',
            'image_spectrum'
    )
    
    template_fields = ("_output_path",)


    @apply_defaults
    def __init__(self, conn_id, output_path, **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id
        self._output_path = output_path



    def execute(self, context):
        self.log.info(f"Exporting StreetSpectra classifications to CSV file {self._output_path}")
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        aggregated_classifications = hook.get_records('''
            SELECT
                '1',  -- CSV file format export version
                subject_id || '-' || cluster_id,
                source_x,
                source_y,
                spectrum_type,
                spectrum_distr,
                spectrum_absfreq,
                cluster_size,
                epsilon,
                rejection_tag,
                image_url,
                image_long,
                image_lat,
                image_comment,
                image_source,
                image_created_at,
                image_spectrum
            FROM spectra_aggregate_t
            -- WHERE rejection_tag != 'Never classified'
            ORDER BY image_created_at DESC
        ''');
        # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path,'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(self.HEADER)
            for classification in aggregated_classifications:
                writer.writerow(classification)
        self.log.info(f"Exported StreetSpectra classifications to CSV file {self._output_path}")


class SQLInsertObservationsOperator(BaseOperator):
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


class AddClassificationsOperator(BaseOperator):
    '''
    Operator that ACTION JSON Observations and adds
    classification data from Zooniverse 

    Parameters
    —————
    conn_id : str
    Aiflow connection id to connect to ACTION internal SQLite database. 
    input_path : str
    (Templated) Path with ACTION observations in JSON format.
    output_path : str
    (Templated) Path to write the new enriched observations in JSON format.
    '''

    template_fields = ("_input_path",  "_output_path")

    @apply_defaults
    def __init__(self, conn_id, input_path, output_path, **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id
        self._output_path = output_path
        self._input_path  = input_path


    def _add_classifications(self, hook, observations):
        keys = ('cluster_id', 'source_x', 'source_y', 'cluster_size', 'spectrum_type', 'spectrum_absfreq', 'spectrum_distr')
        for observation in observations:
            classifications = hook.get_records(
                '''
                SELECT
                    cluster_id,    
                    source_x,    
                    source_y,    
                    cluster_size,   
                    spectrum_type,
                    spectrum_absfreq,    
                    spectrum_distr  
                FROM spectra_aggregate_t
                WHERE image_id = :id
            ''',
            observation
            )
            classifications = [dict(zip(keys, classif)) for classif in classifications]
            if classifications:
                observation['classifications'] = classifications
            

    def execute(self, context):
        # Read input JSON file
        with open(self._input_path) as fd:
            observations = json.load(fd)
            self.log.info(f"Parsed observations from {self._input_path}")
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        self._add_classifications(hook, observations)
        # Write results
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path, "w") as fd:
            json.dump(observations, indent=2,fp=fd)
            self.log.info(f"Written {len(observations)} entries to {self._output_path}")
    

# ========================
# Map generation operators
# ========================

class FoliumMapOperator(BaseOperator):

    template_fields = ("_input_path", "_output_path")
    ROOT_DIR = 'https://guaix.fis.ucm.es/~jaz/Street-Spectra/StreetSpectra_pictures/'

    @apply_defaults
    def __init__(self, input_path, output_path, center_latitude, center_longitude, zoom_start=6, max_zoom=19, **kwargs):
        super().__init__(**kwargs)
        self._input_path = input_path
        self._output_path = output_path
        self._map = folium.Map(
            location   = [center_latitude, center_longitude],
            zoom_start = zoom_start , 
            max_zoom   = max_zoom   # Máximum for Open Street Map
        )
        self._context = {}
        self._template_path = resource_filename(__name__, 'templates/maps.j2')

    def _render(self):
        template_path = self._template_path 
        if not os.path.exists(template_path):
            raise IOError("No Jinja2 template file found at {0}. Exiting ...".format(template_path))
        path, filename = os.path.split(template_path)
        return jinja2.Environment(
            loader=jinja2.FileSystemLoader(path or './')
        ).get_template(filename).render(self._context)

    def _popups(self, observations):
        self.log.info("Generating pop-ups")
        for context in observations:
            self._context = context
            pop_html = self._render()
            pop_html =  folium.Html(pop_html, script=True)
            cm = folium.CircleMarker(
                location     = (context['location']['latitude'], context['location']['longitude']), 
                radius       = 4, 
                popup        = folium.Popup(pop_html), 
                tooltip      = context['id'],
                fill         = True, 
                fill_opacity = 0.7, 
                parse_html   = False
            )
            cm.add_to(self._map)


    def _remap_entry(self, item):
        observer = item.get('observer',"anomymous")
        observer = "anonymous" if observer == '' else observer
        item['observer'] = strip_email(observer)
        # Get the name parameter of URL
        item['new_url'] = self.ROOT_DIR + item['url'].split('name=')[-1]
        return item

    def _coordinates(self, item):
        return item['location']['longitude'] != '' and item['location']['latitude'] != ''

    def _transform(self, entries):
        '''Map Epicollect V metadata to an ernal, more convenient representation'''
        # Use generators instead of lists
        g1 = map(self._remap_entry, entries)
        g2 = filter(self._coordinates, g1)
        return g2

    def execute(self, context):
        with open(self._input_path) as fd:
            observations = json.load(fd)
            self.log.info(f"Parsed {len(observations)} observations from {self._input_path}")
        observations = tuple(self._transform(observations)) # preprocess for Map generation
        self.log.info(f"{len(observations)} observations left after filtering")
        self._popups(observations) # generate the popups
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        self.log.info(f"Generating HTML map {self._output_path}")
        self._map.save(self._output_path)


class ImagesSyncOperator(BaseOperator):
    '''
    Operator that downloads actual images from Epicollect5 
    and uplaodes into StreetSpectra storage server. 
    
    Parameters
    —————
    sql_conn_id : str
    Aiflow connection id to connect to StrretSpectra SQLite database. 
    ssh_conn_id : str
    Aiflow connection id to connect to StrretSpectra image storage. 
    temp_dir : str
    (Templated) Directory to tenporary download the images.
    remote_dir : str
    (Templated) Directory into StreetSpectra storage server to upload the images.
    project : str
    Epicollect5 project
    '''
    
    template_fields = ("_temp_dir", "_remote_dir")

    @apply_defaults
    def __init__(self, sql_conn_id, ssh_conn_id, temp_dir, remote_dir, project, **kwargs):
        super().__init__(**kwargs)
        self._sql_conn_id = sql_conn_id
        self._ssh_conn_id = ssh_conn_id
        self._temp_dir    = temp_dir
        self._remote_dir  = remote_dir
        self._project     = project


    def _transaction(self, sqlite_hook, scp_hook, image_id, image_url):
        '''For each image, download from Epicollect and upload to GUAIX must be a single transaction'''
        temp_dir   = self._temp_dir
        remote_dir = self._remote_dir
        basename = image_url.split('name=')[-1]
        filename = os.path.join(temp_dir, basename)
        if os.path.exists(filename):
            self.log.info(f"Getting cached image from {filename}")
        else:
            self.log.info(f"Downloading image from {image_url}")
            response = requests.get(image_url)
            with open(filename,'wb') as fd:
                fd.write(response.content)
        ctime = time.gmtime(os.path.getctime(filename))
        downloaded_at = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", ctime)
        filter_dict = {
            'image_id'     : image_id, 
            'downloaded_at': downloaded_at,
        }      
        # Upload to GUAIX
        remote_file = os.path.join(remote_dir, basename)
        status_code = scp_hook.scp_to_remote(filename, remote_file)
        if status_code == 0:
            tstamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            filter_dict['uploaded_at'] = tstamp     
            # this should be the last step to make in the transaction
            sqlite_hook.run(
                '''
                INSERT INTO images_t (image_id, uploaded_at) VALUES (:image_id, :uploaded_at)
                ''',
                parameters = filter_dict,
            )
            os.remove(filename) # We no longer need it
        else:
            self.log.error(f"Error uploafding to image storage server. Code = {status_code}")


    def _iterate(self, sqlite_hook, scp_hook):
        filter_dict = { 'project': self._project}
        url_list = sqlite_hook.get_records('''
            SELECT image_id, url    
            FROM epicollect5_t
            WHERE project = :project
            AND image_id NOT IN (select image_id FROM images_t)
        ''',
            filter_dict
        )
        for image_id, image_url in url_list:
            self._transaction(sqlite_hook, scp_hook, image_id, image_url)
            


    def execute(self, context):
        os.makedirs(self._temp_dir, exist_ok=True)
        self._iterate(
            sqlite_hook = SqliteHook(sqlite_conn_id = self._sql_conn_id), 
            scp_hook    = SCPHook(ssh_conn_id = self._ssh_conn_id),
        )