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
import datetime
import collections

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# -------------------
# Third party imports
# -------------------

import numpy as np
import sklearn.cluster as cluster

#--------------
# local imports
# -------------

# This hook knows how to insert StreetSpectra metadata on subjects
from airflow_actionproject.hooks.streetspectra import ZooSpectraHook
from airflow_actionproject.hooks.sqlite import SqliteHook



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
         - no individual light source data (source_x, source_y or spectrum_type)
        
         '''
        return  classification.get("image_url")  and \
                classification.get("image_long") and \
                classification.get("image_lat")  and \
                len(classification.get("sources")) > 0


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
                    alist = list( map(lambda t: {'cluster_id': cl+1 if cl>-1 else cl, 'source_x':t[0], 'source_y': t[1], 'classification_id': t[2]}, alist))
                    clustered_classifications.extend(alist)
                else:
                    start = max(clusters)+2 # we will shift also the normal ones ...
                    for i in range(len(X)) :
                        cluster_id = start + i
                        row = {'cluster_id': cluster_id, 'source_x': X[i], 'source_y': Y[i], 'classification_id': ID[i]}
                        clustered_classifications.append(row)
                        
        hook.run_many(
            '''
            UPDATE light_sources_t
            SET 
                cluster_id    = :cluster_id
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
   