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
        new = dict()
        # General info
        new["classification_id"] = classification["classification_id"]
        new["subject_id"]        = classification["subject_ids"]
        new["started_at"]        = classification["metadata"]["started_at"]
        new["finished_at"]       = classification["metadata"]["finished_at"]
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
            if len(value) > 1:  # The user really used this tool
                new["spectrum_x"] = value[1].get("x")
                new["spectrum_y"] = value[1].get("y")
                new["spectrum_width"]  = value[1].get("width")
                new["spectrum_height"] = value[1].get("height")
                new["spectrum_angle"]  = value[1].get("angle")
                details   = value[1].get("details")
                if details:
                    new["spectrum_type"] = details[0]["value"]
                    new["spectrum_type"] = self.SPECTRUM_TYPE.get(new["spectrum_type"], new["spectrum_type"]) # remap spectrum type codes to strings
                else:
                    new["spectrum_type"] = None
            else:
                new["spectrum_x"] = None
                new["spectrum_y"] = None
                new["spectrum_width"]  = None
                new["spectrum_height"] = None
                new["spectrum_angle"]  = None
                new["spectrum_type"]   = None
        else:   # The user skipped this observation
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
        new["image_id"]         = value.get("id")
        new["image_url"]        = value.get("url")
        new["image_long"]       = value.get("longitude")
        new["image_lat"]        = value.get("latitude")
        new["image_observer"]   = value.get("observer")
        new["image_comment"]    = value.get("comment")
        new["image_source"]     = value.get("source")
        new["image_created_at"] = value.get("created_at")
        new["image_spectrum"]   = value.get("spectrum_type")  # original classification made by observer
        if new["image_spectrum"] == "":
            new["image_spectrum"] = None
        return new


    def _is_useful(self, classification):
        '''False for classifications with either:
         - no source_x or source_y 
         - no spectrum_type
         - missing image_url metadata
         - No GPS pòsition
         '''

        return classification.get("source_x") and classification.get("source_y") and \
                classification.get("spectrum_type") and classification.get("image_url") and \
                classification.get("image_long") and classification.get("image_lat")


    def _insert(self, classifications):
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        self.log.info(f"Logging classifications differences")
        hook.insert_dict_rows(
            table        = 'spectra_classification_t',
            dict_rows    = classifications,
            commit_every = 500,
            replace      = False,
            ignore       = False,
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

    RADIUS = 15  # light source dispersion radius in pixels
    CATEGORIES = ('HPS','MV','LED','MH', None)

    @apply_defaults
    def __init__(self, conn_id, **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id


    def _cluster(self, hook):
        '''Perform clustering analysis over source light selection'''
        user_selections = hook.get_records('''
            SELECT subject_id, source_x, source_y
            FROM spectra_classification_t
            WHERE source_id IS NULL
        ''')
        classifications_per_subject = dict()
        for subject_id, source_x, source_y in user_selections:
            coordinates = classifications_per_subject.get(subject_id, [])
            coordinates.append((source_x, source_y))
            classifications_per_subject[subject_id] = coordinates
        clustered_classifications = list()
        for subject_id, coordinates in classifications_per_subject.items():
            N_Classifications = len(coordinates)
            if N_Classifications < 2:
                self.log.debug(f"Skipping cluster analysis in subject {subject_id} [N = {N_Classifications}]")
                clustered_classifications.append({'source_id': 1, 'source_x':coordinates[0][0] , 'source_y':coordinates[0][1]})
            else:
                # Define the model
                coordinates = np.array(coordinates)
                model = cluster.DBSCAN(eps=self.RADIUS, min_samples=2)
                # Fit the model and predict clusters
                yhat = model.fit_predict(coordinates)
                # retrieve unique clusters
                clusters = np.unique(yhat)
                self.log.info(f"Subject {subject_id}: {len(clusters)} clusters from {N_Classifications} classifications, ids: {clusters}")
                # create scatter plot for samples from each cluster
                for cl in clusters:
                    # get row indexes for samples with this cluster
                    row_ix = np.where(yhat == cl)
                    X = coordinates[row_ix, 0][0]; Y = coordinates[row_ix, 1][0]
                    alist = np.column_stack((X,Y))
                    alist = list( map(lambda t: {'source_id': cl+1 if cl>-1 else cl, 'source_x':t[0], 'source_y': t[1]}, alist))
                    clustered_classifications.extend(alist)
        hook.run_many_dict_rows(
            dict_rows = clustered_classifications,
            sql = '''
                UPDATE spectra_classification_t
                SET 
                    source_id    = :source_id
                WHERE source_x   = :source_x
                AND  source_y    = :source_y
                ''',
            commit_every = 500,
        )



    def _classify(self, hook):
        ratings = list()
        info1 = hook.get_records('''
            SELECT DISTINCT subject_id, source_id, spectrum_type
            FROM spectra_classification_t 
            WHERE clustered IS NULL
            ''',
        )
        counters = dict()
        for subject_id, source_id, spectrum_type in info1:
            key = tuple([subject_id, source_id])
            spectra_type = counters.get(key,[])
            spectra_type.append(spectrum_type)
            counters[key] = spectra_type
        # Compute ratings per source_id per subject
        ratings = dict()
        for key, spectra_type in counters.items():
            subject_id = key[0]
            source_id  = key[1]
            counter = collections.Counter(spectra_type)
            votes = counter.most_common()
            self.log.info(f" VOTES {votes}")
            if len(votes) > 1 and votes[0][1] == votes[1][1]:
                spectrum_type  = None
                spectrum_count = votes[0][1]
                rejection_tag  = 'Ambiguous'
            elif votes[0][0] is None:
                 spectrum_type  = None
                 spectrum_count = 0
                 rejection_tag  = 'Never classified'
            else:
                spectrum_type  = votes[0][0]
                spectrum_count = votes[0][1]
                rejection_tag  = None
            rating = {
                'subject_id'    : subject_id, 
                'source_id'     : source_id, 
                'spectrum_type' : spectrum_type,
                'spectrum_count': spectrum_count,
                'spectrum_users': sum(counter[key] for key in counter),
                'spectrum_dist' : str(votes), 
                'rejection_tag' : rejection_tag,
                'counter'       : counter,
            }
            rateds = ratings.get(key,[])
            rateds.append(rating)
            ratings[key] = rateds
        # Compute Fleiss' Kappa per source_id per subject
        final_classifications = list()
        for key, rateds in ratings.items():
            kappa, n_users = self._kappa_fleiss(self.CATEGORIES, rateds)
            for rated in rateds:
                rated['kappa']       = kappa
                rated['users_count'] = n_users
                rated['clustered']   = 2 # Changed the state so as not to process all aver again
            final_classifications.extend(rateds)
        self._update(final_classifications, hook)
    


    def _kappa_fleiss(self, categories, ratings):
        '''
        Calculates Fleiss' kappa.
        categories - sequence of categories
        ratings    - sequence of classifications given each one by collection.Counters
        See https://en.wikipedia.org/wiki/Fleiss%27_kappa=  
        '''
        matrix     = dict()
        p_j        = dict()
        P_i        = dict()
        N          = len(ratings)
        
        # find out the number of raters by aggregating 
        # all counters for all source_ids of the same subject_id
        n = sum(sum(rating['counter'][key] for key in rating['counter']) for rating in ratings)
        if n == 1:
            return None, n
        subjects   = set(rating['source_id'] for rating in ratings)
        # Build the rating matrix
        for rating in ratings:
            for category in categories:
                row    = rating['source_id']
                column = category
                number = rating['counter'][category]
                matrix[(row,column)] = number
    
        # Calculate the different p_j by summing columns
        for category in categories:
            s = sum(matrix[(subject,category)] for subject in subjects)
            p_j[category] = s / float(N * n)
        # Calculate the different P_i by summing rows
        for subject in subjects:
            s = sum(matrix[(subject,category)]**2 for category in categories)
            P_i[subject] = (s - n)/ float(n*(n-1))
        P_bar   = sum(P_i[key]    for key in P_i) / N
        P_e_bar = sum(p_j[key]**2 for key in p_j)
        if P_e_bar == 1.0:
            kappa = math.inf
        else:
            kappa = (P_bar - P_e_bar) / (1 - P_e_bar)
        return kappa, n


    def _update(self, final_classifications, hook):
        # Update everything not depending on the aggregate classifications first
        hook.run('''
                INSERT OR IGNORE INTO spectra_aggregate_t (
                subject_id,
                source_id,
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
                source_id, 
                width,
                height,
                ROUND(AVG(source_x),2), 
                ROUND(AVG(source_y),2),
                image_id, 
                image_url, 
                image_long, 
                image_lat, 
                image_observer, 
                image_comment, 
                image_source, 
                image_created_at, 
                image_spectrum
            FROM spectra_classification_t 
            GROUP BY subject_id, source_id
            '''
        )
        hook.run_many_dict_rows(
            dict_rows = final_classifications,
            sql = '''
                UPDATE spectra_classification_t
                SET 
                    clustered      = :clustered
                WHERE subject_id = :subject_id
                AND source_id    = :source_id
                ''',
            commit_every = 500,
        )
        hook.run_many_dict_rows(
            dict_rows = final_classifications,
            sql = '''
                UPDATE spectra_aggregate_t
                SET 
                    spectrum_type  = :spectrum_type,
                    spectrum_dist  = :spectrum_dist,
                    spectrum_users = :spectrum_users,
                    spectrum_count = :spectrum_count,
                    rejection_tag  = :rejection_tag,
                    kappa          = :kappa,
                    users_count    = :users_count
                WHERE subject_id = :subject_id
                AND source_id    = :source_id
                ''',
            commit_every = 500,
        )


    def execute(self, context):
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        self._cluster(hook)
        #self._classify(hook)


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
            'classification_id',
            'started_at',
            'finished_at',
            'width',
            'height',
            'source_x',
            'source_y',
            'spectrum_x',
            'spectrum_y',
            'spectrum_width',
            'spectrum_height',
            'spectrum_angle',
            'spectrum_type',
            'image_url',
            'image_long',
            'image_lat',
            'image_observer',
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
        self.log.info(f"Exporting StreetSpectra individual classifications to CSV file {self._output_path}")
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        individual_classifications = hook.get_records('''
            SELECT
                '1.0',  -- CSV file format export version
                subject_id,
                classification_id,
                started_at,
                finished_at,
                width,
                height,
                source_x,
                source_y,
                spectrum_x,
                spectrum_y,
                spectrum_width,
                spectrum_height,
                spectrum_angle,
                spectrum_type,
                image_url,
                image_long,
                image_lat,
                image_observer,
                image_comment,
                image_source,
                image_created_at,
                image_spectrum
            FROM spectra_classification_t
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
            'source_label',
            'source_x',
            'source_y',
            'spectrum_type',
            'spectrum_dist',
            'agreement',
            'rejection_tag',
            'kappa',
            'users_count',
            'image_url',
            'image_long',
            'image_lat',
            'image_observer',
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
                '1.0',  -- CSV file format export version
                subject_id || '-' || source_id,
                source_x,
                source_y,
                spectrum_type,
                spectrum_dist,
                spectrum_count || '/' || spectrum_users,
                rejection_tag,
                kappa,
                users_count,
                image_url,
                image_long,
                image_lat,
                image_observer,
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
   