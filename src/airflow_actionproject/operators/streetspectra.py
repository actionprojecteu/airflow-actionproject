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
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


#--------------
# local imports
# -------------

# This hook knows how to insert StreetSpectra metadata o subjects
from airflow_actionproject.hooks.streetspectra import ZooniverseHook


# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------



class EC5TransformOperator(BaseOperator):
    """
    Operator that transforms entries exported from 
    Epicollect V API to ACTION  StreetSpectra JSON.
    
    Parameters
    —————
    input_path : str
    (Templated) Path to read the input JSON to transform to.
    output_path : str
    (Templated) Path to write the output transformed JSON.
    """

    # This mapping is unique to StreetSpectra
    NAME_MAP = {
        'ec5_uuid'            : 'id',
        'created_at'          : 'created_at',
        'uploaded_at'         : 'uploaded_at',
        'title'               : 'title',
        # New form names
        '1_Share_your_nick_wi': 'observer', 
        '2_Location'          : 'location',
        '3_Take_an_image_of_a': 'url',
        '4_Observations'      : 'comment',
        # Old form names
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
            'latitude' : location.get('latitude', None), 
            'longitude': location.get('longitude', None),
            'accuracy':  location.get('accuracy', None)
        }
        # Add extra items
        item["project"] = "street-spectra"
        item["source"] = "Epicollect5"
        item["obs_type"] = "observation"
        return item


    def _ec5_remapper(self, entries):
        '''Map Epicollect V metadata to an ernal, more convenient representation'''
        # Use generators instead of lists
        g1 = ({self.NAME_MAP[name]: val for name, val in entry.items()} for entry in entries)
        g2 =  map(self._remap, g1)
        return g2


# --------------------------------------------------------------
# This class is specific to StreetSpectra because 
# this particular ZooniverseHook is specific to StreetSpectra
# --------------------------------------------------------------

class ZooniverseImportOperator(BaseOperator):

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
        with ZooniverseHook(self._conn_id) as hook:
            hook.add_subject_set(self._display_name, subjects_metadata)




class ClassificationsOperator(BaseOperator):
    """
    Operator that extracts a subset of Zooniverse export file
    relevant to StreetSpectra and writes into a handy, 
    simplified classification table.
    
    Parameters
    —————
    input_path : str
    (Templated) Path to read the transformed input JSON file.
    conn_id : str
    SQLite connection identifier for the output table
    """
    
    template_fields = ("_input_path",)

    SPECTRUM_TYPE = {
        0: 'LED',   # Light Emitting Diode
        1: 'HPS',   # High Pressure Sodium
        2: 'LPS',   # Low Pressure Sodium
        3: 'MH',    # Metal Halide
        4: 'MV',    # Mercury Vapor
    }

    @apply_defaults
    def __init__(self, input_path, conn_id, **kwargs):
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
                new["spectrum_x"] = value[1]["x"]
                new["spectrum_y"] = value[1]["y"]
                new["spectrum_width"]  = value[1]["width"]
                new["spectrum_height"] = value[1]["height"]
                new["spectrum_angle"]  = value[1]["angle"]
                new["spectrum_type"]   = value[1]["details"][0]["value"]
                new["spectrum_type"] = self.SPECTRUM_TYPE[new["spectrum_type"]] # remap spectrum type codes to strings
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
        new["image_id"]         = value["id"]
        new["image_url"]        = value["url"]
        new["image_long"]       = value["longitude"]
        new["image_lat"]        = value["latitude"]
        new["image_observer"]   = value["observer"]
        new["image_comment"]    = value["comment"]
        new["image_source"]     = value["source"]
        new["image_created_at"] = value["created_at"]
        new["image_spectrum"]   = value.get("spectrum", None)
        return new

    def _insert(self, classifications):
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        for classification in classifications:
            hook.run(
                    '''
                    INSERT OR IGNORE INTO spectra_classification_t (
                        classification_id   ,
                        subject_id          ,
                        started_at          ,
                        finished_at         ,
                        width               ,
                        height              ,
                        source_x            ,
                        source_y            ,
                        spectrum_x          ,
                        spectrum_y          ,
                        spectrum_width      ,
                        spectrum_height     ,
                        spectrum_angle      ,
                        spectrum_type       ,
                        image_id            ,
                        image_url           ,
                        image_long          ,
                        image_lat           ,
                        image_observer      ,
                        image_comment       ,
                        image_source        ,
                        image_created_at    ,
                        image_spectrum
                    ) VALUES (
                        :classification_id   ,
                        :subject_id          ,
                        :started_at          ,
                        :finished_at         ,
                        :width               ,
                        :height              ,
                        :source_x            ,
                        :source_y            ,
                        :spectrum_x          ,
                        :spectrum_y          ,
                        :spectrum_width      ,
                        :spectrum_height     ,
                        :spectrum_angle      ,
                        :spectrum_type       ,
                        :image_id            ,
                        :image_url           ,
                        :image_long          ,
                        :image_lat           ,
                        :image_observer      ,
                        :image_comment       ,
                        :image_source        ,
                        :image_created_at    ,
                        :image_spectrum
                    )
                    ''', parameters=classification)


    def execute(self, context):
        with open(self._input_path) as fd:
            classifications = json.load(fd)
        classifications = list(map(self._extract, classifications))
        self._insert(classifications)
        


class AggregateOperator(BaseOperator):
    """
    Operator that aggregates individual classifications form users
    into a combined classification, for each source identridfied
    in a given Zooniverse subject. 
    
    Parameters
    —————
    conn_id : str
    SQLite connection identifier for the output table
    """

    RADIUS = 10
    CATEGORIES = ('LED','HPS','LPS','MV','MH', None)

    @apply_defaults
    def __init__(self, conn_id, **kwargs):
        super().__init__(**kwargs)
        self._conn_id     = conn_id

    def _setup_source_ids(self, subject_id, hook):
        '''Assume that each user has classified a different source within a subject'''
        classif_ids = hook.get_records('''
            SELECT classification_id 
            FROM spectra_classification_t
            WHERE subject_id = :subject_id
            AND source_id IS NULL
            ORDER BY classification_id ASC
            ''', 
            parameters={'subject_id': subject_id}
        )
        for source_id, classif_id in enumerate(classif_ids, start=1):
            classif_id = classif_id[0]
            self.log.info(f"Subject_id={subject_id} => Classif id={classif_id} => initial source id={source_id}")
            hook.run('''
                UPDATE spectra_classification_t
                SET source_id = :source_id
                WHERE subject_id = :subject_id AND classification_id = :classif_id
                ''', 
                parameters={'classif_id': classif_id, 'source_id': source_id, 'subject_id': subject_id}
            )

    
    def _cluster(self, subject_id, hook):
        '''Examine each source and refer it to the same id if they are near enough'''
        info1 = hook.get_records('''
            SELECT classification_id, source_x, source_y
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            ORDER BY classification_id ASC
            ''', 
            parameters={'subject_id': subject_id}
        )
        for clsf1, x1, y1 in info1:
            info2 = hook.get_records('''
                SELECT classification_id, source_x, source_y, source_id
                FROM spectra_classification_t 
                WHERE subject_id = :subject_id AND classification_id > :classif_id
                ''', 
                parameters={'classif_id': clsf1, 'subject_id': subject_id}
            )
            if info2:
                for clsf2, x2, y2, source2 in info2:
                    distance = math.sqrt((x1-x2)**2 + (y1-y2)**2)
                    if distance < self.RADIUS:
                        (source1,) = hook.get_first('''
                            SELECT source_id 
                            FROM spectra_classification_t 
                            WHERE subject_id = :subject_id AND classification_id = :classif_id
                            ''',
                            parameters={'classif_id': clsf1, 'subject_id': subject_id})
                        self.log.info(f"Subject_id={subject_id} => clsf1={clsf1}, clsf2={clsf2} => source id from {source2} to {source1} inhertited from clsf1 {clsf1}")
                        hook.run('''
                            UPDATE spectra_classification_t
                            SET source_id = :source_id
                            WHERE subject_id = :subject_id AND classification_id = :classif_id
                            ''', 
                            parameters={'classif_id': clsf2, 'source_id': source1, 'subject_id': subject_id}
                        )


    def _classify(self, subject_id, hook):
        ratings = list()
        source_ids = hook.get_records('''
                SELECT DISTINCT source_id
                FROM spectra_classification_t 
                WHERE subject_id = :subject_id
                ''', 
                parameters={'subject_id': subject_id}
            )
        
        for (source_id,) in source_ids:
            spectra_type = hook.get_records('''
                    SELECT spectrum_type
                    FROM spectra_classification_t 
                    WHERE subject_id = :subject_id AND source_id = :source_id
                    ''', 
                    parameters={'source_id': source_id, 'subject_id': subject_id}
                )
            counter = collections.Counter(s[0] for s in spectra_type)
            votes = counter.most_common()
            self.log.info(f" ############## VOTES {votes}")
            if len(votes) > 1 and votes[0][1] == votes[1][1]:
                spectrum_type = None
                spectrum_count = votes[0][1]
                rejection_tag = 'Ambiguous'
            elif votes[0][0] is None:
                 spectrum_type = None
                 spectrum_count = 0
                 rejection_tag = 'Never classified'
            else:
                spectrum_type  = votes[0][0]
                spectrum_count = votes[0][1]
                rejection_tag = None
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
            ratings.append(rating)
        kappa, n_users = self._kappa_fleiss(self.CATEGORIES, ratings)
        return ratings, kappa, n_users


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
        for classification, kappa, n_users in final_classifications:
            for source in classification:
                #self.log.info(source)
                source['kappa'] = kappa          # Common to all classifications in the subject
                source['users_count'] = n_users  # Common to all classifications in the subject
                hook.run('''
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
                    parameters=source
                )


    def execute(self, context):
        hook = SqliteHook(sqlite_conn_id=self._conn_id)
        final_classifications = list()
        subjects = hook.get_records('''
            SELECT DISTINCT subject_id 
            FROM spectra_classification_t 
            WHERE source_id IS NULL
        ''')
        for (subject_id,) in subjects:
            self._setup_source_ids(subject_id, hook)
            self._cluster(subject_id, hook)
            ratings, kappa, n_users = self._classify(subject_id, hook)
            final_classifications.append((ratings,kappa,n_users))
        if subjects:
            self._update(final_classifications, hook)

class IndividualExportCSVOperator(BaseOperator):
    """
    Operator that export raw individual StreetSpectra classifications 
    into a CSV file
    
    Parameters
    —————
    conn_id : str
    SQLite connection identifier for the output table
    output_path : str
    (Templated) Path to write the output CSV file.
    """
    
    HEADER = ( 
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
   


class AggregateExportCSVOperator(BaseOperator):
    """
    Operator that export aggregated StreetSpectra classifications 
    into a CSV file
    
    Parameters
    —————
    conn_id : str
    SQLite connection identifier for the output table
    output_path : str
    (Templated) Path to write the output CSV file.
    """
    
    HEADER = (  
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
   