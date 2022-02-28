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
import time
import datetime
import itertools

# Access Jinja2 templates withing the package
from pkg_resources import resource_filename

# ---------------
# Airflow imports
# ---------------

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# -------------------
# Third party imports
# -------------------

import requests
import folium
import sklearn.cluster as cluster
import jinja2

#--------------
# local imports
# -------------

from airflow_actionproject import __version__
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


# ========================
# Map generation operators
# ========================


class AddClassificationsOperator(BaseOperator):
    '''
    Operator that reads an ACTION JSON Observations file and adds classification data from Zooniverse 

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
        classifications = hook.get_records(
            '''
            SELECT
                image_id,
                cluster_id,    
                source_x,    
                source_y,    
                cluster_size,   
                spectrum_type,
                spectrum_absfreq,    
                spectrum_distr  
            FROM spectra_aggregate_t
            ''')
        for obs in observations:
            obs['classifications'] = list()
        N = 0
        for key, group in itertools.groupby(classifications, key=lambda item: item[0]):
            group = tuple(dict(zip(keys, item[1:])) for item in group)
            for obs in observations:
                if obs['id'] == key:
                    obs['classifications'] = group
                    N += 1
        self.log.info(f"Updated {N} observations with classification data")
       
            

    def execute(self, context):
        self.log.info(f"{self.__class__.__name__} version {__version__}")
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
    


class FoliumMapOperator(BaseOperator):

    template_fields = ("_input_path", "_output_path")

    @apply_defaults
    def __init__(self, input_path, output_path, ssh_conn_id, remote_slug, center_latitude, center_longitude, zoom_start=6, max_zoom=19, **kwargs):
        super().__init__(**kwargs)
        self._ssh_conn_id = ssh_conn_id
        self._remote_slug = remote_slug
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
        item['new_url'] = self._urlbase + item['url'].split('name=')[-1]
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
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        with SCPHook(self._ssh_conn_id) as hook:
            host, _, _, _,_ = hook.get_conn()
            doc_root = hook.doc_root()
            self._urlbase = f"https://{host}/{doc_root}/{self._remote_slug}/"

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
    remote_slug : str
    (Templated) remote directory (relative to a document root) where to upload the images.
    project : str
    Epicollect5 project
    '''
    
    template_fields = ("_temp_dir", "_remote_slug")

    @apply_defaults
    def __init__(self, sql_conn_id, ssh_conn_id, temp_dir, remote_slug, project, **kwargs):
        super().__init__(**kwargs)
        self._sql_conn_id = sql_conn_id
        self._ssh_conn_id = ssh_conn_id
        self._temp_dir    = temp_dir
        self._remote_slug = remote_slug
        self._project     = project


    def _transaction(self, sqlite_hook, scp_hook, image_id, image_url):
        '''For each image, download from Epicollect and upload to GUAIX must be a single transaction'''
        temp_dir   = self._temp_dir
        remote_slug = self._remote_slug
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
        remote_file = os.path.join(remote_slug, basename) # scp hook also prefixes this with a doc root
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
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        os.makedirs(self._temp_dir, exist_ok=True)
        self._iterate(
            sqlite_hook = SqliteHook(sqlite_conn_id = self._sql_conn_id), 
            scp_hook    = SCPHook(ssh_conn_id = self._ssh_conn_id),
        )