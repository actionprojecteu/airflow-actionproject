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

class IncorrectTimestampError(ValueError):
    '''Could not parse such timestamp'''
    def __str__(self):
        s = self.__doc__
        if self.args:
            s = ' {0}: {1}'.format(s, str(self.args[0]))
        s = '{0}.'.format(s)
        return s


class ActionDatabaseHook(BaseHook):

    DEFAULT_HOST      = "api.actionproject.eu" 
    API_SLUG          = "observations"
    DEFAULT_CONN_TYPE = "https"
    DEFAULT_PORT      = 443
    DEFAULT_PAGE_SIZE = 100
    DEFAULT_TPS       = 1


    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._session = None


    def get_conn(self):
        if self._session is None:
            self.log.info(f"{self.__class__.__name__} version {__version__}")
            self.log.debug(f"getting connection information from {self._conn_id}")
            config = self.get_connection(self._conn_id)
            # Define API base url.
            ctyp   = config.conn_type or self.DEFAULT_CONN_TYPE
            host   = config.host      or self.DEFAULT_HOST
            port   = config.port      or self.DEFAULT_PORT
            slug   = config.schema    or self.API_SLUG
            token  = config.password
            self._page_size = self.DEFAULT_PAGE_SIZE
            self._delay     = 1.0/self.DEFAULT_TPS
            if config.extra:
                try:
                    extra  = json.loads(config.extra)
                except json.decoder.JSONDecodeError:
                    pass
                else:
                    self._page_size = extra.get("page_size", self.DEFAULT_PAGE_SIZE)
                    self._delay = 1.0/extra.get("tps", self.DEFAULT_TPS)
            self._base_url = f"{ctyp}://{host}:{port}/{slug}"
            self._session  = requests.Session()
            self._session.headers.update({'Authorization': f"Bearer {token}"})
        return self._session, self._base_url, self._page_size, self._delay


    def _paginated_get_entries(self, session, url, params, page_size, n_entries):
        page  = 1
        total = 0
        premature_exit = False
        if page_size > n_entries:
            page_size = n_entries
        while not premature_exit:
            self.log.debug(f"Requesting page {url}")
            response = session.get(
                url, params={**params, **{"page": page, "limit": page_size}}
            )
            if not response.ok:
                self.log.error(f"{response.text}")
                response.raise_for_status()
            response_json = response.json()
            yield from response_json
            n = len(response_json)
            page  += 1
            total += n
            premature_exit = (n == 0) or (total >= n_entries)
            time.sleep(self._delay)



    def _parse_timestamp(self, input_date):
        tstamp_obj = None
        for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S.%fZ'):
            try:
                tstamp_obj = datetime.datetime.strptime(input_date, fmt)
            except ValueError as e:
                continue
            else:
                break
        if not tstamp_obj:
            raise IncorrectTimestampError(tstamp)
        else:
            return tstamp_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")



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


    def upload(self, observations):
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
        session, url, page_size, delay = self.get_conn()
        self.log.info(f"Uploading {len(observations)} observations to ACTION Database")
        for observation in observations:
            observation["written_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            response = session.post(url, json=observation)
            response.raise_for_status()
            time.sleep(delay)


    def download(self, start_date, end_date, project, obs_type, n_entries):
        '''
        Fetches entries from ACTION database for a given project between given start/end date 
        or up to n_entries, whichever occurs sooner.
        
        Parameters
        —————
        start_date : str
        Start date to start fetching ratings from (inclusive). Expected
        format is YYYY-MM-DD (equal to Airflow"s ds formats).
        end_date : str
        End date to fetching ratings up to (exclusive). Expected
        format is YYYY-MM-DD (equal to Airflow"s ds formats).
        project: str
        The ACTION project name
        obs_type : str
        observation type. Either ·"observation" or "classification".
        n_entries : int
        maximun number of entries to download. 
        Request are internally paged by the "page-size" connection parameter
        '''
        session, url, page_size, delay = self.get_conn()
        start_date = self._parse_timestamp(start_date)
        end_date   = self._parse_timestamp(end_date)
        self.log.info(f"Getting from ACTION Database {n_entries} {obs_type}(s) for {project} from {start_date} to {end_date}")
        params = {
            "begin_date" : start_date,
            "finish_date": end_date,
            "project"    : project,
            "obs_type"   : obs_type,
        }
        yield from self._paginated_get_entries(session, url, params, page_size, n_entries)
        
    def close(self):
        self.log.info(f"Closing ACTION database hook")
        self._session = None
