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

from datetime import datetime, timedelta

# ---------------
# Airflow imports
# ---------------

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email  import EmailOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator


#-----------------------
# custom Airflow imports
# ----------------------

from airflow_actionproject.operators.epicollect5   import EC5ExportEntriesOperator
from airflow_actionproject.operators.zooniverse    import ZooniverseExportOperator, ZooniverseDeltaOperator
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.action        import ActionDownloadFromVariableDateOperator, ActionUploadOperator
from airflow_actionproject.operators.streetspectra import EC5TransformOperator, ZooniverseImportOperator, ZooniverseTransformOperator
from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets
from airflow_actionproject.callables.action        import check_number_of_entries

# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ['rafael08@ucm.es'],
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# =========================
# Observations ETL Workflow
# =========================

# 1. Extract from observation sources (currently Epicollect 5)
# 2. Transform into internal format for ACTION PROJECT Database
# 3. Load into ACTION PROJECT Observations Database

street_spectra_dag = DAG(
    'street_spectra',
    default_args      = default_args,
    description       = 'StreetSpectra Observations ETL',
    schedule_interval = '@monthly',
    start_date        = datetime(year=2019, month=1, day=1),
    tags              = ['ACTION PROJECT'],
)

# -----
# Tasks
# -----

export_ec5_observations = EC5ExportEntriesOperator(
    task_id      = "export_ec5_observations",
    conn_id      = "epicollect5-streetspectra",
    start_date   = "{{ds}}",
    end_date     = "{{next_ds}}",
    output_path  = "/tmp/ec5/street-spectra/{{ds}}.json",
    dag          = street_spectra_dag,
)

transform_ec5_observations = EC5TransformOperator(
    task_id      = "transform_ec5_observations",
    input_path   = "/tmp/ec5/street-spectra/{{ds}}.json",
    output_path  = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag          = street_spectra_dag,
)

load_ec5_observations = ActionUploadOperator(
    task_id    = "load_ec5_observations",
    conn_id    = "action-database-streetspectra",
    input_path = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag        = street_spectra_dag,
)

export_ec5_observations >> transform_ec5_observations >> load_ec5_observations

