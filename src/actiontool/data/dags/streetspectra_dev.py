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
    'street_spectra_ec5',
    default_args      = default_args,
    description       = 'StreetSpectra Observations ETL',
    schedule_interval = '@monthly',
    start_date        = datetime(year=2019, month=1, day=1),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
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
    conn_id    = "action-database-streetspectra-streetspectra",
    input_path = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag        = street_spectra_dag,
)

export_ec5_observations >> transform_ec5_observations >> load_ec5_observations

# ===========================
# Zooniverse Feeding Workflow
# ===========================

street_spectra_zoo = DAG(
    'street_spectra_zoo_import',
    default_args      = default_args,
    description       = 'Zooniverse image feeding workflow',
    schedule_interval = '@daily',
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

email_team = EmailOperator(
    task_id      = "email_team",
    to           = ["astrorafael@gmail.com","rafael08@ucm.es"],
    subject      = "New StreetSpectra Subject Set being uploaded to Zooniverse",
    html_content = "Subject Set {{ds}} being uploaded.",
    dag          = street_spectra_zoo,
)

manage_subject_sets = ShortCircuitOperator(
    task_id         = "manage_subject_sets",
    python_callable = zooniverse_manage_subject_sets,
    op_kwargs = {
        "conn_id"  : "zooniverse-streetspectra-test",
        "threshold": 75,    # 75% workflow completion status
    },
    dag           = street_spectra_zoo
)

check_enough_observations = ShortCircuitOperator(
    task_id         = "check_enough_observations",
    python_callable = check_number_of_entries,
    op_kwargs = {
        "conn_id"    : "zooniverse-streetspectra-test",
        "start_date" : "2019-09-01",    # ESTA ES LA PRIMERA FECHA EN LA QUE HAY ALGO
        "n_entries"  : 10,              # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
        "project"    : "street-spectra",
        "obs_type"   : 'observation',
    },
    dag           = street_spectra_zoo
)

download_from_action = ActionDownloadFromVariableDateOperator(
    task_id        = "download_from_action",
    conn_id        = "action-database-streetspectra",
    output_path    = "/tmp/zooniverse/streetspectra/action-{{ds}}.json",
    variable_name  = "action_ss_read_tstamp",
    n_entries      = 10,                # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = street_spectra_zoo,
)

from airflow_actionproject.operators.action        import ActionDownloadFromStartDateOperator
download_from_action = ActionDownloadFromStartDateOperator(
    task_id        = "download_from_action2",
    conn_id        = "action-database-streetspectra",
    output_path    = "/tmp/zooniverse/streetspectra/action-{{ds}}.json",
    start_date     = "2020-01-01T17:15:49.000000Z",
    n_entries      = 10,                # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = street_spectra_zoo,
)

if False:
    upload_new_subject_set = ZooniverseImportOperator(
        task_id         = "upload_new_subject_set",
        input_path      = "/tmp/zooniverse/streetspectra/action-{{ds}}.json", 
        display_name    = "Subject Set {{ds}}",
        dag = street_spectra_zoo,
    )
else:
    upload_new_subject_set = DummyOperator(
        task_id         = "upload_new_subject_set",
        dag = street_spectra_zoo,
    )

# Task dependencies
manage_subject_sets >> email_team >> check_enough_observations >> download_from_action >> upload_new_subject_set


# ===================================
# CLASSIFICATIONS EXPORT ETL WORKFLOW
# ===================================

# Aqui hay que tener en cuenta que el exportado de Zooniverse es completo
# y que la BD de ACTION NO detecta duplicados
# Asi que hay que usar variables de la ventana de clasificaciones subida
# Este "enventanado" debe ser lo primero que se haga tras la exportacion para evitar
# que los procesados posteriores sean largos


classifications_dag = DAG(
    'street_spectra_zoo_export',
    default_args      = default_args,
    description       = 'Zooniverse classifications export workflow',
    schedule_interval = '@monthly',
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

export_classifications = ZooniverseExportOperator(
    task_id     = "export_classifications",
    conn_id     = "zooniverse-streetspectra-test",
    output_path = "/tmp/zooniverse/whole-{{ds}}.json",
    generate    = True, 
    wait        = True, 
    timeout     = 600,
    dag         = classifications_dag,
)

only_new_classifications = ZooniverseDeltaOperator(
    task_id       = "only_new_classifications",
    conn_id       = "streetspectra-temp-db",
    input_path    = "/tmp/zooniverse/whole-{{ds}}.json",
    output_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    dag           = classifications_dag,
)

transform_classfications = ZooniverseTransformOperator(
    task_id      = "transform_classfications",
    input_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    output_path  = "/tmp/ec5/street-spectra/transformed-subset-{{ds}}.json",
    dag          = classifications_dag,
)


load_classfications = DummyOperator(task_id="load_classfications", dag=classifications_dag)

export_classifications >> only_new_classifications >> transform_classfications >> load_classfications

################### TESTING ZENODO
# THERE ARE MISSING TASKS LIKE:
#  * EXPORT GLOBAL CLASSIFICATIONS
# BEFORE PUBLISHING TO ZENODO

publishing_dag = DAG(
    'street_spectra_zenodo',
    default_args      = default_args,
    description       = 'Publication workflow',
    schedule_interval = '@monthly',
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

publish_to_zenodo = ZenodoPublishDatasetOperator(
    task_id     = "publish_to_zenodo",
    conn_id     = "zenodo-sandbox",
    title       = "Prueba 15",
    file_path   = "example.txt",
    description = "Testing Prueba 15",
    version     = '21.05',
    creators    = [{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = publishing_dag,
)
