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
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
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
from airflow_actionproject.operators.streetspectra import ExtractClassificationOperator, AggregateClassificationOperator
from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets
from airflow_actionproject.callables.action        import check_number_of_entries


# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ("astrorafael@gmail.com",), # CAMBIAR AL VERDADERO EN PRODUCCION
    'email_on_failure': False,                      # CAMBIAR A True EN PRODUCCION
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

streetspectra_collect_dag = DAG(
    'streetspectra_collect_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: collect observations',
    schedule_interval = '@monthly',
    start_date        = datetime(year=2019, month=1, day=1),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----

export_ec5_observations = EC5ExportEntriesOperator(
    task_id      = "export_ec5_observations",
    conn_id      = "streetspectra-epicollect5",
    start_date   = "{{ds}}",
    end_date     = "{{next_ds}}",
    output_path  = "/tmp/ec5/street-spectra/{{ds}}.json",
    dag          = streetspectra_collect_dag,
)

transform_ec5_observations = EC5TransformOperator(
    task_id      = "transform_ec5_observations",
    input_path   = "/tmp/ec5/street-spectra/{{ds}}.json",
    output_path  = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag          = streetspectra_collect_dag,
)

load_ec5_observations = ActionUploadOperator(
    task_id    = "load_ec5_observations",
    conn_id    = "streetspectra-action-database",
    input_path = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag        = streetspectra_collect_dag,
)

clean_up_ec5_files = BashOperator(
    task_id      = "clean_up_ec5_files",
    bash_command = "rm /tmp/ec5/street-spectra/{{ds}}.json ; rm /tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag        = streetspectra_collect_dag,
)

# -----------------
# Task dependencies
# -----------------

export_ec5_observations >> transform_ec5_observations >> load_ec5_observations >> clean_up_ec5_files

# ===========================
# Zooniverse Feeding Workflow
# ===========================

streetspectra_zoo_import_dag = DAG(
    'streetspectra_zoo_import_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Zooniverse image feeding workflow',
    schedule_interval = '@daily',
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----

manage_subject_sets = ShortCircuitOperator(
    task_id         = "manage_subject_sets",
    python_callable = zooniverse_manage_subject_sets,
    op_kwargs = {
        "conn_id"  : "streetspectra-zooniverse-test",
        "threshold": 75,    # 75% workflow completion status
    },
    dag           = streetspectra_zoo_import_dag
)

check_enough_observations = BranchPythonOperator(
    task_id         = "check_enough_observations",
    python_callable = check_number_of_entries,
    op_kwargs = {
        "conn_id"       : "streetspectra-action-database",
        "start_date"    : "2019-09-01T00:00:00.00000Z",    # ESTA ES LA PRIMERA FECHA EN LA QUE HAY ALGO
        "n_entries"     : 10,              # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
        "project"       : "street-spectra",
        "true_task_id"  : "download_from_action",
        "false_task_id" : "email_no_images",
        "obs_type"      : 'observation',
    },
    dag           = streetspectra_zoo_import_dag
)

email_no_images = EmailOperator(
    task_id      = "email_no_images",
    to           = ("astrorafael@gmail.com",),
    subject      = "[StreetSpectra] Airflow warn: No ACTION images left",
    html_content = "No images left in ACTION database to create an new Zooniverse Subject Set.",
    dag          = streetspectra_zoo_import_dag,
)

download_from_action = ActionDownloadFromVariableDateOperator(
    task_id        = "download_from_action",
    conn_id        = "streetspectra-action-database",
    output_path    = "/tmp/zooniverse/streetspectra/action-{{ds}}.json",
    variable_name  = "streetspectra_read_tstamp",
    n_entries      = 10,                # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = streetspectra_zoo_import_dag,
)

upload_new_subject_set = ZooniverseImportOperator(
    task_id         = "upload_new_subject_set",
    conn_id         = "streetspectra-zooniverse-test",
    input_path      = "/tmp/zooniverse/streetspectra/action-{{ds}}.json", 
    display_name    = "Subject Set {{ds}}",
    dag             = streetspectra_zoo_import_dag,
)

# This needs to be configured:
# WARNING - section/key [smtp/smtp_user] not found in config
# See https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email

email_new_subject_set = EmailOperator(
    task_id      = "email_new_subject_set",
    to           = ("astrorafael@gmail.com",),
    subject      = "[StreetSpectra] Airflow info: new Zooniverse Subject Set",
    html_content = "New Zooniverse Subject Set {{ds}} created.",
    dag          = streetspectra_zoo_import_dag,
)

clean_up_action_obs_file = BashOperator(
    task_id      = "clean_up_action_obs_file",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    bash_command = "rm /tmp/zooniverse/streetspectra/action-{{ds}}.json",
    dag          = streetspectra_zoo_import_dag,
)

# -----------------
# Task dependencies
# -----------------

manage_subject_sets  >> check_enough_observations >> [download_from_action,  email_no_images]
download_from_action >> upload_new_subject_set >> email_new_subject_set
[email_new_subject_set, email_no_images] >> clean_up_action_obs_file


# ===================================
# CLASSIFICATIONS EXPORT ETL WORKFLOW
# ===================================

# Aqui hay que tener en cuenta que el exportado de Zooniverse es completo
# y que la BD de ACTION NO detecta duplicados
# Asi que hay que usar ventanas de clasificaciones subida
# Este "enventanado" debe ser lo primero que se haga tras la exportacion para evitar
# que los procesados posteriores sean largos.
# Este enventanado no se hace por variables de Airflow 
# sino en una tabla de la BD Sqlite interna


streetspectra_zoo_export_dag = DAG(
    'streetspectra_zoo_export_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Zooniverse classifications export workflow',
    schedule_interval = '@monthly',
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----

export_classifications = ZooniverseExportOperator(
    task_id     = "export_classifications",
    conn_id     = "streetspectra-zooniverse-test",
    output_path = "/tmp/zooniverse/complete-{{ds}}.json",
    generate    = True, 
    wait        = True, 
    timeout     = 600,
    dag         = streetspectra_zoo_export_dag,
)

only_new_classifications = ZooniverseDeltaOperator(
    task_id       = "only_new_classifications",
    conn_id       = "streetspectra-temp-db",
    input_path    = "/tmp/zooniverse/complete-{{ds}}.json",
    output_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    dag           = streetspectra_zoo_export_dag,
)

transform_classfications = ZooniverseTransformOperator(
    task_id      = "transform_classfications",
    input_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    output_path  = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    dag          = streetspectra_zoo_export_dag,
)

load_zoo_classifications = ActionUploadOperator(
    task_id    = "load_zoo_classifications",
    conn_id    = "streetspectra-action-database",
    input_path = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    dag        = streetspectra_zoo_export_dag,
)

# UNDER TEST
internal_db_classifications = ExtractClassificationOperator(
    task_id    = "internal_db_classifications",
    conn_id    = "streetspectra-temp-db",
    input_path = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    dag        = streetspectra_zoo_export_dag,
)

# UNDER TEST
under_test = AggregateClassificationOperator(
    task_id    = "under_test",
    conn_id    = "streetspectra-temp-db",
    dag        = streetspectra_zoo_export_dag,
)


clean_up_classif_files = BashOperator(
    task_id      = "clean_up_classif_files",
    bash_command = "rm /tmp/zooniverse/complete-{{ds}}.json; rm /tmp/zooniverse/subset-{{ds}}.json; rm /tmp/zooniverse/transformed-subset-{{ds}}.json",
    dag          = streetspectra_zoo_export_dag,
)

# -----------------
# Task dependencies
# -----------------

export_classifications >> only_new_classifications >> transform_classfications >> load_zoo_classifications >> clean_up_classif_files

################### TESTING ZENODO
# THERE ARE MISSING TASKS LIKE:
#  * EXPORT GLOBAL CLASSIFICATIONS
# BEFORE PUBLISHING TO ZENODO

streetspectra_publishing_dag = DAG(
    'streetspectra_publishing_dag',
    default_args      = default_args,
    description       = 'StreetSpectra publication workflow',
    schedule_interval = '@monthly',
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

publish_to_zenodo = ZenodoPublishDatasetOperator(
    task_id     = "publish_to_zenodo",
    conn_id     = "streetspectra-zenodo-sandbox",
    title       = "Prueba 15",
    file_path   = "example.txt",
    description = "Testing Prueba 15",
    version     = '21.05',
    creators    = [{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = streetspectra_publishing_dag,
)

