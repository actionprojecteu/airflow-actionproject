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
from airflow_actionproject.operators.zooniverse    import ZooniverseExportOperator, ZooniverseDeltaOperator, ZooniverseTransformOperator
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.action        import ActionDownloadFromVariableDateOperator, ActionUploadOperator
from airflow_actionproject.operators.streetspectra import EC5TransformOperator, ZooImportOperator
from airflow_actionproject.operators.streetspectra import PreprocessClassifOperator, AggregateOperator, AggregateCSVExportOperator, IndividualCSVExportOperator
from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets
from airflow_actionproject.callables.action        import check_number_of_entries
from airflow_actionproject.callables.streetspectra import check_new_subjects

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
    bash_command = "rm /tmp/ec5/street-spectra/*{{ds}}.json",
    dag        = streetspectra_collect_dag,
)

# Old StreetSpectra Epicollect V project
# included for compatibilty only
export_ec5_old = EC5ExportEntriesOperator(
    task_id      = "export_ec5_old",
    conn_id      = "oldspectra-epicollect5",
    start_date   = "{{ds}}",
    end_date     = "{{next_ds}}",
    output_path  = "/tmp/ec5/street-spectra/old-{{ds}}.json",
    dag          = streetspectra_collect_dag,
)

transform_ec5_old = EC5TransformOperator(
    task_id      = "transform_ec5_old",
    input_path   = "/tmp/ec5/street-spectra/old-{{ds}}.json",
    output_path  = "/tmp/ec5/street-spectra/old-transformed-{{ds}}.json",
    dag          = streetspectra_collect_dag,
)

load_ec5_old = ActionUploadOperator(
    task_id    = "load_ec5_old",
    conn_id    = "streetspectra-action-database",
    input_path = "/tmp/ec5/street-spectra/old-transformed-{{ds}}.json",
    dag        = streetspectra_collect_dag,
)

# -----------------
# Task dependencies
# -----------------

export_ec5_observations >> transform_ec5_observations >> load_ec5_observations >> clean_up_ec5_files
export_ec5_old          >> transform_ec5_old          >> load_ec5_old          >> clean_up_ec5_files

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
        "start_date"    : "2019-09-01T00:00:00.00000Z",     # ESTA ES LA PRIMERA FECHA EN LA QUE HAY ALGO
        "n_entries"     : 10,                               # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
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
    n_entries      = 10,                                    # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = streetspectra_zoo_import_dag,
)

upload_new_subject_set = ZooImportOperator(
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

cleanup_action_obs_file = BashOperator(
    task_id      = "cleanup_action_obs_file",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    bash_command = "rm /tmp/zooniverse/streetspectra/*{{ds}}.json",
    dag          = streetspectra_zoo_import_dag,
)

# -----------------
# Task dependencies
# -----------------

manage_subject_sets  >> check_enough_observations >> [download_from_action,  email_no_images]
download_from_action >> upload_new_subject_set >> email_new_subject_set
[email_new_subject_set, email_no_images] >> cleanup_action_obs_file


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

# Perform the whole Zooniverse export from the beginning of the project
export_classifications = ZooniverseExportOperator(
    task_id     = "export_classifications",
    conn_id     = "streetspectra-zooniverse-test",
    output_path = "/tmp/zooniverse/complete-{{ds}}.json",
    generate    = True, 
    wait        = True, 
    timeout     = 600,
    dag         = streetspectra_zoo_export_dag,
)

# Produces an output file with only new classifications
# made since the last export
# This valid for any Zooniverse project
only_new_classifications = ZooniverseDeltaOperator(
    task_id       = "only_new_classifications",
    conn_id       = "streetspectra-db",
    input_path    = "/tmp/zooniverse/complete-{{ds}}.json",
    output_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    dag           = streetspectra_zoo_export_dag,
)

# Transforms the new Zooniverse classifcations file in to a JSON
# file suitable to be loaded into the ACTION database as 'classifications'
# This is valid for any project that uses the ACTION database API
transform_classfications = ZooniverseTransformOperator(
    task_id      = "transform_classfications",
    input_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    output_path  = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    project      = "street-spectra",
    dag          = streetspectra_zoo_export_dag,
)

# Preprocess the transformed file into a StreetSpectra
# relational database, getting only the most relevant information
# This is valid only for StreetSpectra
preprocess_classifications = PreprocessClassifOperator(
    task_id    = "preprocess_classifications",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    dag        = streetspectra_zoo_export_dag,
)

# Check new spectra to be classified and aggregated
# This is valid only for StreetSpectra
check_new_spectra = BranchPythonOperator(
    task_id         = "check_new_spectra",
    python_callable = check_new_subjects,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "true_task_id"  : "aggregate_classifications",
        "false_task_id" : "skip_to_end",
    },
    dag           = streetspectra_zoo_export_dag
)

# needed for branching
skip_to_end = DummyOperator(
    task_id    = "skip_to_end",
    dag        = streetspectra_zoo_export_dag,
)


# Aggregates classifications into a single combined value 
# for every light source selected by Zooniverse users when classifying
# This is valid only for StreetSpectra
aggregate_classifications = AggregateOperator(
    task_id    = "aggregate_classifications",
    conn_id    = "streetspectra-db",
    dag        = streetspectra_zoo_export_dag,
)

# Export aggregated classifications into CSV file format
# This is valid only for StreetSpectra
export_aggregated_csv = AggregateCSVExportOperator(
    task_id     = "export_aggregated_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/zooniverse/streetspectra-aggregated.csv",
    dag         = streetspectra_zoo_export_dag,
)

# Export individuyal classifications into CSV format
# This is valid only for StreetSpectra
export_individual_csv = IndividualCSVExportOperator(
    task_id     = "export_individual_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/zooniverse/streetspectra-individual.csv",
    dag         = streetspectra_zoo_export_dag,
)

# Publish the aggregated dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_aggregated_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_aggregated_csv",
    conn_id     = "streetspectra-zenodo-sandbox",
    title       = "Street Spectra aggregated classifications",
    file_path   = "/tmp/zooniverse/streetspectra-aggregated.csv",
    description = "CSV file containing aggregated classifications for light sources data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = [{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = streetspectra_zoo_export_dag,
)

# Publish the individual dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_individual_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_individual_csv",
    conn_id     = "streetspectra-zenodo-sandbox",
    title       = "Street Spectra individual classifications",
    file_path   = "/tmp/zooniverse/streetspectra-individual.csv",
    description = "CSV file containing individual classifications for subjects data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = [{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = streetspectra_zoo_export_dag,
)

# needed for parallel export+publish operations
join_published = DummyOperator(
    task_id    = "join_published",
    dag        = streetspectra_zoo_export_dag,
)

# Clean up temporary files
clean_up_classif_files = BashOperator(
    task_id      = "clean_up_classif_files",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    bash_command = "rm /tmp/zooniverse/*.csv; rm /tmp/zooniverse/*-{{ds}}.json",
    dag          = streetspectra_zoo_export_dag,
)

# -----------------
# Task dependencies
# -----------------

export_classifications >> only_new_classifications >> transform_classfications >> preprocess_classifications
preprocess_classifications >> check_new_spectra >> [aggregate_classifications, skip_to_end]
aggregate_classifications >> [export_aggregated_csv, export_individual_csv]
export_aggregated_csv >> publish_aggregated_csv
export_individual_csv >> publish_individual_csv
[publish_aggregated_csv, publish_individual_csv] >> join_published
[join_published, skip_to_end] >> clean_up_classif_files
