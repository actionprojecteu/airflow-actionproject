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
from airflow.operators.python import BranchPythonOperator

#-----------------------
# custom Airflow imports
# ----------------------

from airflow_actionproject.operators.zooniverse    import ZooniverseExportOperator, ZooniverseDeltaOperator, ZooniverseTransformOperator
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.streetspectra import PreprocessClassifOperator, AggregateOperator, AggregateCSVExportOperator, IndividualCSVExportOperator
from airflow_actionproject.callables.streetspectra import check_new_subjects

# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ("developer@actionproject.eu",), # CAMBIAR AL VERDADERO EN PRODUCCION
    'email_on_failure': True,                       # CAMBIAR A True EN PRODUCCION
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



# ====================================================
# CLASSIFICATIONS AGGREGATION AND PUBLICATION WORKFLOW
# ====================================================

# Aqui hay que tener en cuenta que el exportado de Zooniverse es completo
# y que la BD de ACTION NO detecta duplicados
# Asi que hay que usar ventanas de clasificaciones subida
# Este "enventanado" debe ser lo primero que se haga tras la exportacion para evitar
# que los procesados posteriores sean largos.
# Este enventanado no se hace por variables de Airflow 
# sino en una tabla de la BD Sqlite interna


streetspectra_aggregate_dag = DAG(
    'streetspectra_aggregate_dag',
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
    conn_id     = "streetspectra-zooniverse",               # CAMBIAR AL conn_id DE PRODUCCION
    output_path = "/tmp/zooniverse/complete-{{ds}}.json",
    generate    = True, 
    wait        = True, 
    timeout     = 600,
    dag         = streetspectra_aggregate_dag,
)

# Produces an output file with only new classifications
# made since the last export
# This valid for any Zooniverse project
only_new_classifications = ZooniverseDeltaOperator(
    task_id       = "only_new_classifications",
    conn_id       = "streetspectra-db",
    input_path    = "/tmp/zooniverse/complete-{{ds}}.json",
    output_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    dag           = streetspectra_aggregate_dag,
)

# Transforms the new Zooniverse classifcations file in to a JSON
# file suitable to be loaded into the ACTION database as 'classifications'
# This is valid for any project that uses the ACTION database API
transform_classfications = ZooniverseTransformOperator(
    task_id      = "transform_classfications",
    input_path   = "/tmp/zooniverse/subset-{{ds}}.json",
    output_path  = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    project      = "street-spectra",
    dag          = streetspectra_aggregate_dag,
)

# Preprocess the transformed file into a StreetSpectra
# relational database, getting only the most relevant information
# This is valid only for StreetSpectra
preprocess_classifications = PreprocessClassifOperator(
    task_id    = "preprocess_classifications",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/zooniverse/transformed-subset-{{ds}}.json",
    dag        = streetspectra_aggregate_dag,
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
    dag           = streetspectra_aggregate_dag
)

# needed for branching
skip_to_end = DummyOperator(
    task_id    = "skip_to_end",
    dag        = streetspectra_aggregate_dag,
)


# Aggregates classifications into a single combined value 
# for every light source selected by Zooniverse users when classifying
# This is valid only for StreetSpectra
aggregate_classifications = AggregateOperator(
    task_id    = "aggregate_classifications",
    conn_id    = "streetspectra-db",
    dag        = streetspectra_aggregate_dag,
)

# Export aggregated classifications into CSV file format
# This is valid only for StreetSpectra
export_aggregated_csv = AggregateCSVExportOperator(
    task_id     = "export_aggregated_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/zooniverse/streetspectra-aggregated.csv",
    dag         = streetspectra_aggregate_dag,
)

# Export individuyal classifications into CSV format
# This is valid only for StreetSpectra
export_individual_csv = IndividualCSVExportOperator(
    task_id     = "export_individual_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/zooniverse/streetspectra-individual.csv",
    dag         = streetspectra_aggregate_dag,
)

# Publish the aggregated dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_aggregated_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_aggregated_csv",
    conn_id     = "streetspectra-zenodo",                   # CAMBIAR AL conn_id DE PRODUCCION
    title       = "Street Spectra aggregated classifications",
    file_path   = "/tmp/zooniverse/streetspectra-aggregated.csv",
    description = "CSV file containing aggregated classifications for light sources data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = [{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = streetspectra_aggregate_dag,
)

# Publish the individual dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_individual_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_individual_csv",
    conn_id     = "streetspectra-zenodo",               # CAMBIAR AL conn_id DE PRODUCCION
    title       = "Street Spectra individual classifications",
    file_path   = "/tmp/zooniverse/streetspectra-individual.csv",
    description = "CSV file containing individual classifications for subjects data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = [{'name': "Zamorano, Jaime"}, {'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = streetspectra_aggregate_dag,
)

# needed for parallel export+publish operations
join_published = DummyOperator(
    task_id    = "join_published",
    dag        = streetspectra_aggregate_dag,
)

# Clean up temporary files
clean_up_classif_files = BashOperator(
    task_id      = "clean_up_classif_files",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    bash_command = "rm /tmp/zooniverse/*.csv; rm /tmp/zooniverse/*-{{ds}}.json",
    dag          = streetspectra_aggregate_dag,
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
