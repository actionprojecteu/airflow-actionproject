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
from airflow.operators.python import ShortCircuitOperator, BranchPythonOperator
from airflow.operators.email  import EmailOperator


#-----------------------
# custom Airflow imports
# ----------------------

from airflow_actionproject.operators.action        import ActionDownloadFromVariableDateOperator
from airflow_actionproject.operators.streetspectra import ZooImportOperator
from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets
from airflow_actionproject.callables.action        import check_number_of_entries

# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ("developer@actionproject.eu","astrorafael@gmail.com"), # CAMBIAR AL VERDADERO EN PRODUCCION
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


# ===========================
# Zooniverse Feeding Workflow
# ===========================

streetspectra_feed_dag = DAG(
    'streetspectra_feed_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Zooniverse image feeding workflow',
    schedule_interval = '@daily',
    start_date        = days_ago(1),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----

manage_subject_sets = ShortCircuitOperator(
    task_id         = "manage_subject_sets",
    python_callable = zooniverse_manage_subject_sets,
    op_kwargs = {
        "conn_id"  : "streetspectra-zooniverse",                # CAMBIAR AL conn_id DE PRODUCCION
        "threshold": 75,    # 75% workflow completion status
    },
    dag           = streetspectra_feed_dag
)

check_enough_observations = BranchPythonOperator(
    task_id         = "check_enough_observations",
    python_callable = check_number_of_entries,
    op_kwargs = {
        "conn_id"       : "streetspectra-action-database",
        "start_date"    : "2019-09-01T00:00:00.00000Z",     # ESTA ES LA PRIMERA FECHA EN LA QUE HAY ALGO
        "n_entries"     : 100,                              # ESTO TIENE QUE CAMBIARSE A 100 PARA PRODUCCION
        "project"       : "street-spectra",
        "true_task_id"  : "download_from_action",
        "false_task_id" : "email_no_images",
        "obs_type"      : 'observation',
    },
    dag           = streetspectra_feed_dag
)

email_no_images = EmailOperator(
    task_id      = "email_no_images",
    to           = ("developer@actionproject.eu","astrorafael@gmail.com"),      # Cambiar al email verdadero en produccion
    subject      = "[StreetSpectra] Airflow warn: No ACTION images left",
    html_content = "No images left in ACTION database to create an new Zooniverse Subject Set.",
    dag          = streetspectra_feed_dag,
)

download_from_action = ActionDownloadFromVariableDateOperator(
    task_id        = "download_from_action",
    conn_id        = "streetspectra-action-database",
    output_path    = "/tmp/zooniverse/streetspectra/action-{{ds}}.json",
    variable_name  = "streetspectra_read_tstamp",
    n_entries      = 100,                                    # ESTO TIENE QUE CAMBIARSE A 100 PARA PRODUCCION
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = streetspectra_feed_dag,
)

upload_new_subject_set = ZooImportOperator(
    task_id         = "upload_new_subject_set",
    conn_id         = "streetspectra-zooniverse",           # CAMBIAR AL conn_id DE PRODUCCION
    input_path      = "/tmp/zooniverse/streetspectra/action-{{ds}}.json", 
    display_name    = "Subject Set {{ds}}",
    dag             = streetspectra_feed_dag,
)

# This needs to be configured:
# WARNING - section/key [smtp/smtp_user] not found in config
# See https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email

email_new_subject_set = EmailOperator(
    task_id      = "email_new_subject_set",
    to           = ("developer@actionproject.eu","astrorafael@gmail.com"),
    subject      = "[StreetSpectra] Airflow info: new Zooniverse Subject Set",
    html_content = "New Zooniverse Subject Set {{ds}} created.",
    dag          = streetspectra_feed_dag,
)

cleanup_action_obs_file = BashOperator(
    task_id      = "cleanup_action_obs_file",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    bash_command = "rm /tmp/zooniverse/streetspectra/*{{ds}}.json",
    dag          = streetspectra_feed_dag,
)

# -----------------
# Task dependencies
# -----------------

manage_subject_sets  >> check_enough_observations >> [download_from_action,  email_no_images]
download_from_action >> upload_new_subject_set >> email_new_subject_set
[email_new_subject_set, email_no_images] >> cleanup_action_obs_file
