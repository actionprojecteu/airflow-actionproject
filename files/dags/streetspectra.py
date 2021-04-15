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
from airflow_actionproject.operators.zooniverse    import ZooniverseExportOperator
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.action        import ActionDownloadOperator, ActionUploadOperator
from airflow_actionproject.operators.streetspectra import EC5TransformOperator, ZooniverseImportOperator
from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets

from airflow_actionproject.operators import SQL_STREETSPECTRA_SCHEMA

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

dag1 = DAG(
    'street-spectra',
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
    conn_id      = "epicollect5",
    start_date   = "{{ds}}",
    end_date     = "{{next_ds}}",
    output_path  = "/tmp/ec5/street-spectra/{{ds}}.json",
    dag          = dag1,
)

transform_ec5_observations = EC5TransformOperator(
    task_id      = "transform_ec5_observations",
    input_path   = "/tmp/ec5/street-spectra/{{ds}}.json",
    output_path  = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
    dag          = dag1,
)

# Esto hay que editarlo cuando el API de ACTION PROJECT esté listo
if True:
    load_ec5_observations = DummyOperator(
        task_id = "load_ec5_observations",
        dag     = dag1,
    )
else:
    load_ec5_classfications = ActionUploadOperator(
       task_id    = "load_ec5_observations",
       conn_id    = "action-database",
       input_path = "/tmp/ec5/street-spectra/transformed-{{ds}}.json",
       dag        = dag1,
    )

export_ec5_observations >> transform_ec5_observations >> load_ec5_observations

# ===========================
# Zooniverse Feeding Workflow
# ===========================

dag2 = DAG(
    'zooniverse',
    default_args      = default_args,
    description       = 'Zooniverse feeding workflow',
    schedule_interval = '@daily',
    start_date        = days_ago(2),
    tags              = ['ACTION PROJECT'],
)


manage_subject_sets = ShortCircuitOperator(
    task_id         = "manage_subject_sets",
    python_callable = zooniverse_manage_subject_sets,
    op_kwargs = {
        "conn_id"  : "zooniverse-streetspectra-test",
        "threshold": 75,
    },
    dag           = dag2
)

if True:
    def _print_context(**context):
        print(context)

    download_from_action = PythonOperator(
        task_id         = "download_from_action",
        python_callable =_print_context,
        dag = dag2,
    )

    upload_new_subject_set = PythonOperator(
        task_id         = "upload_new_subject_set",
        python_callable =_print_context,
        dag = dag2,
    )

else:
    # AQUI HAY QUE VER LO DE LAS FECHAS, QUE HAY QUE COGERLAS DE VARIABLES, EN LUGAR DEL PERIODO DE EJECUCION
    download_from_action = ActionDownloadOperator(
       task_id        = "download_from_action",
       conn_id        = "action-database",
       output_path    = "/tmp/zooniverse/streetspectra/action-{{ds}}.json",
       start_datetime = "{{ds}}",
       end_datetime   = "{{next_ds}}",
       project        = "street-spectra", 
       obs_type       = "observation",
       dag            = dag2,
    )

    upload_new_subject_set = ZooniverseImportOperator(
        task_id         = "upload_new_subject_set",
        input_path      = "/tmp/zooniverse/streetspectra/action-{{ds}}.json", 
        display_name    = "Subject Set {{ds}}",
        dag = dag2,
    )

email_team = EmailOperator(
    task_id      = "email_team",
    to           = ["astrorafael@gmail.com","rafael08@ucm.es"],
    subject      = "New StreetSpectra Subject Set being uploaded to Zooniverse",
    html_content = "Subject Set {{ds}} being uploaded."
)
manage_subject_sets >> email_team >> download_from_action >> upload_new_subject_set


# ============================
# CLASSIFICATIONS ETL WORKFLOW
# ============================

# Aqui hay que tener en cuenta que el exportado de Zooniverse es completo
# y que la BD de ACTION PROJECTO detecta dupñlicados
# Asi que hay que usar variables de la ventana de clasificaciones subida
# Este "enventanado" debe ser lo primero que se haga tras la exportacion para evitar
# que los procesados posteriores sean largos

dag3 = DAG(
    'classifications',
    default_args      = default_args,
    description       = 'Testing Zooniverse classification workflows',
    schedule_interval = '@monthly',
    start_date        = days_ago(2),
    tags              = ['ACTION PROJECT'],
)

export_classifications = ZooniverseExportOperator(
    task_id     = "export_classifications",
    conn_id     = "zooniverse-streetspectra-test",
    output_path = "/tmp/zooniverse/{{ds}}.json",
    generate    = False, 
    wait        = True, 
    timeout     = 120,
    dag         = dag3,
)

transform_classfications = DummyOperator(task_id="transform_classfications")

load_classfications = DummyOperator(task_id="load_classfications")

export_classifications >> transform_classfications >> load_classfications

################### TESTING ZENODO
dag_zen = DAG(
    'zenodo',
    default_args      = default_args,
    description       = 'Testing Zenodo publication workflows',
    schedule_interval = '@monthly',
    start_date        = days_ago(2),
    tags              = ['ACTION PROJECT'],
)

publish_to_zenodo = ZenodoPublishDatasetOperator(
    task_id     = "publish_to_zenodo",
    conn_id     = "zenodo-sandbox",
    title       = "Prueba 15",
    file_path   = "example.txt",
    description = "Testing Prueba 15",
    version     = '21.05',
    creators    = [{'name': "Gonzalez, Rafael"}],
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project"}],
    dag         = dag_zen,
)

# Example of creating a task that calls an sql command from an external file.
create_temp_database = SqliteOperator(
    task_id='create_table_sqlite_external_file',
    sqlite_conn_id='streetspectra-temp-db',
    sql=SQL_STREETSPECTRA_SCHEMA + ' ', # This is a hack for Jinja2 template not to raise error
    dag=dag_zen,
)