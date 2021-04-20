# airflow-actionproject
Airflow components for [ACTION](https://actionproject.eu/) project pilots.

# Components description

The `airflow_actionproject` package includes three sub-packages:

* `hooks` with custom hooks
* `operaqtors` with custom operators
* `callables` with custom callables for `PythonOperators` and derived, like `ShortCircuitOperator`.

***NOTE:*** We prefer to include these callables inside the package rather than cluttering the main DAG definition file with auxiliar functions.

## Hooks

* Module `action`. Hook to upload/download observations from the ACTION database. Handles paging internally.
* Module `epicollect5`. Hook to get entries from Epicollect 5 server. Handles paging internally.
* Module `zooniverse`. Hook to manage Zooniverse subjects and subject sets, export classifications and get workflow progress summaries.
* Module `zenodo`. Hook to publish a digital object to Zenodo. Handles paging internally.
* Module `streetspectra`. Personalized hooks for any of the other generic hooks. So far, it contains personalization to create StreetSpectra subjects with personalized metadata in Zooniverse.

## Operators

* Module `action`. Operators to upload/download observations from the ACTION database to/from files using the `ActionDatabaseHook`.
* Module `epicollect5`. Operator to export entries from Epicollect 5 server to a file using the `EpiCollect5Hook`.
* Module `zooniverse`. Operators to export Classifications from Zooniverse using the `ZooniverseHook`.
* Module `zenodo`. Operator to publish a digital object to Zenodo from a file path using the `ZenodoHook`.
* Module `streetspectra`. Operators to transform StreetSpectra data among Epicollect V, Zooniverse and the ACTION database.

## Sensors

No custom sensor has been developed for the time being.

## Callables

* Module `zooniverse`. Callable to manage deactivation of Zooniverse subject sets and loading of a new subje3ct set. To be used with `ShortCircuitOperator`

*Example:*

```python
	manage_subject_sets = ShortCircuitOperator(
	    task_id         = "manage_subject_sets",
	    python_callable = zooniverse_manage_subject_sets,
	    op_kwargs = {
	        "conn_id"  : "zooniverse-streetspectra",
	        "threshold": 75,
	    },
	    dag           = dag2
	)
```
## Airflow Connections

### Connection to Epicollect 5

```bash
airflow connections add \
--conn-type https \
--conn-host five.epicollect.net \
--conn-port 443 \
--conn-schema "<your project slug>" \
--conn-extra '{"page_size": 100}' \
--conn-description "Connection to Epicollect V mobile gathering platform" \
epicollect5-<your project>
```
* The `schema` field contains the specific project slug (URL fragment) for the given Epicollect 5 URL.
* The `extra` field contains the HTTP page size fro downloads.

### Connection to Zooniverse

```bash
airflow connections add \
--conn-type https \
--conn-host www.zooniverse.org \
--conn-port 443 \
--conn-login "<zooniverse login>" \
--conn-password "<zooniverse password>" \
--conn-schema "<your project slug>" \
--conn-description "Connection to Zooniverse citizen science web portal" \
zooniverse-<your project>
```

* The `schema` field contains the specific project slug (URL fragment) for the given Zooniverse project (not counting the login slug).

### Connection to ACTION Database

```bash
airflow connections add \
--conn-type https \
--conn-host api.actionproject.eu \
--conn-port 443 \
--conn-password "<API key>" \
--conn-schema "observations" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to ACTION observations database" \
action-database-<your project>
```

* The `password` field contains the ACTION API Key.
* The `schema` field contains "observations" as fixed slug for the time being.
* The `extra` field contains the HTTP page size for downloads and the transactions per second rate limit.


### Connection to Zenodo

```bash
airflow connections add \
--conn-type https \
--conn-host zenodo.org \
--conn-port 443 \
--conn-password "<API key>" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to Zenodo" \
zenodo
```

* The `password` field contains the Zenodo API Key.
* The `extra` field contains the HTTP page size and the transactions per second rate limit for search requests.


### Temporary database to deduplicate Zooniverse exports and other things

```bash
airflow connections add \
--conn-type sqlite \
--conn-host  "/home/rafa/airflow/extra_dbase/street_spectra.db" \
--conn-description "Connection to Temporary SQLite Database for StreetSpectra" \
streetspectra-temp-db
```
## Airflow Variables

This variable keeps track of the next observation of the database to be read

```bash
airflow variables set action_ss_read_tstamp "2000-01-01T00:00:00.000Z"
```

This variable keeps track of the new classifications window.
The 'before' timestamp should be excluded, the 'after' timestamp should be included in this window


```bash
airflow variables set action_ss_classifications_window "{'before': null, 'after': null}"
```

# Testing from the command line

1. Activate the virtual environment

2. Initialize Airflow DB
```bash
airflow db init
airflow dags list
airflow tasks list <dag id>
```

3. Setup connections

```bash
airflow connections add \
--conn-type https \
--conn-host five.epicollect.net \
--conn-port 443 \
--conn-schema "action-street-spectra" \
--conn-extra '{"page_size": 10}' \
--conn-description "Connection to Epicollect V mobile gathering platform" \
epicollect5
```

```bash
airflow connections add \
--conn-type https \
--conn-host www.zooniverse.org \
--conn-port 443 \
--conn-login "<my login>" \
--conn-password "<my password>" \
--conn-schema "street-spectra-test-1" \
--conn-description "Connection to Zooniverse citizen science web portal" \
zooniverse-streetspectra-test
```

```bash
airflow connections add \
--conn-type https \
--conn-host api.actionproject.eu \
--conn-port 443 \
--conn-password "<API key>" \
--conn-schema "observations" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to ACTION observations database" \
action-database
```

```bash
airflow connections add \
--conn-type https \
--conn-host sandbox.zenodo.org \
--conn-port 443 \
--conn-password "<API key>" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to Zenodo sandbox environment" \
zenodo-sandbox
```

```bash
airflow connections add \
--conn-type sqlite \
--conn-host  "/home/rafa/airflow/extra_dbase/street_spectra.db" \
--conn-description "Connection to Temporary SQLite Database for StreetSpectra" \
streetspectra-temp-db
```

4. Test each task individually and sequentially
```bash
airflow tasks test street_spectra fetch_observations 2020-01-01
airflow tasks test street_spectra transform_observations 2020-01-01
```
5. Test a complete dag
```bash
airflow dags test street_spectra 2020-01-01
```