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

## Airflow Connections & Variables

See the [StreetSpectra provisioning document](./StreetSpectra.md) for more details on this.


# Testing from the command line

1. Activate the virtual environment

2. Initialize Airflow DB
```bash
airflow db init
airflow dags list
airflow tasks list <dag id>
```

3. Setup connections

As per secion above. Use test connections whenever possible

4. Test each task individually and sequentially
```bash
airflow tasks test street_spectra fetch_observations 2020-01-01
airflow tasks test street_spectra transform_observations 2020-01-01
```
5. Test a complete dag
```bash
airflow dags test street_spectra 2020-01-01
```