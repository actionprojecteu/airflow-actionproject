# Street Spectra Airflow provisioning

This file describes the needed provisioning actions for Street Spectra

***Prerequisite***: Python package `airflow-actionproject` installed 
in the same virtual envoironment where `apache-airflow` is installed.

There are three workflows (DAGs) to execute for StreetSpectra:

* The [observations extraction workflow](#observations-extraction-workflow), `streetspectra_collect_dag` that extracts observations from the Observing Platform (Epicollect 5), transforms them and loads them into the ACTION database. This is executed monthly.

* The [Zooniverse feeding workflow](#zooniverse-feeding-workflow) `streetspectra_zoo_import_dag` that creates and loads new Zooniverse subject sets from the ACTION observations as the previous classifications are being completed. This is executed on a daily basis.

* The [aggregation and publication workflow](#aggregation-and-publication-workflow) `streetspectra_zoo_export_dag` , where all the individual classifications made in Zooniverse  are aggregated (per light source in image) and published into a Open Data scence portal (Zenodo). This is executed monthly.


# Observations extraction workflow

The following items must be provisioned:

1. Airflow connection to the Epicollect 5 StreetSpectra project:

```bash
airflow connections add \
--conn-type https \
--conn-host five.epicollect.net \
--conn-port 443 \
--conn-schema "action-street-spectra" \
--conn-extra '{"page_size": 100}' \
--conn-description "Connection to Epicollect V StreetSpectra" \
streetspectra-epicollect5
```

* The `schema` field contains the specific project slug (URL fragment) for the given Epicollect 5 URL.
* The `extra` field contains the HTTP page size for downloads.


```bash
airflow connections add \
--conn-type https \
--conn-host five.epicollect.net \
--conn-port 443 \
--conn-schema "street-spectra" \
--conn-extra '{"page_size": 10}' \
--conn-description "Connection to Epicollect V old StreetSpectra project" \
oldspectra-epicollect5
```

2. Airflow connection to the ACTION database:


```bash
airflow connections add \
--conn-type https \
--conn-host api.actionproject.eu \
--conn-port 443 \
--conn-password "<ACTION DBase API key>" \
--conn-schema "observations" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to ACTION observations database" \
streetspectra-action-database
```

* The `password` field contains the ACTION API Key.
* The `schema` field contains "observations" as fixed slug for the time being.
* The `extra` field contains the HTTP page size for downloads and the transactions per second rate limit.

# Zooniverse feeding workflow

The following items must be provisioned:

1. Airflow connection to the Zooniverse StreetSpectra project:

```bash
airflow connections add \
--conn-type https \
--conn-host www.zooniverse.org \
--conn-port 443 \
--conn-login "<your zooniverse login>" \
--conn-password "<your zooniverse password>" \
--conn-schema "<our street spectra slug>" \
--conn-description "Connection to Zooniverse citizen science web portal" \
streetspectra-zooniverse
```

The `schema` field contains the specific project slug (URL fragment) for the given Zooniverse project (not counting the login slug).

Optionally, we can have an additional tetsing project, whose connection must also be created:

```bash
airflow connections add \
--conn-type https \
--conn-host www.zooniverse.org \
--conn-port 443 \
--conn-login "<another zooniverse login>" \
--conn-password "<another zooniverse password>" \
--conn-schema "street-spectra-test-2" \
--conn-description "Connection to Zooniverse citizen science web portal (test)" \
streetspectra-zooniverse-test
```

2. An Airflow variable

This variable keeps track of the next observation of the ACTION database to be read.
It has to be initialized to a past value before StreetSpectra project was launched.

```bash
airflow variables set streetspectra_read_tstamp "2000-01-01T00:00:00.000Z"
```
3. An operator email address

As part of this workflow, some emails may be sent. You need to have an email account ready 
and also configure Airflow's SMTP capabilities. See Stack's overflow [How to set up airflow for sending emails.](https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email)

# Aggregation and publication workflow

In addition to the items mentioned in the [Zooniverse feeding workflow](#zooniverse-feeding-workflow), the following items must be provisioned:

1.  SQlite classification database creation


Using the `actiontool` command installed as par of the `airflow-actionproject` Python package.


Example:

```bash
actiontool database install streetspectra /home/rafa/airflow/extra
```

installs the `streetspectra.db` SQLite database inside the `/home/rafa/airflow/extra` directory, creating all the intermediate directories if necessary.

2. Airflow connection to SQlite classification database:


```bash
airflow connections add \
--conn-type sqlite \
--conn-host  "/home/rafa/airflow/extra/streetspectra.db" \
--conn-description "Connection to StreetSpectra SQLite database" \
streetspectra-db
```

Where `conn-host` specifies the full, absolute path to the SQLite database.

3. Airflow connection to Zenodo:

```bash
airflow connections add \
--conn-type https \
--conn-host sandbox.zenodo.org \
--conn-port 443 \
--conn-password "<Zenodo API key>" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to Zenodo environment" \
streetspectra-zenodo
```

* The `password` field contains the Zenodo API Key.
* The `extra` field contains the HTTP page size and the transactions per second rate limit for search requests.

For testing purposes, an extra connection to Zenodo sandbox environment may be created:

```bash
airflow connections add \
--conn-type https \
--conn-host sandbox.zenodo.org \
--conn-port 443 \
--conn-password "<Zenodo sandbox API key>" \
--conn-extra '{"page_size": 100, "tps": 2}' \
--conn-description "Connection to Zenodo sandbox environment" \
streetspectra-zenodo-sandbox
```
