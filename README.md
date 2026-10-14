# Prescriber Pipeline ETL

![Build](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.13-blue)
![Spark](https://img.shields.io/badge/spark-TBD-orange)
![Status](https://img.shields.io/badge/status-active-success)

> An end-to-end PySpark ETL pipeline that ingests US city reference data and Medicare Part D prescriber claims, transforms them, and publishes the results to Hive, PostgreSQL, local storage, and AWS S3.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation / Setup](#installation--setup)
- [Configuration](#configuration)
- [How to Run](#how-to-run)
- [Pipeline Stages](#pipeline-stages)
- [Data Schemas](#data-schemas)
- [Sample Output](#sample-output)
- [Outputs](#outputs)
- [Logging & Monitoring](#logging--monitoring)
- [Error Handling](#error-handling)
- [Testing / Validation](#testing--validation)
- [Troubleshooting](#troubleshooting)
- [License](#license)
- [Author](#author)
- [Acknowledgements](#acknowledgements)


---

## Overview

Prescriber Pipeline ETL processes two datasets: a US city reference dataset in Parquet format (about 28,338 rows) and Medicare Part D prescriber drug claims in CSV format (about 1,329,329 rows). The goal is to produce city-level aggregates and top-prescriber summaries that can be queried in Hive and PostgreSQL and archived in S3.

The pipeline runs as a set of stages orchestrated by a KornShell master script. Shell steps move data between the local filesystem, HDFS, and S3. A PySpark job running on YARN with 28 executors performs ingestion, validation, preprocessing, transformation, and persistence.

Each stage logs its progress, and the job validates row counts, sample records, and schemas before moving on. This makes failures easy to locate and re-runs predictable.

## Key Features

- Orchestrated end-to-end run through a single KornShell script
- Local-to-HDFS ingestion with automatic cleanup of stale output paths
- PySpark job on YARN with configurable executor count
- Built-in validation of row counts, top records, and schemas at each stage
- Standardisation of text fields to uppercase and derivation of new columns
- City-level aggregation and top-prescriber transformations
- Dual persistence: Hive (`prescpipeline` database) and PostgreSQL via JDBC
- Compressed and columnar outputs: JSON.bz2 for city data, Snappy ORC for the fact table
- Automated upload of outputs to AWS S3 with timestamped prefixes
- Per-script log files plus a consolidated pipeline log

## Architecture

```text
 ┌────────────────┐     ┌──────────────────┐     ┌─────────────────────────────┐
 │  Local FS      │     │  HDFS            │     │  Spark ETL (YARN, 28 exec)  │
 │                │     │                  │     │                             │
 │ staging/       │────▶│ PrescPipeline/   │────▶│ create_objects              │
 │  - city.parquet│     │  staging/        │     │ run_data_ingestion          │
 │  - presc.csv   │     │                  │     │ validations                 │
 │                │     │ (stale output    │     │ preprocessing               │
 │                │     │  paths deleted)  │     │ transform                   │
 └────────────────┘     └──────────────────┘     │ extract                     │
                                                 │ persist                     │
                                                 └──────┬──────────────┬───────┘
                                                        │              │
                                                        ▼              ▼
                                       ┌──────────────────────┐  ┌──────────────┐
                                       │ Hive                 │  │ PostgreSQL   │
                                       │  prescpipeline       │  │ (via JDBC)   │
                                       │   - city_df          │  │              │
                                       │   - fact_df          │  │              │
                                       └──────────────────────┘  └──────────────┘
                                                        │
                                                        ▼
 ┌────────────────┐     ┌──────────────────┐     ┌─────────────────────────────┐
 │  Local output/ │◀────│  HDFS output/    │     │  AWS S3                     │
 │  dimension_city│     │  dimension_city  │────▶│  presc-pipeline-pro/        │
 │  presc         │     │  presc           │     │  <table>/<timestamp>/       │
 └────────────────┘     └──────────────────┘     └─────────────────────────────┘
```

## Tech Stack

| Component        | Purpose                                              |
|------------------|------------------------------------------------------|
| Python           | Pipeline code         |
| PySpark          | Distributed ETL: ingestion, transformation, output   |
| Apache Spark     | Execution engine running on YARN                     |
| Apache Hadoop    | HDFS storage and YARN resource management            |
| Apache Hive      | Warehouse tables in the `prescpipeline` database     |
| PostgreSQL       | Relational persistence target via JDBC               |
| PostgreSQL JDBC  | JDBC driver (`lib/postgresql-42.7.7.jar`)            |
| AWS CLI          | Uploading outputs to S3                              |
| KornShell (ksh)  | Orchestration and HDFS/S3 file operations            |

## Project Structure

```text
prescriber_pipeline/
├── .gitignore
├── README.md
├── .idea/
└── src/
    └── main/
        └── python/
            ├── bin/
            │   ├── __pycache__/
            │   ├── .delete_hdfs_output_path.ksh.swp
            │   ├── copy_files_hdfs_to_local.ksh
            │   ├── copy_files_to_hdfs.ksh
            │   ├── copy_files_to_s3.ksh
            │   ├── create_objects.py
            │   ├── delete_hdfs_output_path.ksh
            │   ├── get_all_variables.py
            │   ├── presc_run_data_extract.py
            │   ├── presc_run_data_persist.py
            │   ├── presc_run_data_transform.py
            │   ├── run_data_ingestion.py
            │   ├── run_presc_data_preprocessing.py
            │   ├── run_presc_pipeline.py
            │   ├── run_presc_pipeline_project.ksh
            │   ├── udfs.py
            │   └── validations.py
            ├── config/
            ├── lib/
            │   └── postgresql-42.7.7.jar
            ├── logs/
            ├── outcome/
            │   └── presc/
            ├── output/
            │   ├── dimension_city/
            │   └── presc/
            ├── sql/
            ├── staging/
            │   ├── dimension_city/
            │   │   └── us_cities_dimension.parquet
            │   └── fact/
            │       └── USA_Presc_Medicare_Data_12021.csv
            └── util/
                └── logging_to_file.conf
```

## Prerequisites

| Software                   | Notes                                                                 |
|----------------------------|-----------------------------------------------------------------------|
| Linux                      | Scripts use ksh and Linux paths. Windows users should work via SSH.   |
| Python                     | Matches the bytecode cache in `bin/__pycache__/`.                     |
| Java (JDK)                 | Required by Spark. Must match the cluster's Spark build.              |
| Apache Spark               | Must support `--master yarn`.                                         |
| Hadoop (HDFS + YARN)       | A reachable cluster with `hdfs` on the PATH of the executing user.    |
| Apache Hive                | Metastore reachable from Spark, with `prescpipeline` database access. |
| PostgreSQL                 | Reachable from the cluster. Credentials must have write access.       |
| AWS CLI                    | Configured with credentials that can write to the target bucket.      |
| KornShell (`ksh`)          | Required to run the `.ksh` orchestration scripts.                     |
| Git                        | To clone the repository.                                              |

## Installation / Setup

**1. Clone the repository**

```bash
git clone <repository-url> prescriber_pipeline
cd prescriber_pipeline
```

**2. Create and activate a virtual environment**

```bash
python3.13 -m venv .venv
source .venv/bin/activate
```

**3. Install Python dependencies**

```bash
pip install --upgrade pip
pip install pyspark
```

If your cluster provides its own PySpark, install the matching version instead.

**4. Make the shell scripts executable**

```bash
chmod +x src/main/python/bin/*.ksh
```

**5. Verify the PostgreSQL JDBC driver is present**

```bash
ls -l src/main/python/lib/postgresql-42.7.7.jar
```

**6. Configure AWS CLI credentials**

```bash
aws configure
aws s3 ls s3://presc-pipeline-pro/
```

**7. Stage the source files**

Place the input files at the local paths below. The copy script expects them here.

```text
src/main/python/staging/dimension_city/us_cities_dimension.parquet
src/main/python/staging/fact/USA_Presc_Medicare_Data_12021.csv
```

**8. Review the configuration**

Edit `src/main/python/bin/get_all_variables.py` as described in [Configuration](#configuration). Do this before the first run, particularly the database credentials.

## Configuration

All runtime settings are defined in `src/main/python/bin/get_all_variables.py`. Other modules import these values.

| Variable           | Default Value                                      | Description                                                                                     |
|--------------------|----------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `envn`             | `PROD`                                             | Environment label, set via `os.environ`.                                                        |
| `header`           | `True`                                             | Passed to Spark CSV/Parquet readers to indicate a header row. Stored as the string `'True'`.    |
| `inferSchema`      | `True`                                             | Passed to Spark readers to infer column types. Stored as the string `'True'`.                   |
| `username`         | `sparkuser1`                                       | Database user for JDBC writes to PostgreSQL.                                                    |
| `password`         | *(hardcoded in source; do not copy)*              | Database password for JDBC writes. See the security note below.                                 |
| `appName`          | `USA Prescriber research report`                   | Spark application name shown in the YARN UI.                                                    |
| `staging_dim_city` | `hdfs:///PrescPipeline/staging/dimension_city`     | HDFS path to the city input data.                                                               |
| `staging_fact`     | `hdfs:///PrescPipeline/staging/fact`               | HDFS path to the prescriber claims input data.                                                  |
| `output_city`      | `hdfs:///PrescPipeline/output/dimension_city`      | HDFS path where the city output is written before being copied to local and S3.                |
| `output_fact`      | `hdfs:///PrescPipeline/output/presc`               | HDFS path where the prescriber output is written before being copied to local and S3.          |

**Security note:** `username` and `password` are currently hardcoded in `get_all_variables.py`. Move them to environment variables or a secrets manager before sharing or committing the code.

```python
# Recommended pattern
import os

username = os.environ["PG_USERNAME"]
password = os.environ["PG_PASSWORD"]
```

**Notes:**

- `envn`, `header`, and `inferSchema` are set and read back within the same file, so changing them requires editing the file.
- The `staging_*` and `output_*` paths point to HDFS. Commented-out local paths (`../staging/...`) remain in the file for local development.

## How to Run

### Full pipeline via the master script

From the `bin/` directory, run the orchestration script:

```bash
cd src/main/python/bin
./run_presc_pipeline_project.ksh
```

The master script runs these steps in order:

1. `copy_files_to_hdfs.ksh` copies source files from local staging to HDFS.
2. `delete_hdfs_output_path.ksh` removes stale HDFS output directories.
3. `run_presc_pipeline.py` runs the Spark job via `spark-submit` on YARN.
4. `copy_files_hdfs_to_local.ksh` copies HDFS output to the local `output/` folder.
5. `copy_files_to_s3.ksh` uploads the output to S3.

Progress is written to `logs/presc_run_pipeline.log`.

### Individual Spark job via spark-submit

To run only the Spark job, for example after fixing a bug in a transformation, execute it directly from `bin/`:

```bash
spark-submit \
  --master yarn \
  --num-executors 28 \
  --jars ../lib/postgresql-42.7.7.jar \
  run_presc_pipeline.py
```

Make sure the HDFS input paths exist and the HDFS output paths have been removed before running. Otherwise, the job fails with the error described in [Error Handling](#error-handling).

## Pipeline Stages

### Spark ETL stages

| Stage                          | Script                            | Description                                                                                  |
|--------------------------------|-----------------------------------|----------------------------------------------------------------------------------------------|
| `create_objects`               | `create_objects.py`               | Creates the Spark session and validates that it is working.                                  |
| `run_data_ingestion`           | `run_data_ingestion.py`           | Reads the city Parquet file and the prescriber CSV file from HDFS.                           |
| `validations`                  | `validations.py`                  | Checks row counts, top records, and schemas of the ingested data.                            |
| `run_presc_data_preprocessing` | `run_presc_data_preprocessing.py` | Standardises text to uppercase and derives new columns.                                      |
| `presc_run_data_transform`     | `presc_run_data_transform.py`     | Produces city-level aggregates and the top-prescriber summary.                               |
| `presc_run_data_extract`       | `presc_run_data_extract.py`       | Writes city output as JSON.bz2 and the fact output as Snappy ORC to HDFS.                    |
| `presc_run_data_persist`       | `presc_run_data_persist.py`       | Writes results to Hive (`prescpipeline.city_df`, `prescpipeline.fact_df`) and PostgreSQL.    |

### Shell orchestration steps

| Step                      | Script                           | Description                                                              |
|---------------------------|----------------------------------|--------------------------------------------------------------------------|
| Copy to HDFS              | `copy_files_to_hdfs.ksh`         | Copies staged input files from local disk to HDFS.                       |
| Delete HDFS outputs       | `delete_hdfs_output_path.ksh`    | Deletes `output/dimension_city` and `output/presc` before extraction.    |
| Copy HDFS to local        | `copy_files_hdfs_to_local.ksh`   | Copies HDFS output into the local `output/` folder.                      |
| Copy to S3                | `copy_files_to_s3.ksh`           | Uploads local output files to `s3://presc-pipeline-pro/`.                |
| Orchestrator              | `run_presc_pipeline_project.ksh` | Runs all steps above in sequence.                                        |

`udfs.py` contains user-defined functions used by the preprocessing and transformation stages. `run_presc_pipeline.py` is the Spark job entry point that runs the Spark stages in order.

## Data Schemas

Column types below are inferred from the pipeline's use of `inferSchema=True`. Confirm them with `df.printSchema()` against your input files before relying on them.

### a) `df_city_sel`

| Column       | Type    | Description                                              |
|--------------|---------|----------------------------------------------------------|
| `city`       | string  | City name, uppercased during preprocessing.              |
| `state_id`   | string  | Two-letter state abbreviation.                           |
| `state_name` | string  | Full state name, uppercased during preprocessing.        |
| `county_name`| string  | County name, uppercased during preprocessing.            |
| `population` | integer | Estimated city population.                               |
| `zips`       | string  | ZIP codes associated with the city.                      |

### b) `df_fact_sel`

| Column             | Type    | Description                                                     |
|--------------------|---------|-----------------------------------------------------------------|
| `presc_id`         | string  | Prescriber identifier.                                          |
| `presc_city`       | string  | Prescriber city, uppercased during preprocessing.               |
| `presc_state`      | string  | Prescriber state, uppercased during preprocessing.              |
| `presc_spclt`      | string  | Prescriber specialty.                                           |
| `years_of_exp`     | integer | Years of experience reported for the prescriber.                |
| `drug_name`        | string  | Name of the prescribed drug.                                    |
| `trx_cnt`          | integer | Number of prescription transactions (claims).                   |
| `total_day_supply` | integer | Total days of medication supplied across claims.                |
| `total_drug_cost`  | double  | Total cost of the drug across claims.                           |
| `country_name`     | string  | Country name, derived during preprocessing.                     |
| `presc_fullname`   | string  | Derived full prescriber name.                                   |

### c) `transformed_data`

| Column               | Type    | Description                                                  |
|----------------------|---------|--------------------------------------------------------------|
| `City Name`          | string  | City name.                                                   |
| `State Name`         | string  | State name.                                                  |
| `County Name`        | string  | County name.                                                 |
| `City Population`    | integer | Population of the city.                                      |
| `Number of Zips`     | integer | Count of ZIP codes for the city.                             |
| `Prescriber Counts`  | integer | Number of distinct prescribers in the city.                  |
| `Total Trx counts`   | integer | Total prescription transactions in the city.                 |

## Sample Output

A single row from `transformed_data`:

```json
{
  "City Name": "ANAHEIM",
  "State Name": "CALIFORNIA",
  "County Name": "ORANGE",
  "City Population": 350365,
  "Number of Zips": 16,
  "Prescriber Counts": 498,
  "Total Trx counts": 82010
}
```

## Outputs

| Destination                      | Location                                                 | Format         | Contents                                         |
|----------------------------------|----------------------------------------------------------|----------------|--------------------------------------------------|
| Hive table                       | `prescpipeline.city_df`                                  | Hive table     | City dimension data.                             |
| Hive table                       | `prescpipeline.fact_df`                                  | Hive table     | Prescriber fact data.                            |
| PostgreSQL                       | Tables defined in `presc_run_data_persist.py`            | JDBC tables    | City and prescriber results.                     |
| Local folder                     | `src/main/python/output/dimension_city/`                 | JSON.bz2       | City output (`part-*.json.bz2`).                 |
| Local folder                     | `src/main/python/output/presc/`                          | Snappy ORC     | Prescriber output (`part-*.snappy.orc`).         |
| AWS S3 (city)                    | `s3://presc-pipeline-pro/dimension_city/<timestamp>/`    | JSON.bz2       | City output, one prefix per run.                 |
| AWS S3 (prescriber)              | `s3://presc-pipeline-pro/presc/<timestamp>/`             | Snappy ORC     | Prescriber output, one prefix per run.           |

The S3 timestamp uses the format `YYYY-MM-DD-HH-MM-SS`, for example `2026-08-16-17-54-34`.

## Logging & Monitoring

Logs are written to `src/main/python/logs/`.

**Per-script logs** are named `<script>_<YYYY-MM-DD_HH_MM_SS>.log`. Each run creates new files. Examples:

```text
copy_files_hdfs_to_local.ksh_2026-08-16_17_54_30.log
copy_files_to_s3.ksh_2026-08-16_17_54_34.log
delete_hdfs_output_paths_2026-08-16_17_53_18.log
```

Each file records the start and end time of its script using the `date` format, for example `Sun Aug 16 17:54:30 UTC 2026`. The S3 log also records each file uploaded and its upload speed.

**Consolidated log:** `presc_run_pipeline.log` contains the combined output of the master script and the Spark job. Check this file first when diagnosing a failed run.

**Logging configuration:** `util/logging_to_file.conf` holds the logging configuration used by the Python modules.

## Error Handling

### Known issue: `LOCATION_ALREADY_EXISTS` (Hive)

**Symptom:** The Spark persist stage fails with a `LOCATION_ALREADY_EXISTS` error when it tries to create a Hive table at a location that already contains data from a previous run.

**Cause:** The Hive table location still points to an HDFS directory from an earlier run, and the job tries to create it again.

**Resolution:** Do one of the following before re-running:

1. Drop the affected Hive table (`prescpipeline.city_df` or `prescpipeline.fact_df`):

```sql
   DROP TABLE IF EXISTS prescpipeline.city_df;
   DROP TABLE IF EXISTS prescpipeline.fact_df;
```

2. Delete the conflicting HDFS path:

```bash
   hdfs dfs -rm -r -f /PrescPipeline/output/dimension_city
   hdfs dfs -rm -r -f /PrescPipeline/output/presc
```

The master script already deletes the HDFS output paths in step 2 of the run. This error usually appears when the Spark job is run on its own, or when Hive still holds a reference to the old location.

### General approach

- Read `presc_run_pipeline.log` from the start of the failing run.
- Identify the failing stage from the log timestamps.
- Fix the root cause, clean up any partial outputs, and re-run. Re-running the full pipeline is safe because it cleans HDFS output paths first.

## Testing / Validation

The pipeline runs built-in validations after each stage, implemented in `validations.py`:

| Check          | What it verifies                                                                 |
|----------------|----------------------------------------------------------------------------------|
| Row count      | The number of rows read matches the expected count (about 28,338 for cities and about 1,329,329 for claims). |
| Top records    | A sample of the top records is displayed so the data can be inspected visually.  |
| Schema         | Column names and types match the expected schema for the stage.                  |

A failed validation stops the job so that bad data does not reach Hive, PostgreSQL, or S3.

## Troubleshooting

| Issue                                                        | Likely cause                                              | Fix                                                                                                      |
|--------------------------------------------------------------|-----------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `Permission denied` when running `.ksh` scripts              | Scripts are not executable.                               | Run `chmod +x src/main/python/bin/*.ksh`.                                                                |
| `/bin/ksh^M: bad interpreter`                                | Scripts were saved with Windows (CRLF) line endings.      | Convert with `dos2unix` or `sed -i 's/\r$//' <file>`. Configure Git to use LF line endings.              |
| `LOCATION_ALREADY_EXISTS` on Hive write                      | Hive table or HDFS path left over from a previous run.    | See [Known issue](#known-issue-location_already_exists-hive).                                            |
| `ClassNotFoundException: org.postgresql.Driver`              | JDBC driver not on the Spark classpath.                   | Confirm `--jars ../lib/postgresql-42.7.7.jar` is passed to `spark-submit`.                              |
| `Input path does not exist` on HDFS                          | Staging files were not copied to HDFS.                    | Run `copy_files_to_hdfs.ksh` or check the paths in `get_all_variables.py`.                               |
| S3 upload fails with credential errors                       | AWS CLI is not configured for the executing user.         | Run `aws configure` as that user and check access with `aws s3 ls s3://presc-pipeline-pro/`.            |
| YARN job stays in `ACCEPTED` state                           | Cluster does not have resources for 28 executors.         | Reduce `--num-executors` or wait for resources to free up.                                               |
| PostgreSQL authentication failure                            | Credentials in `get_all_variables.py` are wrong or expired. | Update credentials, preferably via environment variables (see [Configuration](#configuration)).         |
| Output directory missing after the run                       | Extraction failed before writing.                         | Check `presc_run_pipeline.log` for errors in `presc_run_data_extract`.                                   |


## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Author

**Hosea Mutwiri Kinyua**
Developed: 16-08-2026

## Acknowledgements

Thanks to my instruct instructor **Sibaram Kumar**

Sibaram Kumar is a CCA Spark and Hadoop Developer certified professional with over 13 years of experience in data engineering, including data warehousing, data analytics, data modelling, reporting, and database design and development. He holds certifications in AWS and Oracle SQL/PL/SQL, and enjoys teaching PySpark, Python, AWS, and Oracle to students.


