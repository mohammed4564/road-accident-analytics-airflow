# Road Accident Analytics using Apache Airflow

An end-to-end data engineering learning project using **Apache Airflow, Docker, Python, Pandas, and Streamlit** to process road accident data and generate analytics summaries.

## Project Overview

The project demonstrates how to build a data pipeline that validates a CSV dataset, processes accident records, and generates summary reports for analysis.

## Architecture

```text
Road Accident CSV
       |
       v
Input Validation
       |
       v
Data Processing & Aggregation
       |
       v
Analytics CSV Reports
       |
       v
Streamlit Dashboard
```

## Technology Stack

* **Apache Airflow** — orchestrates pipeline tasks
* **Docker Compose** — runs the Airflow environment and supporting services
* **Python & Pandas** — processes and aggregates accident data
* **PostgreSQL & Redis** — supporting services in the Airflow setup
* **Streamlit** — visualizes analytics results
* **Git & GitHub** — version control and project documentation

## Pipeline Tasks

1. `validate_input` — checks the input CSV and its columns.
2. `process_and_aggregate` — processes records and generates analytics summaries.
3. `verify_outputs` — checks the generated output files.

## Results

The pipeline generated these six analytics summary files:

* Accidents by severity
* Accidents by hour
* Accidents by weekday
* Accidents by weather
* Accidents by city
* Road feature presence

### Airflow DAG

![Airflow DAG](screenshots/airflow-dag.png)

### Pipeline Execution

![Pipeline execution](screenshots/pipeline-success.png)

### Generated Analytics Files

![Analytics results](screenshots/analytics-results.png)

### Streamlit Dashboard

![Analytics dashboard](screenshots/dashboard.png)

> Replace these screenshot placeholders by adding your actual screenshots to the `screenshots/` folder.

## Learning Progress

* [x] Set up the project using Docker Compose.
* [x] Start the Airflow services.
* [x] Create an Airflow DAG with three tasks.
* [x] Validate the accident CSV dataset.
* [x] Process accident records and generate six analytics summaries.
* [x] Create a Streamlit dashboard script.
* [ ] Run the dashboard and capture its screenshot.
* [ ] Verify the complete DAG run and all task statuses.
* [ ] Document final dashboard results.

## Run the Project

Start the Airflow environment from the project directory:

```powershell
docker compose -f docker-compose.yaml up -d
```

Run the Streamlit dashboard locally:

```powershell
py -m pip install streamlit pandas
py -m streamlit run dashboard.py
```

The dashboard expects the analytics CSV files in `output/analytics/`.

## Repository Structure

```text
airflow-learning/
├── dags/
│   └── accident_pipeline.py
├── output/
│   └── analytics/
├── screenshots/
├── dashboard.py
├── docker-compose.yaml
├── README.md
└── .gitignore
```

## Future Improvements

* Add more accident trend analyses.
* Improve dashboard filtering and visualization.
* Add automated data-quality checks.
* Document pipeline execution and testing.

---

**Project status:** Learning project in progress.
