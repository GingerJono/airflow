# Airflow

This repository contains DAGs for [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

The DAGs are synced to the deployed Airflow instance using Git-Sync, with the `main` branch of the Azure DevOps wiki repo being used a source. To deploy a DAG to Airflow the changes must simply be merged into the `main` branch. Note that the folder `/dags` is specifically defined as the source file for DAGs.
