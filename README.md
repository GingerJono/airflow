# Airflow

This repository contains DAGs for [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

The DAGs are synced to the deployed Airflow instance using Git-Sync, with the `main` branch of the Azure DevOps wiki repo being used a source. To deploy a DAG to Airflow the changes must simply be merged into the `main` branch. Note that the folder `/dags` is specifically defined as the source file for DAGs.

[[_TOC_]]

## Local Development Set Up

This codebase uses [uv](https://docs.astral.sh/uv/) as a project and package manager. Install this as per the documentation[here](https://docs.astral.sh/uv/getting-started/installation/).

### Linting

This codebase uses [ruff](https://docs.astral.sh/ruff/) for both linting and code formatting. The easiest way to run it is through uv, and it is specified as a project dependency for this.

To run ruff over the codebase, run:

```sh
uvx ruff check       # Lint all files in the current directory.
uvx ruff check --fix # Lint all files in the current directory and fix any fixable errors.
uvx ruff format      # Format all files in the current directory.
```

Ruff can be integrated with your code editor to make it easy to run, see documentation [here](https://docs.astral.sh/ruff/editors/setup/). The relevant VS Code extension is listed as a recommended extension for the repo.

## Developing in Airflow

In order to test DAGs under development it may be desirable to deploy these to the Azure environment for testing, before raising a PR and merging to main. In order to do this the Airflow deployment can be updated in order to point at a different branch - you should ensure your branch is up to date with main, and that no one else is looking to do the same thing.

This update has to be done in the terraform repo, but is done by:

1. In the `custom-values.yaml` file update the `gitSync.branch` value to the name of your branch
2. Authenticate with the Kubernetes cluster in your CLI `az aks get-credentials --resource-group poc-dalint-k8s-rg --name poc-dalint-k8s-cluster`
3. Upgrade the airflow deployment using `helm upgrade airflow airflow-stable/airflow --values ./custom-values.yaml`

See the terraform docs for more information on this, such as prerequisite installations.

To check the status of deployments it may be helpful to use:

* `helm status airflow`
* `helm history airflow`

