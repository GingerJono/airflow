# Airflow

This repository contains DAGs for [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

The DAGs are synced to the deployed Airflow instance using Git-Sync, with the `main` branch of the Azure DevOps wiki repo being used a source. To deploy a DAG to Airflow the changes must simply be merged into the `main` branch. Note that the folder `/dags` is specifically defined as the source file for DAGs.

[[_TOC_]]

## Local Development Set Up

This codebase uses [uv](https://docs.astral.sh/uv/) as a project and package manager. Install this as per the documentation[here](https://docs.astral.sh/uv/getting-started/installation/).

New dependencies can be added with `uv add <dependency>`, or `uv add --dev <dependency>` for dev dependencies. After updating dependencies the `requirements.txt` file for the deployment should also be updated by running `uv pip compile pyproject.toml -o ./deployment/requirements.txt`. Note: if already running airflow locally you must run `docker compose up --build` for the new dependency to work.

### Linting

This codebase uses [ruff](https://docs.astral.sh/ruff/) for both linting and code formatting. The easiest way to run it is through uv, and it is specified as a project dependency for this.

To run ruff over the codebase, run:

```sh
uv run ruff check       # Lint all files in the current directory.
uv run ruff check --fix # Lint all files in the current directory and fix any fixable errors.
uv run ruff format      # Format all files in the current directory.
```

Ruff can be integrated with your code editor to make it easy to run, see documentation [here](https://docs.astral.sh/ruff/editors/setup/). The relevant VS Code extension is listed as a recommended extension for the repo.

### Running Airflow Locally

During development it can be useful to run airflow locally, in order to test DAGs. A docker-compose file is included in the repo to enable this.

These instructions are based on the Airflow guide [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

Prerequisites:

* Install Docker (if using Docker Desktop for Windows you will need a license)
* Install Docker Compose (note this comes automatically with Docker Desktop)

1. To initialise the database, run the database migrations and create the first user account.

  ```sh
  docker compose up airflow-init
  ```

1. To run airflow, run `docker compose up`
1. You will then be able to access the airflow UI at localhost:8080
   1. The account created by the init step has the login `airflow` and password `airflow`

Changes made to the dags will automatically be picked up, so you can get quick feedback. Logs are saved to the directory `.local/logs`, ensure you run all commands from the root of this repo so the created files are ignored by git.

#### Setting up Connections and Variables

Variables in the local environment are configured through the `development.env` file.

1. Duplicate the `development.env template` file and rename to `development.env`
2. Fill in the missing (sensitive) values

Note that variables defined through environment variables are not visible in the airflow UI, so don't worry if you look and can't see them there! This does unfortunately mean that the only way to confirm that they have been set properly is to try running a DAG that uses them.

Connections must be set up manually through the UI. There are two connections that may be required for local development testing:

* `microsoft_graph`: connection to Microsoft Graph, using for interacting with the subscription for the email monitoring DAGs
* `wasb`: connection to Azure Blob Storage, used for saving data between tasks.

See [connections table](#connections)

#### Cleaning up

If your local set up gets messed up you'll likely find the best thing to do is to clean it up and start again - you don't need to do this as part of standard development, just if you run into issues and want to have a clean local slate! To do this, from the root of the repo, run:

```sh
docker compose down --volumes --remove-orphans
```

## Writing DAGs

Airflow provides a number of [best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) which should be considered when writing DAGs and generally working with Airflow.

Some particular best practices and conventions for this codebase:

* Prefer using the [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html) style of writing DAGs, using annotated methods over use of traditional Operators where possible
* Avoid expensive top level imports, e.g. of modules like `pandas`
  * Use specific imports to reduce the expense of top-level imports, e.g. `from pandas import read_excel`

## Deploying Airflow to Azure

For the deployed environments we are hosting Airflow on Azure Kubernetes Service, using the [User-Community Airflow Helm Chart](https://github.com/airflow-helm/charts). We have extended the base Docker image to install our own dependencies, and we include our DAGs in the Docker image.

The deployment of the kubernetes cluster, and the creation of secrets are handled in the [infrastructure repo](https://dev.azure.com/DaleBI/DaleSoftwireIntegrationLayer/_git/infrastructure) for the project; this repository is responsible only for the deployment of Airflow onto the cluster.

Prerequisites:

* Helm 3.0+ ([installing helm](https://helm.sh/docs/intro/install/))
* [Kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)
* Azure CLI
* Docker

### Build Docker Image and Deploy to Azure Container Registry

Our custom Docker image is defined in `Dockerfile`, and builds from the official airflow slim image. The docker image contains our DAG definitions, and therefore must be updated when we update DAGs.

The docker image tag is used to identify the correct image for the airflow deployment- **tags must never be re-used**. For this reason use something unique such as a git hash when tagging builds.

1. Build the image locally

   ```sh
   docker build . -t commondalintairflowcontainerregistry.azurecr.io/dalint-airflow:<git-hash>
   ```

   1. The first part of the image name (before the `:`) is the path to where in ACR we want to save the image
   2. The second part (after the `:`) is the tag, this is the bit that changes every time and must be unique
2. Authenticate with the container registry

   ```sh
   az login
   az acr login --name commondalintairflowcontainerregistry
   ```
  
3. Push the docker image to the registry

   ```sh
   docker push commondalintairflowcontainerregistry.azurecr.io/dalint-airflow:<git-hash>
   ```

4. Update the file `deployment/custom-values.yaml` and set the `tag` value to be the tag (i.e. `<git-hash>`) that you used for your image.

### Update Airflow Deployment

All instructions are written to be run from the root of this repo.

1. Login to Azure CLI using `az login` and select the DaleUW directory.
2. Get credentials for Kubernetes CLI. Take care here, the `<env>` value set below will fully determine which environment you are deploying to!

   ```sh
   az aks get-credentials --resource-group <env>-dalint-k8s-rg --name <env>-dalint-k8s-cluster
   ```

3. Set the `integration-layer-airflow` namespace as default

   ```sh
   kubectl config set-context --current --namespace=integration-layer-airflow
   ```
  
4. Add the repository to your helm and update repos

   ```sh
   ## add this helm repository
   ## [Only needed the first time you do this]
   helm repo add airflow-stable https://airflow-helm.github.io/charts

   ## update your helm repo cache
   helm repo update
   ```

5. Update the airflow release

   ```sh
   helm upgrade airflow airflow-stable/airflow --values ./deployment/custom-values.yaml
   ```

   * Before doing this is may be sensible to confirm the currently kubectl context,
   which determines the environment this affects. This can be done using the command `kubectl config current-context`.
   * You can use `kubectl config view` to see all available contexts, and `kubectl config use-context` to set the context

To check if everything has worked you may find the following commands useful:

* `helm status airflow` - check the status of the helm deployment
* `helm history airflow` - check the history of the helm deployment
* `kubectl describe pod airflow` - check on the actual airflow pod

### Initial Airflow deployment

The first time that Airflow is deployed to AKS, there are a couple of changes required to this flow.

* In step 3 you must create the namespace:

  ```sh
  kubectl create ns integration-layer-airflow
  kubectl config set-context --current --namespace=integration-layer-airflow
  ```

* In step 5 you must install the chart:

  ```sh
  helm install airflow airflow-stable/airflow --namespace integration-layer-airflow --version "8.X.X" --values ./deployment/custom-values.yaml
  ```

## Variables and Connections

There are a number of variables and connections which must be configured in Airflow for the DAGs to work. At present these must be set manually in the UI, though in future we should update this so they are created through the custom-values.yaml file, pulling values from Kubernetes Secrets and ConfigMaps.

### Variables

Variables are set in the UI at Admin > Variables

| Variable Name                                    | Value                                                                                                                                      | Purpose                                                                                                                                                                                                                     |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| azureai_api_key                                  | Value from key vault secret `email-monitoring-openai-api-key`                                                                              | API Key for interacting with Azure Open AI client                                                                                                                                                                           |
| azureai_endpoint                                 | `https://common-dalint-openai.openai.azure.com/openai/deployments/common-dalint-openai-deployment/chat/completions?api-version=2024-10-21` | URL for requests to Azure Open AI.                                                                                                                                                                                          |
| email_monitoring_apim_change_notification_url    | `https://common-dalint-api-management.azure-api.net/dalint-internal/api/v1/email-monitoring/notification`                                  | Endpoint for email change notifications to be sent to.                                                                                                                                                                      |
| email_monitoring_apim_lifecycle_notification_url | `https://common-dalint-api-management.azure-api.net/dalint-internal/api/v1/email-monitoring/lifecycle-notification`                        | Email for lifecycle notifications to be sent to.                                                                                                                                                                            |
| email_monitoring_client_state                    | Value from key vault secret `email-monitoring-client-secret`                                                                               | Client secret for service principal used to authenticate with Microsoft Graph for email subscription.                                                                                                                       |
| email_monitoring_mailbox                         | `autosubs.dev@daleuw.com`                                                                                                                  | The mailbox which is being monitored for submissions.                                                                                                                                                                       |
| email_monitoring_recipients                      | `["autosubs.dev@daleuw.com", "group-dale-integrationlayer-internal@softwire.com"]`                                                         | The recipients that the LLM summary email should be sent to. Must be formatted as a list, and should include the mailbox that is being monitored. In non-production environments should include the development team email. |

### Connections

Connections are set in the UI at Admin > Connections

| Connection ID     | Connection Type                 | Configuration                                                                                                                                                                                                                                                            | Purpose                                                                                |
| ----------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| microsoft_graph   | `msgraph` (Microsoft Graph API) | Client ID: From key vault secret `email-monitoring-client-id`</br>Client Secret: From key vault secret `email-monitoring-client-secret`</br>Tenant ID: `cd4ffd59-5c74-4b7a-b992-9fc58efba60c`</br>API Version: `v1.0`</br>Scopes: `https://graph.microsoft.com/.default` | Connection to Microsoft Graph, used for requests relating to the mailbox subscription. |
| wasb              | `wasb` (Azure Blob Storage)     | SAS Token: In the Azure Portal generate a SAS Token copy the `Blob service SAS URL` into the `SAS Token` field                                                                                                                                                           | Connection to Azure Blob storage, used for saving data to blobs to pass between tags.  |
| wasb-airflow-logs | `wasb` (Azure Blob Storage)     | Same as wasb connection                                                                                                                                                                                                                                                  | Connection to Azure Blob storage, used for reading and writing logs from/to blobs.     |
