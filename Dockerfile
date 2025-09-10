ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_MAJOR_MINOR_VERSION=3.12
ARG BASE_IMAGE="apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}"
FROM ${BASE_IMAGE}

# Install Airflow and any additional dependencies
WORKDIR /

ARG AIRFLOW_VERSION
ARG PYTHON_MAJOR_MINOR_VERSION
ARG AIRFLOW_EXTRAS="cncf.kubernetes,postgres,microsoft.azure"
ARG AIRFLOW_CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"

COPY --chown=airflow:root /dags /opt/airflow/dags
COPY --chown=airflow:root /plugins /opt/airflow/plugins

COPY deployment /deployment
RUN pip install -U pip==25.0.1 && \
    pip install --no-cache-dir apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION} --no-cache-dir --constraint ${AIRFLOW_CONSTRAINTS_LOCATION} && \
    pip install -r deployment/requirements.txt
