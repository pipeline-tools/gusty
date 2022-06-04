FROM python:3.10.4-slim-buster
USER root

# PSQL Requirements
RUN apt-get update && apt-get install -y libpq-dev build-essential

# pip3 upgrade
RUN pip3 install --upgrade pip

# Linting
RUN pip3 install flake8

# Testing
RUN pip3 install pytest
RUN pip3 install pytest-cov

# Dev Requirements
ADD dev-requirements.txt .
RUN pip3 install -r dev-requirements.txt

# Airflow env
ENV AIRFLOW_HOME='/usr/local/airflow'

# wd
WORKDIR /gusty

# Sleep forever
CMD sleep infinity
