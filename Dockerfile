FROM python:3.10.4-slim-buster
USER root

# PSQL Requirements
RUN apt-get update && apt-get install -y libpq-dev build-essential

# pip3 upgrade
RUN pip3 install --upgrade pip

# Linting
RUN pip3 install flake8 --trusted-host pypi.org

# Testing
RUN pip3 install pytest --trusted-host pypi.org
RUN pip3 install pytest-cov --trusted-host pypi.org

# Dev Requirements
ADD dev-requirements.txt .
RUN pip3 install -r dev-requirements.txt --trusted-host pypi.org

# Airflow env
ENV AIRFLOW_HOME='/usr/local/airflow'

# Nope
ENV SCARF_ANALYTICS=false

# wd
WORKDIR /gusty

# Sleep forever
CMD sleep infinity
