FROM python:3.9.1-slim-buster
USER root

# PSQL Requirements
RUN apt-get update && apt-get install -y libpq-dev build-essential

# Python Requirements
ADD requirements.txt .
RUN pip3 install -r requirements.txt
RUN pip3 install flake8
RUN pip3 install pytest
RUN pip3 install pytest-cov

# Airflow Env Vars
ENV AIRFLOW_HOME='/usr/local/airflow'

# wd
WORKDIR /gusty

# Sleep forever
CMD sleep infinity
