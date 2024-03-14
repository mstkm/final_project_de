FROM apache/airflow:2.8.2
USER root 

# USER airflow

# Install dependencies
COPY ./requirements.txt /
RUN pip install -r /requirements.txt

