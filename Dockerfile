FROM apache/airflow:2.9.2

USER root

# Install necessary system packages and the Microsoft ODBC driver for SQL Server
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         g++ \
         unixodbc-dev \
         libpq-dev \
         python3-pip \
         python3-pyodbc \
         python3-requests \
         curl \
         apt-transport-https \
         gnupg \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install pyodbc
RUN pip3 install pyodbc
RUN pip3 install pymssql
RUN pip3 install watchdog

USER airflow
