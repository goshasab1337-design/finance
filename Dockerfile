FROM apache/airflow:2.9.3

WORKDIR ${AIRFLOW_HOME}

# Root user to have permissions to install dependencies and create
# a separate virtual environment for Soda inside the image.
USER root

COPY requirements.txt /requirements.txt

RUN python${PYTHON_VERSION%.*} -m pip install --no-cache-dir pip \
 && python${PYTHON_VERSION%.*} -m pip install --no-cache-dir -r /requirements.txt

# Our requirements use protobuf 5 but for the object 
# that connects soda-core with Clichkouse, we need protobuf 3
RUN python${PYTHON_VERSION%.*} -m venv /opt/soda-venv \
 && /opt/soda-venv/bin/pip install --no-cache-dir pip \
 && /opt/soda-venv/bin/pip install --no-cache-dir \
      setuptools==70.0.0 \
      soda-core==3.5.5 \
      soda-core-mysql-utf8-hotfix==3.5.5.post1 \
      mysql-connector-python==8.0.33 \
      protobuf==3.20.1

# Airflow user to execute the image without unnecessary privileges
USER ${AIRFLOW_UID}
