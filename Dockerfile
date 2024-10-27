FROM bitnami/spark:latest
WORKDIR /main
COPY main /main
COPY jars/* $SPARK_HOME/jars/
RUN pip install kafka-python-ng