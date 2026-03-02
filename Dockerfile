FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# JDBC and Delta Lake JARs (persistent; avoid --packages at runtime)
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar -P /opt/spark/jars/ \
    && wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar -P /opt/spark/jars/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER spark