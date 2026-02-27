FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /opt/spark/jars/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER spark