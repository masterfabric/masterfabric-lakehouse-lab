# Spark 3.5.5 + Iceberg 1.8.1 runtime (Apache-2.0 image by Tabular), plus the lab's
# configuration and test tooling.
FROM tabulario/spark-iceberg:3.5.5_1.8.1
RUN pip install --no-cache-dir pytest==8.3.5
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties
