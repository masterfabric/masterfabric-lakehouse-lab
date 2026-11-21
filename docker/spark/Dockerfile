# Spark 3.5.5 + Iceberg 1.8.1 runtime (Apache-2.0 image by Tabular), plus the lab's
# configuration, test tooling, data-quality library and lineage listener.
FROM tabulario/spark-iceberg:3.5.5_1.8.1
# Great Expectations (Apache-2.0) validates Spark DataFrames in the write-audit-publish gate.
RUN pip install --no-cache-dir pytest==8.3.5 great_expectations==1.25.0
# OpenLineage Spark listener (Apache-2.0): emits run/job/dataset events to Marquez when enabled.
ADD https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.55.0/openlineage-spark_2.12-1.55.0.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/openlineage-spark_2.12-1.55.0.jar
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties
