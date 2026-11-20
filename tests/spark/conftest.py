import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark with an isolated file-system Iceberg catalog, so tests never touch the lab's lake."""
    wh = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]").appName("lakehouse-tests")
         .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.test.type", "hadoop")
         .config("spark.sql.catalog.test.warehouse", f"file://{wh}")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .config("spark.eventLog.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
