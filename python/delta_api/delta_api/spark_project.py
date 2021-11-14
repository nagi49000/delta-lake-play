import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import delta
import os


def get_spark():
    """ returns a SparkSession """
    builder = pyspark.sql.SparkSession.builder.appName("delta-api-app") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def get_or_create_names_table(spark, delta_dir):
    """ spark - SparkSession
        delta_dir - str or path-like - location of the data for file read

        retrieve, or (on fail), create and retrieve DeltaTable

        returns a tuple of (DeltaTable, path), where path is to the table on disk
    """
    delta_table_location = os.path.join(delta_dir, "names-delta-table")
    try:
        table = delta.tables.DeltaTable.forPath(spark, delta_table_location)
    except pyspark.sql.utils.AnalysisException:
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("firstname", StringType(), False),
            StructField("lastname", StringType(), False)
        ])
        data = [(1, "James", "Bond"), (2, "Alice", "Rogers"), (3, "Joe", "Bloggs")]
        df = spark.createDataFrame(data=data, schema=schema)
        df.write.format("delta").save(delta_table_location)
        table = delta.tables.DeltaTable.forPath(spark, delta_table_location)
    return table, delta_table_location
