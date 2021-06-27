import pyspark
import delta

def get_spark():
    """ returns a spark handle """
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
