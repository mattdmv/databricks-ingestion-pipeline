from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions as F


spark = SparkSession.getActiveSession()


def bronze_ingestion_flow(path: str, schema: StructType) -> DataFrame:
    return (
      spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaEvolutionMode", "rescue")
          .option("header", "true")
          .schema(schema)
          .load(path)
          .withColumn("_file_path", F.col("_metadata.file_path"))
          .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
          .withColumn("_ingestion_time", F.current_timestamp())
          )
