from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.session import SparkSession, DataFrame


class DataIngester:

    def ingest_data(self, spark: SparkSession, input_path, input_format=None, output_path=None, output_format=None,
                    write_mode='append'):
        data_frame: DataFrame = spark.read.format(input_format).option("header", True).load(f"{input_path}")
        data_frame.write.mode(write_mode).format(output_format).save(output_path)
