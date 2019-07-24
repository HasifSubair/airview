from pyspark.sql.session import SparkSession, DataFrame


class DataManager:

    def read_data(self, spark: SparkSession, path, format):
        return spark.read.format(format).option("header", True).load(f"{path}")

    def write_data(self, data_frame: DataFrame, output_path, output_format, write_mode):
        data_frame.write.mode(write_mode).format(output_format).save(output_path)

    def ingest_data(self, spark: SparkSession, input_path, input_format, output_path, output_format, write_mode):
        data_frame: DataFrame = self.read_data(spark, input_path, input_format)
        self.write_data(data_frame, output_path, output_format, write_mode)
