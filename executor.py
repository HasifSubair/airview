import sys
from extract import DataManager
from transform import FlightDataManager
import pyspark.sql.session as session


class Executor:

    def get_spark_session(self, app_name, master='local[*]'):
        """
        Function initiates a SparkSession instance. Master is defaulted to local. Can be extended to
        property driven or removed to be executed with spark-submit.
        :param app_name: str
                        Application name
        :param master: str
                        Spark master URL
        :return: pyspark.sql.session.SparkSession
                        An instance of SparkSession
        """
        return session.SparkSession.Builder().appName(app_name).master(master).getOrCreate()

    def show_schema(self, input_path, input_format, header=True):
        data_frame = self.get_spark_session(f'Showing schema for {input_path}').read.format(input_format).option(
            "header", header).load(f"{input_path}")
        data_frame.printSchema()

    def ingest_data(self, *params):
        DataManager().ingest_data(self.get_spark_session(f"Ingesting Data from {params[0]}"), *params)

    def clean_flight_data(self, *params):
        FlightDataManager().clean_flight_data(self.get_spark_session(f"Flight Quality Check : {path}"), params)


if __name__ == "__main__":
    args = sys.argv
    getattr(Executor(), args[1])(*args[2:])
