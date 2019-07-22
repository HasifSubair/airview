import sys
from ingest_data import FlightDataIngest
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

    def ingest_flight(self, *params):
        FlightDataIngest().ingest_data(input_path=params[0], input_format=params[1], output_path=params[2],
                                              output_format=params[3], write_mode=params[4],
                                              spark=self.get_spark_session("Ingest Flight Data"))


if __name__ == "__main__":
    args = sys.argv
    getattr(Executor(), args[1])(*args[2:])
    # Executor().execute_task(func)
