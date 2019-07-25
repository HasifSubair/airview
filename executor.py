import sys

import pyspark.sql.session as session

from data_manager import DataManager
from transform import FlightManager, AircraftManager


class Executor:
    """
    Executor class acts as the entry point to the program. You can execute the program by passing the method name as
    the first argument and followed by the arguments for the method. For example, for data ingestion you can execute the
    command in below format

    executor.py ingest_data <INPUT_LOCATION> <INPUT_FORMAT> <OUTPUT_LOCATION> <OUTPUT_FORMAT>
    """

    def get_spark_session(self, app_name, master='local[*]'):
        """
        Function initiates a SparkSession instance. Master is defaulted to local. Can be extended to property driven or
        removed to be executed with spark-submit.
        :param app_name: str
                        Application name
        :param master: str
                        Spark master URL
        :return: pyspark.sql.session.SparkSession
                        An instance of SparkSession
        """
        return session.SparkSession.Builder().appName(app_name).master(master).getOrCreate()

    def show_schema(self, input_path, input_format, header=True):
        """
        Method prints the schema of the file in the input path. Method uses sparks internal schema inference to
        determine the file schema. It reads a few sample records to determine the type of the record.
        :param input_path: Path to the file whose schema is to determined.
        :param input_format: File format of the input file.
        :param header: For files with header information, doesn't have an effect on files without it.
        :return: NoneType
        """
        data_frame = self.get_spark_session(f'Showing schema for {input_path}').read.format(input_format).option(
            "header", header).load(f"{input_path}")
        data_frame.printSchema()

    def ingest_data(self, *params):
        """
        Method reads files from the based on the parameters and writes to the output location based on the parameters.
        The method invokes DataManager to ingest the data by creating and passing a SparkSession instance along with the
        input parameters.
        :param params: Parameters for the ingesting the data.
        :return: NoneType.
        """
        DataManager().ingest_data(self.get_spark_session(f"Ingesting Data from {params[0]}"), *params)

    def clean_flight_data(self, *params):
        """
        Method reads the data from the input location in the parameter and cleans the flight data and stores it in the
        output location specified in the parameters. Method invokes FlightManager class' clean flight data method with
        an instance of SparkSession along with the input parameters.
        :param params: Parameter for clean the flight data.
        :return: NoneType.
        """
        FlightManager().clean_flight_data(self.get_spark_session(f"Flight data cleaning: {params[0]}"), *params)

    def clean_aircraft_data(self, *params):
        """
        Method reads the data from the input location in the parameter and cleans the aircraft data and stores it in the
        output location specified in the parameters. Method invokes AircraftManager class' clean aircraft data method with
        an instance of SparkSession along with the input parameters.
        :param params: Parameter for clean the aircraft data.
        :return: NoneType.
        """
        AircraftManager().clean_aircraft_data(self.get_spark_session(f"Aircraft data cleaning: {params[0]}"), *params)

    def enhance_flight_data(self, *params):
        """
        Method creates the analytical dataset by joining airline, airport and aircraft data with the flight dataset.
        Method invoke FlightManager class' enhance flight details method with an instance of SparkSession anf input
        parameters.
        :param params: Parameter for enhance the flight dataset.
        :return: NoneType.
        """
        FlightManager().enhance_flight_details(self.get_spark_session('Enhancing Flight details'), *params)


if __name__ == "__main__":
    # Getting the python arguments.
    args = sys.argv
    # Method to be called in executor class is set as the first argument, rest of the arguments are passed to that
    # method as tuples.
    getattr(Executor(), args[1])(*args[2:])
