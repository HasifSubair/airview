"""
The script contains all the transformation to be performed on the input data.
"""
from data_manager import DataManager
from pyspark.sql.functions import col, regexp_replace, concat, concat_ws, lpad, to_timestamp, unix_timestamp, \
    from_unixtime, to_date


class FlightManager:
    """
    Class contains all the transformation and data cleaning methods for flight data.
    """

    def clean_flight_data(self, spark, path, format, output_path, output_format, write_mode='error'):
        """
        Method converts the time columns scheduled departure time(CRSDepTime), scheduled arrival time(CRSArrTime),
        actual departure time(DepTime) and actual arrival time(ArrTime) to a timestamp columns. The original format for
        these columns in 'HHmm' and is converted to 'yyyy-MM-dd HH:mm:ss' format, which is much more human readable. The
        columns 'DayofMonth' and 'DayOfWeek' dropped as it is no longer required. The data frame is written to the
        output folder in the specified format.
        :param spark: SparkSession instance.
        :param path: Path to the input folder.
        :param format: Input file format.
        :param output_path: Output file path
        :param output_format: Output file format.
        :param write_mode: File write mode defaulted to 'error'
        :return: NoneType
        """
        data_frame = DataManager().read_data(spark, path, format)
        data_frame = self.clean_datetime_columns(data_frame, 'CRSDepTime')
        data_frame = self.clean_datetime_columns(data_frame, 'CRSArrTime')
        data_frame = self.clean_datetime_columns(data_frame, 'DepTime')
        data_frame = self.clean_datetime_columns(data_frame, 'ArrTime')
        data_frame = self.calculate_actual_time(data_frame, 'CRSDepTime', 'DepDelay', 'DepTime')
        data_frame = self.calculate_actual_time(data_frame, 'CRSArrTime', 'ArrDelay', 'ArrTime') \
            .drop('DayofMonth', 'DayOfWeek')
        DataManager().write_data(data_frame, output_path, output_format, write_mode)

    def clean_datetime_columns(self, data_frame, column):
        """
        Method concatenates Year, Month and DayOfMonth columns together and adds ':' to the time columns which was a
        float and is concatenated to the above columns. The final value is casted as a timestamp.
        :param data_frame: Spark data frame instance with the input data to be transfomed.
        :param column: Column on which the transformation needs to be applied.
        :return: Spark data frame instance with the transformation applied to the given column.
        """
        return data_frame.withColumn('Date', concat_ws('-', 'Year', 'Month', 'DayOfMonth')) \
            .withColumn('Time', lpad(data_frame[column], 4, '0')) \
            .withColumn("Time", regexp_replace(col("Time"), "(\\d{2})(\\d{2})", "$1:$2")) \
            .withColumn(column, concat_ws(' ', 'Date', 'Time')).drop('Date', 'Time') \
            .withColumn(column, to_timestamp(column, 'yyyy-MM-dd HH:mm'))

    def calculate_actual_time(self, data_frame, column, delay, rename_column):
        """
        Method calculates actual departure and arrival time. DepTime and ArrTime columns are not used since there is
        chance corruption of data close to midnight, since it contains only the time part. So instead the DepDelay or
        ArrDelay value is added to the scheduled departure and scheduled arrival columns respectively to get the actual
        departure and arrival timestamp.
        :param data_frame: Spark dataframe whose actual times need to be calculated.
        :param column: Column containing scheduled value for departure or arrival.
        :param delay: Column containing the delays for departure or arrival.
        :param rename_column: Column to which the data needs to be stored.
        :return: Spark data frame with the update the timestamps.
        """
        return data_frame.withColumn(rename_column,
                                     to_timestamp(from_unixtime(unix_timestamp(column) + (col(delay) * 60))))

    def enhance_flight_details(self, spark, flight_path, aircraft_path, airport_path, carrier_path, output_path,
                               output_format, write_mode='error'):
        """
        Method enhances the flight information with aircraft, airport and carrier data and store it in output folder in
        the specified format.
        :param spark: SparkSession instance.
        :param flight_path: Path to clean flight data.
        :param aircraft_path: Path to aircraft data.
        :param airport_path:  Path to airport data.
        :param carrier_path: Path to carrier data.
        :param output_path: Path where enhanced flight data to be stored.
        :param output_format: Format in which the enhanced flight data to be stored.
        :param write_mode: File write mode with default to 'error', which throws an error, if the output folder is not
        empty.
        :return: NoneType.
        """
        data_manager = DataManager()
        flights = data_manager.read_data(spark, flight_path, 'parquet') \
            .withColumn('FlightNum', concat('UniqueCarrier', 'FlightNum'))
        aircraft = data_manager.read_data(spark, aircraft_path, 'parquet') \
            .select('tailnum', 'manufacturer', 'issue_date', 'model', 'aircraft_type', 'engine_type') \
            .withColumn('issue_date', to_date('issue_date', 'MM/dd/yyyy'))

        airport = data_manager.read_data(spark, airport_path, 'parquet')
        carrier = data_manager.read_data(spark, carrier_path, 'parquet')

        # Joining Flight data with aircraft data on tailnum.
        flights = flights.join(aircraft, 'tailnum', 'left_outer') \
            .withColumnRenamed('manufacturer', 'AircraftManufacturer') \
            .withColumnRenamed('issue_date', 'AircraftIssueDate') \
            .withColumnRenamed('model', 'AircraftModel') \
            .withColumnRenamed('aircraft_type', 'AircraftType') \
            .withColumnRenamed('engine_type', 'EngineType')

        # Joining flight data with airport data for origin airport information.
        flights = flights.join(airport, flights['Origin'] == airport['iata'], 'left_outer') \
            .withColumnRenamed('Origin', 'OriginIATACode') \
            .withColumnRenamed('airport', 'OriginAirport') \
            .withColumnRenamed('city', 'OriginCity') \
            .withColumnRenamed('state', 'OriginState') \
            .withColumnRenamed('country', 'OriginCountry') \
            .withColumnRenamed('lat', 'OriginLatitude') \
            .withColumnRenamed('long', 'OriginLongitude') \
            .drop('iata')

        # Joining flight data with airport data for destination information.
        flights = flights.join(airport, flights['Dest'] == airport['iata'], 'left_outer') \
            .withColumnRenamed('Dest', 'DestinationIATACode') \
            .withColumnRenamed('airport', 'DestinationAirport') \
            .withColumnRenamed('city', 'DestinationCity') \
            .withColumnRenamed('state', 'DestinationState') \
            .withColumnRenamed('country', 'DestinationCountry') \
            .withColumnRenamed('lat', 'DestinationLatitude') \
            .withColumnRenamed('long', 'DestinationLongitude') \
            .drop('iata')

        # Joining flight data with carrier information on carrier code.
        flights = flights.join(carrier, flights['UniqueCarrier'] == carrier['code'], 'left_outer') \
            .withColumnRenamed('UniqueCarrier', 'CarrierCode').withColumnRenamed('Description', 'Carrier').drop('code')

        data_manager.write_data(flights, output_path, output_format, write_mode)


class AircraftManager:
    """
    Class contains all the transformation and data cleaning methods for aircraft data.
    """

    def clean_aircraft_data(self, spark, path, format, output_path, output_format, write_mode='error'):
        """
        Method keeps only the aircraft whose 'status' column whose value equals 'Valid'.
        :param spark: SparkSessin instance.
        :param path: Path to aircraft data.
        :param format: Format in which the aircraft data is stored.
        :param output_path: Output path the where the clean aircraft to be stored.
        :param output_format: Output format in which the clean aircraft data to be stored.
        :param write_mode: File write mode with default to 'error', which throws an error, if the output folder is not
        empty.
        :return: NoneType.
        """
        data_frame = DataManager().read_data(spark, path, format)
        data_frame = data_frame.filter("status == 'Valid'")
        DataManager().write_data(data_frame, output_path, output_format, write_mode)
