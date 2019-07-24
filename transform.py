from extract import DataManager
from pyspark.sql.functions import col, regexp_replace, concat, concat_ws, lpad, to_timestamp, unix_timestamp, \
    from_unixtime, to_date


class FlightDataManager:
    def clean_flight_data(self, spark, path, format, output_path, output_format, write_mode='error'):
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
        return data_frame.withColumn('Date', concat_ws('-', 'Year', 'Month', 'DayOfMonth')) \
            .withColumn('Time', lpad(data_frame[column], 4, '0')) \
            .withColumn("Time", regexp_replace(col("Time"), "(\\d{2})(\\d{2})", "$1:$2")) \
            .withColumn(column, concat_ws(' ', 'Date', 'Time')).drop('Date', 'Time') \
            .withColumn(column, to_timestamp(column, 'yyyy-MM-dd HH:mm'))

    def calculate_actual_time(self, data_frame, column, delay, rename_column):
        return data_frame.withColumn(rename_column,
                                     to_timestamp(from_unixtime(unix_timestamp(column) + (col(delay) * 60))))

    def enhance_flight_details(self, spark, flight_path, aircraft_path, airport_path, carrier_path, output_path,
                               output_format, write_mode='error'):
        data_manager = DataManager()
        flights = data_manager.read_data(spark, flight_path, 'parquet') \
            .withColumn('FlightNum', concat('UniqueCarrier', 'FlightNum'))
        aircraft = data_manager.read_data(spark, aircraft_path, 'parquet') \
            .select('tailnum', 'manufacturer', 'issue_date', 'model', 'aircraft_type', 'engine_type') \
            .withColumn('issue_date', to_date('issue_date', 'MM/dd/yyyy'))

        airport = data_manager.read_data(spark, airport_path, 'parquet')
        carrier = data_manager.read_data(spark, carrier_path, 'parquet')

        flights = flights.join(aircraft, 'tailnum', 'left_outer') \
            .withColumnRenamed('manufacturer', 'AircraftManufacturer') \
            .withColumnRenamed('issue_date', 'AircraftIssueDate') \
            .withColumnRenamed('model', 'AircraftModel') \
            .withColumnRenamed('aircraft_type', 'AircraftType') \
            .withColumnRenamed('engine_type', 'EngineType')

        flights = flights.join(airport, flights['Origin'] == airport['iata'], 'left_outer') \
            .withColumnRenamed('Origin', 'OriginIATACode') \
            .withColumnRenamed('airport', 'OriginAirport') \
            .withColumnRenamed('city', 'OriginCity') \
            .withColumnRenamed('state', 'OriginState') \
            .withColumnRenamed('country', 'OriginCountry') \
            .withColumnRenamed('lat', 'OriginLatitude') \
            .withColumnRenamed('long', 'OriginLongitude') \
            .drop('iata')

        flights = flights.join(airport, flights['Dest'] == airport['iata'], 'left_outer') \
            .withColumnRenamed('Dest', 'DestinationIATACode') \
            .withColumnRenamed('airport', 'DestinationAirport') \
            .withColumnRenamed('city', 'DestinationCity') \
            .withColumnRenamed('state', 'DestinationState') \
            .withColumnRenamed('country', 'DestinationCountry') \
            .withColumnRenamed('lat', 'DestinationLatitude') \
            .withColumnRenamed('long', 'DestinationLongitude') \
            .drop('iata')

        flights = flights.join(carrier, flights['UniqueCarrier'] == carrier['code'], 'left_outer') \
            .withColumnRenamed('UniqueCarrier', 'CarrierCode').withColumnRenamed('Description', 'Carrier').drop('code')

        data_manager.write_data(flights, output_path, output_format, write_mode)


class AircraftManager:
    def clean_aircraft_data(self, spark, path, format, output_path, output_format, write_mode='error'):
        data_frame = DataManager().read_data(spark, path, format)
        data_frame = data_frame.filter("status == 'Valid'")
        DataManager().write_data(data_frame, output_path, output_format, write_mode)
