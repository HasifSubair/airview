from extract import DataManager
from pyspark.sql.functions import col, regexp_replace, concat_ws, lpad, to_timestamp, unix_timestamp, from_unixtime
from pyspark.sql.types import TimestampType


class FlightDataManager:
    def clean_flight_data(self, spark, path, format, output_path, output_format, write_mode):
        data_frame = DataManager().read_data(spark, path, format)
        data_frame = self.clean_datetime_columns(data_frame, 'CRSDepTime')
        data_frame = self.clean_datetime_columns(data_frame, 'CRSArrTime')
        data_frame = self.clean_datetime_columns(data_frame, 'DepTime')
        data_frame = self.clean_datetime_columns(data_frame, 'ArrTime')
        data_frame = self.calculate_actual_time(data_frame, 'DepTime', 'DepDelay')
        data_frame = self.calculate_actual_time(data_frame, 'ArrTime', 'ArrDelay') \
            .drop('DayofMonth', 'DayOfWeek')
        DataManager().write_data(data_frame, output_path, output_format, write_mode)

    def clean_datetime_columns(self, data_frame, column):
        return data_frame.withColumn('Date', concat_ws('-', 'Year', 'Month', 'DayOfMonth')) \
            .withColumn('Time', lpad(data_frame[column], 4, '0')) \
            .withColumn("Time", regexp_replace(col("Time"), "(\\d{2})(\\d{2})", "$1:$2")) \
            .withColumn(column, concat_ws(' ', 'Date', 'Time')).drop('Date', 'Time') \
            .withColumn(column, to_timestamp(column, 'yyyy-MM-dd HH:mm'))

    def calculate_actual_time(self, data_frame, column, delay):
        return data_frame.withColumn(column, from_unixtime(unix_timestamp(column) + (col(delay) * 60)))


class CarrierManager:
    def read_carrier_data(self, path):
        pass
