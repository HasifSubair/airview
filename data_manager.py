from pyspark.sql.session import SparkSession, DataFrame


class DataManager:
    """
    Class manages the data reads and writes using spark.
    """

    def read_data(self, spark: SparkSession, path, format):
        """
        Method reads the data from the path using spark and returns a spark data frame.
        :param spark: SparkSession instance.
        :param path: The input file/folder path.
        :param format: The file format.
        :return: Spark data frame object with the data from the input path/file.
        """
        return spark.read.format(format).option("header", True).load(f"{path}")

    def write_data(self, data_frame: DataFrame, output_path, output_format, write_mode):
        """
        Method writes the data from the spark data frame to output path and in specified format and write mode.
        :param data_frame: Spark data frame.
        :param output_path: The output folder.
        :param output_format: The output file format.
        :param write_mode:  Can have 4 values
                            1. 'append'     -   if data already exists, contents of the DataFrame are expected to be
                                                appended to existing data.
                            2. 'overwrite'  -   if data already exists, existing data is expected to be overwritten by
                                                the contents of the DataFrame.
                            3. 'error'      -   throws an error if data already exists.
                            4. 'ignore'     -   if data already exists, the save operation is expected to not save the
                                                contents of the DataFrame and to not change the existing data.
        :return: NoneType
        """
        data_frame.write.mode(write_mode).format(output_format).save(output_path)

    def ingest_data(self, spark: SparkSession, input_path, input_format, output_path, output_format,
                    write_mode='error'):
        """
        Method the reads data from the input path and writes it the output path in the specified format.
        :param spark: SparkSession instance.
        :param input_path: The input file/folder path.
        :param input_format: The input file format.
        :param output_path: The output file/folder path.
        :param output_format: The output file format.
        :param write_mode: The output write mode. Default to 'error'
        :return: NoneType.
        """
        data_frame: DataFrame = self.read_data(spark, input_path, input_format)
        self.write_data(data_frame, output_path, output_format, write_mode)
