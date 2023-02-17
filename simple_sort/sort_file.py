import findspark
from pyspark.sql import SparkSession
import sys


if __name__ == "__main__":
    # Total Arguments
    n = len(sys.argv)
    print(sys.argv)

    # Check if user inputted the right number of arguments. Should be 3.
    # 0: name of executable file
    # 1: input path
    # 2: output path
    if n != 3:
        print("Invalid number of inputs:", n)
        exit(-1)

    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    # Init Spark
    findspark.init('/home/ubuntu/spark-3.3.1-bin-hadoop3')

    # Start Spark session
    spark = SparkSession.builder\
        .appName('Spark Sort File')\
        .master('spark://172.31.82.177:7077')\
        .getOrCreate()

    # Read the input file to a dataframe
    print('Reading from {}...'.format(input_file_path))
    input_df = spark.read.load(
        input_file_path,
        format = 'csv',
        inferSchema = 'true',
        sep = ',',
        header = 'true'
    )

    # Sort dataframe by the 'cca2' and 'timestamp' columns
    print('Sorting dataframe...')
    sorted_df = input_df.sort('cca2', 'timestamp')

    # Write the resulting dataframe to the output file path provided
    # Set mode to overwrite in the case that the file already exists
    print('Writing to {}...'.format(output_file_path))
    sorted_df.write.csv(
        output_file_path,
        header = 'true',
        mode = 'overwrite'
    )

    # Stop Spark session
    spark.stop()

    print('Done!')