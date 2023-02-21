### Problem 2: A Simple Spark Application

#### Pre-requisite Steps

- Download the data from this [link](https://tddg.github.io/ds5110-spring23/assets/export.csv)
- Add the file to HDFS using `hdfs dfs -copyFromLocal export.csv /`
- In `sort_file.py`, change the value in line 32 `master(<Your Spark Master>)` to the addres of your Spark Master server.

#### Running the file
- To run the script, go to the directory where `sort_file.py` is and use the following command:

`~/spark-3.3.1-bin-hadoop3/bin/spark-submit sort_file.py hdfs://<Your HDFS IP Address>:9000/export.csv hdfs://<Your HDFS IP Address>:9000/export_sorted.csv`

- The command line arguments include the: *input_file_path* and *output_file_path*
- NOTE: The path to `spark-submit` may vary depending on your installation of Spark.

#### Output
- The output will be a new csv file sorted by the `ccr2` and `timestamp` columns.
- You can access the file at `hdfs://<Your HDFS IP Address>:9000/export_sorted.csv`