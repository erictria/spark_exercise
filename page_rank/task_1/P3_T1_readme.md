### Problem 3 Task 1: PySpark implementation of the PageRank algorithm

#### Pre-requisite Steps

- Download the data from this [link](https://snap.stanford.edu/data/web-BerkStan.html)
- Unzip the file using this command `gunzip web-BerkStan.txt.gz`
- Add the unzipped file to HDFS using `hdfs dfs -copyFromLocal web-BerkStan.txt /`
- In `page_rank_spark.py`, change the value in line 44 `master(<Your Spark Master>)` to the addres of your Spark Master server.

#### Running the file
- To run the script, go to the directory where `page_rank_spark.py` is and use the following command:

`~/spark-3.3.1-bin-hadoop3/bin/spark-submit page_rank_spark.py hdfs://<Your HDFS IP Address>:9000/web-BerkStan.txt hdfs://<Your HDFS IP Address>:9000/page_rank.csv`

- The command line arguments include the: *input_file_path* and *output_file_path*
- NOTE: The path to `spark-submit` may vary depending on your installation of Spark.

#### Output
- The output will be a new csv file with the result of running the PageRank algorithm.
- You can access the file at `hdfs://<Your HDFS IP Address>:9000/page_rank.csv`