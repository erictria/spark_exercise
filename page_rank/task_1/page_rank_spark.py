# https://github.com/apache/spark/blob/master/examples/src/main/python/pagerank.py
# https://github.com/ashishvshenoy/pagerank-spark
# https://medium.com/swlh/pagerank-on-mapreduce-55bcb76d1c99

import re
import sys
import time
from operator import add

import findspark
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def compute_contributions(urls, rank):
    total_urls = len(urls)
    for url in urls:
        yield (url, rank / total_urls)


def convert_to_lines(data):
    line = '\n'.join(str(d) for d in data)
    return line

def parse_neighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

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

    start_time = time.time()
    
    findspark.init('/home/ubuntu/spark-3.3.1-bin-hadoop3')
    spark = SparkSession.builder\
        .appName('Spark Page Rank')\
        .master('spark://172.31.82.177:7077')\
        .getOrCreate()
    
    # Read the input file to a dataframe
    # Convert dataframe to RDD in order to apply map functions
    print('Reading from {}...'.format(input_file_path))
    lines = spark.read.text(
        input_file_path
    ).rdd.map(lambda r: r[0])

    # Parse text lines to node connections
    links = lines.map(lambda urls: parse_neighbors(urls)).distinct().groupByKey()

    # Set initial rank of each page to be 1
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # 10 iterations for computing contributions
    # Each page p contributes to its outgoing neighbors a value of rank(p)/(# of outgoing neighbors of p)
    for iteration in range(10):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_contributions(url_urls_rank[1][0], url_urls_rank[1][1])
        )

        # Update each page’s rank to be 0.15 + 0.85 * (sum of contributions)
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    
    # Convert RDD to dataframe
    df_column_names = ['link', 'rank']
    ranks_df = ranks.toDF(df_column_names)#.sort(col('rank').desc)
#     ranks_df.show(10)
    
    # Write the resulting dataframe to the output file path provided
    # Set mode to overwrite in the case that the file already exists
    print('Writing to {}...'.format(output_file_path))
    ranks_df.write.csv(
        output_file_path,
        header = 'true',
        mode = 'overwrite'
    )
    
    # Stop Spark session
    spark.stop()
    
    end_time = time.time()
    print('Done! Finished in {} seconds'.format(end_time - start_time))