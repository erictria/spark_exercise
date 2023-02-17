import re
import sys
from operator import add

import findspark
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


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

    if n != 3:
        print("Invalid number of inputs:", n)
        exit(-1)
    
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    
    findspark.init('/home/ubuntu/spark-3.3.1-bin-hadoop3')
    spark = SparkSession.builder\
        .appName('Spark Page Rank')\
        .master('spark://172.31.82.177:7077')\
        .getOrCreate()
    
    lines = spark.read.text(
        input_file_path,
        format = 'txt',
        header = 'true'
    )

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

    ranks.write.txt(
        output_file_path,
        header = 'true',
        mode = 'overwrite'
    )
    
    spark.stop()