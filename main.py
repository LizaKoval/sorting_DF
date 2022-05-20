import findspark
import pyspark
import re
import csv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
            .appName("Dataframes")\
            .getOrCreate()

    #df = spark.read.json('test_data')
    # rows = []
    # FILENAME = 'vocabulary.csv'
    # with open(FILENAME, 'r') as file:
    #     csvreader = csv.reader(file)
    #     for row in csvreader:
    #         rows.append(row)

    #reading vocabulary
    vocabulary_df = spark.read.csv("words")
    vocabulary_df.show(10)
    vocabulary_bs=spark.broadcast(vocabulary_df)













