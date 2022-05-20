import findspark
import pyspark
import re
import csv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
            .appName("Dataframes")\
            .getOrCreate()

    df = spark.read.json('test_data')
    df.show(10)

    rows = []
    FILENAME = "words/vocabulary.csv"
    with open(FILENAME, 'r') as file:
        csvreader = csv.reader(file)
        for row in csvreader:
             rows.append(row)

    vocabulary_bs = spark.sparkContext.broadcast(rows)
    #===================================================================================================================
    # regular expression
    reg_exp = r'\s' + r'\w≈[a-zA-Z]'+ word[1:]+ r'\s|\S'
    # reg_exp = r'\s' + word + r'\s|\S'
    # reg_exp = fr"'{re.escape(variable)}\s'"
    re.findall(reg_exp, vocabulary_bs, flags = re.IGNORECASE)#	Не различать заглавные и маленькие буквы, говорят, медленнее, но удобно

    # re.findall([(r'\s') + word + (r'\s')], vocabulary_bs, flags = re.IGNORECASE)

    filtered_df = df.map(df.frame_text)

#=======================================================================================================================
    #reading vocabulary as csv into DF and trying to broadcast it
    # vocabulary_df = spark.read.csv("words/vocabulary.csv")
    # vocabulary_df.show(10)
    # vocabulary_bs=spark.sparkContext.broadcast(vocabulary_df)















