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
    df.printSchema()

    bin_df = spark.read.csv("words").collect()
    vocabulary_df = set(bin_df)
    vocabulary_bs = spark.sparkContext.broadcast(vocabulary_df)

    def FindWords(row):
        final_arr = []
        for word in vocabulary_bs.value:
            final_arr.append(re.findall([(r'\s') + word + (r'\s|\S')], row, flags=re.IGNORECASE))
        return final_arr

    rows = df.rdd.flatMap(lambda x: (x[0], x[1], x[2], FindWords(x[2])))

    schema = ["channel", "datetime", "frametext", "words"]
    df2 = rows.toDF(schema=schema).show(10)

    #===================================================================================================================
    # regular expression
    # reg_exp = r'\s' + word + r'\s|\S'\
    # re.findall(reg_exp, строка во frametext???????????, flags = re.IGNORECASE)#	Не различать заглавные и маленькие буквы, говорят, медленнее, но удобно

    # re.findall([(r'\s') + WORD + (r'\s|\S')], row, flags = re.IGNORECASE)
    # а WORD нужно достать из vocabulary

















