import findspark
import pyspark
import re
from pyspark.sql import SparkSession, functions as sf

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
            .appName("Dataframes")\
            .getOrCreate()

    df = spark.read.json('test')
    df.show(10)
    df.printSchema()

    bin_df = spark.read.csv("words").collect()
    vocabulary_bs = spark.sparkContext.broadcast(set(bin_df))


    def FindWords(channel, datetime, row):
        final_arr = []

        for word in vocabulary_bs.value:
            # reg_exp = r'\b' + word[0] + r'\b'
            # final_arr.extend(re.findall(reg_exp, row[0], flags=re.IGNORECASE))  # re.findall возвращает список строк
            if word[0].lower() in row[0].lower():
                final_arr.append(word[0])

        result_arr = []
        for value in final_arr:
            result_arr.append((value, row[0], datetime, channel))
        return result_arr
#
    rows = df.rdd.flatMap(lambda x: FindWords(x[0], x[1], x[2]))

    df2 = rows.toDF(['word', 'row', 'datetime', 'channel'])
    df2.show(10)
    print(df2.count())


















