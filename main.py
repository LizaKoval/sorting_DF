import findspark
import pyspark
import re
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


    def FindWords(channel, datetime, row):
        final_arr = []

        for word in vocabulary_bs.value:

            print("==========================================================")
            reg_exp = '\b' + word + '\b'
            temp = re.findall(reg_exp, row, flags=re.IGNORECASE)
            print(type(temp))
            final_arr.append(re.findall(reg_exp, row, flags=re.IGNORECASE))  # re.findall возвращает список строк

        result_arr = []
        for value in final_arr:
            result_arr.append((value, row, datetime, channel))
        return result_arr

    rows = df.rdd.flatMap(lambda x: FindWords(str(x[0]), str(x[1]), str(x[2])))

    schema = ["channel", "datetime", "frametext", "word"]
    df2 = rows.toDF(schema=schema).show(10)


















