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

    def FindWords(row):
        final_arr = []
        for word in vocabulary_bs.value:
            final_arr.append(re.findall(r'\s' + word + r'\s|\S', str(row), flags=re.IGNORECASE)) # re.findall возвращает список строк
        return final_arr
    # FindWords returns list of strings, but I need just one particular word in the last column instead
    # File "E:\intern\task_4\sorting_DF\main.py", line 23, in FindWords
    # TypeError: can only concatenate str (not "Row") to str

    rows = df.rdd.flatMap(lambda x: (x[0], x[1], x[2], FindWords(x[2])))

    schema = ["channel", "datetime", "frametext", "word"]
    df2 = rows.toDF(schema=schema).show(10)


















