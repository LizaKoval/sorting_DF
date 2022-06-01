import findspark
import pyspark
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, create_map
from pyspark.sql.types import StructType, StructField, MapType, TimestampType, StringType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
            .appName("Dataframes")\
            .getOrCreate()

    temp_df = spark.read.json('test')
    temp_df.printSchema()
    df = temp_df.select('channel.name','datetime','frame_text')
    df.show(3)
    df.printSchema()

    bin_df = spark.read.csv("words").collect()
    vocabulary_bs = spark.sparkContext.broadcast(bin_df)


    def FindWords(channel, datetime, row):
        final_arr = []

        for word in vocabulary_bs.value:
            reg_exp = r'\b' + word[0] + r'\b'
            final_arr.extend(re.findall(reg_exp, row[0], flags=re.IGNORECASE))  # re.findall возвращает список строк

        result_arr = []
        for value in final_arr:
            result_arr.append((channel, datetime, value, row[0]))
        return result_arr
#
    rows = df.rdd.flatMap(lambda x: FindWords(x[0], x[1], x[2]))

    df_with_words = rows.toDF(['channel_name', 'datetime', 'word', 'row'])

    df_with_words.printSchema()
    #df_with_words.show(3)
    #print(df_with_words.count())
    df_with_words.createOrReplaceTempView("words")
    df_word_channel_mentions = spark.sql("select words.word as word, words.channel_name as channel_name, count(words.word) as ment_by_channel_times from words group by word, channel_name order by words.word")
    df_word_channel_mentions.createOrReplaceTempView("words")
    df_with_arr = df_word_channel_mentions.withColumn("arr", create_map(
        lit("channel_name"), col("channel_name"),
        lit("ment_times"), col("ment_by_channel_times")
        )).drop("channel_name", "ment_by_channel_times")
    df_with_arr.printSchema()
    df_with_arr.createOrReplaceTempView("words")
    # df_with_arr.show(3, truncate=False)
    df_word_arr = spark.sql(" select words.word, array(words.ment_times) from words group by words.word")
    df_word_arr.printSchema()
    df_word_arr.show(3, truncate=False)
    #df_with_arr.show(3, truncate=False)

    # df_with_array = spark.sql("select words.word, json_object('channel_name' VALUE 'words.channel_name', 'ment_times' VALUE 'words.ment_by_channel_times' ) from words")
    # df_with_array.printSchema()
    # df_with_array.show(3, truncate=False)





    # PySpark MapType (also called map type)
    # is a data type to represent Python Dictionary (dict) to store key-value pair

    #PySpark SQL function create_map() is used
    # to convert selected DataFrame columns to MapType,
    # create_map() takes a list of columns you wanted
    # to convert as an argument and returns a MapType column.


    # schema = StructType([StructField("channel_name", StringType(), True),
    #                      StructField("datetime", TimestampType(), False),
    #                      StructField("word", StringType(), True),
    #                      StructField("row", StringType(), True)])





















