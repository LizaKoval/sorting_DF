from select import select

import findspark
import pyspark
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, create_map, collect_set, to_json, collect_list, udf, map_values
from pyspark.sql.types import StructType, StructField, MapType, TimestampType, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]") \
            .appName("Dataframes")\
            .getOrCreate()

    temp_df = spark.read.json('test')
    temp_df.printSchema()
    df = temp_df.select('channel.name','datetime','frame_text')
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
    df_with_words.createOrReplaceTempView("words")
    df_word_channel_mentions = spark.sql("select words.word as word, words.channel_name as channel_name, count(words.word) as ment_by_channel_times from words group by word, channel_name order by words.word")

    df_with_arr = df_word_channel_mentions.withColumn("arr", create_map(
        lit("channel_name"), col("channel_name"),
        lit("ment_times"), col("ment_by_channel_times")
        )).drop("channel_name", "ment_by_channel_times")
    df_with_arr.printSchema()

    df_with_arr.groupBy('word')\
        .agg(collect_list('arr'))\
        .alias("ment_by_channel")\
        #.show(100, truncate=False)
    df_with_arr.printSchema()

    df_temp = df_with_arr.select("*")\
        .where(col("word") =='PHONE')\
        .groupBy('word')\
        .sum(select("arr.ment_times"))

    #df_with_arr.withColumn("ment_by_channel", )







    # def total_mentions(word, info_dict):
    #     temp = list(map(lambda x: map_values(x), info_dict))
    #     result = sum(map(lambda x: int(x[1]), temp))
    #     return [word, info_dict, result]
    #
    # df_result = df_with_arr.rdd.map(lambda x: total_mentions(x[0], x[1])).toDF(['word', 'info', 'total_mentions']).show(100, truncate=False )


    #df_with_arr.withColumn('total_mentions', lit(total_mentions('ment_by_channel'))).show(100)
    df_with_arr.printSchema()




    # =============================================================================== converts maps -> str ятв then aggragetes it into list of stringd
    # df_with_arr.printSchema()
    # df2 = df_with_arr.rdd.map(lambda x: [x[0], repr(x[1])]).toDF(['word', 'str_arr'])
    # df2.printSchema()
    # df2.groupBy('word').agg(collect_set('str_arr')).alias("str_arr")#.show(10, truncate=False)























