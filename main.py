import findspark
import pyspark
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

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

    # schema = StructType([StructField("channel_name", StringType(), True),
    #                      StructField("datetime", TimestampType(), False),
    #                      StructField("word", StringType(), True),
    #                      StructField("row", StringType(), True)])

    df_with_words = rows.toDF(['channel_name', 'datetime', 'word', 'row'])

    df_with_words.printSchema()
    #df_with_words.show(3)
    #print(df_with_words.count())
    df_with_words.createOrReplaceTempView("words")
    #df_result = spark.sql("select count(word) as total_mentions, word as key from words group by word order by word").show(10)
    df_word_channel_mentions = spark.sql("select words.word, words.channel_name, count(words.word) as ment_by_channel_times from words group by word, channel_name").show(10)






















