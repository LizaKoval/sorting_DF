import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, create_map, collect_set, to_json, collect_list, udf, map_values, \
    to_timestamp, to_date, hour
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType, MapType, TimestampType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]") \
            .appName("Dataframes")\
            .getOrCreate()

    temp_df_first = spark.read.json('test')
    temp_df = temp_df_first.repartition(6)
    temp_df.printSchema()
    df = temp_df.select('channel.name','datetime','frame_text')
    df.printSchema()

    bin_df = spark.read.csv("words").collect()
    vocabulary_bs = spark.sparkContext.broadcast(bin_df)

    def FindWords(channel, datetime, row):
        final_arr = []

        for word in vocabulary_bs.value:
            reg_exp = r'\b' + word[0] + r'\b'
            final_arr.extend(re.findall(reg_exp, row[0], flags=re.IGNORECASE))

        result_arr = []
        for value in final_arr:
            result_arr.append((channel, datetime, value, row[0]))
        return result_arr

    rows = df.rdd.flatMap(lambda x: FindWords(x[0], x[1], x[2]))
    df_with_words = rows.toDF(['channel_name', 'datetime', 'word', 'row'])
    df_with_words.printSchema()
    #
    # df_total_mentions = df_with_words.groupBy("word").count()
    # df_total_mentions.printSchema()
    #
    # df_word_channel_mentions = df_with_words.select("word","channel_name")\
    #     .groupBy('word', 'channel_name')\
    #     .count()
    # df_word_channel_mentions.printSchema()
    #
    # df_with_map = df_word_channel_mentions.withColumn("arr", create_map(
    #     lit("channel_name"), col("channel_name"),
    #     lit("mentions"), col("count")
    #     ))
    # df_with_map.printSchema()
    #
    # df_with_list = df_with_map.groupBy('word')\
    #         .agg(collect_list('arr'))\
    #         .alias("ment_by_channel")\
    #
    # df_with_list.printSchema()

    df_with_date_converted = df_with_words.withColumn('datetime', to_timestamp(col('datetime')))
    df_with_date_converted.printSchema()

    df_date = df_with_date_converted.groupBy('word', to_date('datetime')).count()
    #df_date.printSchema()

    df_ment_by_hour = df_with_date_converted.groupBy('word', to_date('datetime'), hour('datetime')).count()
    #df_ment_by_hour.printSchema()

    df_date_with_map = df_ment_by_hour.withColumn('map', create_map(
        lit("hour"), col("hour(datetime)"),
        lit("mentions"), col("count")
    )).drop('hour(datetime)', 'count')
    df_date_with_map.printSchema()


    df_date_with_datelist = df_date_with_map.groupBy(df_date_with_map.columns[:2])\
         .agg(collect_list('map')).alias("mentions_by_hour")

    df_date_with_datelist.show(100, truncate=False)





    # df_join = df_with_list.join(df_total_mentions, ["word"])
    # df_join.printSchema()

    # schema = StructType([
    #     StructField("key", StringType(), False),
    #     StructField("mentions_by_channel", ArrayType(MapType(StringType(), StringType(), True))),
    #     StructField("total_mentions", IntegerType(), True)
    # ])
    #
    # rdd_result = df_join.rdd
    # df_result = rdd_result.toDF(schema=schema)
    #df_result.printSchema()
    #df_result.coalesce(1).write.format("json").save("output")


















