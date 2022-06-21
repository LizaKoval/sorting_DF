import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, create_map, collect_list, to_timestamp, to_date, hour, struct
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType, MapType, DateType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]") \
            .appName("Dataframes")\
            .getOrCreate()

    temp_df_first = spark.read.json('test')
    bin = temp_df_first.repartition(6)
    df = bin.select('channel.name', 'datetime', 'frame_text')
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

    df_total_mentions = df_with_words.groupBy("word").count()
    df_total_mentions.printSchema()

    df_word_channel_mentions = df_with_words\
        .groupBy('word', 'channel_name')\
        .count()
    print("df_word_channel_mentions")
    df_word_channel_mentions.printSchema()

    df_with_map = df_word_channel_mentions.withColumn("arr", create_map(
        lit("channel_name"), col("channel_name"),
        lit("mentions"), col("count").cast(IntegerType())
        )).drop("channel_name", "count")
    print("df_with_map")
    df_with_map.printSchema()

    ment_by_channel_schema = StructType([
        StructField("word", StringType(), False),
        StructField("arr", ArrayType(MapType(StringType(), IntegerType(), True)))
    ])

    temp_channel_df = df_with_map.rdd\
        .map(lambda x: int(x[1][1]))\
        .toDF(schema=ment_by_channel_schema)
    temp_channel_df.printSchema()

    df_with_list = df_with_map.groupBy('word')\
            .agg(collect_list('arr'))\
            .alias("ment_by_channel")\

    df_with_list.printSchema()


    df_with_date_converted = df_with_words.withColumn('datetime', to_timestamp(col('datetime')))

    df_date = df_with_date_converted.groupBy('word', to_date('datetime')).count()
    df_date.printSchema()

    df_ment_by_hour = df_with_date_converted.groupBy('word', to_date('datetime'), hour('datetime')).count()
    print("df_ment_by_hour")
    df_ment_by_hour.printSchema()

    df_date_with_map = df_ment_by_hour.withColumn('map', create_map(
        lit("hour"), col("hour(datetime)"),
        lit("mentions"), col("count")
    )).drop('hour(datetime)', 'count')
    print("df_date_with_map")
    df_date_with_map.printSchema()

    temp_rdd = df_date_with_map.rdd
    schema_date = StructType([
        StructField("word", StringType(), False),
        StructField("date", DateType(), False),
        StructField("mentions_by_hour", MapType(StringType(), IntegerType(), True))
    ])
    temp_df = temp_rdd.toDF(schema=schema_date)

    df_date_with_datelist = temp_df.groupBy(temp_df.columns[:2])\
         .agg(collect_list('mentions_by_hour')).alias("mentions_by_hour")

    df_date_join = df_date_with_datelist.join(df_date, ["word"])
    df_date_join.printSchema()

    df_res_before_last_struct = df_date_join.drop(df_date_join.columns[3])
    df_res_before_last_struct.printSchema()

    df_with_mentions_by_date = df_res_before_last_struct.withColumn("mentions_by_date",
        struct(col("date").alias("date"),
               col("collect_list(mentions_by_hour)").alias("mentions_by_hour"),
               col("count").alias("mentions")
               )
    ).drop("date", "collect_list(mentions_by_hour)", "count")
    df_with_mentions_by_date.printSchema()

    df_with_mentions_by_date_list = df_with_mentions_by_date.groupBy("word")\
        .agg(collect_list('mentions_by_date'))
    df_with_mentions_by_date_list.printSchema()

    df_almost_result_join = df_with_list.join(df_with_mentions_by_date_list, ["word"])
    df_almost_result_join.printSchema()

    df_result_join = df_almost_result_join.join(df_total_mentions, ["word"])
    df_result_join.printSchema()

    rdd_result = df_result_join.rdd

    schema = StructType([
        StructField("key", StringType(), False),
        StructField("mentions_by_channel", ArrayType(MapType(StringType(), StringType(), True))),
        StructField("mentions_by_date",ArrayType(
            StructType([
            StructField("date", DateType(), False),
            StructField("mentions_by_hour", ArrayType(MapType(StringType(), IntegerType(), True))),
            StructField("mentions", IntegerType(), True)
        ]))),
        StructField("total_mentions", IntegerType(), False)
    ])

    df_result = rdd_result.toDF(schema=schema)
    df_result.printSchema()
    df_result.coalesce(1).write.format("json").save("output")


















