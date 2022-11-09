from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f


def extract(spark: SparkSession, config: Dict, logger) -> DataFrame:
    schema = (
        StructType()
        .add("user_id", "string")
        .add("app", "string")
        .add("time_spent_in_seconds", "integer")
        .add("user_agent", "string")
        .add("age", "integer")
    )
    logger.info("Reading data")
    data = (
        spark.readStream.csv(config.get("input_path"), schema=schema)
    )
    return data


def join_df(spark, data_df):
    app_df = spark.createDataFrame(
        [
            ('FB', 'Facebook'), ('Insta', 'Instagram'), ('Twitter', 'Twitter'),
            ('WA', 'WhatsApp'), ('TikTok', 'TikTok'), ('Sig', 'Signal'),
            ('BSky', 'Bluesky'), ('Snap', 'Snapchat')
        ],
        ['app', 'app_name']
    )
    df = data_df.join(app_df, 'app')

    df.createOrReplaceTempView("smTable")

    total_time_spent = spark.sql("select app, sum(time_spent_in_seconds) time_spent_in_seconds from smTable "
                                 "where app is not null group by app "
                                 "order by time_spent_in_seconds desc")
    total_time_spent.show()

    avg_time_spent = spark.sql(
        "select app, avg(time_spent_in_seconds) avg_time_spent_in_seconds from smTable "
        "where app is not null group by app "
        "order by time_spent_in_seconds desc"
    )
    avg_time_spent.show()

    return df


def filter_fb_data(spark: SparkSession, df: DataFrame, config: Dict):
    fb_data = df.filter(df['app'] == 'FB')
    fb_avg_time = fb_data.groupBy('user_id').agg(f.avg('time_spent_in_seconds'))
    fb_query = (
        fb_avg_time.writeStream.
        queryName('fb_query')
        .outputMode('complete')
        .format('memory')
        .option("path", config["output_path"])
        .start()
    )
    spark.sql("select * from fb_query").toPandas()


def run(spark, config, logger):
    data = extract(spark, config, logger)
    df = join_df(spark, data)
    filter_fb_data(spark, df, config)
    logger.info("Pipeline completed")
    return True
