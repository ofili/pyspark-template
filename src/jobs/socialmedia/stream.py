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
        spark.readStream.csv(config["relative_path"] + config["input_path"], schema=schema)
    )
    return data


def filter_fb_data(spark: SparkSession, df: DataFrame, config: Dict):
    fb_data = df.filter(df['app'] == 'FB')
    fb_avg_time = fb_data.groupBy('user_id').agg(f.avg('time_spent_in_seconds'))
    fb_query = (
        fb_avg_time.writeStream.
        queryName('fb_query')
        .outputMode('complete')
        .format('parquet')
        .option("path", config["output_path"])
        .start()
    )
    spark.sql("select * from fb_query").toPandas()


def run(spark, config, logger):
    df = extract(spark, config, logger)
    filter_fb_data(spark, df, config)
    logger.info("Pipeline completed")
    return True
