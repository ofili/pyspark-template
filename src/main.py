import argparse
import importlib
import json
import os
import sys
from typing import Tuple, Dict, Any

from pyspark.sql import SparkSession
from setuptools.command.setopt import config_file

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


def get_config(filename: str) -> Dict:
    try:
        with open(filename) as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        raise FileNotFoundError("%s not found" % filename)
    

def parse_job_args(job_args: str) -> Dict:
    return {a.split('=')[0]: a.split('=')[1] for a in job_args}


def create_spark_session(job_name) -> Tuple:
    spark = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    app_id: str = spark.conf.get('spark.app.id')
    log4j = spark._jvm.org.apache.log4j
    message_prefix = f"< {job_name} {app_id} >"
    logger = log4j.LogManager.getLogger(message_prefix)
    return spark, logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Job submitter",
        usage="""--job module_name --job-args libs=libs.zip dep=jobs.zip"""
    )
    parser.add_argument('--job', type=str, dest='job_name', required=True,
                        help='The name of the spark job you want to run')
    parser.add_argument('--conf-file', required=False, help='Path to config file')
    parser.add_argument('--job-args', type=str, nargs="*",
                        help='Path to the jobs resources',)

    args = parser.parse_args()
    print("Called arguments: %s" % args)

    job_name = args.job_name
    log: Any
    spark, log = create_spark_session(job_name)
    
    config_file: Any = args.conf_file if args.conf_file else '/spark-streaming/src/jobs/resources/configs/config.json'
    config_dict = get_config(config_file)
    
    if args.job_args:
        job_args = parse_job_args(args.job_args)
        config_dict.update(job_args)

    log.info('\nRunning job %s...\nenvironment with %s\n' % (args.job_name, config_dict))

    module_name = f"jobs.{args.job_name}.{args.job_name}"
    module = importlib.import_module('jobs.socialmedia.%s' % args.job_name)
    res = module.run(spark, config_dict, log)
    
    print(f'[JOB {args.job_name} RESULT]: {res}')
