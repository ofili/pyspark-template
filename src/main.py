import argparse
import importlib
import json
import os
import sys
from typing import Tuple, Dict

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


def get_config(path, job):
    try:
        filepath = f"{path}/{job}/resources/configs/config.json"
        with open(filepath, encoding='utf-8') as json_file:
            config = json.loads(json_file.read())
        config['relative_path'] = path
        return config
    except FileNotFoundError:
        raise FileNotFoundError("file not found")


def parse_job_args(job_args) -> Dict:
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
        usage="""--job wordcount --job-args libs=libs.zip dep=jobs.zip"""
    )
    parser.add_argument('--job', help='job name', dest='job_name', required=True)
    # parser.add_argument('--conf-file', help='Config file path', required=False)
    parser.add_argument('--job-args', help='Dynamic job arguments', required=False, nargs='*')

    args = parser.parse_args()
    print("Called arguments: %s" % args)

    job_name = args.job_name
    spark, log = create_spark_session(job_name)

    # config_file = args.conf_file if args.conf_file else "src/jobs/resources/configs/config.json"
    # config_dict = get_config(config_file)

    environment = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    }

    if args.job_args:
        job_args = parse_job_args(args.job_args)

    log.info('\nRunning job %s...\nenvironment is %s\n' % (args.job_name, environment))
    os.environ.update(environment)

    module_name = f"jobs.{args.job_name}.{args.job_name}"
    module = importlib.import_module('jobs.%s' % args.job_name)
    result = module.run(spark, get_config(args.res_path, args.job_name))
