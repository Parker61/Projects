import argparse
import os
os.environ['SPARK_VERSION'] = '3.1'

import pydeequ
from pydeequ.analyzers import *
from pyspark.sql import SparkSession


def process(spark, data_path, report_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param report_path: путь сохранения отчета
    """
    # TODO Ваш код


def main(data_path, report_path):
    spark = _spark_session()
    process(spark, data_path, report_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession \
        .builder.appName('PySparkAnalyzer') \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='data.parquet', help='Please set datasets path.')
    parser.add_argument('--report_path', type=str, default='report', help='Please set target report path.')
    args = parser.parse_args()
    data_path = args.data_path
    report_path = args.report_path
    main(data_path, report_path)
