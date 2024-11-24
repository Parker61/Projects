import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    flights_df = (
        spark
        .read
        .parquet(flights_path)
    )

    top_flights_df = (
        flights_df
        .filter(f.col("TAIL_NUMBER").isNotNull())
        .groupBy("TAIL_NUMBER")
        .agg(f.count(f.lit("*")).alias("count"))
        .orderBy(f.col("count").desc())
        .limit(10)
    )
    top_flights_df.show()
    # (
    #     top_flights_df
    #     .repartition(1)
    #     .write
    #     .parquet(result_path)
    # )


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
