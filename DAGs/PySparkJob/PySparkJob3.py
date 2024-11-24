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

    # (flights_df
    #  .orderBy("ORIGIN_AIRPORT", "DESTINATION_AIRPORT")
    #  .show())

    top_flights_df = (
        flights_df
        .filter(f.col("DEPARTURE_DELAY") > 1000)
        .groupBy("ORIGIN_AIRPORT")
        .agg(
            f.avg(f.col("DEPARTURE_DELAY")).alias("avg_delay"),
            f.min(f.col("DEPARTURE_DELAY")).alias("min_delay"),
            f.max(f.col("DEPARTURE_DELAY")).alias("max_delay"),
            f.corr(f.col("DEPARTURE_DELAY"), f.col("DAY_OF_WEEK")).alias("corr_delay2day_of_week"))
    )

    top_flights_df.show()

    (
        top_flights_df
        .repartition(1)
        .write('overwrite')
        .parquet(result_path)
    )


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
