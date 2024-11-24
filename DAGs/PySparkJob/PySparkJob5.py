import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights_df = (
        spark
        .read
        .parquet(flights_path)
    )

    airlines_df = (
        spark
        .read
        .parquet(airlines_path)
    )

    flights_df.show()

    dashboard_df = (
        flights_df
        .withColumnRenamed("AIRLINE", "IATA_CODE")
        .join(airlines_df, ["IATA_CODE"], "inner")
        .groupBy(f.col("AIRLINE").alias("AIRLINE_NAME"))
        .agg(
            f.sum(
                f.when((f.col("DIVERTED") == f.lit(0)) & (f.col("CANCELLED") == f.lit(0)), 1)
                .otherwise(0))
            .alias("correct_count"),
            f.sum(
                f.when((f.col("DIVERTED") == f.lit(1)), 1)
                .otherwise(0))
            .alias("diverted_count"),
            f.sum(
                f.when((f.col("CANCELLED") == f.lit(1)), 1)
                .otherwise(0))
            .alias("cancelled_count"),
            f.avg(f.col("distance")).alias("avg_distance"),
            f.avg(f.col("air_time")).alias("avg_air_time"),
            f.sum(
                f.when((f.col("CANCELLATION_REASON") == f.lit("A")), 1)
                .otherwise(0))
            .alias("airline_issue_count"),
            f.sum(
                f.when((f.col("CANCELLATION_REASON") == f.lit("B")), 1)
                .otherwise(0))
            .alias("weather_issue_count"),
            f.sum(
                f.when((f.col("CANCELLATION_REASON") == f.lit("C")), 1)
                .otherwise(0))
            .alias("nas_issue_count"),
            f.sum(
                f.when((f.col("CANCELLATION_REASON") == f.lit("D")), 1)
                .otherwise(0))
            .alias("security_issue_count")
        )
    )

    dashboard_df.show()

    (
        dashboard_df
        .repartition(1)
        .write
        .parquet(result_path)
    )


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
