import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
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

    airports_df = (
        spark
        .read
        .parquet(airports_path)
        .cache()
    )

    dashboard_df = (
        flights_df.alias("FLIGHTS")
        .join(airlines_df, f.col("FLIGHTS.AIRLINE") == f.col("IATA_CODE"), "inner")
        .join(airports_df.alias("ORIGIN"), f.col("FLIGHTS.ORIGIN_AIRPORT") == f.col("ORIGIN.IATA_CODE"), "inner")
        .join(airports_df.alias("DESTINATION"), f.col("FLIGHTS.DESTINATION_AIRPORT") == f.col("DESTINATION.IATA_CODE"),
              "inner")
        .select(
            f.col("FLIGHTS.AIRLINE").alias("AIRLINE_NAME"),
            f.col("FLIGHTS.TAIL_NUMBER").alias("TAIL_NUMBER"),
            f.col("ORIGIN.COUNTRY").alias("ORIGIN_COUNTRY"),
            f.col("ORIGIN.AIRPORT").alias("ORIGIN_AIRPORT_NAME"),
            f.col("ORIGIN.LATITUDE").alias("ORIGIN_LATITUDE"),
            f.col("ORIGIN.LONGITUDE").alias("ORIGIN_LONGITUDE"),
            f.col("DESTINATION.COUNTRY").alias("DESTINATION_COUNTRY"),
            f.col("DESTINATION.AIRPORT").alias("DESTINATION_AIRPORT_NAME"),
            f.col("DESTINATION.LATITUDE").alias("DESTINATION_LATITUDE"),
            f.col("DESTINATION.LONGITUDE").alias("DESTINATION_LONGITUDE"),
        )
    )

    dashboard_df.show()

    (
        dashboard_df
        .repartition(1)
        .write('overwrite')
        .parquet(result_path)
    )


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet',
                        help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
