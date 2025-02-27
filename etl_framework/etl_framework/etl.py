from pyspark.sql import SparkSession


def read_data(path_to_file: str, format: str = "csv"):
    """
    :param path_to_file: path to the source file
    :param format: format of the file csv or json
    :return: returns a dataframe
    """

    spark = SparkSession.builder.appName("ETL").getOrCreate()
    if format == "csv":
        df = spark.read.option("header", "true").csv(path_to_file)
    elif format == "json":
        df = spark.read.json(path_to_file)
    else:
        raise ValueError("Invalid format")

    return df


if __name__ == "__main__":
    run_etl()
