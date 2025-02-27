from typing import Optional, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DataType, StructType


def read_data(spark: SparkSession, path_to_file: str,
              format: str = "csv", schema: Optional[Union[str, StructType]] = None) -> DataFrame:
    """
    Reads data from a file and returns a Spark DataFrame with cleaned column names.

    :param spark: SparkSession instance.
    :param path_to_file: Path to the source file.
    :param format: Format of the file ('csv' or 'json'). Defaults to 'csv'.
    :param schema: Optional schema definition as a StructType or string.
    :return: A Spark DataFrame with formatted column names.
    """

    reader = spark.read.format(format)

    if schema:
        reader = reader.schema(schema)

    if format == "csv":
        df = reader.option("header", "true").load(path_to_file)
    else:
        df = reader.load(path_to_file)

    # Standardizing column names: lowercase, replacing spaces and slashes with underscores
    new_columns = [col.lower().replace(" ", "_").replace("/", "_") for col in df.columns]
    df = df.toDF(*new_columns)

    return df


def main():
    pass


if __name__ == "__main__":
    main()
