"""
data_eng.py
~~~~~~~~

Module containing helper functions for use with Apache Spark
"""


from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession


from pyspark.sql.functions import col, concat_ws, lit, udf
from pyspark.sql.types import StructField, StructType, StringType
from dependencies import logging
import requests
import geocoder
import __main__

def extract_data_module(spark):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .parquet('tests/test_data/employees'))

    return df

def geocode_address(address, api_key):
    geo_address_schema = StructType([StructField("city", StringType()),\
                     StructField("country", StringType()),\
                     StructField("hostname", StringType()),\
                     StructField("ip", StringType()),\
                     StructField("loc", StringType()),\
                     StructField("org", StringType()),\
                     StructField("phone", StringType()),\
                     StructField("postal", StringType()),\
                     StructField("region", StringType())])
    g = geocoder.google(address, key=api_key, exactly_one=True)
    # x = json.dumps(g.raw) #['place_id'] #json.dumps(response.json()) #["org"]
    place_id = g.raw['place_id']
    return place_id
geocode_address_udf = udf(geocode_address, StringType())


def transform_data(df, steps_per_floor_):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
                (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk'))
                )

    return df_transformed
def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None

def get_data_from_csv(file_path, spark):
    """Load data from a csv location

    :param file_path: path the the file[s]
    :param spark: Spark session object

    :return Dataframe
    """
    temp_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(file_path)
    return temp_df

def read_from_postgres(spark, host, user, password, table_name):
    """Function to create a Spark Dataframe from a PostgreSQL table

    :param spark: SparkSession Object
    :param host: databse host
    :param user: JDBC username
    :param password: JDBC password
    :param table_name: Name of table to read from

    :return Dataframe
    """
    df_tmp = spark.read.format('jdbc').options(
        url="jdbc:postgresql://{0}:5432/postgres?user={1}&password={2}"\
            .format(host, user, password),
        database='postgres',
        dbtable='{0}'.format(table_name)
    ).load().cache()
    return df_tmp
