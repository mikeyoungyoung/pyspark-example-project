"""
data_eng.py
~~~~~~~~

Module containing helper functions for use with Apache Spark
"""


# from os import environ, listdir, path
import os
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from collections import OrderedDict

from pyspark.sql.functions import col, concat_ws, lit, udf
from pyspark.sql.types import StructField, StructType, StringType
from dependencies import logging
import requests
import geocoder
import urllib
import pandas as pd
import json
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

def get_chd_file(username, password):
    """Download new data file from CHD
    :param username: site user name
    :param password: password

    :returns filename
    """
    ftp_url = 'ftp://'+username+':'+password+'@ftp.chdnorthamerica.com/{0}.csv'.format("birch_hill_equity_us_chd_data_delivery_file")
    filename, headers = urllib.request.urlretrieve(ftp_url, 'tests/chd/birch_hill_equity_us_chd_data_delivery_file.csv')
    return filename

def geocode_address(address):
    """If you are getting a fork split error: export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
    """
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    print("Request geocode for address: {}".format(address))
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    with requests.Session() as session:
        try:
        # TODO: Handle None being passed in as an address
            g = geocoder.google(address, key=os.environ['GOOGLE_API_KEY'], exactly_one=True, session=session)
            if g.status == 'OK':
                place_id = g.raw['place_id']
                return place_id
            else:
                return g.status
                # return "no api call"
        except Exception as e:
            return e
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

def get_data_from_csv(file_path, spark, delimeter):
    """Load data from a csv location

    :param file_path: path the the file[s]
    :param spark: Spark session object

    :return Dataframe
    """
    temp_df = spark.read\
        .format('csv')\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("delimiter", delimeter)\
        .load(file_path)
    
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

def match_salesforce_to_chd(sf_df, chd_df):
    """Match two DFs based on CHD id
    """
    temp_df = chd_df.join(sf_df, chd_df.chd_id == sf_df.CHD_Id__c)
    return temp_df

def get_sf_df(query, salesforce, spark):
    """Create a spark dataframe from simple_salesforce query_all
    """
    temp_list = salesforce.query_all(query)

    tmp_pdf = pd.DataFrame(temp_list['records'])
    spark_df = spark.createDataFrame(tmp_pdf)
    return spark_df

def convert_simple_salesforce_ordered_dictionary_to_pandas_dataframe(records):

    # Get records list from the ordered dictionary
    tmp_pdf = pd.Dataframe(records)

    tmp_pdf['parsed_address'] = tmp_pdf['ShippingAddress'].apply(lambda x: json.dumps(x))
    cols_to_drop = ["attributes", "ShippingAddress"]
    tmp_pdf = tmp_pdf.drop(cols_to_drop, axis=1)

    return tmp_pdf