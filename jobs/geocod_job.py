"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).

pyspark-example-project michaelyoung$ spark-submit --packages com.springml:spark-salesforce_2.11:1.1.0 --driver-class-path ../../Drivers/postgres/postgresql-42.2.6.jar  --py-files packages.zip --files configs/etl_config.json jobs/geocod_job.py --ftp_user=<user>  --ftp_password=<password> --api_key=<key>

"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit, concat, from_json

from dependencies.spark import start_spark
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from dependencies.data_eng import extract_data_module, load_data, transform_data, read_from_postgres
from dependencies.data_eng import get_chd_file, geocode_address, geocode_address_udf, get_data_from_csv, match_salesforce_to_chd
import argparse
from simple_salesforce import Salesforce
import pandas as pd
# from os import environ, listdir, path
import os

def main():
    """Main ETL script definition.

    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--ftp_user', dest='ftp_user', help='FTP user name')
    parser.add_argument('--ftp_password', dest='ftp_password', help='FTP Password')
    parser.add_argument('--api_key', dest='api_key', help='Google Maps API Key')
    parser.add_argument('--sf_user', dest='sf_user', help='SF user')
    parser.add_argument('--sf_password', dest='sf_password', help='SF password')
    # parser.add_argument('--sf_token',dest='sf_token',help='SF token')
    known_args = parser.parse_args()

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    #Define SF address schema
    address_schema = StructType([
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("geoCodeAccuracy", StringType()),
        StructField("latitude", FloatType()),
        StructField("longitude", FloatType()),
        StructField("postalCode", StringType()),
        StructField("state", StringType()),
        StructField("street", StringType())
        ])
    #start gecode job
    # accounts_spark = spark.read.parquet('tests/chd/sf_accounts')
    sf_accounts_with_ids = spark.read.parquet('tests/chd/sf_accounts')

    sf_accounts_to_geocode = spark.read\
        .parquet('tests/chd/sf_accounts_to_geocode')\
        .withColumn("json_address", from_json(col("parsed_address"), address_schema))
    sf_accounts_to_geocode = sf_accounts_to_geocode.withColumn(
        "full_address",
        concat(
            col("Name"), lit(" "), # TODO: I might be able to remove the address data here because it's capture in the name?
            col("json_address.street"), lit(" "),
            col("json_address.city"), lit(" "),
            col("json_address.state"), lit(" "),
            col("json_address.country"), lit(" "),
            col("json_address.postalCode")
        )
    )
    sf_accounts_to_geocode.show(10, False)
    # accounts_no_chd_OR_google = accounts_spark.where((col('CHD_Id__c').isNull()) & (col('Google_Place_ID__c').isNull()))

    chd_data = get_data_from_csv(
        'tests/chd/birch_hill_equity_us_chd_data_delivery_file.csv', \
        spark,
        ";")

    new_chd_records = chd_data.join(
        sf_accounts_with_ids, chd_data.chd_id == sf_accounts_with_ids.CHD_Id__c, how='left_anti'
        )
    num_new_chd = new_chd_records.count()

    print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    print(num_new_chd)
    # chd_data.printSchema()
    # chd_data = chd_data #.limit(100)
    # chd_data = chd_data\
    #     .withColumn("full_address",
    #         concat(
    #             col("chd_company_name"), lit(" "),
    #             col("chd_address"), lit(" "),
    #             col("chd_city"), lit(" "),
    #             col("chd_state"), lit(" "),
    #             col("chd_zip")
    #         )
    #     )
    # match_df = match_salesforce_to_chd(accounts_spark, chd_data)
    # num_chd = chd_data.count()
    # num_sf = accounts_spark.count()
    # num_matches = match_df.count()
    # print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    # print("THERE ARE {0} accounts matched out of {1} Salesforce and {2} CHD".format(num_matches, num_sf, num_chd))
    # print(match_df.show(30))
    # print(chd_data.select("chd_company_name", "chd_id", "full_address").show(10, False))
    # sf_geocoded = sf_accounts_to_geocode.select("Id", "Name", "full_address")\
    #     .withColumn(
    #         "place_id", geocode_address_udf(col("full_address"))
    #     )
    # # print(sf_geocoded.select("id", "full_address", "place_id").show(10, False))
    # sf_geocoded.write.parquet('tests/chd/sf_geocoded', mode='overwrite')

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()