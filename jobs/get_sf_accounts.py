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
from pyspark.sql.functions import col, concat_ws, lit, concat

from dependencies.spark import start_spark
from dependencies.data_eng import extract_data_module, load_data, transform_data, read_from_postgres, get_sf_df, convert_simple_salesforce_ordered_dictionary_to_pandas_dataframe
from dependencies.data_eng import get_chd_file, geocode_address, geocode_address_udf, get_data_from_csv, match_salesforce_to_chd
import argparse
from simple_salesforce import Salesforce
import pandas as pd
import json
# from os import environ, listdir, path
import os

def main():
    """Main ETL script definition.

    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--sf_user', dest='sf_user', help='SF user')
    parser.add_argument('--sf_password', dest='sf_password', help='SF password')
    parser.add_argument('--sf_token',dest='sf_token',help='SF token')
    known_args = parser.parse_args()

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')
    sf_user = known_args.sf_user
    sf_password = known_args.sf_password
    salesforce = Salesforce(
        username=sf_user,
        password=sf_password,
        security_token='')
    
    # query = "select id, name, annual_sales__c,average_check__c, chain_id__c, chain_name__c, chd_id__c, confidence_level__c, county__c,credit_rating__c,dayparts__c ,dma_name__c ,group_health_system__c ,hours__c ,location_name__c ,menu_items__c ,msa_name__c ,number_of_employees__c ,number_of_rooms__c ,number_of_seats__c ,operation_type__c ,parent_id__c,phone ,units__c ,website,years_in_business__c,yelp_url__c,chd_chain_id__c,Google_Place_ID__c,Qualification_Status__c,current_month_match__c,CustomerStatus__c, ShippingCity,ShippingLatitude,ShippingLongitude,ShippingPostalCode,ShippingState,ShippingStreet,market_segment_list__c,Current_Chd_Name__c, Data_Update_Case__c, exclude_from_chd_match__c, Current_Chd_Shipping_Street__c, Current_Chd_Shipping_City__c, Current_Chd_Shipping_State__c,Current_Chd_Shipping_Postal_Code__c from Account"
    query = "SELECT id, chd_id__c, Google_Place_ID__c from Account" #Google_Place_ID__c
    # accounts_spark = get_sf_df(query, salesforce, spark)

    query_to_geocode = "SELECT id, Name, ShippingAddress from Account where chd_id__c = null AND google_place_id__c = null"
    accounts_to_geocode_list = salesforce.query_all(query_to_geocode)
    accounts_to_geocode_records = accounts_to_geocode_list['records']
    accounts_to_geocode_pdf = pd.DataFrame(accounts_to_geocode_records)
    # accounts_to_geocode_pdf = convert_simple_salesforce_ordered_dictionary_to_pandas_dataframe(accounts_to_geocode_records)
    accounts_to_geocode_pdf['parsed_address'] = accounts_to_geocode_pdf['ShippingAddress'].apply(
        lambda x: json.dumps(x)
    )
    accounts_to_geocode_pdf = accounts_to_geocode_pdf.drop(["attributes", "ShippingAddress"], axis=1)
    accounts_to_geocode_spark = spark.createDataFrame(accounts_to_geocode_pdf)
    accounts_to_geocode_spark.printSchema()
    print(accounts_to_geocode_spark.count())
    accounts_to_geocode_spark.write.parquet('tests/chd/sf_accounts_to_geocode', mode='overwrite')

    accounts = salesforce.query_all(query)
    accounts_pandas = pd.DataFrame(accounts['records'])
    
    accounts_spark = spark.createDataFrame(accounts_pandas)
    accounts_spark.printSchema()
    accounts_spark.write.parquet('tests/chd/sf_accounts', mode='overwrite')

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
