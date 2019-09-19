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
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, lit

from dependencies.spark import start_spark
from dependencies.data_eng import extract_data_module, load_data, transform_data, read_from_postgres
from dependencies.data_eng import get_chd_file, geocode_address, geocode_address_udf
import argparse

def main():
    """Main ETL script definition.

    :return: None
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--ftp_user',dest='ftp_user',help='FTP user name')
    parser.add_argument('--ftp_password',dest='ftp_password',help='FTP Password')
    parser.add_argument('--api_key',dest='api_key',help='Google Maps API Key')
    
    known_args = parser.parse_args()

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data_module(spark) # extract_data(spark)
    customer_data = read_from_postgres(
        spark, "localhost",
        "golang_user",
        "go",
        "customer_price_list"
        )
    print(customer_data.show())
    print(data.show())
    data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data_transformed)

    """Start the geocoding portion
    """
    address1 = Row(id='123456', address='14 Maryland Blvd Toronto')
    address2 = Row(id='789012', address='Suite 2300 100 Wellington St West Toronto')
    address3 = Row(id='345678', address='10 Bay Street Toronto')
    address4 = Row(id='901234', address='373 Glebeholme Blvd Toronto')
    addresses = [address1, address2, address3, address4]
    address_df = spark.createDataFrame(addresses)
    geo_enriched_data = address_df.withColumn(
        "PlaceID", geocode_address_udf(col("address"), lit(known_args.api_key))
        )
    print(geo_enriched_data.show())
    file_name = get_chd_file(known_args.ftp_user, known_args.ftp_password)
    print(file_name)
    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='14 Maryland Blvd', floor=1),
        Row(id=2, first_name='Dan', second_name='16 Maryland Blvd', floor=1),
        Row(id=3, first_name='Alex', second_name='100 Wellington St West Floor 2300 Toronto', floor=2),
        Row(id=4, first_name='Ken', second_name='373 Glebehome Blvd Toronto', floor=2),
        Row(id=5, first_name='Stu', second_name='75 Main Street Hamilton', floor=3),
        Row(id=6, first_name='Mark', second_name='1 University Drive Regina', floor=3),
        Row(id=7, first_name='Phil', second_name='19 1st Street North Saskatoon', floor=4),
        Row(id=8, first_name='Kim', second_name='12 Suter Ave', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
