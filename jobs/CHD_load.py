# Databricks notebook source
# MAGIC %md 
# MAGIC ## Script to Update Cozzini Salesforce with Updated CHD data every month

# COMMAND ----------

import googlemaps
import time
import datetime
import json
import pandas as pd
import os
import urllib
import calendar
import pyspark
from google.cloud import storage
from pyspark.sql import functions as F
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType, StructType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lower
from pyspark.sql.functions import col,lit,concat
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark.sql.types import *
import pyarrow as pa
import pyarrow.parquet as pq
import re
current_day = datetime.datetime.today()
ts = time.time()
date_time = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H-%M-%S')
month_year = datetime.datetime.fromtimestamp(ts).strftime('%B%Y')
print(date_time)
print(month_year)

# Change this for the delivery file name every month
# Datetime changes every month. Ex: filename for june was: birch_hill_equity_us_chd_data_delivery_file_20190603
DATE_SUFFIX = ''

sf_user = dbutils.secrets.get(scope = "salesforce_cozzini", key = "username")
sf_password = dbutils.secrets.get(scope = "salesforce_cozzini", key = "password")
chd_ftp_username = dbutils.secrets.get(scope = "chd", key = "username")
chd_ftp_password = dbutils.secrets.get(scope = "chd", key = "password")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/dbfs/keys/cozzini_bq_storage.json"
client = storage.Client()
bucket = client.get_bucket("tracking-update-files")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Write Data to S3

# COMMAND ----------

#change to month directory 
def write_to_s3(write_file_name, input_file):
  '''
  Description: Function that writes a given .csv input file 
  into s3 in parquet format. Note that this function parses the csv file first for delimeter,
   headers, and infers schema. 
  Parameters:
    write_file_name: Name of the file to be saved as
    input_file: Name of the input file
  Returns: Full filepath of where the file is saved
  '''
  try:
    
    if isinstance(input_file, str) and '/dbfs' in input_file:
      input_file = input_file.replace('/dbfs/', '')
    
    saved_file_path = "/mnt/databricks-bhep/chd/{0}/{1}.parquet".format(month_year,write_file_name)
    print('Saving file: {0}'.format(write_file_name))

    if isinstance(input_file, pyspark.sql.dataframe.DataFrame):
      print('Input is a DataFrame')
      input_file.write.parquet(saved_file_path, mode='overwrite')
    elif input_file.endswith('.csv'):
      spark.read.format('csv').option("header","true").option("inferSchema","true").option("delimiter",";").load(input_file).write.parquet(saved_file_path, mode='overwrite')
    elif input_file.endswith('.parquet'):
      sqlContext.read.parquet(input_file).write.parquet(saved_file_path, mode='overwrite')
    else:
      raise Exception("Error saving file to S3. input_file should be a .csv or a .parquet file")
    
    return saved_file_path
  except Exception as e:
    print('Error saving file to S3: {0}'.format(e))
    return None


# COMMAND ----------

# MAGIC %md 
# MAGIC ##Read Data from S3

# COMMAND ----------

def read_from_s3(read_file_path):
  '''
  Description: Function that reads a stored parquet file from s3 
  Parameters:
    read_file_path: Full file path including name of the file exclude .parquet extension
  Returns: Pyspark df of the read parquet file
  '''
  if not '.parquet' in read_file_path:
    return sqlContext.read.parquet(read_file_path+'.parquet')
  else:
    return sqlContext.read.parquet(read_file_path)

# COMMAND ----------

ftp_url='ftp://'+chd_ftp_username+':'+chd_ftp_password+'@ftp.chdnorthamerica.com/birch_hill_equity_us_chd_data_delivery_file{0}.csv'.format(DATE_SUFFIX)
print(ftp_url)
filename, headers = urllib.request.urlretrieve(ftp_url, '/dbfs/birch_hill_equity_us_chd_data_delivery_file{0}.csv'.format(DATE_SUFFIX))
print(filename, headers)
delivery_filepath = write_to_s3('birch_hill_equity_us_chd_data_delivery_file_'+date_time+'_.csv', 'birch_hill_equity_us_chd_data_delivery_file{0}.csv'.format(DATE_SUFFIX))

# COMMAND ----------

# MAGIC %md #Get Data From GCS

# COMMAND ----------

# MAGIC %scala
# MAGIC val google_maps_APIKey = dbutils.secrets.get(scope = "google", key = "maps_enterprise")

# COMMAND ----------

# MAGIC %md #Load the CHD data that we got from GCS bucket

# COMMAND ----------

chdUpdateFile = read_from_s3(delivery_filepath)
chdUpdateFile = chdUpdateFile.na.fill('')
chdUpdateFile = chdUpdateFile.withColumn('chd_market_segment', F.regexp_replace('chd_market_segment', 'FSR - CAFÉ-RESTAURANTS, FAMILY-STYLE, DINER', 'FSR - CAFE-RESTAURANTS, FAMILY-STYLE, DINER'))

# COMMAND ----------

# MAGIC %md # Get Salesforce accounts from salesforce springml connector

# COMMAND ----------

accounts_soql = "select id, name, annual_sales__c,average_check__c, chain_id__c, chain_name__c, chd_id__c, confidence_level__c, county__c,credit_rating__c,dayparts__c ,dma_name__c ,group_health_system__c ,hours__c ,location_name__c ,menu_items__c ,msa_name__c ,number_of_employees__c ,number_of_rooms__c ,number_of_seats__c ,operation_type__c ,parent_id__c,phone ,units__c ,website,years_in_business__c,yelp_url__c,chd_chain_id__c,Google_Place_ID__c,Qualification_Status__c,current_month_match__c,CustomerStatus__c, ShippingCity,ShippingLatitude,ShippingLongitude,ShippingPostalCode,ShippingState,ShippingStreet,market_segment_list__c,Current_Chd_Name__c, Data_Update_Case__c, exclude_from_chd_match__c, Current_Chd_Shipping_Street__c, Current_Chd_Shipping_City__c, Current_Chd_Shipping_State__c,Current_Chd_Shipping_Postal_Code__c from Account"

# COMMAND ----------

sfDF = spark.read.\
        format("com.springml.spark.salesforce").\
        option("username", sf_user).\
        option("password", sf_password).\
        option("soql", accounts_soql).\
        option("version", "41.0").\
        load() 

# COMMAND ----------

market_segments = sfDF.groupby('market_segment_list__c').count()
market_segments_list = []
for segments in market_segments.collect():
  market_segments_list.append(segments['market_segment_list__c'].upper())
chd_new_segments = chdUpdateFile.where(~col('chd_market_segment').isin(market_segments_list))
chdUpdateFile = chdUpdateFile.where(col('chd_market_segment').isin(market_segments_list))

# COMMAND ----------

window = Window.partitionBy(chd_new_segments['chd_market_segment']).orderBy(chd_new_segments['chd_id'].desc())
chd_new_segments = chd_new_segments.select('*', rank().over(window).alias('rank')).filter(col('rank') < 2)
write_to_s3('chd_new_segments',chd_new_segments)

# COMMAND ----------

# What's the purpose of moving this around
sf_saved_name = 'sf_saved_at_'+date_time
sfDF.toPandas().to_csv(sf_saved_name)
dbutils.fs.cp("file:/databricks/driver/"+sf_saved_name, "dbfs:/")

# COMMAND ----------

# MAGIC %md Check for duplicates chd id in salesforce

# COMMAND ----------

sfDF_no_chd = sfDF.where(col('chd_id__c') != 'null')
sfDF_chd_count = sfDF_no_chd.groupby('chd_id__c').count()                                                                                     
sfDF_chd_distinct = sfDF_chd_count.where(col('count') == 1)
sfDF_chd_duplicate = sfDF_chd_count.where(col('count') > 1)
duplicate_id = []
for ids in sfDF_chd_duplicate.collect():
  duplicate_id.append(ids['chd_id__c'])

if sfDF_chd_duplicate.count() > 0:
  sfDF_duplicate = sfDF.where(col('chd_id__c').isin(duplicate_id))
  #sfDF_duplicate.toPandas().to_csv('sfDF_duplicates_chd.csv')
  write_to_s3('sfDF_duplicates_chd', sfDF_duplicate)


# COMMAND ----------

sfDF = sfDF.withColumn('Current_Month_Match__c', lit(False))
sfDF = sfDF.replace('null', '')

# COMMAND ----------

# MAGIC %md
# MAGIC # Get new CHD data which has been newly added
# MAGIC 
# MAGIC ## To Verfiy
# MAGIC 1. Which CHD rows to remove prior to comparison, if any - i.e. which rows are manually removed

# COMMAND ----------

df1 = chdUpdateFile.alias("df1")
df2 = sfDF.alias("df2")
excluded_segments = ["LIQUOR STORES", "SECONDARY SCHOOL",
"RETAIL BAKERY, PASTRY",
"PRE-K, KINDERGARTEN, CHILD CARE",
"OTHER FOOD RETAILERS",
"SCHOOL DISTRICTS",
"CONFECTIONERS",
"SOCIAL AND PRIVATE CLUBS, LEGIONS, FRATERNITIES",
"OTHER EDUCATION ESTABLISHMENTS",
"CENTERS FOR CHILDREN, SUMMER CAMPS",
"AIRPORTS AND IN-FLIGHT CATERING",
"C-STORES / GAS STATIONS",
"PRIMARY SCHOOL",
"CLUB STORES / PROFESSIONAL SHOP / CASH & CARRY",
"OTHER SEGMENTS",
"AMUSEMENT ESTABLISHMENT, BOWLING CENTER, POOL HALL",
"LSR - ICE CREAM PARLORS, FROZEN DESSERTS",
"BED & BREAKFAST, GUESTHOUSES, CHAMBRES D'HOTES",
"LSR - SMOOTHIE, JUICE",
"LSR - COFFEE SHOPS, TEA HOUSES",
"RECREATION, LEISURE SITES, ATTRACTION PARKS",
"CAMPING, RV PARKS, CAMPGROUNDS",
"UNCODED HOTELS & MOTELS WITHOUT RESTAURANT",
"EXHIBITION/CONFERENCE CENTERS, HALL RENTAL",
"1* HOTELS WITHOUT RESTAURANT-ECONOMY",
"NIGHTLIFE VENUES",
"CASINO, GAMING",
"CULTURAL SITES, MUSEUMS, CINEMAS, THEATERS, ZOO",
"2* HOTELS WITHOUT RESTAURANT-MIDSCALE"
]
chd_new = df1.join(df2, df1.chd_id == df2.CHD_Id__c, "left_anti")
print(chd_new.count())
#print(chd_new.toPandas()['chd_market_segment'].unique())
for items in excluded_segments:
  chd_new = chd_new.where(col("chd_market_segment") != items )
print(chd_new.count())
chd_old = df1.join(df2, df1.chd_id == df2.CHD_Id__c)
print(chd_old.count())

# COMMAND ----------

print("SF count: ", sfDF.count())
print("CHD count: ", chdUpdateFile.count())

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val baseURL = "https://maps.googleapis.com/maps/api/geocode/json?key="
# MAGIC val googleMapsURL =  baseURL.concat(google_maps_APIKey)

# COMMAND ----------

# MAGIC %md Create a new temp table with the name column being renamed to address

# COMMAND ----------

chd_new = chd_new.withColumn("Name", concat(chd_new["chd_company_name"], lit(" "), chd_new["chd_address"], lit(" "), chd_new["chd_city"], lit(" "), chd_new["chd_state"], lit(" "), chd_new["chd_zip"]))
chd_new.select("Name").withColumnRenamed("Name","address").createOrReplaceTempView("inputtbl")
chd_new.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC val params = Map("url" -> googleMapsURL, "input" -> "inputtbl", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10")
# MAGIC //params ={"url": googleMapsURL, "input":"inputtbl","method":"GET"}

# COMMAND ----------

# MAGIC %md #Geocode each address which has no google place id using the Google Maps API

# COMMAND ----------

# MAGIC %scala 
# MAGIC val chd_not_coded_GEOCODED = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(params).load().cache()

# COMMAND ----------

# MAGIC %scala
# MAGIC chd_not_coded_GEOCODED.createOrReplaceTempView("chd_not_coded_GEOCODED_table")

# COMMAND ----------

chd_not_identified_GEOCODED = spark.sql("SELECT * FROM chd_not_coded_GEOCODED_table")

# COMMAND ----------

# MAGIC %md
# MAGIC * Checking of Maps API response to match the chd address (2 conditions i.e check zip and check first 8 letters of the address)

# COMMAND ----------

chd_test_id = chd_not_identified_GEOCODED.join(chd_new, chd_new.Name == chd_not_identified_GEOCODED.address)
chd_test_id = chd_test_id.withColumn("new_address",concat(chd_new["chd_address"], lit(" "), chd_new["chd_city"], lit(" "), chd_new["chd_state"], lit(" "), chd_new["chd_zip"] ))
fields_list = chd_not_identified_GEOCODED.columns
fields_list.append("new_address")
chd_test_id = chd_test_id.select([cols for cols in fields_list])

def match_address(rows):
  try:
    lis = rows.asDict()['results']
  except:
    lis = []
  if len(lis) != 0:
    return "true"
  else:
    return "false"

match_udf = udf(lambda z: match_address(z), StringType())
chd_exists_GEOCODED = chd_test_id.withColumn('place_id_exists', match_udf('output'))
chd_exists_GEOCODED = chd_exists_GEOCODED.filter(col('place_id_exists') == 'true')
#chd_exists_GEOCODED.count()
def zip_code_matched(x, y):
  try:
    if x.asDict()['results'][0].asDict()['formatted_address'].split(',')[-2].split()[-1] == y.split()[-1] and x.asDict()['results'][0].asDict()['formatted_address'][:8].lower() == y[:8].lower():
      return "Yes"
    else:
      return "No"
  except:
    return "No"

zip_udf = udf(lambda x,y: zip_code_matched(x,y), StringType())
chd_matched_GEOCODED = chd_exists_GEOCODED.withColumn('matched', zip_udf('output', 'new_address'))
chd_matched_GEOCODED = chd_matched_GEOCODED.where(col('matched') == 'Yes')

#print(chd_matched_GEOCODED.count())

# COMMAND ----------

# MAGIC %md Get Google Place ID from the maps API response

# COMMAND ----------

def get_google_place_id(rows):
  try:
    lis = rows.asDict()['results']
  except:
    lis = []
  if len(lis) != 0:
    return rows.asDict()['results'][0].asDict()['place_id']
  else:
    return "NULL"

place_id_udf = udf(lambda z: get_google_place_id(z), StringType())
chd_identified_GEOCODED = chd_matched_GEOCODED.withColumn('Place_id', place_id_udf('output'))

if "_corrupt_record" in chd_identified_GEOCODED.columns:
  chd_identified_GEOCODED = chd_identified_GEOCODED.drop("_corrupt_record")
#chd_identified_GEOCODED.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC * Join the google places ID with the salesforce accounts which doesn't have a places ID.

# COMMAND ----------

# MAGIC %md  * MatchDF_inner contains all the accounts from salesforce which matches the CHD_ID from CHD's file

# COMMAND ----------

MatchDF_inner = chdUpdateFile.join(sfDF, chdUpdateFile.chd_id == sfDF.CHD_Id__c)
print("Matched CHD accounts :- ",MatchDF_inner.count())

# COMMAND ----------

# MAGIC %md # Update the current_month_match__c to true

# COMMAND ----------

MatchDF_inner_update = MatchDF_inner.where(col("Exclude_From_CHD_Match__c") == "false")
MatchDF_inner_update = MatchDF_inner_update.withColumn('Current_Month_Match__c', lit(True))
MatchDF_inner_update.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC * Remove all the extra columns(CHD's columns) from the output Dataframe. Which inturn just leaves the salesforce accounts in which the current_month_match__c has been updated to true

# COMMAND ----------

sfDF_updated_month_match = MatchDF_inner_update.select(["`"+cols+"`" for cols in sfDF.columns])
print("Updated SF: ", sfDF_updated_month_match.count())

# COMMAND ----------

# MAGIC %md * Get all accounts from salesforce whose qualification status is **Not Yet Addressed**

# COMMAND ----------

sf_accounts_not_addressed = sfDF_updated_month_match.filter(col("Qualification_Status__c")=="Not Yet Addressed")
print(sf_accounts_not_addressed.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC * Get all the columns in salesforce data having **CHD** Values.

# COMMAND ----------

discard_list_1 = ["Current_Month_Match__c", "Google_Place_ID__c", "Qualification_Status__c", "Location_Name__c", "old_chd_id__c", "Current_CHD_Name__c", "Exclude_from_CHD_Match__c", "Current_CHD_Shipping_Street__c", "Current_CHD_Shipping_City__c", "CustomerStatus__c", "Current_CHD_Shipping_State__c","Current_CHD_Shipping_Postal_Code__c", "ShippingState", "ShippingLatitude", "ShippingLongitude", "ShippingCity", "ShippingStreet", "ShippingPostalCode", "Phone", "Website", "Data_Update_Case__c"]

def salesforce_to_chd(cols):
  if cols not in discard_list_1 and "__c" in cols:
    if "CHD" not in cols:
      if "Number_of_Employees" in cols:
        cols = "number_employees_com__c"
      elif "Number_of_Rooms__c" in cols:
        cols = "number_rooms__c"
      elif "County__c" in cols:
        cols = "county_name__c"
      elif "Location_Name__c" in cols:
        cols = "company_name__c"
      elif "Market_Segment_List__c" in cols:
        cols = "market_segment__c"
      cols = "`chd_"+cols[:-3].lower()+"`"
      
    else:
      cols = "`"+cols[:-3].lower()+"`"
      #cols_list.append(cols)
    return cols
  elif "ShippingCity" in cols or "ShippingLatitude" in cols or "ShippingLongitude" in cols or "ShippingState" in cols:
    cols = "chd_"+cols[8:].lower()
    return cols
  elif "ShippingPostalCode" in cols:
    cols = "chd_zip"
    return cols
  elif "ShippingStreet" in cols:
    cols = "chd_address"
    return cols
  elif "Phone" in cols:
    cols = "chd_phone"
    return cols
  elif "Website" in cols:
    cols = "chd_website"
    return cols
  else:
    return False

def update_chd_fields(df):
  df1 = df.alias("df1")
  df2 = chdUpdateFile.alias("df2")
  Match_ID = df2.join(df1, df2.chd_id == df1.CHD_Id__c)
  for cols in df1.columns:
    if salesforce_to_chd(cols):
      Match_ID = Match_ID.withColumn(cols, Match_ID[salesforce_to_chd(cols)])
  return Match_ID

discard_list = ["Current_Month_Match__c", "Google_Place_ID__c", "Qualification_Status__c", "Location_Name__c", "ShippingState", "ShippingLatitude", "ShippingLongitude", "ShippingCity", "ShippingStreet", "ShippingPostalCode", "CustomerStatus__c", "old_chd_id__c", "Current_CHD_Name__c", "Exclude_from_CHD_Match__c", "Current_CHD_Shipping_Street__c", "Current_CHD_Shipping_City__c", "Current_CHD_Shipping_State__c","Current_CHD_Shipping_Postal_Code__c", "Data_Update_Case__c"]
def salesforce_to_chd_no_location(cols):
  if "__c" in cols and cols not in discard_list:
    if "CHD" not in cols:
      if "Number_of_Employees" in cols:
        cols = "number_employees_com__c"
      elif "Number_of_Rooms__c" in cols:
        cols = "number_rooms__c"
      elif "County__c" in cols:
        cols = "county_name__c"
      elif "Market_Segment_List__c" in cols:
        cols = "market_segment__c"
      cols = "`chd_"+cols[:-3].lower()+"`"
    else:
      cols = "`"+cols[:-3].lower()+"`"
      #cols_list.append(cols)
    return cols
  elif "Phone" in cols:
    cols = "chd_phone"
    return cols
  elif "Website" in cols:
    cols = "chd_website"
    return cols
  else:
    return False
  
def update_chd_fields_general(df):
  df1 = df.alias("df1")
  df2 = chdUpdateFile.alias("df2")
  Match_ID = df2.join(df1, df2.chd_id == df1.CHD_Id__c)
  for cols in Match_ID.columns:
    if salesforce_to_chd_no_location(cols):
      Match_ID = Match_ID.withColumn(cols,
                                    F.when(Match_ID["CHD_Id__c"]==Match_ID["chd_id"], Match_ID[salesforce_to_chd_no_location(cols)]).otherwise(Match_ID[cols]))

  return Match_ID



# COMMAND ----------

def update_current_fields(df):
  df = df.withColumn("Current_CHD_Name__c", col("chd_company_name"))
  df = df.withColumn("Current_CHD_Shipping_Street__c", col("chd_address"))
  df = df.withColumn("Current_CHD_Shipping_City__c", col("chd_city"))
  df = df.withColumn("Current_CHD_Shipping_State__c", col("chd_state"))
  df = df.withColumn("Current_CHD_Shipping_Postal_Code__c", col("chd_zip"))
  return df

def update_salesforce(df,name,run_date):
  df.printSchema()
  df.write.format('com.databricks.spark.csv').mode('overwrite').save('/mnt/databricks-bhep/cozzini/CHD_Loads/{0}/{1}.csv'.format(month_year, name), header = 'true')
#   df.sort('CHD_Id__c').write.\
#     format("com.springml.spark.salesforce").\
#     option("username", sf_user).\
#     option("password", sf_password).\
#     option("sfObject", "Account").\
#     option("upsert", "true").\
#     option("bulk", "true").\
#     save()
  #option("pkChunking", "true").\ option("chunkSize", "8000").\

# COMMAND ----------

sfDF_updated_chd_records = update_chd_fields(sf_accounts_not_addressed)
sfDF_updated_chd_records = update_current_fields(sfDF_updated_chd_records)

sfDF_updated_chd_records = sfDF_updated_chd_records.select(["`"+cols+"`" for cols in sf_accounts_not_addressed.columns])

sf_accounts_not_addressed.sort('CHD_Id__c')
write_to_s3("sf_accounts_not_addressed", sf_accounts_not_addressed)

sfDF_updated_chd_records = sfDF_updated_chd_records.select([cols for cols in sfDF_updated_chd_records.columns if cols != 'Data_Update_Case__c'])





# COMMAND ----------

########### Update data in salesforce ###############
update_salesforce(sfDF_updated_chd_records,"sfDF_updated_chd_records",date_time)

# COMMAND ----------

chdUpdateFile1 = sfDF_updated_chd_records.join(chdUpdateFile,chdUpdateFile.chd_id==sfDF_updated_chd_records.CHD_Id__c, how="left" )
chdUpdateFile1 = chdUpdateFile1.select(["`"+cols+"`" for cols in chdUpdateFile.columns])
chdUpdateFile1.sort('CHD_Id__c')
write_to_s3("chdUpdateFile1", chdUpdateFile1)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get all the salesforce data which has qualification status as **Disqualified**

# COMMAND ----------

sf_accounts_disqualified = sfDF_updated_month_match.filter(col("Qualification_Status__c")=="Disqualified")
sf_accounts_disqualified.count()

# COMMAND ----------

sfDF_updated_chd_disqualified_records = update_chd_fields_general(sf_accounts_disqualified)
sfDF_updated_chd_disqualified_records = update_current_fields(sfDF_updated_chd_disqualified_records)
print(sfDF_updated_chd_disqualified_records.count())

sfDF_updated_chd_disqualified_records = sfDF_updated_chd_disqualified_records.select(["`"+cols+"`" for cols in sf_accounts_disqualified.columns])

sfDF_updated_chd_disqualified_records.sort('CHD_Id__c')
write_to_s3("sfDF_updated_chd_disqualified", sfDF_updated_chd_disqualified_records)

sf_accounts_disqualified.sort('CHD_Id__c')
write_to_s3("sf_accounts_disqualified",sf_accounts_disqualified)

sfDF_updated_chd_disqualified_records = sfDF_updated_chd_disqualified_records.select([cols for cols in sfDF_updated_chd_disqualified_records.columns if cols != 'Data_Update_Case__c'])
sfDF_updated_chd_disqualified_records.count()

# COMMAND ----------

########### Update data in salesforce ###############
update_salesforce(sfDF_updated_chd_disqualified_records,"sfDF_updated_chd_disqualified_records",date_time)

# COMMAND ----------

chdUpdateFile2 = sfDF_updated_chd_disqualified_records.join(chdUpdateFile,chdUpdateFile.chd_id==sfDF_updated_chd_disqualified_records.CHD_Id__c, how="left" )
chdUpdateFile2 = chdUpdateFile2.select(["`"+cols+"`" for cols in chdUpdateFile.columns])
print(chdUpdateFile2.count())
chdUpdateFile2.sort('CHD_Id__c')
write_to_s3("chdUpdateFile2", chdUpdateFile2)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get all salesforce accounts whose qualification status is **Qualified** and customer status is **Never a Customer** or **Past Customer**

# COMMAND ----------

sf_accounts_qualified = sfDF_updated_month_match.filter((col("Qualification_Status__c")=="Qualified") & ((col("CustomerStatus__c") == "Never a Customer") | (col("CustomerStatus__c") == "Past Customer")))
sf_accounts_qualified.count()

# COMMAND ----------

sfDF_updated_chd_qualified_records = update_chd_fields_general(sf_accounts_qualified)
print(sfDF_updated_chd_qualified_records.count())

# COMMAND ----------

df1 = sfDF_updated_chd_qualified_records.select(["`"+cols+"`" for cols in sf_accounts_qualified.columns]).alias("df1")
df2 = chdUpdateFile.alias("df2")
Match_ID = df1.join(df2, df2.chd_id == df1.CHD_Id__c)

#Old sf accounts in which the location name is not updated

old_sfDF_qualified_rep_location = Match_ID.filter(((lower(col('chd_company_name')) != lower(col('Location_Name__c'))) & (lower(col('chd_company_name')) != lower(col('current_chd_name__c')))) | ((lower(col('chd_address')) != lower(col('ShippingStreet'))) & (lower(col('chd_address')) != lower(col('Current_CHD_Shipping_Street__c'))) & (lower(col('chd_city')) != lower(col('ShippingCity'))) & (lower(col('chd_city')) != lower(col('Current_CHD_Shipping_City__c'))) & (lower(col('chd_state')) != lower(col('ShippingState'))) & (lower(col('chd_state')) != lower(col('Current_CHD_Shipping_State__c'))) & (lower(col('chd_zip')) != lower(col('ShippingPostalCode'))) & (lower(col('chd_zip')) != lower(col('Current_CHD_Shipping_Postal_Code__c')))))

old_sfDF_qualified_rep_location.select([cols for cols in sf_accounts_qualified.columns]).sort('CHD_Id__c')
write_to_s3('old_sfDF_qualified_rep_location', old_sfDF_qualified_rep_location)

#Corresponding Chd accounts having new location name

chd_qualified_rep_location = old_sfDF_qualified_rep_location.select([cols for cols in df2])
chd_qualified_rep_location.select(['chd_company_name', 'chd_company_name', 'chd_address', 'chd_city', 'chd_state']).show()
chd_qualified_rep_location.sort('CHD_Id__c')
write_to_s3('chd_qualified_rep_location', chd_qualified_rep_location)


### New Updation of SF records ###

sfDF_qualified_rep_location = Match_ID.withColumn("Data_Update_Case__c", 
                                        F.when((((lower(col('chd_company_name')) != lower(col('Location_Name__c'))) &
                                        (lower(col('chd_company_name')) != lower(col('current_chd_name__c'))))) | ((lower(col('chd_address')) != lower(col('ShippingStreet'))) & (lower(col('chd_address')) != lower(col('Current_CHD_Shipping_Street__c'))) & (lower(col('chd_city')) != lower(col('ShippingCity'))) & (lower(col('chd_city')) != lower(col('Current_CHD_Shipping_City__c'))) & (lower(col('chd_state')) != lower(col('ShippingState'))) & (lower(col('chd_state')) != lower(col('Current_CHD_Shipping_State__c'))) & (lower(col('chd_zip')) != lower(col('ShippingPostalCode'))) & (lower(col('chd_zip')) != lower(col('Current_CHD_Shipping_Postal_Code__c')))), "chd_qualified_never_past_customer_rep"))

sfDF_qualified_rep = sfDF_qualified_rep_location.where(col("Data_Update_Case__c") == "chd_qualified_never_past_customer_rep")

sfDF_qualified_rep = sfDF_qualified_rep.select([cols for cols in sf_accounts_qualified.columns])
sfDF_qualified_rep.sort('CHD_Id__c')
write_to_s3('sfDF_qualified_current_csr',sfDF_qualified_rep)

print("Qualified Rep location = ", sfDF_qualified_rep.count())

########### Update flag data in salesforce ###############

sfDF_qualified_rep.sort('CHD_Id__c').limit(10).select('CHD_Id__c').show(n=10)
update_salesforce(sfDF_qualified_rep,"sfDF_qualified_rep",date_time)
# sfDF_qualified_rep.coalesce(1).write.format('com.databricks.spark.csv').save('/mnt/databricks-bhep/cozzini/CHD_Loads/sfDF_qualified_rep_201707.csv'.format(),header = 'true')


# Update the current_* fields
sfDF_updated_chd_qualified_records = update_current_fields(sfDF_updated_chd_qualified_records)

sfDF_updated_chd_qualified_records = sfDF_updated_chd_qualified_records.select(["`"+cols+"`" for cols in sf_accounts_qualified.columns])

sfDF_updated_chd_qualified_records.sort('CHD_Id__c')
write_to_s3("sfDF_updated_chd_qualified", sfDF_updated_chd_qualified_records)

sf_accounts_qualified.sort('CHD_Id__c')
write_to_s3("sf_accounts_qualified",sf_accounts_qualified)

sfDF_updated_chd_qualified_records = sfDF_updated_chd_qualified_records.select([cols for cols in sf_accounts_qualified.columns if cols != 'Data_Update_Case__c'])


########### Update data in salesforce ###############
sfDF_updated_chd_qualified_records.sort('CHD_Id__c').limit(10).select('CHD_Id__c').show()
update_salesforce(sfDF_updated_chd_qualified_records,"sfDF_updated_chd_qualified_records",date_time)
# sfDF_updated_chd_qualified_records.write.format('com.databricks.spark.csv').save('/mnt/databricks-bhep/cozzini/CHD_Loads/sfDF_updated_chd_qualified_records_201707.csv'.format(),header = 'true')

# COMMAND ----------

chdUpdateFile3 = sfDF_updated_chd_qualified_records.join(chdUpdateFile,chdUpdateFile.chd_id==sfDF_updated_chd_qualified_records.CHD_Id__c, how="left" )
chdUpdateFile3 = chdUpdateFile3.select(["`"+cols+"`" for cols in chdUpdateFile.columns])
print(chdUpdateFile3.count())
chdUpdateFile3.sort('CHD_Id__c')
write_to_s3("chdUpdateFile3", chdUpdateFile3)

# COMMAND ----------

# MAGIC %md 
# MAGIC * Get all salesforce accounts whose qualification status is **Qualified** and customer status is **Current Customer**

# COMMAND ----------

sf_accounts_qualified_current = sfDF_updated_month_match.filter((col("Qualification_Status__c")=="Qualified") & (col("CustomerStatus__c") == "Current Customer"))
sf_accounts_qualified_current.count()

# COMMAND ----------

sfDF_updated_chd_qualified_current_records = update_chd_fields_general(sf_accounts_qualified_current)
sfDF_updated_chd_qualified_current_records.count()


# COMMAND ----------

df1 = sfDF_updated_chd_qualified_current_records.select(["`"+cols+"`" for cols in sf_accounts_qualified_current.columns]).alias("df1")
df2 = chdUpdateFile.alias("df2")
Match_ID = df1.join(df2, df2.chd_id == df1.CHD_Id__c)

#Old sf accounts in which the location name is not updated

old_sfDF_qualified_current_csr_location = Match_ID.filter(((lower(col('chd_company_name')) != lower(col('Location_Name__c'))) & (lower(col('chd_company_name')) != lower(col('current_chd_name__c')))) | ((lower(col('chd_address')) != lower(col('ShippingStreet'))) & (lower(col('chd_address')) != lower(col('Current_CHD_Shipping_Street__c'))) & (lower(col('chd_city')) != lower(col('ShippingCity'))) & (lower(col('chd_city')) != lower(col('Current_CHD_Shipping_City__c'))) & (lower(col('chd_state')) != lower(col('ShippingState'))) & (lower(col('chd_state')) != lower(col('Current_CHD_Shipping_State__c'))) & (lower(col('chd_zip')) != lower(col('ShippingPostalCode'))) & (lower(col('chd_zip')) != lower(col('Current_CHD_Shipping_Postal_Code__c')))))

old_sfDF_qualified_current_csr_location.select([cols for cols in sf_accounts_qualified_current.columns]).sort('CHD_Id__c')
write_to_s3('old_sfDF_qualified_current_csr_location', old_sfDF_qualified_current_csr_location)

# Corresponding Chd accounts having new location name

chd_qualified_current_csr_location = old_sfDF_qualified_current_csr_location.select([cols for cols in df2])
chd_qualified_current_csr_location.sort('CHD_Id__c')
write_to_s3('chd_qualified_current_csr_location', chd_qualified_current_csr_location)

sfDF_qualified_current_csr_location = Match_ID.withColumn("Data_Update_Case__c", 
                                        F.when((((lower(col('chd_company_name')) != lower(col('Location_Name__c'))) &
                                        (lower(col('chd_company_name')) != lower(col('current_chd_name__c'))))) | ((lower(col('chd_address')) != lower(col('ShippingStreet'))) & (lower(col('chd_address')) != lower(col('Current_CHD_Shipping_Street__c'))) & (lower(col('chd_city')) != lower(col('ShippingCity'))) & (lower(col('chd_city')) != lower(col('Current_CHD_Shipping_City__c'))) & (lower(col('chd_state')) != lower(col('ShippingState'))) & (lower(col('chd_state')) != lower(col('Current_CHD_Shipping_State__c'))) & (lower(col('chd_zip')) != lower(col('ShippingPostalCode'))) & (lower(col('chd_zip')) != lower(col('Current_CHD_Shipping_Postal_Code__c')))), "chd_qualified_current_customer_csr"))

sfDF_qualified_current_csr = sfDF_qualified_current_csr_location.where(col("Data_Update_Case__c") == "chd_qualified_current_customer_csr")

sfDF_qualified_current_csr = sfDF_qualified_current_csr.select([cols for cols in sf_accounts_qualified_current.columns])
##### Update flags in salesforce #####
update_salesforce(sfDF_qualified_current_csr,"sfDF_qualified_current_csr",date_time)
# sfDF_qualified_current_csr.coalesce(1).write.format('com.databricks.spark.csv').save('/mnt/databricks-bhep/cozzini/CHD_Loads/sfDF_qualified_current_csr_201707.csv'.format(),header = 'true')

#SF updated location name accounts

sfDF_qualified_current_csr = sfDF_qualified_current_csr.select([cols for cols in sf_accounts_qualified_current.columns])
sfDF_qualified_current_csr.sort('CHD_Id__c')
write_to_s3('sfDF_qualified_current_csr',sfDF_qualified_current_csr)

print("Qualified Current CSR location = ", sfDF_qualified_current_csr.count())

#Update the current_* fields

sfDF_updated_chd_qualified_current_records = update_current_fields(sfDF_updated_chd_qualified_current_records)

sfDF_updated_chd_qualified_current_records = sfDF_updated_chd_qualified_current_records.select(["`"+cols+"`" for cols in sf_accounts_qualified_current.columns])

sfDF_updated_chd_qualified_current_records.sort('CHD_Id__c')
write_to_s3("sfDF_updated_chd_qualified_current", sfDF_updated_chd_qualified_current_records)

sf_accounts_qualified_current.sort('CHD_Id__c')
write_to_s3("sf_accounts_qualified_current", sf_accounts_qualified_current)

sfDF_updated_chd_qualified_current_records = sfDF_updated_chd_qualified_current_records.select([cols for cols in sf_accounts_qualified_current.columns if cols != 'Data_Update_Case__c'])


########### Update data in salesforce ###############
# sfDF_updated_chd_qualified_current_records.sort('CHD_Id__c').limit(10).select('CHD_Id__c').show()

update_salesforce(sfDF_updated_chd_qualified_current_records,"sfDF_updated_chd_qualified_current_records",date_time)
# sfDF_updated_chd_qualified_current_records.coalesce(1).write.format('com.databricks.spark.csv').save('/mnt/databricks-bhep/cozzini/CHD_Loads/sfDF_updated_chd_qualified_current_records_201707.csv'.format(),header = 'true')



# COMMAND ----------

chdUpdateFile4 = sfDF_updated_chd_qualified_current_records.join(chdUpdateFile,chdUpdateFile.chd_id==sfDF_updated_chd_qualified_current_records.CHD_Id__c, how="left" )
chdUpdateFile4 = chdUpdateFile4.select(["`"+cols+"`" for cols in chdUpdateFile.columns])
chdUpdateFile4.count()
chdUpdateFile4.sort('CHD_Id__c')
write_to_s3("chdUpdateFile4", chdUpdateFile4)

# COMMAND ----------

# MAGIC %md
# MAGIC Find all the accounts for which Google ID Matches (Finding distinct place id sf records)

# COMMAND ----------

sfDF_alias = sfDF.filter((col('Chd_Id__c') == "") & (col('exclude_from_chd_match__c') == "false"))
sfDF_places_count = sfDF_alias.groupby('Google_Place_Id__c').count()
sfDF_places_duplicate = sfDF_places_count.filter(col('count') > 1)
sfDF_places_distinct = sfDF_places_count.filter(col('count') == 1)
sfDF_places_distinct = sfDF_alias.join(sfDF_places_distinct, 'Google_Place_Id__c')
sfDF_places_distinct = sfDF_places_distinct.drop("count")
print("sf duplicates = ", sfDF_places_duplicate.count())
write_to_s3('sfDF_places_duplicate', sfDF_places_duplicate)

sfDF_places_count = sfDF_places_count.filter(col('count') == 1)
print("sf Distinct = ",sfDF_places_count.count())

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
chd_with_address = chdUpdateFile.withColumn("Address",concat(chdUpdateFile["chd_company_name"], lit(" "), chdUpdateFile["chd_address"], lit(" "), chdUpdateFile["chd_city"], lit(" "), chd_new["chd_state"], lit(" "), chd_new["chd_zip"]))

df1 = sfDF_places_distinct.alias("df1")
df2 = chd_identified_GEOCODED.alias("df2")
df3 = chd_with_address.alias("df3")
chd_places_coded = df2.join(df3, df2.address == df3.Address)
#print("chd matched with address = ", chd_places_coded.count())
chd_places_count = chd_places_coded.groupby('Place_id').count()
chd_places_duplicates = chd_places_count.filter(col('count') > 1)
#print("chd duplicates = ", chd_places_duplicates.count())
write_to_s3('chd_places_duplicates', chd_places_duplicates)

chd_places_count = chd_places_count.filter(col('count') == 1)

chd_places_coded = chd_places_coded.join(chd_places_count, 'Place_id')

df4 = chd_places_coded.alias("df4")
Match_DF_Places = df1.join(df4, df4.Place_id==df1.Google_Place_ID__c).select([col('df1.'+xx) for xx in df1.columns] + [col('df4.Place_id')])

Match_DF_Places = Match_DF_Places.drop('Place_id')
#print("Match_DF_Places = ", Match_DF_Places.count())
Match_DF_Places = Match_DF_Places.withColumn('Current_Month_Match__c', lit(True))

chd_places_coded = chd_places_coded.drop("output")
chd_places_coded = chd_places_coded.drop("new_address")
chd_places_coded = chd_places_coded.drop("address")
chd_places_coded = chd_places_coded.drop("place_id_exists")
chd_places_coded = chd_places_coded.drop("matched")
#print("chd distinct = ",chd_places_coded.count())

# COMMAND ----------

# MAGIC %md
# MAGIC * Find all the logical parts for which google place id has matched.

# COMMAND ----------

places_accounts_not_addressed = Match_DF_Places.filter(col("Qualification_Status__c")== "Not Yet Addressed")
places_accounts_disqualified = Match_DF_Places.filter(col("Qualification_Status__c") == "Disqualified")
places_accounts_qualified = Match_DF_Places.filter((col("Qualification_Status__c")=="Qualified") & ((col("CustomerStatus__c") == "Never a Customer") | (col("CustomerStatus__c") == "Past Customer")))
places_accounts_qualified_current = Match_DF_Places.filter(((col("Qualification_Status__c") == "Qualified") | (col("Qualification_Status__c") == "Customer")) & (col("CustomerStatus__c") == "Current Customer"))

print("not addressed count = ", places_accounts_not_addressed.count())
print("Disqualified count = ", places_accounts_disqualified.count())
print("Qualified count = ", places_accounts_qualified.count())
print("Qualified current count = ", places_accounts_qualified_current.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to update hidden chd fields

# COMMAND ----------

discard_list_1 = ["Current_Month_Match__c", "Google_Place_ID__c", "Qualification_Status__c", "Location_Name__c", "old_chd_id__c", "Current_CHD_Name__c", "Exclude_from_CHD_Match__c", "Current_CHD_Shipping_Street__c", "Current_CHD_Shipping_City__c", "CustomerStatus__c", "Current_CHD_Shipping_State__c","Current_CHD_Shipping_Postal_Code__c", "ShippingState", "ShippingLatitude", "ShippingLongitude", "ShippingCity", "ShippingStreet", "ShippingPostalCode", "Phone", "Website", "Data_Update_Case__c"]

def places_salesforce_to_chd(cols):
  if "__c" in cols and cols not in discard_list_1:
    if "CHD" not in cols:
      if "Number_of_Employees" in cols:
        cols = "number_employees_com__c"
      elif "Number_of_Rooms__c" in cols:
        cols = "number_rooms__c"
      elif "County__c" in cols:
        cols = "county_name__c"
      elif "Location_Name__c" in cols:
        cols = "company_name__c"
      elif "Market_Segment_List__c" in cols:
        cols = "market_segment__c"
      cols = "`chd_"+cols[:-3].lower()+"`"
    else:
      cols = "`"+cols[:-3].lower()+"`"
      #cols_list.append(cols)
    return cols
  elif "ShippingCity" in cols or "ShippingLatitude" in cols or "ShippingLongitude" in cols or "ShippingState" in cols:
    cols = "chd_"+cols[8:].lower()
    return cols
  elif "ShippingPostalCode" in cols:
    cols = "chd_zip"
    return cols
  elif "ShippingStreet" in cols:
    cols = "chd_address"
    return cols
  else:
    return False

def place_update_chd_field(df):
  df1 = df.alias("df1")
  df2 = chd_places_coded.alias("df2")
  Match_ID = df2.join(df1, df2.Place_id == df1.Google_Place_ID__c, how="inner")
  print(Match_ID.count())
  for cols in df1.columns:
    if places_salesforce_to_chd(cols):
      Match_ID = Match_ID.withColumn(cols, Match_ID[places_salesforce_to_chd(cols)])
  #print(Match_ID.select("County__c").show(n=10))
  return Match_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Google Id Match Not addressed customer with any status

# COMMAND ----------

places_updated_chd_records = place_update_chd_field(places_accounts_not_addressed)
places_updated_chd_records = update_current_fields(places_updated_chd_records)

#print(sfDF_updated_chd_records.select("County__c").show(n=10))
places_updated_chd_records = places_updated_chd_records.select(["`"+cols+"`" for cols in places_accounts_not_addressed.columns])
places_updated_chd_records.sort('CHD_Id__c')
print(places_updated_chd_records.count())
write_to_s3('places_updated_chd_records', places_updated_chd_records)

places_updated_chd_records = places_updated_chd_records.select([cols for cols in places_accounts_not_addressed.columns if cols != 'Data_Update_Case__c'])


########### Update data in salesforce ###############
update_salesforce(places_updated_chd_records,"places_updated_chd_records",date_time)

places_accounts_not_addressed.sort('CHD_Id__c')
write_to_s3("places_accounts_not_addressed", places_accounts_not_addressed)

# COMMAND ----------

df1 = places_accounts_not_addressed.alias("df1")
df2 = chd_places_coded.alias("df2")

placesChdUpdateFile1 = df1.join(df2, df2.Place_id==df1.Google_Place_ID__c).select([col('df2.'+xx) for xx in df2.columns] + [col('df1.Google_Place_ID__c')])
placesChdUpdateFile1 = placesChdUpdateFile1.drop('Google_Place_ID__c')
placesChdUpdateFile1.count()
placesChdUpdateFile1.sort('CHD_Id__c')
write_to_s3("placesChdUpdateFile1", placesChdUpdateFile1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions for updating hidden chd fields

# COMMAND ----------

discard_list = ["Current_Month_Match__c", "Google_Place_ID__c", "Qualification_Status__c", "Location_Name__c", "old_chd_id__c", "Current_CHD_Name__c", "Exclude_from_CHD_Match__c", "Current_CHD_Shipping_Street__c", "Current_CHD_Shipping_City__c", "CustomerStatus__c", "Current_CHD_Shipping_State__c","Current_CHD_Shipping_Postal_Code__c", "ShippingState", "ShippingLatitude", "ShippingLongitude", "ShippingCity", "ShippingStreet", "ShippingPostalCode", "Phone", "Website", "Data_Update_Case__c"]
def places_salesforce_to_chd_no_location(cols):
  if "__c" in cols and cols not in discard_list:
    if "CHD" not in cols:
      if "Number_of_Employees" in cols:
        cols = "number_employees_com__c"
      elif "Number_of_Rooms__c" in cols:
        cols = "number_rooms__c"
      elif "County__c" in cols:
        cols = "county_name__c"
      elif "Market_Segment_List__c" in cols:
        cols = "market_segment__c"
      cols = "`chd_"+cols[:-3].lower()+"`"
    else:
      cols = "`"+cols[:-3].lower()+"`"
      #cols_list.append(cols)
    return cols
  else:
    return False
  
def place_update_chd_fields_general(df):
  df1 = df.alias("df1")
  df2 = chd_places_coded.alias("df2")
  Match_ID = df2.join(df1, df2.Place_id == df1.Google_Place_ID__c)
  for cols in Match_ID.columns:
    if places_salesforce_to_chd_no_location(cols):
      Match_ID = Match_ID.withColumn(cols, Match_ID[places_salesforce_to_chd_no_location(cols)])

  return Match_ID

# COMMAND ----------

places_updated_chd_records_disqualified = place_update_chd_fields_general(places_accounts_disqualified)
places_updated_chd_records_disqualified = update_current_fields(places_updated_chd_records_disqualified)

places_updated_chd_records_disqualified_1 = places_updated_chd_records_disqualified.select([cols for cols in places_accounts_disqualified.columns if cols != 'Data_Update_Case__c'])


########### Update data in salesforce ###############

update_salesforce(places_updated_chd_records_disqualified_1,"places_updated_chd_records_disqualified_1",date_time)

places_updated_chd_records_disqualified_1.sort('CHD_Id__c')
places_updated_chd_records_disqualified_1.count()
write_to_s3('places_updated_chd_records_disqualified',places_updated_chd_records_disqualified_1)

places_accounts_disqualified.sort('CHD_Id__c').toPandas()
write_to_s3("places_accounts_disqualified", places_accounts_disqualified)

# COMMAND ----------

df1 = places_updated_chd_records_disqualified.select([cols for cols in places_accounts_disqualified.columns]).alias("df1")
df2 = chd_places_coded.alias("df2")
Match_ID = df1.join(df2, df2.Place_id == df1.Google_Place_ID__c)

#old places matched accounts having disqualified status

old_places_updated_disqualified_rep = Match_ID.select([cols for cols in df1])
old_places_updated_disqualified_rep.sort('CHD_Id__c')
write_to_s3('old_places_updated_disqualified_rep', old_places_updated_disqualified_rep)

# Corresponding chd accounts having disqualified status

chd_places_updated_disqualified_rep = Match_ID.select([cols for cols in df2])
chd_places_updated_disqualified_rep.sort('CHD_Id__c')
write_to_s3('chd_places_updated_disqualified_rep', chd_places_updated_disqualified_rep)

places_updated_disqualified_rep = Match_ID.withColumn("Data_Update_Case__c", lit("google_disqualified_rep"))

fields_list = df1.columns
places_updated_disqualified_rep = places_updated_disqualified_rep.select([cols for cols in fields_list])
print(places_updated_disqualified_rep.count())

########### Update data in salesforce ###############

update_salesforce(places_updated_disqualified_rep,"places_updated_disqualified_rep",date_time)

#Updated sf fields 
places_updated_disqualified_rep.sort('CHD_Id__c')
write_to_s3('places_updated_disqualified_rep', places_updated_disqualified_rep)


# COMMAND ----------

df1 = places_updated_chd_records_disqualified_1.alias("df1")
df2 = chd_places_coded.alias("df2")

placesChdUpdateFile4 = df1.join(df2, df2.Place_id==df1.Google_Place_ID__c).select([col('df2.'+xx) for xx in df2.columns] + [col('df1.Google_Place_ID__c')])
print(placesChdUpdateFile4.count())
placesChdUpdateFile4 = placesChdUpdateFile4.drop('Google_Place_ID__c')
placesChdUpdateFile4.sort('CHD_Id__c')
write_to_s3("placesChdUpdateFile4", placesChdUpdateFile4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Google Id Match Qualified and Never any customer or past customer

# COMMAND ----------

places_updated_chd_records_qualified = place_update_chd_fields_general(places_accounts_qualified)
#print(sfDF_updated_chd_records.select("County__c").show(n=10))
places_updated_chd_records_qualified = update_current_fields(places_updated_chd_records_qualified)

places_updated_chd_records_qualified_1 = places_updated_chd_records_qualified.select(["`"+cols+"`" for cols in places_accounts_qualified.columns if cols != 'Data_Update_Case__c'])

places_updated_chd_records_qualified_1.sort('CHD_Id__c')
print(places_updated_chd_records_qualified_1.count())
write_to_s3('places_updated_chd_records_qualified', places_updated_chd_records_qualified_1)


########### Update data in salesforce ###############
update_salesforce(places_updated_chd_records_qualified_1,"places_updated_chd_records_qualified_1",date_time)

places_accounts_qualified.sort('CHD_Id__c')
write_to_s3("places_accounts_qualified", places_accounts_qualified)

# COMMAND ----------

df1 = places_updated_chd_records_qualified.select(["`"+cols+"`" for cols in places_accounts_qualified.columns]).alias("df1")
df2 = chd_places_coded.alias("df2")
Match_ID = df1.join(df2, df2.Place_id == df1.Google_Place_ID__c)

#old places matched accounts having disqualified status

old_places_updated_qualified_rep = Match_ID.select([cols for cols in df1])
old_places_updated_qualified_rep.sort('CHD_Id__c')
write_to_s3('old_places_updated_qualified_rep', old_places_updated_qualified_rep)

# Corresponding chd accounts having disqualified status

chd_places_updated_qualified_rep = Match_ID.select([cols for cols in df2])
chd_places_updated_qualified_rep.sort('CHD_Id__c')
write_to_s3('chd_places_updated_qualified_rep', chd_places_updated_qualified_rep)

### Update Salesforce rep ###

places_updated_qualified_rep = Match_ID.withColumn("Data_Update_Case__c", lit("google_qualified_never_past_customer_rep"))

fields_list = df1.columns
places_updated_qualified_rep = places_updated_qualified_rep.select([cols for cols in fields_list])
print(places_updated_qualified_rep.count())

########### Update data in salesforce ###############

update_salesforce(places_updated_qualified_rep,"places_updated_qualified_rep",date_time)

#Updated sf fields 
places_updated_qualified_rep.sort('CHD_Id__c')
write_to_s3('places_updated_qualified_rep', places_updated_qualified_rep)

# COMMAND ----------

df1 = places_updated_chd_records_qualified_1.alias("df1")
df2 = chd_places_coded.alias("df2")

placesChdUpdateFile2 = df1.join(df2, df2.Place_id==df1.Google_Place_ID__c).select([col('df2.'+xx) for xx in df2.columns] + [col('df1.Google_Place_ID__c')])
print(placesChdUpdateFile2.count())
placesChdUpdateFile2 = placesChdUpdateFile2.drop('Google_Place_ID__c')
placesChdUpdateFile2.sort('CHD_Id__c')
write_to_s3("placesChdUpdateFile2", placesChdUpdateFile2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Google Id Match - Qualified & Current Customer

# COMMAND ----------

places_updated_chd_records_qualified_current = place_update_chd_fields_general(places_accounts_qualified_current)
places_updated_chd_records_qualified_current = update_current_fields(places_updated_chd_records_qualified_current)

places_updated_chd_records_qualified_current_1 = places_updated_chd_records_qualified_current.select(["`"+cols+"`" for cols in places_accounts_qualified_current.columns if cols != 'Data_Update_Case__c'])

places_updated_chd_records_qualified_current_1.sort('CHD_Id__c')
print(places_updated_chd_records_qualified_current_1.count())
write_to_s3('places_updated_chd_records_qualified_current', places_updated_chd_records_qualified_current_1)

########### Update data in salesforce ###############
update_salesforce(places_updated_chd_records_qualified_current_1,"places_updated_chd_records_qualified_current_1",date_time)

places_accounts_qualified_current.sort('CHD_Id__c')
write_to_s3("places_accounts_qualified_current", places_accounts_qualified_current)

# COMMAND ----------

df1 = places_updated_chd_records_qualified_current.select(["`"+cols+"`" for cols in places_accounts_qualified_current.columns]).alias("df1")
df2 = chd_places_coded.alias("df2")
Match_ID = df1.join(df2, df2.Place_id == df1.Google_Place_ID__c)
#Old sf accounts in which the location name and address is not updated

places_qualified_current_csr_location = Match_ID.filter(((lower(col('chd_company_name')) != lower(col('Location_Name__c')))) | ((lower((col('chd_address'))) != lower(col('Current_CHD_Shipping_Street__c'))) & (lower(col('chd_city')) != lower(col('Current_CHD_Shipping_City__c'))) & (lower(col('chd_state')) != lower(col('Current_CHD_Shipping_State__c'))) & (lower(col('chd_zip')) != lower(col('Current_CHD_Shipping_Postal_Code__c')))))

places_qualified_current_csr_location.select([cols for cols in places_updated_chd_records_qualified_current]).sort('CHD_Id__c')
write_to_s3('places_qualified_current_csr_location', places_qualified_current_csr_location)

#Corresponding Chd accounts having new location name and address

places_chd_qualified_current_csr_location = places_qualified_current_csr_location.select([cols for cols in df2])
places_chd_qualified_current_csr_location.sort('CHD_Id__c')
write_to_s3('places_chd_qualified_current_csr_location', places_chd_qualified_current_csr_location)

# Update the flag

places_updated_qualified_current_location_csr = Match_ID.withColumn("Data_Update_Case__c",
                                                      F.when(((lower(col('chd_company_name'))!=lower(col('Location_Name__c')))) | ((lower((col('chd_address'))) != lower(col('Current_CHD_Shipping_Street__c'))) & (lower(col('chd_city')) != lower(col('Current_CHD_Shipping_City__c'))) & (lower(col('chd_state')) != lower(col('Current_CHD_Shipping_State__c'))) & (lower(col('chd_zip')) != lower(col('Current_CHD_Shipping_Postal_Code__c'))))
                                                      , "google_qualified_current_customer_csr"))

places_updated_qualified_current_csr = places_updated_qualified_current_location_csr.filter(col("Data_Update_Case__c") == "google_qualified_current_customer_csr")


fields_list = df1.columns

places_updated_qualified_current_csr = places_updated_qualified_current_csr.select([cols for cols in fields_list])

########### Update data in salesforce ###############
update_salesforce(places_updated_qualified_current_csr,"places_updated_qualified_current_csr",date_time)

places_updated_qualified_current_csr.sort('CHD_Id__c')
write_to_s3('places_updated_qualified_current_csr', places_updated_qualified_current_csr)

print(places_updated_qualified_current_csr.count())


# COMMAND ----------

df1 = places_updated_chd_records_qualified_current_1.alias("df1")
df2 = chd_places_coded.alias("df2")

placesChdUpdateFile4 = df1.join(df2, df2.Place_id==df1.Google_Place_ID__c).select([col('df2.'+xx) for xx in df2.columns] + [col('df1.Google_Place_ID__c')])
print(placesChdUpdateFile4.count())
placesChdUpdateFile4 = placesChdUpdateFile4.drop('Google_Place_ID__c')
placesChdUpdateFile4.sort('CHD_Id__c')
write_to_s3("placesChdUpdateFile4", placesChdUpdateFile4)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## No Matched Records

# COMMAND ----------

no_matched_records = chd_places_coded.join(sfDF, sfDF.Google_Place_ID__c == chd_places_coded.Place_id, how="left_anti")
no_matched_records.count()

# COMMAND ----------

fields_list = ["chd_annual_sales","chd_average_check","chd_chain_name","chd_chain_ownership_type","chd_chain_id", "chd_confidence_level","chd_credit_rating","chd_menu_type","chd_dayparts","chd_dma_name","chd_group_health_system", "chd_hours", "chd_menu_items","chd_msa_name","chd_number_of_seats","chd_operation_type","chd_parent_id","chd_units","chd_years_in_business","chd_yelp_url"]

address_list = ["chd_city","chd_latitude","chd_longitude","chd_state"]
any_list = ["chd_zip", "chd_address"]
def chd_to_salesforce(cols):
  if cols in fields_list :
    cols = cols[4:]+"__c"
  elif "chd_id" in cols:
    cols = "chd_id__c"
  elif "chd_county_name" in cols:
    cols = "county__c"
  elif "chd_company_name" in cols:
    cols = "location_name__c"
  elif "chd_number_employees_com" in cols:
    cols = "number_of_employees__c"
  elif "chd_number_rooms" in cols:
    cols = "number_of_rooms__c"
  elif "chd_phone" in cols:
    cols = "Phone"
  elif "chd_website" in cols:
    cols = "Website"
  elif "Place_id" in cols:
    cols = "Google_Place_ID__c"
  elif cols in address_list:
    cols = "Shipping"+cols[4].upper()+cols[5:]
  elif "chd_zip" in cols:
    cols = "ShippingPostalCode"
  elif "chd_market_segment" in cols:
    cols = "market_segment_list__c"
  elif "chd_address" in cols and "chd_address2" not in cols:
    cols = "ShippingStreet"
  else:
    return False
  return cols
  

# COMMAND ----------

sf_fields = ["annual_sales__c","average_check__c", "chain_id__c", "chain_name__c", "chd_id__c", "confidence_level__c", "county__c","credit_rating__c","dayparts__c" ,"dma_name__c" ,"group_health_system__c" ,"hours__c" ,"location_name__c" ,"menu_items__c" ,"msa_name__c" ,"number_of_employees__c" ,"number_of_rooms__c" ,"number_of_seats__c" ,"operation_type__c" ,"parent_id__c","phone" ,"units__c" ,"website","years_in_business__c","yelp_url__c","Google_Place_ID__c","ShippingCity","ShippingLatitude","ShippingLongitude","ShippingPostalCode","ShippingState","ShippingStreet", "Name", "Qualification_Status__c", "CustomerStatus__c", "market_segment_list__c"]

# COMMAND ----------

df1 = no_matched_records.alias("df1")
for cols in df1.columns:
  if chd_to_salesforce(cols):
    no_matched_records = no_matched_records.withColumn(chd_to_salesforce(cols), no_matched_records[cols])
no_matched_records = no_matched_records.withColumn("Name", df1["chd_company_name"])
no_matched_records = no_matched_records.withColumn("Qualification_Status__c", lit("Not Yet Addressed"))
no_matched_records = no_matched_records.withColumn("CustomerStatus__c", lit("Never a Customer"))
write_to_s3("no_matched_records", no_matched_records)

sf_new_records = no_matched_records.select([cols for cols in sf_fields])
print(sf_new_records.count())

sf_new_records.sort('CHD_Id__c').write.\
    format("com.springml.spark.salesforce").\
    option("username", sf_user).\
    option("password", sf_password).\
    option("sfObject", "Account").\
    save()

sf_new_records.sort('CHD_Id__c')
write_to_s3("sf_new_records", sf_new_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE Logic

# COMMAND ----------

new_accounts_soql = "select Id, Name, annual_sales__c,average_check__c, chain_id__c, chain_name__c, chd_id__c, confidence_level__c, county__c,credit_rating__c,dayparts__c ,dma_name__c ,group_health_system__c ,hours__c ,location_name__c ,menu_items__c ,msa_name__c ,number_of_employees__c ,number_of_rooms__c ,number_of_seats__c ,operation_type__c ,parent_id__c,phone ,units__c ,website,years_in_business__c,yelp_url__c,chd_chain_id__c,Google_Place_ID__c,Qualification_Status__c,current_month_match__c,CustomerStatus__c, ShippingCity,ShippingLatitude,ShippingLongitude,ShippingPostalCode,shippingState,ShippingStreet, Current_Chd_Name__c, Data_Update_Case__c, exclude_from_chd_match__c, Current_Chd_Shipping_Street__c, Current_Chd_Shipping_City__c, Current_Chd_Shipping_State__c,Current_Chd_Shipping_Postal_Code__c from Account"

sf_delete_records = spark.read.\
        format("com.springml.spark.salesforce").\
        option("username", sf_user).\
        option("password", sf_password).\
        option("soql", new_accounts_soql).\
        option("version", "41.0").\
        load()

sf_delete_records = sf_delete_records.filter((col('current_month_match__c') == 'false') & (col('chd_id__c') != 'null'))

sf_delete_records.count()


# COMMAND ----------

sf_delete_records = sf_delete_records.replace('null', '')

sf_delete_records_not_addressed = sf_delete_records.filter(col('Qualification_Status__c') == "Not Yet Addressed")
sf_delete_records_not_addressed = sf_delete_records_not_addressed.withColumn('Qualification_Status__c', lit("Disqualified"))
sf_delete_records_not_addressed = sf_delete_records_not_addressed.withColumn('Out_of_Business__c', lit(True))

sf_delete_records_not_addressed = sf_delete_records_not_addressed.select([cols for cols in sf_delete_records_not_addressed.columns if cols != "Data_Update_Case__c"])
########### Update data in salesforce ###############
update_salesforce(sf_delete_records_not_addressed,"sf_delete_records_not_addressed",date_time)

sf_delete_records_not_addressed.sort("CHD_Id__c")
write_to_s3("sf_delete_records_not_addressed", sf_delete_records_not_addressed)

# COMMAND ----------

df1 = sf_delete_records_not_addressed.alias("df1")
df2 = sfDF.alias("df2")
sf_not_updated_delete_records_not_addressed = df1.join(df2, df1.CHD_Id__c == df2.CHD_Id__c, how="left").select([col('df2.'+xx) for xx in df2.columns])
sf_not_updated_delete_records_not_addressed.select(["CHD_Id__c", "Qualification_Status__c"]).sort("CHD_Id__c").show(n=10)
print("not updated count = ",sf_not_updated_delete_records_not_addressed.count())
gr = sf_not_updated_delete_records_not_addressed.groupby('chd_id__c').count().alias("counts")
gr = gr.where(col("count") > 1)
print("duplicate counts = ",gr.count())

sf_not_updated_delete_records_not_addressed.sort("CHD_Id__c")
write_to_s3("sf_not_updated_delete_records_not_addressed", sf_not_updated_delete_records_not_addressed)

# COMMAND ----------

sf_delete_records_disqualified = sf_delete_records.filter(col('Qualification_Status__c') == "Disqualified")
sf_delete_records_disqualified = sf_delete_records_disqualified.withColumn('Out_of_Business__c', lit(True))
sf_delete_records_disqualified.sort("CHD_Id__c")
write_to_s3("sf_delete_records_disqualified", sf_delete_records_disqualified)

sf_delete_records_disqualified = sf_delete_records_disqualified.select([cols for cols in sf_delete_records_disqualified.columns if cols != "Data_Update_Case__c"])
########### Update data in salesforce ###############
update_salesforce(sf_delete_records_disqualified,"sf_delete_records_disqualified",date_time)


# COMMAND ----------

df1 = sf_delete_records_disqualified.alias("df1")
df2 = sfDF.alias("df2")
sf_not_updated_delete_records_disqualified = df1.join(df2, df1.CHD_Id__c==df2.CHD_Id__c, how="inner").select([col('df2.'+xx) for xx in df2.columns])
sf_not_updated_delete_records_disqualified.select(["CHD_Id__c", "Qualification_Status__c", "Current_Month_Match__c"]).sort("CHD_Id__c").show(n=10)
print("not updated count = ",sf_not_updated_delete_records_disqualified.count())
sf_not_updated_delete_records_disqualified.sort("CHD_Id__c")
write_to_s3("sf_not_updated_delete_records_disqualified", sf_not_updated_delete_records_disqualified)

# COMMAND ----------

sf_delete_records_qualified_not_current = sf_delete_records.filter((col("Qualification_Status__c")=="Qualified") & ((col("CustomerStatus__c") == "Never a Customer") | (col("CustomerStatus__c") == "Past Customer")))
sf_delete_records_qualified_not_current = sf_delete_records_qualified_not_current.withColumn('Data_Update_Case__c', lit('delete_qualified_never_past_customer_rep'))

sf_delete_records_qualified_not_current.sort("CHD_Id__c")
write_to_s3("sf_delete_records_qualified_not_current",sf_delete_records_qualified_not_current)

########### Update data in salesforce ###############
update_salesforce(sf_delete_records_qualified_not_current,"sf_delete_records_qualified_not_current",date_time)

sf_delete_records_qualified_not_current.count()

# COMMAND ----------

sf_delete_records_qualified_current = sf_delete_records.filter((col("Qualification_Status__c") == "Qualified") & (col("CustomerStatus__c") == "Current Customer"))
sf_delete_records_qualified_current = sf_delete_records_qualified_current.withColumn('Data_Update_Case__c', lit('delete_qualified_current_customer_csr'))

sf_delete_records_qualified_current.sort("CHD_Id__c")
write_to_s3("sf_delete_records_qualified_current",sf_delete_records_qualified_current)

########### Update data in salesforce ###############
update_salesforce(sf_delete_records_qualified_current,"sf_delete_records_qualified_current",date_time)
sf_delete_records_qualified_current.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bulk Load Data Onto Salesforce

# COMMAND ----------

# Call Salesforce Load Notebook to Load Data Onto Salesforce
print('Loading Salesforce Data...')
PARAMS = {}
dbutils.notebook.run('./CHD Update Cozzini - Salesforce Load', 0, PARAMS)
print('Salesforce Bulk Load Complete.')
