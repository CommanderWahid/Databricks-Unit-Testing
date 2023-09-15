# Databricks notebook source
# MAGIC %md 
# MAGIC ### Setup Phase
# MAGIC ###### Import libraries and initialize notebook parameters

# COMMAND ----------

import itertools
from context import (get_logger,read_raw_data,df_clean,df_col_rename,df_col_add_year_month_day,df_col_compute_trip_time,
                     df_col_compute_trip_type,df_col_add_unchanged,df_drop_duplicate_based_on_row_num,get_partition_list,
                     create_empty_table,get_update_condition,get_merge_set,get_merge_condition,merge_target_delta_table,timed)

# COMMAND ----------

dbutils.widgets.text('Month', '2019-12', 'Process Month (YYYY-MM)')
Month = dbutils.widgets.get('Month')

dbutils.widgets.text('debug', 'true', ['true','false'])
debug = dbutils.widgets.get('debug')

root = '/mnt/main/landingzone/taxiservice/transactionaldata/'
logger = get_logger(
    name='TaxiLogger', debug=f'{debug}'
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Extract Phase
# MAGIC ###### Extract data from source files

# COMMAND ----------

@timed
def extract():
    
    logger.info("Extracting Green/yellow Taxi data ...")

    hdr, csv_sep, infer = 'true', ',', 'true'
    csv_locs = [f'{root}/green/green_tripdata_{Month}.csv.gz',
                f'{root}/yellow/yellow_tripdata_{Month}.csv.gz']
    
    cols=[['VendorID','passenger_count','trip_distance','lpep_pickup_datetime',
           'lpep_dropoff_datetime','PUlocationID','DOlocationID','RatecodeID',
           'total_amount','payment_type'],
          ['VendorID','passenger_count','trip_distance','tpep_pickup_datetime',
           'tpep_dropoff_datetime','PUlocationID','DOlocationID','RatecodeID',
           'total_amount','payment_type']
         ]

    TaxiTripDataDF = list(
        map(
            read_raw_data,
            itertools.repeat(spark),
            csv_locs,
            itertools.repeat(hdr),
            itertools.repeat(csv_sep),
            itertools.repeat(infer),
            cols,
        )
    )
    greenTaxiTripDataDF, yellowTaxiTripDataDF = TaxiTripDataDF[0], TaxiTripDataDF[1]

    return greenTaxiTripDataDF, yellowTaxiTripDataDF

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Transform Phase
# MAGIC ###### Apply transformation

# COMMAND ----------

@timed
def transform(greenTaxiTripDataDF,yellowTaxiTripDataDF):

    logger.info("Transforming Green/Yellow Taxi data ...")
    
    logger.info("-->Rename yellow taxi columns: tpep_pickup_datetime/tpep_dropoff_datetime")
    yellowTaxiTripDataDF = df_col_rename(df=yellowTaxiTripDataDF, torename=['tpep_pickup_datetime','tpep_dropoff_datetime'], replacewith=['lpep_pickup_datetime','lpep_dropoff_datetime'])

    logger.info("-->Add column TripMode=Green/Yellow to yellow/green taxi data")
    greenTaxiTripDataDF = df_col_add_unchanged(df=greenTaxiTripDataDF, toadd=['TripMode'], withvalue=['Green'])
    yellowTaxiTripDataDF = df_col_add_unchanged(df=yellowTaxiTripDataDF, toadd=['TripMode'], withvalue=['Yellow'])

    logger.info("-->Union of Yellow/Green Taxi data")    
    taxidata = greenTaxiTripDataDF.union(yellowTaxiTripDataDF)

    logger.info("-->Rename taxi data columns")
    torename = ['passenger_count', 'trip_distance','lpep_pickup_datetime','lpep_dropoff_datetime','PUlocationID','DOlocationID','total_amount','payment_type']
    replacewith = ['PassengerCount','TripDistance','PickupTime','DropTime','PickupLocationId','DropLocationId','TotalAmount','PaymentType']
    taxidata = df_col_rename(df=taxidata, torename=torename, replacewith=replacewith)

    logger.info("-->Filter and clean Taxi data")  
    taxidata = df_clean(df=taxidata, filtercond='PassengerCount > 0 and TripDistance > 0.0', todropna=['PickupLocationId', 'PickupLocationId'], tofillna={'PaymentType': 5,'RatecodeID': 1})
    
    logger.info("-->Add year month day columns")  
    taxidata = df_col_add_year_month_day(df=taxidata, basedon='PickupTime', outputcolprefix='Trip')
    
    logger.info("-->Compute new column: TripTimeInMinutes")  
    taxidata = df_col_compute_trip_time(df=taxidata, outputcolname='TripTimeInMinutes')

    logger.info("-->Compute new column: TripType")  
    taxidata = df_col_compute_trip_type(df=taxidata, outputcolname='TripType')
    
    logger.info("-->Drop duplication using row number") 
    partitionby= ['VendorID','PassengerCount','PickupTime','DropTime','PickupLocationId','DropLocationId','PaymentType','TripType','TripMode']
    taxidata = df_drop_duplicate_based_on_row_num(df=taxidata, partitionby=partitionby, orderby=['PickupTime'], asc='false')
   
    logger.info("-->Cache Taxi data")    
    taxidata.cache()
    taxidata.count()
    
    logger.info("-->Create sql table if not exist")    
    create_empty_table(spark, df=taxidata, dbname='default', tablename='taxidata', tablepath='/mnt/main/lab/tables/taxidata', identitycol='TaxiDataId')

    return taxidata

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Load Phase
# MAGIC ###### Merge Data into destination table

# COMMAND ----------

@timed
def load(taxidata):
    
    logger.info("Loading Taxi data ...")
    
    logger.info("-->Get the list of partitions to merge")
    partitions = get_partition_list(df=taxidata, partitioncol='TripYear')
   
    logger.info("-->Merge data into dest delta table")
    merge_condition_cols = ['VendorID','PassengerCount','TripYear','PickupTime','DropTime',
                            'PickupLocationId','DropLocationId','TripType','TripMode','PaymentType']
    mergecondition = f'src.TripYear in ({partitions}) and '+ get_merge_condition(srccols=merge_condition_cols,trgcols=merge_condition_cols)

    updatecondition= get_update_condition(df=taxidata, identitycol='TaxiDataId')
    mergeset = get_merge_set(df=taxidata, identitycol='TaxiDataId')

    merge_target_delta_table(spark,df=taxidata, mergecondition=mergecondition, updatecondition=updatecondition, mergeset=mergeset, targetpath='/mnt/main/lab/tables/taxidata')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Workflow Phase
# MAGIC ###### Call extract, transform and load functions

# COMMAND ----------

green,yellow = extract()
trip = transform(green,yellow)
saved = load(trip)