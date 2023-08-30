# Databricks notebook source
# MAGIC %md ##Green/Yellow Taxi Data
# MAGIC
# MAGIC Extract, clean, transform and load Green/Yellow Taxi trip data for a month.

# COMMAND ----------

dbutils.widgets.text("Month", "2018-01", "Process Month (yyyy-mm)")

# COMMAND ----------

pMonth = dbutils.widgets.get("Month")

# COMMAND ----------

print("Starting to extract Green Taxi data")

# Extract and clean Green Taxi Data
defaultValueMapForGreenTaxi = {
    'payment_type': 5, 
    'RatecodeID': 1
}

greenTaxiTripDataDF = (spark
                        .read
                        .option("header", "true")
                        .option("inferSchema", "true")                              
                        .option("delimiter", ",")    
                        .csv(f'/mnt/main/landingzone/taxiservice/transactionaldata/green/green_tripdata_{pMonth}.csv.gz')
)

greenTaxiTripDataDF = (
                       greenTaxiTripDataDF
                       .filter('passenger_count > 0')
                       .filter('trip_distance > 0.0')
                       .na.drop(subset=['PULocationID', 'DOLocationID'])
                       .na.fill(defaultValueMapForGreenTaxi)
                       .dropDuplicates()       
)                       

print("Extracted and cleaned Green Taxi data")

# COMMAND ----------

print("Starting transformation on Green Taxi data")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Apply transformations to Yellow taxi data
greenTaxiTripDataDF = (greenTaxiTripDataDF

                        # Select only limited columns
                        .select(
                                  col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("lpep_pickup_datetime").alias("PickupTime"),                          
                                  col("lpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                               )

                        # Create derived columns for year, month and day
                        .withColumn("TripYear", year("PickupTime"))
                        .withColumn("TripMonth", month("PickupTime"))
                        .withColumn("TripDay", dayofmonth("PickupTime"))
                        
                        # Create a derived column - Trip time in minutes
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp("DropTime") - unix_timestamp("PickupTime")) 
                                                    / 60
                                             )
                                   )

                        # Create a derived column - Trip type, and drop RatecodeID column
                        .withColumn("TripType", 
                                        when(
                                                col("RatecodeID") == 6,
                                                lit("SharedTrip")
                                            )
                                        .otherwise(lit("SoloTrip"))
                                   )
                        .withColumn("TripMode",lit("Green"))
                        .drop("RatecodeID")
                        .withColumn("row_num",row_number().over(Window.partitionBy('VendorID','PassengerCount','PickupTime','DropTime','PickupLocationId','DropLocationId','PaymentType','TripType').orderBy("PickupTime")))
                        .where("row_num = 1")
                        .drop("row_num")
)

print("Applied transformations on Green Taxi data")

# COMMAND ----------

display(greenTaxiTripDataDF)

# COMMAND ----------

print("Starting to extract Yellow Taxi data")

# Extract and clean Yellow Taxi Data
defaultValueMapForYellowTaxi = {
    'payment_type': 5, 
    'RatecodeID': 1
}

# Extract Yellow Taxi file
yellowTaxiTripDataDF =  (spark
                        .read
                        .option("header", "true")
                        .option("inferSchema", "true")                              
                        .option("delimiter", ",")    
                        .csv(f'/mnt/main/landingzone/taxiservice/transactionaldata/yellow/yellow_tripdata_{pMonth}.csv.gz')
)

# Clean Yellow Taxi data
yellowTaxiTripDataDF = (
                       yellowTaxiTripDataDF
                       .filter('passenger_count > 0')
                       .filter('trip_distance > 0.0')
                       .na.drop(subset=['PULocationID', 'DOLocationID'])
                       .na.fill(defaultValueMapForYellowTaxi)
                       .dropDuplicates()       
)                                                     

print("Extracted and cleaned Yellow Taxi data")

# COMMAND ----------

print("Starting transformation on Yellow Taxi data")

from pyspark.sql.functions import *

# Apply transformations to Yellow taxi data
yellowTaxiTripDataDF = (yellowTaxiTripDataDF

                        # Select only limited columns
                        .select(
                                  col("VendorID"),
                                  col("passenger_count").alias("PassengerCount"),
                                  col("trip_distance").alias("TripDistance"),
                                  col("tpep_pickup_datetime").alias("PickupTime"),                          
                                  col("tpep_dropoff_datetime").alias("DropTime"), 
                                  col("PUlocationID").alias("PickupLocationId"), 
                                  col("DOlocationID").alias("DropLocationId"), 
                                  col("RatecodeID"), 
                                  col("total_amount").alias("TotalAmount"),
                                  col("payment_type").alias("PaymentType")
                               )

                        # Create derived columns for year, month and day
                        .withColumn("TripYear", year("PickupTime"))
                        .withColumn("TripMonth", month("PickupTime"))
                        .withColumn("TripDay", dayofmonth("PickupTime"))
                        
                        # Create a derived column - Trip time in minutes
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp("DropTime") - unix_timestamp("PickupTime")) 
                                                    / 60
                                             )
                                   )

                        # Create a derived column - Trip type, and drop RatecodeID column
                        .withColumn("TripType", 
                                        when(
                                                col("RatecodeID") == 6,
                                                lit("SharedTrip")
                                            )
                                        .otherwise(lit("SoloTrip"))
                                   )
                        .withColumn("TripMode",lit("Yellow"))
                        .drop("RatecodeID")
                        .withColumn("row_num",row_number().over(Window.partitionBy('VendorID','PassengerCount','PickupTime','DropTime','PickupLocationId','DropLocationId','PaymentType','TripType').orderBy("PickupTime")))
                        .where("row_num = 1")
                        .drop("row_num")
)

print("Applied transformations on Yellow Taxi data")

# COMMAND ----------

display(yellowTaxiTripDataDF)

# COMMAND ----------

taxidata = greenTaxiTripDataDF.union(yellowTaxiTripDataDF)

# COMMAND ----------

taxidata.head

# COMMAND ----------

taxidata.cache()
# materialize the cache 
taxidata.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.taxidata;
# MAGIC -- create table if not exist
# MAGIC CREATE TABLE IF NOT EXISTS default.taxidata(
# MAGIC   sk_taxidata bigint GENERATED ALWAYS AS IDENTITY,
# MAGIC   VendorID int,
# MAGIC   PassengerCount int, 
# MAGIC   TripDistance double, 
# MAGIC   PickupTime timestamp,
# MAGIC   DropTime timestamp,
# MAGIC   PickupLocationId int,
# MAGIC   DropLocationId int, 
# MAGIC   TotalAmount double, 
# MAGIC   PaymentType int, 
# MAGIC   TripYear int, 
# MAGIC   TripMonth int, 
# MAGIC   TripDay int, 
# MAGIC   TripTimeInMinutes double, 
# MAGIC   TripType string, 
# MAGIC   TripMode string
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/main/lab/tables/taxidata'
# MAGIC PARTITIONED BY (TripYear);

# COMMAND ----------

# get parttition list from target table 
lst = list(taxidata.select('TripYear').distinct().toPandas()['TripYear'])
partitions = ','.join(map(str,lst))
print(partitions)

# COMMAND ----------

# merge destination table 
from delta.tables import DeltaTable

deltatable = DeltaTable.forPath(spark,'/mnt/main/lab/tables/taxidata')
(
    deltatable.alias('trg').merge(taxidata.alias('src'),f'src.TripYear in ({partitions}) and src.VendorID = trg.VendorID and src.PassengerCount = trg.PassengerCount and src.TripYear = trg.TripYear and src.PickupTime = trg.PickupTime and src.DropTime = trg.DropTime and src.PickupLocationId = trg.PickupLocationId and src.DropLocationId = trg.DropLocationId and src.TripType = trg.TripType and src.TripMode = trg.TripMode and src.PaymentType = trg.PaymentType')
    .whenMatchedUpdate(
    'src.VendorID <> trg.VendorID or src.PassengerCount <> trg.PassengerCount or src.PickupTime <> trg.PickupTime or src.DropTime <> trg.DropTime or src.PickupLocationId <> trg.PickupLocationId or src.DropLocationId <> trg.DropLocationId or src.TripType <> trg.TripType or src.TripMode <> trg.TripMode or src.PaymentType <> trg.PaymentType' 
    ,{
    'VendorID': 'src.VendorID',
    'PassengerCount': 'src.PassengerCount' , 
    'TripDistance': 'src.TripDistance' , 
    'PickupTime': 'src.PickupTime' ,
    'DropTime': 'src.DropTime' ,
    'PickupLocationId': 'src.PickupLocationId' ,
    'DropLocationId': 'src.DropLocationId' , 
    'TotalAmount': 'src.TotalAmount' , 
    'PaymentType': 'src.PaymentType' , 
    'TripYear': 'src.TripYear' , 
    'TripMonth': 'src.TripMonth' , 
    'TripDay': 'src.TripDay' , 
    'TripTimeInMinutes': 'src.TripTimeInMinutes' , 
    'TripType': 'src.TripType' , 
    'TripMode': 'src.TripMode'
    })
    .whenNotMatchedInsert(values = {
    'VendorID': 'src.VendorID',
    'PassengerCount': 'src.PassengerCount' , 
    'TripDistance': 'src.TripDistance' , 
    'PickupTime': 'src.PickupTime' ,
    'DropTime': 'src.DropTime' ,
    'PickupLocationId': 'src.PickupLocationId' ,
    'DropLocationId': 'src.DropLocationId' , 
    'TotalAmount': 'src.TotalAmount' , 
    'PaymentType': 'src.PaymentType' , 
    'TripYear': 'src.TripYear' , 
    'TripMonth': 'src.TripMonth' , 
    'TripDay': 'src.TripDay' , 
    'TripTimeInMinutes': 'src.TripTimeInMinutes' , 
    'TripType': 'src.TripType' , 
    'TripMode': 'src.TripMode'
    }
    )
.execute()
)

