from pyspark.sql import SparkSession,DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import col,year,month,dayofmonth,when,unix_timestamp,round,lit,row_number,desc
from pyspark.sql.window import Window
import logging
from functools import wraps
from time import time
# Databricks objects can be imported from Python modules
# To use a Databricks built-in object in a Python module, 
# import it from databricks.sdk.runtime.
# from databricks.sdk.runtime import *


class WindowFunctionWithoutPartitionByColumnsError(Exception):
   """No Partition By columns"""
   pass

class WindowFunctionWithoutOrderByColumnsError(Exception):
   """No Order By Columns"""
   pass

class GetPartitionValuesWithoutaValidPartitionColumnsError(Exception):
   """No Partition Column"""
   pass

class GetMergeConditionOneOrMulitpleEmptyInputListError(Exception):
   """Check Input Lists"""
   pass

class MergeDatafarmeIntoDeltaTableError(Exception):
   """Check Errors Message For More Details"""
   pass

def get_logger(name: str, debug: bool = True):
    if (debug):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
    else:
        log4j_logger = spark.sparkContext._jvm.org.apache.log4j 
        logger = log4j_logger.LogManager.getLogger(name)
    return logger

def read_raw_data(spark,
                  read_loc: str = '*.csv',
                  hdr: str  = 'true',
                  csv_sep: str = ',',
                  infer: str = 'true',
                  columns: list = ['*']
                  ) -> DataFrame:
    raw_data = spark.read.csv(f'{read_loc}', header=f'{hdr}', sep=f'{csv_sep}', inferSchema=f'{infer}')
    output_df = raw_data.select(*columns)
    return output_df


def df_clean(df: DataFrame, 
             filtercond: str = 'passenger_count > 0 and trip_distance > 0.0',
             todropna: list = ['PULocationID', 'DOLocationID'], 
             tofillna: set = {'payment_type': 5,'RatecodeID': 1}
            ) -> DataFrame:
    df = (
         df
         .filter(f'{filtercond}')
         .na.drop(subset= todropna)
         .na.fill(tofillna)
         .dropDuplicates()    
    )
    return df

def df_col_rename(df: DataFrame, 
                  torename: list = [],
                  replacewith: list = []
                 ) -> DataFrame:
    mapping = dict(zip(torename,replacewith))
    df = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    return df


def df_col_add_year_month_day(df: DataFrame, 
                              basedon: str = '',
                              outputcolprefix: str = ''
                             ) -> DataFrame:
    regex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
    df = (
        df
        .withColumn(f'{outputcolprefix}Year',  when(col(f'{basedon}').rlike(regex),year(f'{basedon}')).otherwise(None))
        .withColumn(f'{outputcolprefix}Month', when(col(f'{basedon}').rlike(regex),month(f'{basedon}')).otherwise(None))
        .withColumn(f'{outputcolprefix}Day',   when(col(f'{basedon}').rlike(regex),dayofmonth(f'{basedon}')).otherwise(None))
    )
    return df

def df_col_compute_trip_time(df: DataFrame, 
                             outputcolname: str = ''
                            )-> DataFrame:
    regex_datetime = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"

    df = (
        df
        .withColumn(
            f'{outputcolname}', 
            when(col("DropTime").rlike(regex_datetime) & 
                 col("PickupTime").rlike(regex_datetime),
                 round(
                 (unix_timestamp("DropTime") -
                 unix_timestamp("PickupTime"))/60))
            .otherwise(None)
            )
    )
    return df

def df_col_compute_trip_type(df: DataFrame, 
                             outputcolname: str = ''
                            )-> DataFrame:  
    df = (
        df
        .withColumn(
            f'{outputcolname}', 
            when(col("RatecodeID") == 6,
                 lit("SharedTrip"))
            .otherwise(lit("SoloTrip"))
            )
        .drop("RatecodeID")
    )
    return df

def df_col_add_unchanged(df: DataFrame, 
                         toadd: list = [],
                         withvalue: list = []
                        )-> DataFrame:
    mapping = dict(zip(toadd,withvalue))
    cols = df.columns + [lit(mapping.get(c)).alias(c) for c in toadd]
    df = df.select(cols)
    return df

def df_drop_duplicate_based_on_row_num(df: DataFrame, 
                                       partitionby: list = [],
                                       orderby: list = [],
                                       asc: bool = False
                                      )-> DataFrame:
    if (not partitionby):
        raise WindowFunctionWithoutPartitionByColumnsError()
    
    if (not orderby):
        raise WindowFunctionWithoutOrderByColumnsError()
    
    if (asc):
        window = Window.partitionBy(partitionby).orderBy(orderby)
    else:
        window = Window.partitionBy(partitionby).orderBy(*[desc(c) for c in orderby])

    df = (
        df
        .withColumn("row_num",row_number().over(window))
        .where("row_num = 1")
        .drop("row_num")
    )
    return df

def get_partition_list(df: DataFrame, 
                       partitioncol: str = ''
                      )-> str:
    try:
        lst = list(df.select(f'{partitioncol}').distinct().toPandas()[f'{partitioncol}'])
        partitions = ','.join(map(str,lst))
    except Exception as ex: 
        raise GetPartitionValuesWithoutaValidPartitionColumnsError(ex)

    return partitions

def get_table_schema(df: DataFrame, 
                     identitycol: str = ''
                    )-> str:
    ret = ''
    if (identitycol.strip() == ''):
        ret = ','.join(map(str,[col[0]+' '+col[1] for col in df.dtypes]))
    else:
        ret = ','.join(map(str,[f'{identitycol} bigint generated always as identity'] + [col[0]+' '+col[1] for col in df.dtypes]))
    return ret
    
def create_empty_table(spark: SparkSession, 
                       df: DataFrame, 
                       dbname: str = '',
                       tablename: str = '',
                       tablepath: str = '',
                       identitycol: str = '',
                       partitionCol: list = []
                      ):
    schema= get_table_schema(df, identitycol)
    if (not partitionCol):
        spark.sql(f"create table if not exists {dbname}.{tablename}({schema}) using delta location '{tablepath}';")
    else:
        partitionCol = ','.join(map(str,partitionCol))
        spark.sql(f"create table if not exists {dbname}.{tablename}({schema}) using delta location '{tablepath}' partitioned by ({partitionCol});")

def get_update_condition(df: DataFrame, 
                         identitycol: str = ''
                        )-> str:
    updatecond = ' or '.join(map(str,['trg.'+tbl+'<>'+'src.'+tbl for tbl in df.columns if tbl != identitycol]))
    return updatecond

def get_merge_set(df: DataFrame, 
                  identitycol: str = ''
                 )-> dict:
    mergeset = {}
    cols = [col for col in df.columns if col != identitycol]
    for col in cols:
        mergeset['trg.'+col] = 'src.'+col
    return mergeset

def get_merge_condition(srccols: list = [], 
                  trgcols: list = []
                 )-> str:
    
    if (not srccols or not trgcols):
        raise GetMergeConditionOneOrMulitpleEmptyInputListError()
    
    mapping = dict(zip(srccols,trgcols))
    mergecondition = ' and '.join(map(str,['trg.'+col+'='+'src.'+mapping.get(col) for col in srccols]))
    return mergecondition

def merge_target_delta_table(spark,
                             df: DataFrame,
                             mergecondition:str = '',
                             updatecondition:str = '',
                             mergeset:dict = {},
                             targetpath:str = ''
                             ):
    if (not mergeset):
        raise MergeDatafarmeIntoDeltaTableError()
    
    try:
        deltatable = DeltaTable.forPath(spark,targetpath)
        (
            deltatable
            .alias('trg')
            .merge(df.alias('src'), condition=mergecondition)
            .whenMatchedUpdate(condition=updatecondition, set=mergeset)
            .whenNotMatchedInsert(values=mergeset)
            .execute()
        )
    except Exception as ex: 
        raise MergeDatafarmeIntoDeltaTableError(ex)


def timed(f):
  @wraps(f)
  def wrapper(*args, **kwds):
    start = time()
    result = f(*args, **kwds)
    elapsed = time() - start
    print(f'##### {f.__name__}() took {elapsed} seconds to finish')
    return result
  return wrapper