from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
import numpy as np
import pandas as pd
import random
import string
import pytest

from context import *


root = '/mnt/main/landingzone/taxiservice/testdata'


@pytest.fixture(scope="session")
def spark() -> DatabricksSession:
  # Create a DatabricksSession (the entry point to Spark functionality) on
  # the cluster in the remote Databricks workspace. Unit tests do not
  # have access to this DatabricksSession by default.
  return DatabricksSession.builder.getOrCreate()

@pytest.fixture(scope="session")
def df_columns(spark) -> list:
  df_cols = ['VendorID','passenger_count','trip_distance',
            'lpep_pickup_datetime','lpep_dropoff_datetime',
            'PUlocationID','DOlocationID','RatecodeID',
            'total_amount','payment_type']
  return df_cols

@pytest.fixture(scope="session")
def input_df(spark, df_columns) -> DataFrame: 
  csv_loc=f'{root}/input/tripdata.csv'  
  input_df = read_raw_data (spark,
                            csv_loc, 
                            hdr='true', 
                            csv_sep= ';', 
                            infer= 'true',
                            columns= df_columns)
  return input_df

#@pytest.mark.skip(reason="to skip for now ...")
def test_read_raw_data_should_return_a_list_of_known_column_as_output(input_df, df_columns): 
  # ASSEMBLE
  # ACT
  expected_cols = input_df.columns

  # ASSERT
  assert (df_columns == expected_cols)

#@pytest.mark.skip(reason="to skip for now ...")
def test_clean_extracted_data_should_filter_dropna_fillna_dropduplicates(spark, input_df, df_columns):
  # ASSEMBLE
  csv_loc=f'{root}/expected/tripdata.csv'
  test_df = read_raw_data (spark,csv_loc, hdr='true', csv_sep= ';', infer= 'true', columns= df_columns)
  
  # ACT
  expected_df = df_clean(
      input_df,
      filtercond="passenger_count > 0 and trip_distance > 0.0",
      todropna=["PULocationID", "DOLocationID"],
      tofillna={"payment_type": 5, "RatecodeID": 1},
  )

  # ASSERT
  expected_df = expected_df.orderBy('lpep_pickup_datetime')
  test_df = test_df.orderBy('lpep_pickup_datetime')

  assert (expected_df.collect() == test_df.collect())

#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_rename_should_rename_only_wanted_columns(input_df):
  # ASSEMBLE
  input_cols_to_rename = ['passenger_count','trip_distance']
  input_cols_to_replace_with = ['PassengerCount','TripDistance']
  
  # ACT
  output_df = df_col_rename(input_df,input_cols_to_rename,input_cols_to_replace_with)
  output_cols = output_df.columns
  expected_output_cols_renamed = ['PassengerCount','TripDistance']
  
  # ASSERT
  assert (np.in1d(expected_output_cols_renamed, output_cols).all() == True)

#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_rename_does_nothing_when_no_column_to_rename(input_df):
  # ASSEMBLE
  input_cols_to_rename = []
  input_cols_to_replace_with = []
  
  # ACT
  output_df = df_col_rename(input_df,input_cols_to_rename,input_cols_to_replace_with)
  output_cols = output_df.columns
  expected_output_cols_renamed = input_df.columns
  
  # ASSERT
  assert (output_cols == expected_output_cols_renamed)

#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_add_year_month_day_shoud_return_none_for_none_valid_entries(spark):
  #ASSEMBLE
  input_df = spark.createDataFrame(
      [("2025-04-08 13:00:02",), ("1234",), ("XQDS",), ("2030-01-01",)],
      ["PickupTime"]
  )
  expected_output_df_as_pandas = pd.DataFrame({
        "PickupTime": ["2025-04-08 13:00:02", "1234","XQDS", "2030-01-01"],
        "TripYear":  [2025, None, None, 2030],
        "TripMonth": [4, None, None, 1],
        "TripDay":   [8, None, None, 1]
    })
  
  #ACT
  output_df = df_col_add_year_month_day(input_df,
                                        basedon= 'PickupTime',
                                        outputcolprefix= 'Trip')
  output_df_as_pandas = output_df.toPandas()

  #ASSERT 
  pd.testing.assert_frame_equal(left=expected_output_df_as_pandas,right=output_df_as_pandas, 
                                check_exact=True, check_dtype=False)

#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_compute_trip_time_return_none_when_one_at_least_one_of_input_is_not_valid(spark):
  #ASSEMBLE
  input_df = spark.createDataFrame([
     ("2025-04-08 13:00:02","2025-04-08 13:35:17"),
     ("2026-05-08 15:00:02","2026-05-08 14:05:35"), 
     ("2026-05-08 15:00:02","XQDS",), 
     ("2030-01-01","2030-01-01")
    ],["PickupTime","DropTime"]
  )
  expected_output_df_as_pandas = pd.DataFrame({
    "PickupTime": ["2025-04-08 13:00:02", "2026-05-08 15:00:02","2026-05-08 15:00:02", "2030-01-01"],
    "DropTime": ["2025-04-08 13:35:17", "2026-05-08 14:05:35","XQDS", "2030-01-01"],
    "TripTimeInMinutes":  [35, -54, None, None]
  })

  #ACT
  output_df = df_col_compute_trip_time(input_df,outputcolname= 'TripTimeInMinutes')
  output_df_as_pandas = output_df.toPandas()

  #ASSERT 
  pd.testing.assert_frame_equal(left=expected_output_df_as_pandas,right=output_df_as_pandas, 
                              check_exact=True, check_dtype=False)
    
#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_compute_trip_time_return_none_when_one_at_least_one_of_input_is_not_valid(spark):
  #ASSEMBLE
  input_df = spark.createDataFrame([
     ("2025-04-08 13:00:02","2025-04-08 13:35:17"),
     ("2026-05-08 15:00:02","2026-05-08 14:05:35"), 
     ("2026-05-08 15:00:02","XQDS",), 
     ("2030-01-01","2030-01-01")
    ],["PickupTime","DropTime"]
  )
  expected_output_df_as_pandas = pd.DataFrame({
    "PickupTime": ["2025-04-08 13:00:02", "2026-05-08 15:00:02","2026-05-08 15:00:02", "2030-01-01"],
    "DropTime": ["2025-04-08 13:35:17", "2026-05-08 14:05:35","XQDS", "2030-01-01"],
    "TripTimeInMinutes":  [35, -54, None, None]
  })

  #ACT
  output_df = df_col_compute_trip_time(input_df,outputcolname= 'TripTimeInMinutes')
  output_df_as_pandas = output_df.toPandas()

  #ASSERT 
  pd.testing.assert_frame_equal(left=expected_output_df_as_pandas,right=output_df_as_pandas, 
                                check_exact=True, check_dtype=False)

#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_compute_trip_type(spark):
  #ASSEMBLE
  input_df = spark.createDataFrame(
      [(1,),(2,),(3,),(4,),(5,),(6,)],
      ["RateCodeID"]
  )
  expected_output_df_as_pandas = pd.DataFrame({
        "TripType":   ["SoloTrip", "SoloTrip", "SoloTrip", 
                       "SoloTrip", "SoloTrip", "SharedTrip"]
    })
  
  #ACT
  output_df = df_col_compute_trip_type(input_df,outputcolname='TripType')
  output_df_as_pandas = output_df.toPandas()

  #ASSERT 
  pd.testing.assert_frame_equal(left=expected_output_df_as_pandas,right=output_df_as_pandas, 
                                check_exact=True, check_dtype=False)
  
#@pytest.mark.skip(reason="to skip for now ...")
def test_df_col_add_unchanged(input_df):
  #ASSEMBLE 
  inputcoltoadd = ['col1','col2','col3','col4','col5']
  inputwithvalues = ['abc',999,'2040-01-01',True,0.01]
  expected_output_df_as_pandas = pd.DataFrame({
      "col1": ['abc'],
      "col2": [999],
      "col3": ['2040-01-01'],
      "col4": [True],
      "col5": [0.01]
  })
  
  #ACT
  output_df = df_col_add_unchanged(input_df, toadd=inputcoltoadd, withvalue=inputwithvalues)
  output_df = output_df.select(inputcoltoadd).distinct()
  output_df_as_pandas = output_df.toPandas()

  #ASSERT 
  pd.testing.assert_frame_equal(left=expected_output_df_as_pandas,right=output_df_as_pandas, 
                                check_exact=True, check_dtype=False)
  
#@pytest.mark.skip(reason="to skip for now ...")  
def test_df_drop_duplicate_based_on_row_num_does_not_return_duplicated_rows(input_df):
  #ACT
  output_df = df_drop_duplicate_based_on_row_num(
                input_df, 
                partitionby=['VendorID','passenger_count','lpep_pickup_datetime',
                              'lpep_dropoff_datetime','PUlocationID','DOlocationID'], 
                orderby=['lpep_pickup_datetime'],
                asc='false')
  
  output_df_as_pandas = output_df.toPandas()
  
  #ASSERT  
  assert (output_df_as_pandas.duplicated().all() == False)

#@pytest.mark.skip(reason="to skip for now ...")  
def test_df_drop_duplicate_based_on_row_num_throws_error_when_partition_clause_is_empty(input_df):
  with pytest.raises(WindowFunctionWithoutPartitionByColumnsError) as e_info:
    output_df = df_drop_duplicate_based_on_row_num(
                  input_df, 
                  partitionby= [], 
                  orderby= ['lpep_pickup_datetime'],
                  asc= 'false')

#@pytest.mark.skip(reason="to skip for now ...")  
def test_df_drop_duplicate_based_on_row_num_throws_error_when_order_clause_is_empty(input_df):
  with pytest.raises(WindowFunctionWithoutOrderByColumnsError) as e_info:
    output_df = df_drop_duplicate_based_on_row_num(
                  input_df, 
                  partitionby= ['VendorID','passenger_count','lpep_pickup_datetime',
                              'lpep_dropoff_datetime','PUlocationID','DOlocationID'], 
                  orderby= [],
                  asc= 'false')

#@pytest.mark.skip(reason="to skip for now ...")
def test_get_partition_list(spark):
  #ASSEMBLE
  input_df = spark.createDataFrame(
      [(2020,),(2020,),(2020,),(2020,),(2022,)],
      ["Year"]
  )
  expected_output= '2020,2022'
  #ACT
  output = get_partition_list(input_df, partitioncol='Year')
  
  #ASSERT
  assert(expected_output == output)

#@pytest.mark.skip(reason="to skip for now ...")
def test_get_partition_list_dont_throw_error_when_input_dataframe_is_empty(spark):
  #ASSEMBLE
  input_df = spark.createDataFrame([],'Year: int')
  expected_output= ''
  #ACT
  output = get_partition_list(input_df, partitioncol='Year')
  
  #ASSERT
  assert(expected_output == output)

#@pytest.mark.skip(reason="to skip for now ...")  
def test_get_partition_list_throw_error_when_partition_column_is_empty_or_not_valid(spark):
  with pytest.raises(GetPartitionValuesWithoutaValidPartitionColumnsError) as e_info:
    input_df = spark.createDataFrame([],'Year: int')
    output = get_partition_list(input_df, partitioncol='XXXX')

#@pytest.mark.skip(reason="to skip for now ...")
def test_create_empty_table_can_send_sql_to_spark(spark):
  #ACT
  df = spark.sql('SELECT 1').toPandas()
  
  #ASSERT
  assert(len(df.columns) == 1)

#@pytest.mark.skip(reason="to skip for now ...")
def test_get_table_schema_can_compute_input_with_identity_column(spark):
  #ASSEMBLE
  input_schema= 'col1: int,col1: string,col3: date'  
  input_df = spark.createDataFrame([],input_schema)
  expected_output = 'skcol bigint generated always as identity,' + input_schema.replace(':','')

  #ACT
  output = get_table_schema(input_df,identitycol='skcol')

  #ASSERT
  assert (expected_output == output)

#@pytest.mark.skip(reason="to skip for now ...")
def test_get_table_schema_can_compute_input_without_identity_column(spark):
  #ASSEMBLE
  input_schema= 'col1: int,col1: string,col3: date'  
  input_df = spark.createDataFrame([],input_schema)
  expected_output = input_schema.replace(':','')

  #ACT
  output = get_table_schema(input_df,identitycol='')

  #ASSERT
  assert (expected_output == output)

#@pytest.mark.skip(reason="to skip for now ...")
def test_create_empty_table_does_create_table_with_identity_column(spark,input_df):
  #ACT
  dbname = 'testidentity'
  tablename = 'taxidata' + (''.join(random.choices(string.ascii_lowercase, k=10)))
  tablepath = f'/mnt/main/lab/test/{dbname}/{tablename}'
  identitycol = f'{tablename}id'
  partitioncol= []
  
  spark.sql(f'drop schema if exists {dbname} cascade;')
  spark.sql(f'create schema {dbname};')
  create_empty_table(spark, input_df, dbname, tablename, tablepath, identitycol, partitioncol)
  
  #ASSERT
  assert(spark.catalog.tableExists(f'{dbname}.{tablename}') == True)


#@pytest.mark.skip(reason="to skip for now ...")
def test_create_empty_table_does_create_partitionned_table_when_needed(spark,input_df): 
  #ASSEMBLE
  dbname = 'testpartition'
  tablename = 'taxidata' + (''.join(random.choices(string.ascii_lowercase, k=10)))
  tablepath = f'/mnt/main/lab/test/{dbname}/{tablename}'
  identitycol = f'{tablename}id'
  partitioncol= ['PUlocationID', 'payment_type']

  #ACT
  spark.sql(f'drop schema if exists {dbname} cascade;')
  spark.sql(f'create schema {dbname};')
  create_empty_table(spark, input_df, dbname, tablename, tablepath, identitycol, partitioncol) 

  output_partitioncol = spark.sql(f'describe detail {dbname}.{tablename}').select('partitionColumns').collect()[0][0]
  expected_partitioncol = ['PUlocationID', 'payment_type']
  
  #ASSERT
  assert (output_partitioncol == expected_partitioncol)

#@pytest.mark.skip(reason="to skip for now ...") 
def test_get_update_condition(spark):
  #ASSEMBLE 
  input_df = spark.createDataFrame([],'sk:bigint, bk:string, col1:string, col2:date')
  expected_output = 'trg.bk<>src.bk or trg.col1<>src.col1 or trg.col2<>src.col2'

  #ACT
  output = get_update_condition(df=input_df, identitycol='sk')

  #ASSERT 
  assert (output == expected_output)

#@pytest.mark.skip(reason="to skip for now ...") 
def test_get_merge_set(spark):
  #ASSEMBLE 
  input_df = spark.createDataFrame([],'sk:bigint, bk:string, col1:string, col2:date')
  expected_output = {'trg.bk':'src.bk',
                     'trg.col1':'src.col1',
                     'trg.col2':'src.col2'}
  #ACT
  output = get_merge_set(df=input_df, identitycol='sk')

  #ASSERT 
  assert (output == expected_output)

#@pytest.mark.skip(reason="to skip for now ...") 
def test_get_merge_condition():
  #ASSEMBLE 
  expected_output = 'trg.bk1=src.bk11 and trg.bk2=src.bk22'

  #ACT
  output = get_merge_condition(srccols=['bk1','bk2'], trgcols=['bk11','bk22'])

  #ASSERT 
  assert (output == expected_output)

#@pytest.mark.skip(reason="to skip for now ...")  
def test_get_merge_condition_throws_error_when_source_cols_are_empty(input_df):
  with pytest.raises(GetMergeConditionOneOrMulitpleEmptyInputListError) as e_info:
    output_cond = get_merge_condition(srccols=[], trgcols=['bk11','bk22'])

#@pytest.mark.skip(reason="to skip for now ...")  
def test_get_merge_condition_throws_error_when_target_cols_are_empty(input_df):
  with pytest.raises(GetMergeConditionOneOrMulitpleEmptyInputListError) as e_info:
    output_cond = get_merge_condition(srccols=['bk1','bk2'], trgcols=[])

#@pytest.mark.skip(reason="to skip for now ...")  
def test_merge_target_delta_table_throws_error_when_target_path_is_empty_or_does_not_exist(spark):
  with pytest.raises(MergeDatafarmeIntoDeltaTableError) as e_info:
    input_df = spark.createDataFrame([],'sk:bigint, bk:string, col1:string, col2:date')
    merge_target_delta_table(spark,
                            df=input_df, 
                            mergecondition='trg.bk=src.bk',
                            updatecondition='trg.col1<>src.col1 or trg.col2<>src.col2', 
                            mergeset={'trg.col1':'src.col1','trg.col2':'src.col2'}, 
                            targetpath='/path')

#@pytest.mark.skip(reason="to skip for now ...")     
def test_merge_target_delta_table_throws_error_when_merge_condition_is_empty_or_not_valid(spark):
  with pytest.raises(MergeDatafarmeIntoDeltaTableError) as e_info:
    input_df = spark.createDataFrame([],'sk:bigint, bk:string, col1:string, col2:date')
    merge_target_delta_table(spark,
                            df=input_df, 
                            mergecondition='',
                            updatecondition='trg.col1<>src.col1 or trg.col2<>src.col2', 
                            mergeset={'trg.col1':'src.col1','trg.col2':'src.col2'}, 
                            targetpath=f'{root}/merge/taxidatatomerge')

#@pytest.mark.skip(reason="to skip for now ...")  
def test_merge_target_delta_table_throws_error_when_update_condition_is_empty_or_not_valid(spark):
  with pytest.raises(MergeDatafarmeIntoDeltaTableError) as e_info:
    input_df = spark.createDataFrame([],'sk:bigint, bk:string, col1:string, col2:date')
    merge_target_delta_table(spark,
                            df=input_df, 
                            mergecondition='trg.bk=src.bk',
                            updatecondition='', 
                            mergeset={'trg.col1':'src.col1','trg.col2':'src.col2'}, 
                            targetpath=f'{root}/merge/taxidatatomerge')

#@pytest.mark.skip(reason="to skip for now ...")           
def test_merge_target_delta_table_throws_error_when_merge_set_is_empty_or_not_valid(spark):
  with pytest.raises(MergeDatafarmeIntoDeltaTableError) as e_info:
    input_df = spark.createDataFrame([],'sk:bigint, bk:string, col1:string, col2:date')
    merge_target_delta_table(spark,
                            df=input_df, 
                            mergecondition='trg.bk=src.bk',
                            updatecondition='trg.col1<>src.col1 or trg.col2<>src.col2', 
                            mergeset={}, 
                            targetpath=f'{root}/merge/taxidatatomerge')
    
#@pytest.mark.skip(reason="to skip for now ...")      
def test_merge_target_delta_table(spark):
  #ASSEMBLE
  csv_loc=f'{root}/input/merge_input.csv' 
  targetpath=f'{root}/merge/taxidatatomerge'
  test_df = read_raw_data (spark,
                            csv_loc, 
                            hdr='true', 
                            csv_sep= ',', 
                            infer= 'true',
                            columns= ['bk','col1','col2'])
  
  test_df.write.mode('overwrite').save(targetpath)
  merge_df= spark.createDataFrame([ ('bbb','str12','str222'),
                                    ('ccc','str133','str23'),
                                    ('ddd','str14','str24')],
                                    'bk:string,col1:string,col2:string')
  #ACT
  merge_target_delta_table(spark,
                           df=merge_df, 
                           mergecondition='trg.bk=src.bk',
                           updatecondition='trg.col1<>src.col1 or trg.col2<>src.col2', 
                           mergeset={'trg.bk':'src.bk','trg.col1':'src.col1','trg.col2':'src.col2'}, 
                           targetpath=targetpath)
  
  expected_df_as_pandas= spark.createDataFrame([('aaa','str11','str21'),
                                                ('bbb','str12','str222'),
                                                ('ccc','str133','str23'),
                                                ('ddd','str14','str24')],
                                                'bk:string,col1:string,col2:string').orderBy('bk').toPandas()
  output_df_as_pandas = spark.sql(f'select * from delta.`{targetpath}` order by bk').toPandas()

  #ASSERT
  pd.testing.assert_frame_equal(left=expected_df_as_pandas,right=output_df_as_pandas, 
                                check_exact=True, check_dtype=False)