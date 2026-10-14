# import all project libraries

from create_objects import get_spark_object
import get_all_variables as gav
from validations import get_current_date,validate_by_count,validate_by_top_records,validate_schema
import sys
import logging
import logging.config
import os
from run_data_ingestion import load_file
import pandas as pd
from run_presc_data_preprocessing import data_preprocess
from presc_run_data_extract import extract_files
from presc_run_data_transform import *
from subprocess import Popen, PIPE

from presc_run_data_persist import data_persist_hive,data_persist_postgres

# load the logging configurations file

logging.config.fileConfig(fname = '../util/logging_to_file.conf')


# create main function

def main():
    try:
        # get spark object
        spark = get_spark_object(gav.envn,gav.appName)
        logging.info("Spark object has been created successfully!")

        #validate spark object.
        get_current_date(spark)

        ### Initiate presc_run_data_ingest Script
        # Load the City File


        file_dir="PrescPipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema




        df_city = load_file(spark = spark,\
                            file_dir = file_dir,\
                            file_format = file_format,\
                            header = header,\
                            inferSchema = inferSchema)


        #validate dataframe by count and top records

        validate_by_count(df = df_city,df_name = 'us_cities_dimension')

        validate_by_top_records(df = df_city, df_name = 'us_cities_dimension')


        # Load the Prescriber Fact File

        file_dir="PrescPipeline/staging/fact"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if 'parquet' in out.decode():
           file_format = 'parquet'
           header='NA'
           inferSchema='NA'
        elif 'csv' in out.decode():
           file_format = 'csv'
           header=gav.header
           inferSchema=gav.inferSchema

        df_fact = load_file(spark = spark,\
                            file_dir = file_dir,\
                            file_format = file_format,\
                            header = header,\
                            inferSchema = inferSchema)
        ### Validate city Dimension & Prescriber Fact dataframe
        validate_by_count(df=df_fact, df_name='USA_Presc_Medicare_Data_12021')

        validate_by_top_records(df=df_fact, df_name='USA_Presc_Medicare_Data_12021')


        ### Initiate presc_run_data_preprocessing Script
        ## Perform data Cleaning Operations for df_city and df_fact
        city_df,fact_df = data_preprocess(df1 = df_city,df2 = df_fact)


        # Validation for df_city and df_fact
        validate_by_top_records(df=city_df, df_name='df_city_sel')

        validate_by_top_records(df=fact_df, df_name='df_fact_sel')

        # validation of schemas

        validate_schema(df=city_df, df_name='df_city_sel')

        validate_schema(df=fact_df, df_name='df_fact_sel')

        #transform dfs and oin them

        cleaned = transformer(data1 = city_df,data2 = fact_df)

        #validate

        validate_by_top_records(df=cleaned, df_name='transformed_data')

        validate_schema(df=cleaned, df_name='transformed_data')

        #
        top_prescribers(data1 = fact_df)
	
	    ### Initiate run_data_extraction Script
        CITY_PATH=gav.output_city
        extract_files(city_df,'json',CITY_PATH,1,False,'bzip2')
     
        FACT_PATH=gav.output_fact
        extract_files(fact_df,'orc',FACT_PATH,2,False,'snappy')

        ### intiate run_presc_extract

        data_persist_hive(spark = spark,df =city_df,df_name='city_df',partitionBy = 'delivery_date',mode = 'append')
        data_persist_hive(spark = spark ,df =fact_df,df_name ='fact_df',partitionBy = 'delivery_date',mode ='append')

        data_persist_postgres(spark = spark,df = city_df,df_name = 'city_df',url = 'jdbc:postgresql://localhost:6432/prescpipeline',driver = "org.postgresql.Driver",dbtable = 'city_df',
                              mode = 'append',user = gav.username,password = gav.password)
        data_persist_postgres(spark = spark,df = fact_df,df_name = 'fact_df',url = 'jdbc:postgresql://localhost:6432/prescpipeline',driver = "org.postgresql.Driver",dbtable = 'fact_df',
                              mode = 'append',user = gav.username,password = gav.password)

    except Exception as e:
        logging.error("An error occurred in the main() function".format(str(e)),exc_info=True)
        sys.exit(1)
    else:
        logging.info("pipeline has been executed successfully!")


if __name__ == "__main__":
    logging.info("main function started")
    main()
