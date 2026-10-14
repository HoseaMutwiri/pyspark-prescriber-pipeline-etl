import datetime as date
from pyspark.sql.functions import lit

import logging
import logging.config

logger = logging.getLogger(__name__)

def data_persist_hive(spark, df, df_name, partitionBy, mode):
    try:
        logger.info(f"Data Persist  Hive Script - data_persist() is started for saving dataframe "+ df_name + " into Hive Table...")
        # Add a Static column with Current Date
        df= df.withColumn("delivery_date", lit(date.datetime.now().strftime("%Y-%m-%d")))
        spark.sql(""" create database if not exists prescpipeline location 'hdfs://localhost:9000/user/hive/warehouse/prescpipeline.db' """)
        spark.sql(""" use prescpipeline """)
        df.write.saveAsTable(df_name,partitionBy='delivery_date', mode = mode)
    except Exception as exp:
        logger.error("Error in the method - data_persist_hive(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Data Persist - data_persist_hive() is completed for saving dataframe "+ df_name +" into Hive Table...")

def data_persist_postgres(spark, df, df_name, url, driver, dbtable, mode, user, password):
    try:
        logger.info(f"Data Persist Postgre Script  - data_persist_rdbms() is started for saving dataframe "+ df_name + " into Postgre Table...")
        df.write.format("jdbc")\
                .option("url", url) \
                .option("driver", driver) \
                .option("dbtable", dbtable) \
                .mode(mode) \
                .option("user", user) \
                .option("password", password) \
                .save()
    except Exception as exp:
        logger.error("Error in the method - data_persist_postgre(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Data Persist Postgre- data_persist_postgre() is completed for saving dataframe "+ df_name +" into Postgre Table...")

