#importing libraries


import logging.config
import pandas as pd
import logging
#from pyspark.sql.functions import *
import json


logging.config.fileConfig(fname = '../util/logging_to_file.conf')

log = logging.getLogger(__name__)


def get_current_date(spark):
    try:
        log.info('validation of spark object process starting')

        curr_date = spark.sql("""SELECT current_date""")
        log.info("validate spark object: current date - {}".format(str(curr_date.collect())))
    except Exception as e:
        log.error("error validating spark object: current date - {}".format(str(e)),exc_info=True)
    else:
        log.info("validation of spark object successful")


def validate_by_count(df,df_name):
    try:
        log.info('validation by dataframe count process starting')
        df_count = df.count()
        log.info("validate: dataframe count - {}".format(str(df_count)))
    except Exception as e:
        log.error("error validating by dataframe count - {}".format(str(e)),exc_info=True)
        raise
    else:
        log.info("Dataframe validation successful")

def validate_by_top_records(df,df_name):
    try:
        log.info('validation by dataframe Top Records process starting')
        #df_top_records = [row.asDict().keys() for row in df.limit(1).collect()]
        df_top_records = df.limit(1).collect()[0].asDict()
        #pd.DataFrame(df_top_records)
        log.info("validate: dataframe Top Records - \n%s",
                 json.dumps(df_top_records,indent=4,default=str))
        #df.show(5)
    except Exception as e:
        log.error("error validating by dataframe Top Records - {}".format(str(e)),exc_info=True)
        raise
    else:
        log.info("Dataframe validation by top records successful")


# create print schema function

def validate_schema(df,df_name):
    try:
        log.info(f'validation of schema starting - {df_name}')
        schema = df.schema.fields
        log.info(f"validate: schema - {df_name}")
        for field in schema:
            log.info(f"\t{field}")
    except Exception as e:
        log.error(f"error validating schema {df_name} - {str(e)}",exc_info=True)
        raise
    else:
        log.info(f"schema validation successful - {df_name}")


