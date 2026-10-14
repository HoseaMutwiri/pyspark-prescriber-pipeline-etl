
# import project libraries
from pyspark.sql import *
import logging
import logging.config

# load the logging configuration file

logging.config.fileConfig(fname = '../util/logging_to_file.conf')

logger = logging.getLogger(__name__)

def get_spark_object(envn,appName):
    try:
        logger.info(f"creating spark object under {envn}...")

        if envn == 'PROD':
            master = 'local[*]'
        else:
            master = 'yarn'
        spark = SparkSession.\
            builder.\
            master(master).\
            appName(appName).\
            getOrCreate()
    except Exception as e:
        logger.error("error creating spark object".format(str(e)),exc_info=True)
        raise
    else:
        logger.info("successfully created spark object")
    return spark


