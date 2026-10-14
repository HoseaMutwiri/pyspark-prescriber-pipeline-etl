import logging

import logging.config

logging.config.fileConfig(fname = '../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def load_file(spark,file_dir,file_format,header,inferSchema):
    global df

    try:
        logger.info("loading file started...")
        if file_format == 'parquet':
            df = spark.read.format(file_format)\
                .load(file_dir)
            logger.info("loading parquet file completed")
        elif file_format == 'csv':
            df = spark.read.format(file_format)\
                .load(file_dir,header = header,inferSchema=inferSchema)
            logger.info("loading csv files finished")

    except Exception as e:
        logger.error("Error loading files".format(str(e)),exc_info=True)
        raise

    else:
        logger.info("loading parquet and csv files completed")
    return df
