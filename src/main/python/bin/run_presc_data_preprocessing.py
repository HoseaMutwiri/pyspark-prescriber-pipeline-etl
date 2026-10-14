import logging
from pyspark.sql.functions import *
from pyspark.sql import *

import logging.config

logging.config.fileConfig(fname = '../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def data_preprocess(df1,df2):
    ### Clean df_city DataFrame:
    # 1 Select only required Columns
    # 2 Convert city, state and county fields to Upper Case
    try:
        logger.info('Data data preprocessing Started for df_city')
        df_city_sel = df1.select(
            upper(df1.city).alias('city'),
            df1.state_id,
            upper(df1.state_name).alias('state_name'),
            upper(df1.county_name).alias('county_name'),
            df1.population,
            df1.zips
        )
        ### Clean df_fact DataFrame:
        # 1 Select only required Columns
        # 2 Rename the columns
        logger.info('Data data preprocessing started for df_fact')
        df_fact_sel = df2.select(
            df2.npi.alias("presc_id"),
            df2.nppes_provider_last_org_name.alias("presc_lname"),
            df2.nppes_provider_first_name.alias("presc_fname"),
            df2.nppes_provider_city.alias("presc_city"),
            df2.nppes_provider_state.alias("presc_state"),
            df2.specialty_description.alias("presc_spclt"),
            df2.years_of_exp,
            df2.drug_name,
            df2.total_claim_count.alias("trx_cnt"),
            df2.total_day_supply,
            df2.total_drug_cost
        )
        # add a column using lit
        df_fact_sel = df_fact_sel.withColumn("country_name",lit("USA"))

        # Clean years_of_exp field
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn(
            "years_of_exp",
            regexp_extract(col("years_of_exp"),
            pattern,
            idx))
        # 5 Convert the years_of_exp datatype from string to Number
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))
        # combine the first and last name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname",concat_ws(" ","presc_fname","presc_lname"))

        # drop the columns after concentration
        df_fact_sel = df_fact_sel.drop("presc_fname","presc_lname")

        # check and clean all the NULL values /Nan values

        #df_fact_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_fact_sel.columns]).show()

        #Delete the record where the presc_Id and drug_name is NULL

        df_fact_sel = df_fact_sel.dropna(subset="presc_id")
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

        # 10 Impute TRX_CNT where it is null as avg of trx_cnt for that prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel= df_fact_sel.withColumn("trx_cnt",coalesce("trx_cnt",round(avg("trx_cnt").over(spec))))
        df_fact_sel = df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast("int"))

        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()




        #logger.info('Data preprocessing Completed')
    except Exception as e:
        logger.error(f'An Error occurred while performing data preprocessing {e}')
        raise
    else:
        logger.info('Data preprocessing Completed')
    return df_city_sel, df_fact_sel
