from pyspark.sql.functions import col, size, split, countDistinct, sum ,dense_rank
from pyspark.sql.types import IntegerType
import logging
from pyspark.sql.window import Window
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def transformer(data1, data2):
    """
    City Report:
       Transform Logics:
       1. Calculate the Number of zips in each city.
       2. Calculate the number of distinct Prescribers assigned for each City.
       3. Calculate total TRX_CNT prescribed for each city.
       4. Do not report a city in the final report if no prescriber is assigned to it.

    Layout:
       City Name
       State Name
       County Name
       City Population
       Number of Zips
       Prescriber Counts
       Total Trx counts
    """
    try:
        logger.info("Transformer function called")

        # FIX: Replacing custom Python UDF with native high-performance functions
        df1_transformed = data1.withColumn('zip_counts', size(split(col("zips"), " ")))

        df2_transformed = data2.groupBy("presc_state", "presc_city").agg(
            countDistinct("presc_id").alias("presc_counts"),
            sum("trx_cnt").alias("trx_counts")
        )

        # An inner join naturally filters out cities with no prescribers (Logic 4)
        df_city_join = df1_transformed.join(
            df2_transformed,
            (df1_transformed.state_id == df2_transformed.presc_state) &
            (df1_transformed.city == df2_transformed.presc_city),
            'inner'
        )

        # FIX: Column selection order updated to match your exact docstring layout specifications
        df_city_final = df_city_join.select(
            col("city").alias("City Name"),
            col("state_name").alias("State Name"),
            col("county_name").alias("County Name"),
            col("population").alias("City Population"),
            col("zip_counts").alias("Number of Zips"),
            col("presc_counts").alias("Prescriber Counts"),
            col("trx_counts").alias("Total Trx counts")
        )

    except Exception as exp:
        # FIX: Corrected the f-string syntax error
        logger.error(f"Error in the function transformer(): {str(exp)}", exc_info=True)
        raise
    else:
        logger.info("Transformer is completed...")

    return df_city_final

def top_prescribers(data1):
    """
        # Prescriber Report:
        Top 5 Prescribers with highest trx_cnt per each state.
        Consider the prescribers only from 20 to 50 years of experience.
        Layout:
          Prescriber ID
          Prescriber Full Name
          Prescriber State
          Prescriber Country
          Prescriber Years of Experience
          Total TRX Count
          Total Days Supply
          Total Drug Cost

    """
    try:
        logger.info("function top_prescribers started")
        spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())

        filtered_data = data1.select(
            "presc_id",
            "presc_fullname",
            "presc_state",
            "country_name",
            "years_of_exp",
            "trx_cnt",
            "total_day_supply",
            "total_drug_cost"
        ).\
        filter(((data1.years_of_exp >= 20) & (data1.years_of_exp <= 50))).\
               withColumn("dense_rank", dense_rank().over(spec)).\
               filter(col("dense_rank") >5).\
               select("presc_id",
                      "presc_fullname",
                      "presc_state",
                      "country_name",
                      "years_of_exp",
                      "trx_cnt",
                      "total_day_supply",
                      "total_drug_cost"
                      )
    except Exception as exp:
        logger.error(f"Error in the function top_prescribers(): {str(exp)}", exc_info=True)
        raise
    else:
        logger.info("Top prescribers is completed...")

    return filtered_data




