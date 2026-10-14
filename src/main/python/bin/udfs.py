from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *


@udf(returnType=IntegerType())
def count_zip(zip_column):
    items = len(zip_column.split(' '))
    return items