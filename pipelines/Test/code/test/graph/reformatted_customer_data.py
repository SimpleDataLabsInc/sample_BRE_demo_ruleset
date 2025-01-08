from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test.config.ConfigStore import *
from test.functions import *

def reformatted_customer_data(spark: SparkSession, sample_customer_data: DataFrame) -> DataFrame:
    return sample_customer_data.select(
        col("customer_id"), 
        col("order_id"), 
        col("spending").cast(LongType()).alias("spending"), 
        col("last_purchase_date").cast(DateType()).alias("last_purchase_date"), 
        col("high_value_flag").cast(IntegerType()).alias("high_value_flag"), 
        when((datediff(current_date(), to_date(col("last_purchase_date"))) <= lit(30)), lit(1))\
          .otherwise(lit(0))\
          .alias("is_recent_order")
    )
