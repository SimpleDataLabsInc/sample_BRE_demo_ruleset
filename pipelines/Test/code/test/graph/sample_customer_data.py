from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test.config.ConfigStore import *
from test.functions import *

def sample_customer_data(spark: SparkSession) -> DataFrame:
    from pyspark.sql import Row
    # Sample data creation
    data = [Row(customer_id = 1, order_id = 101, spending = 800.0, last_purchase_date = "2023-10-01", high_value_flag = 1),
            Row(customer_id = 2, order_id = 102, spending = 800.0, last_purchase_date = "2023-09-15", high_value_flag = 0),
            Row(customer_id = 3, order_id = 103, spending = 600.0, last_purchase_date = "2025-01-01", high_value_flag = 1),
            Row(customer_id = 4, order_id = 104, spending = 400.0, last_purchase_date = "2023-10-05", high_value_flag = 0),
            Row(customer_id = 5, order_id = 105, spending = 200.0, last_purchase_date = "2025-01-02", high_value_flag = 1),
            Row(customer_id = 6, order_id = 106, spending = 180.0, last_purchase_date = "2024-12-04", high_value_flag = 0)]
    # Create DataFrame
    df = spark.createDataFrame(data)
    # Sample 10 rows
    out0 = df.limit(10)

    return out0
