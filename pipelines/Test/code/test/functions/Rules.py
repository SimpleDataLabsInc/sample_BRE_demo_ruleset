from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *

@execute_rule
def PromoCodeRule(
        spending: Column=lambda: col("spending"), 
        high_value_flag: Column=lambda: col("high_value_flag"), 
        is_recent_order: Column=lambda: col("is_recent_order"), 
        last_purchase_date: Column=lambda: col("last_purchase_date")
):
    return when(((spending > lit(700)) & (high_value_flag == lit(1))), lit("25% Discount"))\
        .when(((spending > lit(500)) & (is_recent_order == lit(1))), lit("Buy one get one free"))\
        .when((spending > lit(500)), lit("15% discount"))\
        .when((spending > lit(300)), lit("10% Discount"))\
        .when((spending > lit(100)), lit("5% Discount"))\
        .when(
          ((last_purchase_date >= lit("2024-12-01")) & (last_purchase_date <= lit("2024-12-31"))),
          lit("Free two-day shipping")
        )\
        .otherwise(lit("No Discount"))\
        .alias("promo_offer")
