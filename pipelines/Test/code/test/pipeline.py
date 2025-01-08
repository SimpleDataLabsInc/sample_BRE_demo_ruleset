from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from test.config.ConfigStore import *
from test.functions import *
from prophecy.utils import *
from test.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sample_customer_data = sample_customer_data(spark)
    df_reformatted_customer_data = reformatted_customer_data(spark, df_sample_customer_data)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_reformatted_customer_data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Test")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Test")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Test", config = Config)(pipeline)

if __name__ == "__main__":
    main()
