import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

# schema = StructType([
#     StructField('price', StringType(), False),
#     StructField('client_id', StringType(), False),
#     StructField('purchase_date', StringType(), False),
#     StructField('product_name', StringType(), False),
# ])

customer_df = spark.read.csv('/app/file_storage/processed/bronze/sales/*', header=True, inferSchema=True)

customer_df = customer_df.selectExpr("Price as price", "CustomerId as client_id", "PurchaseDate as purchase_date",
                                     "Product as product_name")


def normalize_purchase_date(date_str):
    for fmt in ("%Y-%b-%d", "%Y-%B-%d", "%Y-%m-%d", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


parse_date_udf = F.udf(normalize_purchase_date, StringType())

customer_df = customer_df.select(
    F.regexp_replace(F.col("price"), "\\$", "").alias('price'),
    F.col("client_id"),
    parse_date_udf(F.col("purchase_date")).alias('purchase_date'),
    F.lower(F.col("product_name")).alias('product_name'))

# Add another purchase_date, because partition operation are skipped original field
customer_df = customer_df.select('*', F.col('purchase_date').alias('purchase_date_partition_by'))

customer_df.write.partitionBy('purchase_date_partition_by').csv('/app/file_storage/processed/silver/sales/sales.csv',
                                                                header=True)
