import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, TimestampType, FloatType, DateType

DEFAULT_MIN_AGE = 20
DEFAULT_MAX_AGE = 30
DEFAULT_PRODUCT = 'tv'
DEFAULT_PURCHASE_PERIOD_FROM = '2022-09-01'
DEFAULT_PURCHASE_PERIOD_TO = '2022-09-10'


def main(min_age, max_age, product, purchase_period_from, purchase_period_to):
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("DataFrameAPIExample") \
        .getOrCreate()

    user_profiles_enriched_schema = StructType([
        StructField('client_id', IntegerType(), False),
        StructField('registration_date', TimestampType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('email', StringType(), False),
        StructField('state', StringType(), False),
        StructField('phone_number', StringType(), False),
        StructField('birth_date', DateType(), False),
        StructField('age', IntegerType(), False),
    ])

    sale_schema = StructType([
        StructField('price', FloatType(), False),
        StructField('client_id', IntegerType(), False),
        StructField('purchase_date', StringType(), False),
        StructField('product_name', StringType(), False),
    ])

    user_profiles_enriched_df = spark.read.csv('/app/file_storage/processed/gold/user_profiles/*', header=True,
                                               schema=user_profiles_enriched_schema)
    sales_df = spark.read.csv('/app/file_storage/processed/silver/sales/*', header=True, schema=sale_schema)

    sales_enriched_df = sales_df.join(user_profiles_enriched_df,
                                      user_profiles_enriched_df.client_id == sales_df.client_id,
                                      'left')

    sales_enriched_df = sales_enriched_df.where(user_profiles_enriched_df.age > min_age).where(
        user_profiles_enriched_df.age < max_age)
    sales_enriched_df = sales_enriched_df.where(sales_df.product_name == product)
    sales_enriched_df = sales_enriched_df.where(sales_df.purchase_date > purchase_period_from).where(
        sales_df.purchase_date < purchase_period_to)
    sales_enriched_df = sales_enriched_df.groupBy(user_profiles_enriched_df.state).agg(
        F.sum(sales_df.price).alias('total')).sort(F.desc('total'))

    sales_enriched_df.write.csv('/app/file_storage/processed/gold/sales/tv_sales_leaderboard.csv', header=True)


min_age = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MIN_AGE
max_age = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_AGE
product = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_PRODUCT
purchase_period_from = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_PURCHASE_PERIOD_FROM
purchase_period_to = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_PURCHASE_PERIOD_TO

main(min_age, max_age, product, purchase_period_from, purchase_period_to)
