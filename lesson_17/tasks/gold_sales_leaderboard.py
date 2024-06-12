import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, TimestampType, FloatType, DateType

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
    StructField('product_name', StringType(), False),
])

user_profiles_enriched_df = spark.read.csv('/app/file_storage/processed/gold/user_profiles/*', header=True, schema=user_profiles_enriched_schema)
sales_df = spark.read.csv('/app/file_storage/processed/silver/sales/*', header=True, schema=sale_schema)

age_min = 20
age_max = 30

product = 'tv'

purchase_at_from = '2024-09-01'
purchase_at_to = '2024-09-10'



sales_enriched_df = sales_df.join(user_profiles_enriched_df, user_profiles_enriched_df.client_id == sales_df.client_id, 'left')
sales_enriched_df = sales_enriched_df.where(user_profiles_enriched_df.age > 20).where(user_profiles_enriched_df.age < 30)
sales_enriched_df = sales_enriched_df.groupBy(user_profiles_enriched_df.state).agg(F.sum(sales_df.price).alias('total')).sort(F.desc('total'))

sales_enriched_df.show()
