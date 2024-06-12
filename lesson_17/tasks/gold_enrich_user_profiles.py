from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, TimestampType, FloatType,DateType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

customer_schema = StructType([
    StructField('client_id', IntegerType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('email', StringType(), False),
    StructField('registration_date', TimestampType(), False),
    StructField('state', StringType(), False),
])

sale_schema = StructType([
    StructField('price', FloatType(), False),
    StructField('client_id', IntegerType(), False),
    StructField('product_name', StringType(), False),
])

user_profile_schema = StructType([
    StructField('email', StringType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('state', StringType(), False),
    StructField('birth_date', DateType(), False),
    StructField('phone_number', StringType(), False),
])

customers_df = spark.read.csv('/app/file_storage/processed/silver/customers/*', header=True, schema=customer_schema)
sales_df = spark.read.csv('/app/file_storage/processed/silver/sales/*', header=True, schema=sale_schema)
user_profiles_df = spark.read.csv('/app/file_storage/processed/silver/user_profiles/*', header=True, schema=user_profile_schema)
user_profiles_enriched = customers_df.join(user_profiles_df, user_profiles_df.email == customers_df.email, 'left')
user_profiles_enriched = user_profiles_enriched.select(customers_df.client_id, customers_df.registration_date, user_profiles_df.first_name, user_profiles_df.last_name, user_profiles_df.email, user_profiles_df.state, user_profiles_df.phone_number)

sales_enriched_df = sales_df.join(user_profiles_enriched, user_profiles_enriched.client_id == sales_df.client_id, 'left')
# 96
sales_df.where(sales_df.client_id == 96).show()
sales_enriched_df.where(sales_df.client_id == 96).show(10)
user_profiles_enriched.where(user_profiles_df.email == 'lori_bennett@example.com').show()
user_profiles_df.where(user_profiles_df.email == 'lori_bennett@example.com').show()


customers_df.where(customers_df.client_id == 96)
user_profiles_df.where(user_profiles_df.email == 'lori_bennett@example.com').show()

ff_df = customers_df.join(user_profiles_df, user_profiles_df.email == customers_df.email).where(customers_df.client_id == 96).show()
