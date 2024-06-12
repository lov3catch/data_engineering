from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

schema = StructType([
    StructField('email', StringType(), False),
    StructField('full_name', StringType(), False),
    StructField('state', StringType(), False),
    StructField('birth_date', StringType(), False),
    StructField('phone_number', StringType(), False),
])

user_profiles_df = spark.read.json('/app/file_storage/processed/bronze/user_profiles/*')

split_col = F.split(user_profiles_df['full_name'], ' ')
user_profiles_df = user_profiles_df.withColumn('first_name', split_col.getItem(0))
user_profiles_df = user_profiles_df.withColumn('last_name', split_col.getItem(1))

user_profiles_df = user_profiles_df.select('email', 'first_name', 'last_name', 'state', 'birth_date', 'phone_number')
user_profiles_df.write.csv('/app/file_storage/processed/silver/user_profiles/user_profiles.csv', header=True)
