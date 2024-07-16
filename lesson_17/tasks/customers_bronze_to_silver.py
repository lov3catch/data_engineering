from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

schema = StructType([
    StructField('price', StringType(), False),
    StructField('client_id', StringType(), False),
    StructField('purchase_date', StringType(), False),
    StructField('product_name', StringType(), False),
])

customer_df = spark.read.csv('/app/file_storage/processed/bronze/customers/*', header=True, inferSchema=True)

customer_df = customer_df.selectExpr("Id as client_id", "FirstName as first_name", "LastName as last_name",
                                     "Email as email", "RegistrationDate as registration_date", "State as state")

deduplicated_customer_df = customer_df.dropDuplicates()
deduplicated_customer_df.write.csv('/app/file_storage/processed/silver/customers/customers.csv', header=True,
                                   mode="overwrite")
