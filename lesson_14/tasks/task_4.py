from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

inventory_schema = StructType([
    StructField('inventory_id', IntegerType(), False),
    StructField('film_id', IntegerType(), False),
    StructField('store_id', IntegerType(), False),
    StructField('last_update', DateType(), False),
])

film_schema = StructType(
    [
        StructField('film_id', IntegerType(), False),
        StructField('title', StringType(), False),
        StructField('description', StringType(), False),
        StructField('release_year', IntegerType(), False),
        StructField('language_id', IntegerType(), False),
        StructField('original_language_id', IntegerType(), True),
        StructField('rental_duration', IntegerType(), False),
        StructField('rental_rate', StringType(), False),
        StructField('length', IntegerType(), False),
        StructField('replacement_cost', IntegerType(), False),
        StructField('rating', IntegerType(), False),
        StructField('special_features', StringType(), False),
        StructField('fulltext', StringType(), False),
        StructField('last_update', DateType(), False),
    ]
)

inventory_df = spark.read.csv('./data/inventory.csv', header=True, schema=inventory_schema)
film_df = spark.read.csv('./data/film.csv', header=True, schema=film_schema)

left_join_df = film_df.join(inventory_df, inventory_df.film_id == film_df.film_id, 'left')
left_join_df.show()

filtered_df = left_join_df.filter(F.col('inventory_id').isNull())

filtered_df.select('title').distinct().show()
